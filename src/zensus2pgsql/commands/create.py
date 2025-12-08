"""
zensus2pgsql create command

This command is the primary command for this utility and does the following:

- Downloads the Gitterdaten zip files and stores them in cache
- Extracts those zip files (to a temp location)
- Imports them into the specified PostgreSQL database

The data it downloads can also be viewed here:

- https://atlas.zensus2022.de/
"""

import asyncio
import json
import logging
import shutil
import tempfile
import zipfile
from datetime import datetime
from pathlib import Path
from typing import NamedTuple

import aiocsv
import aiofiles
import asyncpg
import httpx
import typer
from rich import print as rprint
from rich.progress import (
    BarColumn,
    MofNCompleteColumn,
    Progress,
    SpinnerColumn,
    TextColumn,
    TimeRemainingColumn,
)

from ..cache import CACHE, create_cache_dir
from ..constants import GITTERDATEN_FILES
from ..logging import configure_logging, logger
from ..retry import RetryConfig, retry_async
from ._base import (
    DatabaseHost,
    DatabaseName,
    DatabasePassword,
    DatabasePort,
    DatabaseSchema,
    DatabaseUser,
    QuietOption,
    VerboseOption,
)

NULL_VALUE = "–"  # WARNING! This is not the normal dash -> "-"!

#: ID column prefix found in CSV files
ID_COLUMN_PREFIX = "gitter_id"


class DatabaseConfig(NamedTuple):
    """Hold database configuration."""

    host: str
    port: int
    database: str
    user: str
    password: str | None
    schema: str
    srid: int
    drop_existing: bool


class CreateCommandConfig(NamedTuple):
    """Hold create command configuration."""

    quiet: bool
    tables: list[str]
    skip_existing: bool
    verbose: int


def create(
    tables: list[str] = typer.Argument(["all"]),
    host: str = DatabaseHost,
    port: int = DatabasePort,
    database: str = DatabaseName,
    user: str = DatabaseUser,
    password: str | None = DatabasePassword,
    schema: str = DatabaseSchema,
    srid: int = typer.Option(
        3035,
        "--srid",
        help="SRID for the coordinate system (default: 3035 for ETRS89-extended / LAEA Europe)",
    ),
    drop_existing: bool = typer.Option(
        False, "--drop-existing/--no-drop", help="Drop existing tables before import"
    ),
    skip_existing: bool = typer.Option(
        True, "--skip-existing/--overwrite", help="Skip files that already exist"
    ),
    verbose: int = VerboseOption,
    quiet: bool = QuietOption,
) -> None:
    """Download, extract and import Zensus 2022 Gitterdaten (grid data)."""
    # Configure logging based on verbosity
    cmd_config = CreateCommandConfig(
        tables=tables, skip_existing=skip_existing, quiet=quiet, verbose=verbose
    )

    configure_logging(verbose, quiet)

    # Make sure our cache directory exists
    create_cache_dir()

    db_config = DatabaseConfig(host, port, database, user, password, schema, srid, drop_existing)

    # Create output directory if it doesn't exist
    asyncio.run(collect_wrapper(cmd_config, db_config))


async def collect_wrapper(cmd_config: CreateCommandConfig, db_config: DatabaseConfig) -> None:
    """
    Encapsulate all async operations for the collect command
    """
    db_pool = await get_db_pool(db_config)

    files_to_import: list[tuple[str, str]]

    # Build list of files to download and import
    if cmd_config.tables == ["all"]:
        files_to_import = [(f["name"], f["url"]) for f in GITTERDATEN_FILES]
    else:
        files_to_import = [
            (f["name"], f["url"]) for f in GITTERDATEN_FILES if f["name"] in cmd_config.tables
        ]

    if not cmd_config.quiet:
        rprint(f"[cyan]Downloading and importing {len(files_to_import)} Gitterdaten files[/cyan]")

        # Display cache size information
        from ..cache import format_bytes, get_cache_size

        cache_sizes = get_cache_size()
        rprint(
            f"[dim]Cache: {format_bytes(cache_sizes['zip_bytes'])} (ZIPs) + "
            f"{format_bytes(cache_sizes['csv_bytes'])} (CSVs) = "
            f"{format_bytes(cache_sizes['total_bytes'])} total[/dim]"
        )

    http_client = httpx.AsyncClient(http2=False)

    try:
        # Configure progress bar to show counts instead of percentages
        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            BarColumn(),
            MofNCompleteColumn(),  # Shows "X of Y" instead of percentage
            TimeRemainingColumn(),
            disable=cmd_config.quiet,
        ) as progress:
            fetch_manager = FetchManager(
                http_client,
                Path(CACHE),
                progress,
                db_pool,
                db_config,
                total=len(files_to_import),
                skip_existing=cmd_config.skip_existing,
            )

            await fetch_manager.start(files_to_import, cmd_config.tables)

        if not cmd_config.quiet:
            rprint("\n[bold cyan]Import summary:[/bold cyan]")
            rprint(f"  [green]✓ Tables created: {fetch_manager.tables_created}[/green]")
    finally:
        fetch_manager.remove_temp_dir()
        await http_client.aclose()
        await db_pool.close()


class FetchManager:
    """
    Manager that coordinates file downloads, progress display and record insertion into the database.
    """

    #: Pattern used to find CSV files in downloaded zip files
    CSV_FILE_MATCH_PATTERN = "*Gitter.csv"

    def __init__(
        self,
        client: httpx.AsyncClient,
        output_folder: Path,
        progress: Progress,
        db_pool: asyncpg.Pool,
        db_config: "DatabaseConfig",
        total: int | None = None,
        semaphore: int = 10,
        skip_existing: bool = True,
        num_workers: int = 5,
    ) -> None:
        self.client = client
        self.output_folder = output_folder
        self.semaphore = asyncio.Semaphore(semaphore)
        self.progress = progress
        self.db_pool = db_pool
        self.db_config = db_config
        self.skip_existing = skip_existing
        self.csv_cache_dir = Path(CACHE) / "extracted"

        # Retry configuration for fetch operations
        self.retry_config = RetryConfig(
            max_attempts=4,  # 1 initial + 3 retries
            base_delay=1.0,
            max_delay=60.0,
            backoff_multiplier=2.0,
            jitter_range=0.2,
        )

        # Progress bar tasks
        self.fetch_task = progress.add_task("[cyan]Downloading...[/cyan]", total=total)
        self.extract_task = progress.add_task("[cyan]Extracting...[/cyan]", total=total)

        # Total number of database jobs is unknown at the beginning so we
        # increment this as we collect CSV files to import
        self.database_task_total = 0
        self.database_task = progress.add_task(
            "[cyan]Importing...[/cyan]", total=self.database_task_total
        )

        # Import processing statistics
        self.tables_created: int = 0

        # Number of workers for fetch and database workers
        self.num_workers: int = num_workers

        # Create all of our processing queues
        self.fetch_queue: asyncio.Queue[tuple[str, str] | None] = asyncio.Queue()
        self.extract_queue: asyncio.Queue[Path | None] = asyncio.Queue()
        self.database_queue: asyncio.Queue[Path | None] = asyncio.Queue()

        # Create a shared temp directory
        self.temp_dir = tempfile.TemporaryDirectory(ignore_cleanup_errors=True)

    async def start(self, files_to_import: list[tuple[str, str]], tables: list[str]) -> None:
        """
        Start the fetch process and wait for it to finish
        """
        jobs = []

        for filename, url in files_to_import:
            if tables != ["all"] and filename not in tables:
                continue
            await self.fetch_queue.put((url, f"{filename}.zip"))

        for _ in range(self.num_workers):
            jobs.append(self.fetch_worker())
            jobs.append(self.database_worker())

        jobs.append(self.extract_worker())

        # Add coordinator to manage pipeline shutdown
        jobs.append(self.coordinator())

        await asyncio.gather(*jobs)

    def remove_temp_dir(self):
        """Removes the temp dir we create in __init__"""
        self.temp_dir.cleanup()

    async def fetch_worker(self) -> None:
        """
        Fetch single file and write it to `self.output_folder` location
        """
        while True:
            fetch_item = await self.fetch_queue.get()

            if fetch_item is None:
                # Sentinel received - this worker is done
                logger.debug("Fetch worker received sentinel, shutting down")
                self.fetch_queue.task_done()
                break

            url, filename = fetch_item

            output_path = Path(self.output_folder) / filename

            # Skip if file exists and skip_existing is True
            if output_path.exists() and self.skip_existing:
                logger.debug(f"Skipping existing file: {filename}")
                await self.extract_queue.put(output_path)
                self.progress.update(self.fetch_task, advance=1)
                self.fetch_queue.task_done()
                continue

            try:
                logger.info(f"Downloading: {filename}")

                # Extract download logic into an async function for retry
                async def download_file():
                    async with (
                        self.semaphore,
                        self.client.stream(
                            "GET", url, follow_redirects=True, timeout=60.0
                        ) as response,
                    ):
                        response.raise_for_status()

                        async with aiofiles.open(output_path, "wb") as f:
                            async for chunk in response.aiter_bytes(chunk_size=8192):
                                await f.write(chunk)

                # Execute with retry logic
                await retry_async(download_file, config=self.retry_config, filename=filename)

                # Only reached if download succeeded
                self.progress.update(self.fetch_task, advance=1)
                await self.extract_queue.put(output_path)
                logger.debug(f"Successfully downloaded: {filename}")

            except Exception as e:
                # This is reached only if all retries exhausted or non-retryable error
                logger.error(f"Failed to download {filename} after retries: {e}")
                # Note: No extract_queue.put() means extract_worker never sees this file
            finally:
                self.fetch_queue.task_done()

    async def check_csv_cache(self, zip_file: Path) -> list[Path] | None:
        """
        Check if extracted CSVs exist in cache and are valid.

        Parameters
        ----------
        zip_file : Path
            Path to the ZIP file

        Returns
        -------
        list[Path] | None
            List of cached CSV paths if valid cache exists, None otherwise
        """
        # 1. Determine cache directory for this dataset
        dataset_name = zip_file.stem  # Remove .zip
        cache_dir = self.csv_cache_dir / dataset_name

        # 2. Quick existence check
        if not cache_dir.exists():
            logger.debug(f"Cache miss: directory not found for {dataset_name}")
            return None

        # 3. Load and validate metadata
        meta_path = cache_dir / ".cache_meta.json"
        if not meta_path.exists():
            logger.debug(f"Cache miss: metadata not found for {dataset_name}")
            return None

        try:
            with open(meta_path) as f:
                metadata = json.load(f)
        except (json.JSONDecodeError, OSError) as e:
            logger.debug(f"Cache miss: invalid metadata for {dataset_name}: {e}")
            return None

        # 4. Validate ZIP staleness (mtime check)
        zip_mtime = zip_file.stat().st_mtime
        if abs(zip_mtime - metadata.get("zip_mtime", 0)) > 1.0:  # 1 second tolerance
            logger.debug(
                f"Cache miss: ZIP file modified since extraction for {dataset_name} "
                f"(ZIP mtime: {zip_mtime}, cached: {metadata.get('zip_mtime')})"
            )
            return None

        # 5. Validate CSV files exist
        csv_files = []
        for csv_meta in metadata.get("csv_files", []):
            csv_path = cache_dir / csv_meta["name"]
            if not csv_path.exists():
                logger.debug(f"Cache miss: CSV file missing for {dataset_name}: {csv_meta['name']}")
                return None
            csv_files.append(csv_path)

        # 6. Cache hit!
        logger.info(f"Cache hit: using {len(csv_files)} cached CSV(s) for {dataset_name}")
        return csv_files

    async def extract_and_cache_csvs(self, zip_file: Path) -> list[Path]:
        """
        Extract CSVs from ZIP and save to cache.

        Parameters
        ----------
        zip_file : Path
            Path to the ZIP file

        Returns
        -------
        list[Path]
            List of extracted CSV paths (in cache)
        """
        dataset_name = zip_file.stem
        cache_dir = self.csv_cache_dir / dataset_name

        try:
            # 1. Create cache directory
            cache_dir.mkdir(parents=True, exist_ok=True)

            # 2. Extract to temp location first (existing logic)
            temp_extract_dir = Path(self.temp_dir.name) / dataset_name
            await asyncio.to_thread(zipfile.ZipFile(zip_file).extractall, temp_extract_dir)

            # 3. Find CSV files
            csv_files = list(temp_extract_dir.rglob(self.CSV_FILE_MATCH_PATTERN))
            logger.debug(f"Found {len(csv_files)} CSV file(s) in {zip_file.name}")

            # 4. Copy CSVs to cache and build metadata
            cached_csv_paths = []
            csv_metadata = []

            for csv_file in csv_files:
                # Copy to cache
                cached_csv_path = cache_dir / csv_file.name
                await asyncio.to_thread(shutil.copy2, csv_file, cached_csv_path)
                cached_csv_paths.append(cached_csv_path)

                # Collect metadata
                file_stat = cached_csv_path.stat()
                csv_meta = {"name": csv_file.name, "size": file_stat.st_size}
                csv_metadata.append(csv_meta)

            # 5. Write metadata file
            metadata = {
                "zip_filename": zip_file.name,
                "zip_mtime": zip_file.stat().st_mtime,
                "zip_size": zip_file.stat().st_size,
                "extracted_at": datetime.now().isoformat(),
                "csv_files": csv_metadata,
            }

            meta_path = cache_dir / ".cache_meta.json"
            with open(meta_path, "w") as f:
                json.dump(metadata, f, indent=2)

            logger.info(f"Cached {len(cached_csv_paths)} CSV file(s) for {dataset_name}")
            return cached_csv_paths

        except OSError as e:
            logger.warning(
                f"Failed to cache extracted CSVs for {zip_file.name}: {e}. "
                f"Falling back to temporary extraction."
            )
            # Return temp paths instead
            temp_extract_dir = Path(self.temp_dir.name) / dataset_name
            csv_files = list(temp_extract_dir.rglob(self.CSV_FILE_MATCH_PATTERN))
            return csv_files

    async def extract_worker(self):
        """
        Worker that unzips files and manages CSV cache.
        """
        while True:
            zip_file = await self.extract_queue.get()

            if zip_file is None:
                # Sentinel value that terminates `database_worker`
                logger.debug("Extract worker received sentinel, shutting down")
                self.extract_queue.task_done()
                break

            try:
                logger.info(f"Processing: {zip_file.name}")

                # CHECK CACHE FIRST
                csv_files = await self.check_csv_cache(zip_file)

                if csv_files is None:
                    # CACHE MISS: Extract and cache
                    logger.info(f"Extracting: {zip_file.name}")
                    csv_files = await self.extract_and_cache_csvs(zip_file)

                # Queue CSVs for database import
                for csv_file in csv_files:
                    self.database_task_total += 1
                    self.progress.update(self.database_task, total=self.database_task_total)
                    await self.database_queue.put(csv_file)

                self.progress.update(self.extract_task, advance=1)

            finally:
                self.extract_queue.task_done()

    async def database_worker(self):
        """Worker that loads CSV files into the database"""
        while True:
            csv_file = await self.database_queue.get()

            if csv_file is None:
                logger.debug("Database worker received sentinel, shutting down")
                self.database_queue.task_done()
                break

            # Acquire a connection from the pool for this worker
            async with self.db_pool.acquire() as conn:
                try:
                    # Generate table name from file name
                    table_name = sanitize_table_name(csv_file.stem)
                    full_table_name = f"{self.db_config.schema}.{table_name}"
                    temp_table_name = f"{self.db_config.schema}.{table_name}_temp"

                    logger.info(f"Importing: {csv_file.name} → {full_table_name}")
                    logger.debug(f"Table name: {full_table_name} ({len(table_name)} chars)")

                    # Detect file encoding
                    file_encoding = await detect_file_encoding(csv_file)
                    if file_encoding != "utf-8":
                        logger.debug(f"Detected non-UTF-8 encoding: {file_encoding}")

                    # Parse CSV headers and create column mappings
                    headers, column_mapping = await _parse_csv_headers(csv_file, file_encoding)

                    # Identify coordinate columns (using sanitized names)
                    x_col, y_col = _detect_coordinate_columns(headers, column_mapping)

                    async with conn.transaction():
                        # Check table existence and prepare for import
                        if not await self._check_and_prepare_table(
                            conn, table_name, full_table_name, temp_table_name
                        ):
                            continue

                        # Create temporary table and load CSV data
                        await self._create_temp_table_and_load_data(
                            conn,
                            csv_file,
                            temp_table_name,
                            table_name,
                            headers,
                            column_mapping,
                            file_encoding,
                        )

                        # Detect data types for columns
                        column_types = await _detect_column_types(
                            conn, temp_table_name, headers, column_mapping, x_col, y_col
                        )

                        # Create final table with detected types (using sanitized names)
                        final_columns_def = _build_final_schema(
                            headers, column_mapping, column_types, x_col, y_col, self.db_config.srid
                        )

                        # Create the final table (we already checked if it exists above)
                        logger.debug(f"Creating final table: {full_table_name}")
                        create_final_sql = (
                            f"CREATE TABLE {full_table_name} ({', '.join(final_columns_def)});"
                        )
                        await conn.execute(create_final_sql)

                        # Prepare column lists for INSERT INTO ... SELECT (using sanitized names)
                        select_cols, insert_cols, id_column = _build_data_transform_expressions(
                            headers,
                            column_mapping,
                            column_types,
                            x_col,
                            y_col,
                            source_srid=3035,
                            target_srid=self.db_config.srid,
                        )

                        # Insert from temp to final table with geometry transformation
                        logger.debug("Inserting data from temporary table to final table")
                        insert_from_temp_sql = f"""
                            INSERT INTO {full_table_name} ({", ".join(insert_cols)})
                            SELECT {", ".join(select_cols)}
                            FROM {temp_table_name}
                        """
                        await conn.execute(insert_from_temp_sql)

                        # Create indexes on geometry and ID columns
                        await _create_indexes(
                            conn, table_name, full_table_name, x_col, y_col, id_column
                        )

                        # Drop temporary table
                        logger.debug(f"Dropping temporary table: {temp_table_name}")
                        await conn.execute(f"DROP TABLE {temp_table_name};")

                        self.progress.update(self.database_task, advance=1)

                        logger.debug(
                            f"  [green]✓ Imported {full_table_name}"
                            f"{' (with geometry)' if x_col and y_col else ''}[/green]"
                        )
                        self.tables_created += 1

                except Exception as e:
                    logger.error(f"  [red]✗ Failed to import {csv_file.name}: {e!s}[/red]")

                finally:
                    self.database_queue.task_done()

    async def _check_and_prepare_table(
        self, conn: asyncpg.Connection, table_name: str, full_table_name: str, temp_table_name: str
    ) -> bool:
        """Check table existence and prepare for import.

        Parameters
        ----------
        conn : asyncpg.Connection
            Database connection (within transaction)
        table_name : str
            Simple table name
        full_table_name : str
            Schema-qualified table name
        temp_table_name : str
            Schema-qualified temporary table name

        Returns
        -------
        bool
            True if import should proceed, False if should skip
        """
        # Always drop temp table if it exists
        await conn.execute(f"DROP TABLE IF EXISTS {temp_table_name} CASCADE;")

        # Check if final table exists and if it should be recreated
        table_exists = await conn.fetchval(
            """
            SELECT EXISTS (
                SELECT FROM information_schema.tables
                WHERE table_schema = $1 AND table_name = $2
            )
            """,
            self.db_config.schema,
            table_name,
        )

        if table_exists:
            if self.db_config.drop_existing:
                await conn.execute(f"DROP TABLE {full_table_name} CASCADE;")
                logger.debug("[yellow]Dropped existing table[/yellow]")
            else:
                logger.debug(
                    "  [yellow]Table already exists, skipping. Use --drop-existing to recreate.[/yellow]"
                )
                return False

        return True

    async def _create_temp_table_and_load_data(
        self,
        conn: asyncpg.Connection,
        csv_file: Path,
        temp_table_name: str,
        table_name: str,
        headers: list[str],
        column_mapping: dict[str, str],
        file_encoding: str,
    ) -> None:
        """Create temporary table and load CSV data using COPY.

        Parameters
        ----------
        conn : asyncpg.Connection
            Database connection (within transaction)
        csv_file : Path
            Path to CSV file to import
        temp_table_name : str
            Schema-qualified temporary table name
        table_name : str
            Simple table name (for copy_to_table)
        headers : list[str]
            Original CSV headers
        column_mapping : dict[str, str]
            Mapping of original to sanitized column names
        file_encoding : str
            Detected file encoding
        """
        # Create temporary table with all columns as TEXT (sanitized names)
        logger.debug(f"Creating temporary table: {temp_table_name}")
        temp_columns_def = [f"{column_mapping[h]} TEXT" for h in headers]
        create_temp_sql = f"CREATE TABLE {temp_table_name} ({', '.join(temp_columns_def)});"
        await conn.execute(create_temp_sql)

        # Map Python encoding to PostgreSQL encoding name
        pg_encoding_map = {
            "utf-8": "UTF8",
            "iso-8859-1": "LATIN1",
            "windows-1252": "WIN1252",
            "cp1252": "WIN1252",
        }
        pg_encoding = pg_encoding_map.get(file_encoding, "UTF8")

        # Use COPY to bulk load CSV - need to pass schema and table separately
        logger.debug(f"Copying data to temporary table using encoding: {pg_encoding}")
        await conn.copy_to_table(
            f"{table_name}_temp",
            source=str(csv_file),
            schema_name=self.db_config.schema,
            delimiter=";",
            null="-",
            header=True,
            encoding=pg_encoding,
            format="csv",
        )

        # Get row count from temp table
        if logger.level == logging.DEBUG:
            row_count = await conn.fetchval(f"SELECT COUNT(*) FROM {temp_table_name};")
            logger.debug(f"Row count: {row_count}")

    async def coordinator(self):
        """
        Coordinator that manages the pipeline shutdown.

        Waits for all fetch workers to complete, then signals extract worker.
        Waits for extract worker to complete, then signals all database workers.
        """
        # Signal ALL fetch workers to stop (one sentinel per worker)
        for _ in range(self.num_workers):
            await self.fetch_queue.put(None)

        # Wait for all items in fetch queue to be processed
        await self.fetch_queue.join()

        # Signal extract worker to stop
        await self.extract_queue.put(None)

        # Wait for all items in extract queue to be processed
        await self.extract_queue.join()

        # Signal ALL database workers to stop (one sentinel per worker)
        for _ in range(self.num_workers):
            await self.database_queue.put(None)

        # Wait for all items in database queue to be processed
        await self.database_queue.join()


async def _detect_column_types(
    conn: asyncpg.Connection,
    temp_table_name: str,
    headers: list[str],
    column_mapping: dict[str, str],
    x_col: str | None,
    y_col: str | None,
) -> dict[str, str]:
    """Detect appropriate PostgreSQL types for each column.

    Parameters
    ----------
    conn : asyncpg.Connection
        Database connection (within transaction)
    temp_table_name : str
        Temporary table to analyze
    headers : list[str]
        Original CSV headers
    column_mapping : dict[str, str]
        Mapping of original to sanitized column names
    x_col : str | None
        X coordinate column (skip type detection)
    y_col : str | None
        Y coordinate column (skip type detection)

    Returns
    -------
    dict[str, str]
        Mapping of column names to PostgreSQL types (INTEGER, DOUBLE PRECISION, or TEXT)
    """
    column_types = {}
    for header in headers:
        col_name = column_mapping[header]
        # Skip coordinate columns if we're creating geometry
        if x_col and y_col and col_name in [x_col, y_col]:
            continue
        # Detect type for this column
        detected_type = await detect_column_type(conn, temp_table_name, col_name)
        column_types[col_name] = detected_type

    # Report detected types
    numeric_cols = {col: typ for col, typ in column_types.items() if typ != "TEXT"}
    if numeric_cols:
        logger.debug(
            f"Detected {len(numeric_cols)} numeric columns "
            f"({sum(1 for t in numeric_cols.values() if t == 'INTEGER')} INTEGER, "
            f"{sum(1 for t in numeric_cols.values() if t == 'DOUBLE PRECISION')} DOUBLE PRECISION)"
        )

    return column_types


async def _create_indexes(
    conn: asyncpg.Connection,
    table_name: str,
    full_table_name: str,
    x_col: str | None,
    y_col: str | None,
    id_column: str | None,
) -> None:
    """Create indexes on geometry and ID columns.

    Parameters
    ----------
    conn : asyncpg.Connection
        Database connection (within transaction)
    table_name : str
        Simple table name
    full_table_name : str
        Schema-qualified table name
    x_col : str | None
        X coordinate column (determines if spatial index needed)
    y_col : str | None
        Y coordinate column (determines if spatial index needed)
    id_column : str | None
        ID column name (if present)
    """
    # Create spatial index if we have geometry
    if x_col and y_col:
        logger.debug("Creating spatial index on geometry column")
        index_name = f"{table_name}_geom_idx"
        await conn.execute(
            f"CREATE INDEX IF NOT EXISTS {index_name} ON {full_table_name} USING GIST (geom);"
        )

    # Create B-tree index on ID column if present
    if id_column is not None:
        logger.debug(f"Creating index on {table_name} for {id_column}")
        index_name = f"{table_name}_{id_column}_idx"
        await conn.execute(
            f"CREATE INDEX IF NOT EXISTS {index_name} ON {full_table_name} ({id_column});"
        )


def _detect_coordinate_columns(
    headers: list[str], column_mapping: dict[str, str]
) -> tuple[str | None, str | None]:
    """Detect x and y coordinate columns for spatial data.

    Parameters
    ----------
    headers : list[str]
        Original CSV headers
    column_mapping : dict[str, str]
        Mapping of original to sanitized column names

    Returns
    -------
    tuple[str | None, str | None]
        Sanitized x column name and y column name (or None if not found)
    """
    x_col = None
    y_col = None
    for header in headers:
        sanitized = column_mapping[header]
        # Check for x coordinate column (starts with x_mp or is exactly named with coordinate pattern)
        if sanitized.startswith("x_mp") or sanitized.startswith("_x_mp") or "_x_mp_" in sanitized:
            x_col = sanitized
        # Check for y coordinate column (starts with y_mp or is exactly named with coordinate pattern)
        elif sanitized.startswith("y_mp") or sanitized.startswith("_y_mp") or "_y_mp_" in sanitized:
            y_col = sanitized

    # Debug output for coordinate detection
    if x_col and y_col:
        logger.debug(f"Detected coordinate columns: {x_col}, {y_col}")
    else:
        logger.debug(f"No coordinate columns detected (x_col={x_col}, y_col={y_col})")

    return x_col, y_col


def _build_final_schema(
    headers: list[str],
    column_mapping: dict[str, str],
    column_types: dict[str, str],
    x_col: str | None,
    y_col: str | None,
    srid: int,
) -> list[str]:
    """Build final table schema from detected types.

    Parameters
    ----------
    headers : list[str]
        Original CSV headers
    column_mapping : dict[str, str]
        Mapping of original to sanitized column names
    column_types : dict[str, str]
        Detected PostgreSQL types
    x_col : str | None
        X coordinate column
    y_col : str | None
        Y coordinate column
    srid : int
        Spatial reference system ID

    Returns
    -------
    list[str]
        Column definitions for CREATE TABLE statement
    """
    final_columns_def = []
    for header in headers:
        col_name = column_mapping[header]
        # Skip coordinate columns if we're creating geometry
        if x_col and y_col and col_name in [x_col, y_col]:
            continue
        col_type = column_types.get(col_name, "TEXT")
        final_columns_def.append(f"{col_name} {col_type}")

    # Add geometry column if we have coordinates
    if x_col and y_col:
        final_columns_def.append(f"geom GEOMETRY(Point, {srid})")

    return final_columns_def


def _build_data_transform_expressions(
    headers: list[str],
    column_mapping: dict[str, str],
    column_types: dict[str, str],
    x_col: str | None,
    y_col: str | None,
    source_srid: int,
    target_srid: int,
) -> tuple[list[str], list[str], str | None]:
    """Build SELECT and INSERT expressions for data transformation.

    Parameters
    ----------
    headers : list[str]
        Original CSV headers
    column_mapping : dict[str, str]
        Mapping of original to sanitized column names
    column_types : dict[str, str]
        Detected PostgreSQL types
    x_col : str | None
        X coordinate column
    y_col : str | None
        Y coordinate column
    source_srid : int
        Source spatial reference system ID (3035)
    target_srid : int
        Target spatial reference system ID

    Returns
    -------
    tuple[list[str], list[str], str | None]
        SELECT expressions, INSERT column names, and ID column name (if found)
    """
    select_cols = []
    insert_cols = []
    # When an ID column is present we use this later to create the index on it
    id_column = None

    for header in headers:
        col_name = column_mapping[header]
        # Skip coordinate columns if we're creating geometry
        if x_col and y_col and col_name in [x_col, y_col]:
            continue

        col_type = column_types.get(col_name, "TEXT")

        if col_name.startswith(ID_COLUMN_PREFIX):
            id_column = col_name
            select_expr = f"{col_name}::VARCHAR(60)"

        # Build the SELECT expression based on the target type
        elif col_type == "INTEGER":
            # Convert TEXT to INTEGER, handling German decimal format and NULLs
            select_expr = f"""
                NULLIF(
                    REPLACE(
                        REPLACE({col_name}, ',', '.'),
                        '{NULL_VALUE}',
                        ''), '')::INTEGER
            """
        elif col_type == "DOUBLE PRECISION":
            # Convert TEXT to DOUBLE PRECISION, handling German decimal format and NULLs
            select_expr = f"""
                NULLIF(
                    REPLACE(
                        REPLACE({col_name}, ',', '.'),
                        '{NULL_VALUE}',
                        ''), '')::DOUBLE PRECISION
            """
        else:
            # Keep as TEXT
            select_expr = col_name

        select_cols.append(select_expr)
        insert_cols.append(col_name)

    # Add geometry transformation if we have coordinates
    if x_col and y_col:
        logger.debug(f"Adding geometry column with SRID {target_srid}")
        # Create point geometry with source SRID
        point_expr = (
            f"ST_SetSRID(ST_MakePoint("
            f"NULLIF(REPLACE({x_col}, ',', '.'), '')::double precision, "
            f"NULLIF(REPLACE({y_col}, ',', '.'), '')::double precision"
            f"), {source_srid})"
        )

        # Transform to target SRID if different from source
        if target_srid != source_srid:
            logger.debug(f"Transforming coordinates from SRID {source_srid} to SRID {target_srid}")
            geom_expr = f"ST_Transform({point_expr}, {target_srid})"
        else:
            geom_expr = point_expr

        select_cols.append(geom_expr)
        insert_cols.append("geom")

    return select_cols, insert_cols, id_column


async def _parse_csv_headers(
    csv_file: Path, file_encoding: str
) -> tuple[list[str], dict[str, str]]:
    """Parse CSV headers and create sanitized column name mappings.

    Parameters
    ----------
    csv_file : Path
        Path to the CSV file
    file_encoding : str
        Detected encoding of the file

    Returns
    -------
    tuple[list[str], dict[str, str]]
        Original headers list and mapping of original to sanitized column names
    """
    # Read CSV header to determine columns
    async with aiofiles.open(csv_file, encoding=file_encoding) as f:
        reader = aiocsv.readers.AsyncReader(f, delimiter=";")
        headers = await anext(reader)

    # Create mapping of original headers to sanitized column names
    column_mapping = {header: sanitize_column_name(header) for header in headers}

    # Report any renamed columns
    if logger.level == logging.DEBUG:
        renamed_cols = [(orig, san) for orig, san in column_mapping.items() if orig.lower() != san]
        if renamed_cols:
            logging.debug(f"[yellow]Renamed {len(renamed_cols)} columns:[/yellow]")
            for orig, san in renamed_cols[:5]:  # Show first 5
                logging.debug(f"    {orig} → {san}")
            if len(renamed_cols) > 5:
                logging.debug(f"    ... and {len(renamed_cols) - 5} more")

    return headers, column_mapping


def sanitize_column_name(name: str) -> str:
    """Sanitize column name to be PostgreSQL-compliant.

    PostgreSQL identifiers cannot start with a digit.
    This function prefixes such names with an underscore.
    """
    name = name.lower()
    # If the name starts with a digit, prefix with underscore
    if name and name[0].isdigit():
        return f"_{name}"
    return name


def sanitize_table_name(filename: str) -> str:
    """Sanitize table name to be PostgreSQL-compliant.

    PostgreSQL table names have a 63-character limit.
    This function removes redundant prefixes/suffixes and normalizes the name.
    """
    name = filename.lower().replace("-", "_").replace(" ", "_")

    # Remove common redundant prefixes and suffixes
    name = name.replace("zensus2022_", "")
    name = name.replace("_gitter", "")

    # Ensure it's not too long (PostgreSQL limit is 63 chars)
    if len(name) > 63:
        name = name[:63]

    return name


async def detect_file_encoding(file_path: Path) -> str:
    """Detect the encoding of a CSV file.

    Tries common encodings and returns the first one that works.
    """
    encodings = ["utf-8", "iso-8859-1", "windows-1252", "cp1252"]

    for encoding in encodings:
        try:
            async with aiofiles.open(file_path, encoding=encoding) as f:
                # Try to read first 10 lines
                for _ in range(10):
                    await f.readline()
            return encoding
        except (UnicodeDecodeError, UnicodeError):
            continue

    # Default to iso-8859-1 which accepts all byte values
    return "iso-8859-1"


async def detect_column_type(conn: asyncpg.Connection, table_name: str, column_name: str) -> str:
    """Detect the appropriate PostgreSQL type for a column.

    Checks if column values can be converted to INTEGER or DOUBLE PRECISION.
    Returns 'INTEGER', 'DOUBLE PRECISION', or 'TEXT'.

    These datasets have a special value for null, and we accommodate for that.
    """
    # Replace commas with dots for German decimal format
    # Check if all non-null, non-empty values can be cast to numeric types
    row = await conn.fetchrow(
        rf"""
        SELECT
            COUNT(*) as total,
            COUNT(CASE WHEN {column_name} = '{NULL_VALUE}' THEN 1 END) as null_values,
            COUNT(CASE WHEN {column_name} IS NOT NULL AND {column_name} != '' THEN 1 END) as non_empty,
            COUNT(CASE
                WHEN {column_name} IS NOT NULL AND {column_name} != ''
                AND REPLACE({column_name}, ',', '.') ~ '^-?[0-9]+$'
                THEN 1
            END) as integer_count,
            COUNT(CASE
                WHEN {column_name} IS NOT NULL AND {column_name} != ''
                AND REPLACE({column_name}, ',', '.') ~ '^-?[0-9]+\.?[0-9]*$'
                THEN 1
            END) as numeric_count
        FROM {table_name}
        """
    )
    total, null_values, non_empty, integer_count, numeric_count = row

    if non_empty == 0:
        return "TEXT"

    if integer_count + null_values == non_empty:
        return "INTEGER"

    if numeric_count + null_values == non_empty:
        return "DOUBLE PRECISION"

    return "TEXT"


async def get_db_pool(db_config: DatabaseConfig) -> asyncpg.Pool:
    """
    Create a database connection pool and make sure the database is ready for import.

    Returns a connection pool instead of a single connection to support concurrent workers.
    """
    # Test database connection first
    try:
        logger.info(
            f"Connecting to PostgreSQL database '{db_config.database}' at {db_config.host}:{db_config.port}"
        )
        test_conn = await asyncpg.connect(
            user=db_config.user,
            password=db_config.password,
            database=db_config.database,
            host=db_config.host,
            port=db_config.port,
        )
        logger.info(f"Successfully connected to PostgreSQL database '{db_config.database}'")
    except asyncpg.PostgresError as e:
        logger.error(f"Error connecting to PostgreSQL: {e!s}")
        raise typer.Exit(1)

    # Ensure chosen schema exists
    try:
        logger.debug(f"Checking if schema '{db_config.schema}' exists")
        schema_exists = await test_conn.fetchval(
            """
            SELECT EXISTS(
                SELECT schema_name
                FROM information_schema.schemata where schema_name = $1
            )
            """,
            db_config.schema,
        )
        if not schema_exists:
            logger.error(f"Schema does not exist: {db_config.schema}")
            await test_conn.close()
            raise typer.Exit(1)
        logger.debug(f"Schema '{db_config.schema}' exists")
    except asyncpg.PostgresError as e:
        logger.error(f"PostgreSQL error: {e!s}")
        await test_conn.close()
        raise typer.Exit(1)

    await test_conn.close()

    # Create connection pool for concurrent workers
    try:
        pool = await asyncpg.create_pool(
            user=db_config.user,
            password=db_config.password,
            database=db_config.database,
            host=db_config.host,
            port=db_config.port,
            min_size=2,
            max_size=10,
        )
        logger.debug("Created database connection pool")
        return pool
    except asyncpg.PostgresError as e:
        logger.error(f"Error creating connection pool: {e!s}")
        raise typer.Exit(1)
