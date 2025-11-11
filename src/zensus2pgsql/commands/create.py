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
import logging
import tempfile
import zipfile
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


def detect_file_encoding_old(file_path: Path) -> str:
    """Detect the encoding of a CSV file.

    Tries common encodings and returns the first one that works.
    """
    encodings = ["utf-8", "iso-8859-1", "windows-1252", "cp1252"]

    for encoding in encodings:
        try:
            with open(file_path, encoding=encoding) as f:
                # Try to read first 10 lines
                for _ in range(10):
                    f.readline()
            return encoding
        except (UnicodeDecodeError, UnicodeError):
            continue

    # Default to iso-8859-1 which accepts all byte values
    return "iso-8859-1"


async def detect_column_type(conn: asyncpg.Connection, table_name: str, column_name: str) -> str:
    """Detect the appropriate PostgreSQL type for a column.

    Checks if column values can be converted to INTEGER or DOUBLE PRECISION.
    Returns 'INTEGER', 'DOUBLE PRECISION', or 'TEXT'.
    """
    # Replace commas with dots for German decimal format
    # Check if all non-null, non-empty values can be cast to numeric types
    row = await conn.fetchrow(
        rf"""
        SELECT
            COUNT(*) as total,
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
    total, non_empty, integer_count, numeric_count = row

    # If column is empty or has no non-empty values, keep as TEXT
    if non_empty == 0:
        return "TEXT"

    # If all non-empty values are integers
    if integer_count == non_empty:
        return "INTEGER"

    # If all non-empty values are numeric (including decimals)
    if numeric_count == non_empty:
        return "DOUBLE PRECISION"

    # Otherwise, keep as TEXT
    return "TEXT"


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


def collect(
    tables: list[str] = typer.Argument(["all"]),
    host: str = typer.Option("localhost", "--host", "-h", help="PostgreSQL host"),
    port: int = typer.Option(5432, "--port", "-p", help="PostgreSQL port"),
    database: str = typer.Option("zensus", "--database", "--db", help="PostgreSQL database name"),
    user: str = typer.Option("postgres", "--user", "-u", help="PostgreSQL user"),
    password: str | None = typer.Option(
        None,
        "--password",
        help="PostgreSQL password (will prompt if not provided)",
        prompt=True,
        hide_input=True,
    ),
    schema: str = typer.Option("zensus", "--schema", "-s", help="PostgreSQL schema name"),
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
    verbose: int = typer.Option(
        0, "--verbose", "-v", count=True, help="Increase verbosity (-v for INFO, -vv for DEBUG)"
    ),
) -> None:
    """Download, extract and import Zensus 2022 Gitterdaten (grid data)."""
    # Configure logging based on verbosity
    configure_logging(verbose)

    # Make sure our cache directory exists
    create_cache_dir()

    db_config = DatabaseConfig(host, port, database, user, password, schema, srid, drop_existing)

    # Create output directory if it doesn't exist
    asyncio.run(collect_wrapper(tables, skip_existing, db_config))


async def collect_wrapper(tables: list[str], skip_existing: bool, db_config: DatabaseConfig):
    """
    Encapsulate all async operations for the collect command
    """
    db_pool = await get_db_pool(db_config)

    files_to_import: list[tuple[str, str]]

    # Build list of files to download and import
    if tables == ["all"]:
        files_to_import = [(f["name"], f["url"]) for f in GITTERDATEN_FILES]
    else:
        files_to_import = [(f["name"], f["url"]) for f in GITTERDATEN_FILES if f["name"] in tables]

    rprint(f"[cyan]Downloading and importing {len(files_to_import)} Gitterdaten files[/cyan]")

    http_client = httpx.AsyncClient(http2=True)

    try:
        # Configure progress bar to show counts instead of percentages
        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            BarColumn(),
            MofNCompleteColumn(),  # Shows "X of Y" instead of percentage
            TimeRemainingColumn(),
        ) as progress:
            fetch_manager = FetchManager(
                http_client,
                Path(CACHE),
                progress,
                db_pool,
                db_config,
                total=len(files_to_import),
                skip_existing=skip_existing,
            )

            await fetch_manager.start(files_to_import, tables)

        rprint("\n[bold cyan]Download Summary:[/bold cyan]")
        rprint(f"  [green]✓ Downloaded: {fetch_manager.success}[/green]")
        rprint(f"  [yellow]⊘ Skipped: {fetch_manager.skipped}[/yellow]")
        rprint(f"  [red]✗ Failed: {fetch_manager.failed}[/red]")
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
        db_config: DatabaseConfig,
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

        # Progress bar tasks
        self.fetch_task = progress.add_task("[cyan]Downloading...[/cyan]", total=total)
        self.extract_task = progress.add_task("[cyan]Extracting...[/cyan]", total=total)

        # Total number of database jobs is unknown at the beginning so we
        # increment this as we collect CSV files to import
        self.database_task_total = 0
        self.database_task = progress.add_task(
            "[cyan]Importing...[/cyan]", total=self.database_task_total
        )

        # File processing statistics
        self.failed: int = 0
        self.success: int = 0
        self.skipped: int = 0

        # Keeps track of errors
        self.errors: list[str] = []

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
                self.skipped += 1
                await self.extract_queue.put(output_path)
                self.progress.update(self.fetch_task, advance=1)
                self.fetch_queue.task_done()
                continue

            try:
                logger.info(f"Downloading: {filename}")
                async with self.semaphore:  # TODO: this probably isn't needed any more
                    async with self.client.stream(
                        "GET", url, follow_redirects=True, timeout=60.0
                    ) as response:
                        response.raise_for_status()

                        async with aiofiles.open(output_path, "wb") as f:
                            async for chunk in response.aiter_bytes(chunk_size=8192):
                                await f.write(chunk)

                            self.progress.update(self.fetch_task, advance=1)
                            await self.extract_queue.put(output_path)

                    self.success += 1
                    logger.debug(f"Successfully downloaded: {filename}")

            except Exception as e:
                self.failed += 1
                logger.error(f"Failed to download {filename}: {e}")
            finally:
                self.fetch_queue.task_done()

    async def extract_worker(self):
        """
        Worker that unzips files from the zipfile queue.

        TODO: Add a cache for extracted files so we can skip them if they have been extracted
        """
        while True:
            zip_file = await self.extract_queue.get()

            if zip_file is None:
                # Sentinel value that terminates `database_worker`
                logger.debug("Extract worker received sentinel, shutting down")
                self.extract_queue.task_done()
                break

            try:
                logger.info(f"Extracting: {zip_file.name}")
                extract_to = Path(self.temp_dir.name) / zip_file.name.replace(".zip", "")
                await asyncio.to_thread(zipfile.ZipFile(zip_file).extractall, extract_to)

                csv_files = list(Path(extract_to).rglob(self.CSV_FILE_MATCH_PATTERN))
                logger.debug(f"Found {len(csv_files)} CSV file(s) in {zip_file.name}")

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

                    # Read CSV header to determine columns
                    async with aiofiles.open(csv_file, encoding=file_encoding) as f:
                        reader = aiocsv.readers.AsyncReader(f, delimiter=";")
                        headers = await anext(reader)

                    # Create mapping of original headers to sanitized column names
                    column_mapping = {header: sanitize_column_name(header) for header in headers}

                    # Report any renamed columns
                    if logger.level == logging.DEBUG:
                        renamed_cols = [
                            (orig, san)
                            for orig, san in column_mapping.items()
                            if orig.lower() != san
                        ]
                        if renamed_cols:
                            logging.debug(f"[yellow]Renamed {len(renamed_cols)} columns:[/yellow]")
                            for orig, san in renamed_cols[:5]:  # Show first 5
                                logging.debug(f"    {orig} → {san}")
                            if len(renamed_cols) > 5:
                                logging.debug(f"    ... and {len(renamed_cols) - 5} more")

                    # Identify coordinate columns (using sanitized names)
                    x_col = None
                    y_col = None
                    for header in headers:
                        sanitized = column_mapping[header]
                        # Check for x coordinate column (starts with x_mp or is exactly named with coordinate pattern)
                        if (
                            sanitized.startswith("x_mp")
                            or sanitized.startswith("_x_mp")
                            or "_x_mp_" in sanitized
                        ):
                            x_col = sanitized
                        # Check for y coordinate column (starts with y_mp or is exactly named with coordinate pattern)
                        elif (
                            sanitized.startswith("y_mp")
                            or sanitized.startswith("_y_mp")
                            or "_y_mp_" in sanitized
                        ):
                            y_col = sanitized

                    # Debug output for coordinate detection
                    if x_col and y_col:
                        logger.debug(f"Detected coordinate columns: {x_col}, {y_col}")
                    else:
                        logger.debug(
                            f"No coordinate columns detected (x_col={x_col}, y_col={y_col})"
                        )

                    async with conn.transaction():
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
                                continue

                        # Create temporary table with all columns as TEXT (sanitized names)
                        logger.debug(f"Creating temporary table: {temp_table_name}")
                        temp_columns_def = [f"{column_mapping[h]} TEXT" for h in headers]
                        create_temp_sql = (
                            f"CREATE TABLE {temp_table_name} ({', '.join(temp_columns_def)});"
                        )
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
                        logger.debug(
                            f"Copying data to temporary table using encoding: {pg_encoding}"
                        )
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
                            row_count = await conn.fetchval(
                                f"SELECT COUNT(*) FROM {temp_table_name};"
                            )
                            logger.debug(f"Row count: {row_count}")

                        # Detect data types for columns
                        column_types = {}
                        for header in headers:
                            col_name = column_mapping[header]
                            # Skip coordinate columns if we're creating geometry
                            if x_col and y_col and col_name in [x_col, y_col]:
                                continue
                            # Detect type for this column
                            detected_type = await detect_column_type(
                                conn, temp_table_name, col_name
                            )
                            column_types[col_name] = detected_type

                        # Report detected types
                        numeric_cols = {
                            col: typ for col, typ in column_types.items() if typ != "TEXT"
                        }
                        if numeric_cols:
                            logger.debug(
                                f"Detected {len(numeric_cols)} numeric columns "
                                f"({sum(1 for t in numeric_cols.values() if t == 'INTEGER')} INTEGER, "
                                f"{sum(1 for t in numeric_cols.values() if t == 'DOUBLE PRECISION')} DOUBLE PRECISION)"
                            )

                        # Create final table with detected types (using sanitized names)
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
                            final_columns_def.append(f"geom GEOMETRY(Point, {self.db_config.srid})")

                        # Create the final table (we already checked if it exists above)
                        logger.debug(f"Creating final table: {full_table_name}")
                        create_final_sql = (
                            f"CREATE TABLE {full_table_name} ({', '.join(final_columns_def)});"
                        )
                        await conn.execute(create_final_sql)

                        # Prepare column lists for INSERT INTO ... SELECT (using sanitized names)
                        select_cols = []
                        insert_cols = []

                        for header in headers:
                            col_name = column_mapping[header]
                            # Skip coordinate columns if we're creating geometry
                            if x_col and y_col and col_name in [x_col, y_col]:
                                continue

                            col_type = column_types.get(col_name, "TEXT")

                            # Build the SELECT expression based on the target type
                            if col_type == "INTEGER":
                                # Convert TEXT to INTEGER, handling German decimal format and NULLs
                                select_expr = f"NULLIF(REPLACE({col_name}, ',', '.'), '')::INTEGER"
                            elif col_type == "DOUBLE PRECISION":
                                # Convert TEXT to DOUBLE PRECISION, handling German decimal format and NULLs
                                select_expr = (
                                    f"NULLIF(REPLACE({col_name}, ',', '.'), '')::DOUBLE PRECISION"
                                )
                            else:
                                # Keep as TEXT
                                select_expr = col_name

                            select_cols.append(select_expr)
                            insert_cols.append(col_name)

                        # Add geometry transformation if we have coordinates
                        if x_col and y_col:
                            logger.debug(f"Adding geometry column with SRID {self.db_config.srid}")
                            # Use ST_SetSRID and ST_MakePoint for geometry creation
                            select_cols.append(
                                f"ST_SetSRID(ST_MakePoint("
                                f"NULLIF(REPLACE({x_col}, ',', '.'), '')::double precision, "
                                f"NULLIF(REPLACE({y_col}, ',', '.'), '')::double precision"
                                f"), {self.db_config.srid})"
                            )
                            insert_cols.append("geom")

                        # Insert from temp to final table with geometry transformation
                        logger.debug("Inserting data from temporary table to final table")
                        insert_from_temp_sql = f"""
                            INSERT INTO {full_table_name} ({", ".join(insert_cols)})
                            SELECT {", ".join(select_cols)}
                            FROM {temp_table_name}
                        """
                        await conn.execute(insert_from_temp_sql)

                        # Create spatial index if we have geometry
                        if x_col and y_col:
                            logger.debug("Creating spatial index on geometry column")
                            index_name = f"{table_name}_geom_idx"
                            await conn.execute(
                                f"CREATE INDEX IF NOT EXISTS {index_name} "
                                f"ON {full_table_name} USING GIST (geom);"
                            )

                        # Drop temporary table
                        logger.debug(f"Dropping temporary table: {temp_table_name}")
                        await conn.execute(f"DROP TABLE {temp_table_name};")

                        self.progress.update(self.database_task, advance=1)

                        logger.debug(
                            f"  [green]✓ Imported {full_table_name}"
                            f"{' (with geometry)' if x_col and y_col else ''}[/green]"
                        )

                except Exception as e:
                    logger.error(f"  [red]✗ Failed to import {csv_file.name}: {e!s}[/red]")

                finally:
                    self.database_queue.task_done()

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
