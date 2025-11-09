"""Zensus Collector CLI."""
import asyncio
import csv
import tempfile
import zipfile
from pathlib import Path
from typing import Optional

import aiofiles
import httpx
import psycopg
import typer
from rich import print as rprint
from rich.progress import Progress, SpinnerColumn, DownloadColumn, TransferSpeedColumn

from ..cache import CACHE, create_cache_dir

app = typer.Typer(help="Zensus Collector CLI")


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


def detect_file_encoding(file_path: Path) -> str:
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


def detect_column_type(cursor, table_name: str, column_name: str) -> str:
    """Detect the appropriate PostgreSQL type for a column.

    Checks if column values can be converted to INTEGER or DOUBLE PRECISION.
    Returns 'INTEGER', 'DOUBLE PRECISION', or 'TEXT'.
    """
    # Replace commas with dots for German decimal format
    # Check if all non-null, non-empty values can be cast to numeric types
    cursor.execute(
        f"""
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
    result = cursor.fetchone()
    total, non_empty, integer_count, numeric_count = result

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


# All gitterdaten files from the Zensus 2022 publication page
GITTERDATEN_FILES = [
    ("Zensus2022_Bevoelkerungszahl.zip", "https://www.destatis.de/static/DE/zensus/gitterdaten/Zensus2022_Bevoelkerungszahl.zip"),
    ("Deutsche_Staatsangehoerige_ab_18_Jahren.zip", "https://www.destatis.de/static/DE/zensus/gitterdaten/Deutsche_Staatsangehoerige_ab_18_Jahren.zip"),
    ("Auslaenderanteil_in_Gitterzellen.zip", "https://www.destatis.de/static/DE/zensus/gitterdaten/Auslaenderanteil_in_Gitterzellen.zip"),
    ("Auslaenderanteil_ab_18_Jahren.zip", "https://www.destatis.de/static/DE/zensus/gitterdaten/Auslaenderanteil_ab_18_Jahren.zip"),
    ("Zensus2022_Geburtsland_Gruppen_in_Gitterzellen.zip", "https://www.destatis.de/static/DE/zensus/gitterdaten/Zensus2022_Geburtsland_Gruppen_in_Gitterzellen.zip"),
    ("Zensus2022_Staatsangehoerigkeit_in_Gitterzellen.zip", "https://www.destatis.de/static/DE/zensus/gitterdaten/Zensus2022_Staatsangehoerigkeit_in_Gitterzellen.zip"),
    ("Zensus2022_Staatsangehoerigkeit_Gruppen_in_Gitterzellen.zip", "https://www.destatis.de/static/DE/zensus/gitterdaten/Zensus2022_Staatsangehoerigkeit_Gruppen_in_Gitterzellen.zip"),
    ("Zahl_der_Staatsangehoerigkeiten.zip", "https://www.destatis.de/static/DE/zensus/gitterdaten/Zahl_der_Staatsangehoerigkeiten.zip"),
    ("Durchschnittsalter_in_Gitterzellen.zip", "https://www.destatis.de/static/DE/zensus/gitterdaten/Durchschnittsalter_in_Gitterzellen.zip"),
    ("Alter_in_5_Altersklassen.zip", "https://www.destatis.de/static/DE/zensus/gitterdaten/Alter_in_5_Altersklassen.zip"),
    ("Alter_in_10er-Jahresgruppen.zip", "https://www.destatis.de/static/DE/zensus/gitterdaten/Alter_in_10er-Jahresgruppen.zip"),
    ("Anteil_unter_18-jaehrige_in_Gitterzellen.zip", "https://www.destatis.de/static/DE/zensus/gitterdaten/Anteil_unter_18-jaehrige_in_Gitterzellen.zip"),
    ("Anteil_ab_65-jaehrige_in_Gitterzellen.zip", "https://www.destatis.de/static/DE/zensus/gitterdaten/Anteil_ab_65-jaehrige_in_Gitterzellen.zip"),
    ("Alter_in_infrastrukturellen_Altersgruppen.zip", "https://www.destatis.de/static/DE/zensus/gitterdaten/Alter_in_infrastrukturellen_Altersgruppen.zip"),
    ("Familienstand_in_Gitterzellen.zip", "https://www.destatis.de/static/DE/zensus/gitterdaten/Familienstand_in_Gitterzellen.zip"),
    ("Religion.zip", "https://www.destatis.de/static/DE/zensus/gitterdaten/Religion.zip"),
    ("Durchschnittliche_Haushaltsgroesse_in_Gitterzellen.zip", "https://www.destatis.de/static/DE/zensus/gitterdaten/Durchschnittliche_Haushaltsgroesse_in_Gitterzellen.zip"),
    ("Zensus2022_Groesse_des_privaten_Haushalts_in_Gitterzellen.zip", "https://www.destatis.de/static/DE/zensus/gitterdaten/Zensus2022_Groesse_des_privaten_Haushalts_in_Gitterzellen.zip"),
    ("Typ_der_Kernfamilie_nach_Kindern.zip", "https://www.destatis.de/static/DE/zensus/gitterdaten/Typ_der_Kernfamilie_nach_Kindern.zip"),
    ("Groesse_der_Kernfamilie.zip", "https://www.destatis.de/static/DE/zensus/gitterdaten/Groesse_der_Kernfamilie.zip"),
    ("Typ_des_privaren_Haushalts_Lebensform.zip", "https://www.destatis.de/static/DE/zensus/gitterdaten/Typ_des_privaren_Haushalts_Lebensform.zip"),
    ("Typ_des_privaten_Haushalts_Familien.zip", "https://www.destatis.de/static/DE/zensus/gitterdaten/Typ_des_privaten_Haushalts_Familien.zip"),
    ("Seniorenstatus_eines_privaten_Haushalts.zip", "https://www.destatis.de/static/DE/zensus/gitterdaten/Seniorenstatus_eines_privaten_Haushalts.zip"),
    ("Zensus2022_Durchschn_Nettokaltmiete.zip", "https://www.destatis.de/static/DE/zensus/gitterdaten/Zensus2022_Durchschn_Nettokaltmiete.zip"),
    ("Durchschnittliche_Nettokaltmiete_und_Anzahl_der_Wohnungen.zip", "https://www.destatis.de/static/DE/zensus/gitterdaten/Durchschnittliche_Nettokaltmiete_und_Anzahl_der_Wohnungen.zip"),
    ("Eigentuemerquote_in_Gitterzellen.zip", "https://www.destatis.de/static/DE/zensus/gitterdaten/Eigentuemerquote_in_Gitterzellen.zip"),
    ("Leerstandsquote_in_Gitterzellen.zip", "https://www.destatis.de/static/DE/zensus/gitterdaten/Leerstandsquote_in_Gitterzellen.zip"),
    ("Marktaktive_Leerstandsquote_in_Gitterzellen.zip", "https://www.destatis.de/static/DE/zensus/gitterdaten/Marktaktive_Leerstandsquote_in_Gitterzellen.zip"),
    ("Durchschnittliche_Wohnflaeche_je_Bewohner_in_Gitterzellen.zip", "https://www.destatis.de/static/DE/zensus/gitterdaten/Durchschnittliche_Wohnflaeche_je_Bewohner_in_Gitterzellen.zip"),
    ("Durchschnittliche_Flaeche_je_Wohnung_in_Gitterzellen.zip", "https://www.destatis.de/static/DE/zensus/gitterdaten/Durchschnittliche_Flaeche_je_Wohnung_in_Gitterzellen.zip"),
    ("Flaeche_der_Wohnung_10m2_Intervalle.zip", "https://www.destatis.de/static/DE/zensus/gitterdaten/Flaeche_der_Wohnung_10m2_Intervalle.zip"),
    ("Wohnungen_nach_Gebaeudetyp_Groesse.zip", "https://www.destatis.de/static/DE/zensus/gitterdaten/Wohnungen_nach_Gebaeudetyp_Groesse.zip"),
    ("Wohnungen_nach_Zahl_der_Raeume.zip", "https://www.destatis.de/static/DE/zensus/gitterdaten/Wohnungen_nach_Zahl_der_Raeume.zip"),
    ("Gebaeude_nach_Baujahr_Jahrzehnte.zip", "https://www.destatis.de/static/DE/zensus/gitterdaten/Gebaeude_nach_Baujahr_Jahrzehnte.zip"),
    ("Gebaeude_nach_Baujahr_in_Mikrozensus_Klassen.zip", "https://www.destatis.de/static/DE/zensus/gitterdaten/Gebaeude_nach_Baujahr_in_Mikrozensus_Klassen.zip"),
    ("Gebaeude_nach_Anzahl_der_Wohnungen_im_Gebaeude.zip", "https://www.destatis.de/static/DE/zensus/gitterdaten/Gebaeude_nach_Anzahl_der_Wohnungen_im_Gebaeude.zip"),
    ("Gebaeude_mit_Wohnraum_nach_Gebaeudetyp_Groesse.zip", "https://www.destatis.de/static/DE/zensus/gitterdaten/Gebaeude_mit_Wohnraum_nach_Gebaeudetyp_Groesse.zip"),
    ("Zensus2022_Heizungsart.zip", "https://www.destatis.de/static/DE/zensus/gitterdaten/Zensus2022_Heizungsart.zip"),
    ("Gebaeude_mit_Wohnraum_nach_ueberwiegender_Heizungsart.zip", "https://www.destatis.de/static/DE/zensus/gitterdaten/Gebaeude_mit_Wohnraum_nach_ueberwiegender_Heizungsart.zip"),
    ("Zensus2022_Energietraeger.zip", "https://www.destatis.de/static/DE/zensus/gitterdaten/Zensus2022_Energietraeger.zip"),
    ("Gebaeude_mit_Wohnraum_nach_Energietraeger_der_Heizung.zip", "https://www.destatis.de/static/DE/zensus/gitterdaten/Gebaeude_mit_Wohnraum_nach_Energietraeger_der_Heizung.zip"),
    ("Gebaeude_nach_Baujahresklassen_in_Gitterzellen.zip", "https://www.destatis.de/static/DE/zensus/gitterdaten/Gebaeude_nach_Baujahresklassen_in_Gitterzellen.zip"),
    ("Auslaenderanteil_EU_nichtEU_Gitterzellen.zip", "https://www.destatis.de/static/DE/zensus/gitterdaten/Auslaenderanteil_EU_nichtEU_Gitterzellen.zip"),
    ("Shapefile_Zensus2022.zip", "https://www.destatis.de/static/DE/zensus/gitterdaten/Shapefile_Zensus2022.zip"),
]


@app.command()
def collect(
        tables: list[str] = typer.Argument(
            ["all"]
        ),
        skip_existing: bool = typer.Option(
            True,
            "--skip-existing/--overwrite",
            help="Skip files that already exist",
        ),
) -> None:
    """Download all Zensus 2022 gitterdaten (grid data) files."""
    # Mak sure our cache directory exists
    create_cache_dir()

    # Create output directory if it doesn't exist
    asyncio.run(collect_wrapper(tables, skip_existing))


async def collect_wrapper(tables, skip_existing):
    """
    Encapsulate all async opertations for the collect command
    """

    files_to_import = tuple((
        (filename, url) for filename, url in GITTERDATEN_FILES
         if tables == ["all"] or filename in tables)
    )

    rprint(f"[cyan]Downloading and importing {len(files_to_import)} gitterdaten files[/cyan]")

    http_client = httpx.AsyncClient()

    try:
        with Progress() as progress:
            jobs = []

            fetch_manager = FetchManager(
                http_client, Path(CACHE), total=len(files_to_import), progress=progress, skip_existing=skip_existing
            )

            for filename, url in files_to_import:
                if tables != ["all"] and filename not in tables:
                    continue
                await fetch_manager.fetch_queue.put((url, filename))

            # Add sentinel values for each fetch worker (one per worker)
            num_fetch_workers = 10
            for _ in range(num_fetch_workers):
                await fetch_manager.fetch_queue.put(None)

            for _ in range(num_fetch_workers):
                jobs.append(fetch_manager.fetch_worker())

            jobs.append(fetch_manager.extract_worker())
            jobs.append(fetch_manager.database_worker())

            # Add coordinator to manage pipeline shutdown
            jobs.append(fetch_manager.coordinator())

            await asyncio.gather(*jobs)

        rprint("\n[bold cyan]Download Summary:[/bold cyan]")
        rprint(f"  [green]✓ Downloaded: {fetch_manager.success}[/green]")
        rprint(f"  [yellow]⊘ Skipped: {fetch_manager.skipped}[/yellow]")
        rprint(f"  [red]✗ Failed: {fetch_manager.failed}[/red]")
    finally:
        await http_client.aclose()


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
        total: int | None = None,
        progress: Progress | None = None,
        semaphore: int = 10,
        skip_existing: bool = True,
    ) -> None:
        self.client = client
        self.output_folder = output_folder
        self.semaphore = asyncio.Semaphore(semaphore)
        self.progress = progress
        self.skip_existing = skip_existing

        # Progress bar tasks
        self.fetch_task = progress.add_task(f"[cyan]Downloading...[/cyan]", total=total)
        self.extract_task = progress.add_task(f"[cyan]Extracting...[/cyan]", total=total)
        self.database_task = progress.add_task(f"[cyan]Importing...[/cyan]", total=total * 3)

        # File processing statistics
        self.failed = 0
        self.success = 0
        self.skipped = 0

        self.fetch_queue = asyncio.Queue()
        self.extract_queue = asyncio.Queue()
        self.database_queue = asyncio.Queue()

    async def fetch_worker(self) -> None:
        """
        Fetch single file and write it to `self.output_folder` location
        """
        while True:
            fetch_item = await self.fetch_queue.get()

            if fetch_item is None:
                # Sentinel received - this worker is done
                self.fetch_queue.task_done()
                break

            url, filename = fetch_item

            output_path = Path(self.output_folder) / filename

            # Skip if file exists and skip_existing is True
            if output_path.exists() and self.skip_existing:
                await self.extract_queue.put(output_path)
                self.progress.update(self.fetch_task, advance=1)
                self.skipped += 1
                self.fetch_queue.task_done()
                continue

            try:
                async with self.semaphore:  # TODO: this probably isn't needed any more
                    async with self.client.stream("GET", url, follow_redirects=True, timeout=60.0) as response:
                        response.raise_for_status()

                        async with aiofiles.open(output_path, "wb") as f:
                            async for chunk in response.aiter_bytes(chunk_size=8192):
                                await f.write(chunk)
        
                            self.progress.update(self.fetch_task, advance=1)
                            await self.extract_queue.put(output_path)

                    self.success += 1

            except Exception as e:
                self.failed += 1
            finally:
                self.fetch_queue.task_done()

    async def extract_worker(self):
        """
        Worker that unzips files from the zipfile queue.

        TODO: Add a cache for extract files so we can skip them if they have been extracted
        """
        while True:
            zip_file = await self.extract_queue.get()

            if zip_file is None:
                # Sentinel value that terminates `database_worker`
                self.extract_queue.task_done()
                break

            try:
                with tempfile.TemporaryDirectory() as extract_to:
                    await asyncio.to_thread(
                        zipfile.ZipFile(zip_file).extractall,
                        extract_to
                    )

                    for csv_file in Path(extract_to).rglob(self.CSV_FILE_MATCH_PATTERN):
                        await self.database_queue.put(csv_file)

                    self.progress.update(self.extract_task, advance=1)
    
            finally:
                self.extract_queue.task_done()

    async def database_worker(self):
        """Worker that loads CSV files into the database"""
        while True:
            csv_file = await self.database_queue.get()

            if csv_file is None:
                self.database_queue.task_done()
                break

            try:
                self.progress.update(self.database_task, advance=1)

                # ... do database stuff
            finally:
                self.database_queue.task_done()

    async def coordinator(self):
        """
        Coordinator that manages the pipeline shutdown.

        Waits for all fetch workers to complete, then signals extract worker.
        Waits for extract worker to complete, then signals database worker.
        """
        # Wait for all items in fetch queue to be processed
        await self.fetch_queue.join()

        # Signal extract worker to stop
        await self.extract_queue.put(None)

        # Wait for all items in extract queue to be processed
        await self.extract_queue.join()

        # Signal database worker to stop
        await self.database_queue.put(None)

        # Wait for all items in database queue to be processed
        await self.database_queue.join()


@app.command()
def csv2pgsql(
        data_dir: Path = typer.Option(
            Path("/home/thath/volume/zensus/2022/data/"),
            "--data-dir",
            "-d",
            help="Directory containing CSV files to import",
        ),
        host: str = typer.Option(
            "localhost",
            "--host",
            "-h",
            help="PostgreSQL host",
        ),
        port: int = typer.Option(
            5432,
            "--port",
            "-p",
            help="PostgreSQL port",
        ),
        database: str = typer.Option(
            "zensus",
            "--database",
            "--db",
            help="PostgreSQL database name",
        ),
        user: str = typer.Option(
            "postgres",
            "--user",
            "-u",
            help="PostgreSQL user",
        ),
        password: Optional[str] = typer.Option(
            None,
            "--password",
            help="PostgreSQL password (will prompt if not provided)",
            prompt=True,
            hide_input=True,
        ),
        schema: str = typer.Option(
            "zensus",
            "--schema",
            "-s",
            help="PostgreSQL schema name",
        ),
        srid: int = typer.Option(
            3035,
            "--srid",
            help="SRID for the coordinate system (default: 3035 for ETRS89-extended / LAEA Europe)",
        ),
        drop_existing: bool = typer.Option(
            False,
            "--drop-existing/--no-drop",
            help="Drop existing tables before import",
        ),
) -> None:
    """Import CSV files from Zensus data into PostgreSQL with PostGIS using fast COPY."""
    if not data_dir.exists():
        rprint(f"[red]Error: Data directory {data_dir} does not exist[/red]")
        raise typer.Exit(1)

    # Connect to PostgreSQL
    try:
        conn_str = f"host={host} port={port} dbname={database} user={user} password={password}"
        conn = psycopg.connect(conn_str)
        rprint(f"[green]✓ Connected to PostgreSQL database '{database}'[/green]")
    except psycopg.Error as e:
        rprint(f"[red]Error connecting to PostgreSQL: {e!s}[/red]")
        raise typer.Exit(1)

    try:
        with conn.cursor() as cur:
            # Enable PostGIS extension
            cur.execute("CREATE EXTENSION IF NOT EXISTS postgis;")
            rprint("[green]✓ PostGIS extension enabled[/green]")

            # Create schema if it doesn't exist
            cur.execute(f"CREATE SCHEMA IF NOT EXISTS {schema};")
            rprint(f"[green]✓ Schema '{schema}' ready[/green]")
            conn.commit()

        # Find all CSV files in subdirectories
        csv_files = list(data_dir.rglob("*.csv"))
        rprint(f"\n[cyan]Found {len(csv_files)} CSV files to import[/cyan]\n")

        imported_count = 0
        skipped_count = 0
        failed_count = 0

        for csv_file in csv_files:
            try:
                # Generate table name from file name
                table_name = sanitize_table_name(csv_file.stem)
                full_table_name = f"{schema}.{table_name}"
                temp_table_name = f"{schema}.{table_name}_temp"

                rprint(f"[cyan]Processing: {csv_file.name}[/cyan]")
                rprint(f"  [cyan]Table name: {full_table_name} ({len(table_name)} chars)[/cyan]")

                # Detect file encoding
                file_encoding = detect_file_encoding(csv_file)
                if file_encoding != "utf-8":
                    rprint(f"  [yellow]Detected non-UTF-8 encoding: {file_encoding}[/yellow]")

                # Read CSV header to determine columns
                with open(csv_file, encoding=file_encoding) as f:
                    reader = csv.reader(f, delimiter=";")
                    headers = [h.strip() for h in next(reader)]  # Strip whitespace

                # Create mapping of original headers to sanitized column names
                column_mapping = {header: sanitize_column_name(header) for header in headers}

                # Report any renamed columns
                renamed_cols = [
                    (orig, san) for orig, san in column_mapping.items() if orig.lower() != san
                ]
                if renamed_cols:
                    rprint(f"  [yellow]Renamed {len(renamed_cols)} columns:[/yellow]")
                    for orig, san in renamed_cols[:5]:  # Show first 5
                        rprint(f"    {orig} → {san}")
                    if len(renamed_cols) > 5:
                        rprint(f"    ... and {len(renamed_cols) - 5} more")

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
                    rprint(f"  [green]Detected coordinates: {x_col}, {y_col}[/green]")
                else:
                    rprint(
                        f"  [yellow]No coordinate columns detected "
                        f"(x_col={x_col}, y_col={y_col})[/yellow]"
                    )
                    rprint(f"  [yellow]Available columns: {', '.join(column_mapping.values())}[/yellow]")

                with conn.cursor() as cur:
                    # Always drop temp table if it exists
                    cur.execute(f"DROP TABLE IF EXISTS {temp_table_name} CASCADE;")

                    # Check if final table exists and if it should be recreated
                    cur.execute(
                        """
                        SELECT EXISTS (
                            SELECT FROM information_schema.tables
                            WHERE table_schema = %s AND table_name = %s
                        )
                        """,
                        (schema, table_name),
                    )
                    table_exists = cur.fetchone()[0]

                    if table_exists:
                        if drop_existing:
                            cur.execute(f"DROP TABLE {full_table_name} CASCADE;")
                            rprint(f"  [yellow]Dropped existing table[/yellow]")
                        else:
                            rprint(f"  [yellow]Table already exists, skipping. Use --drop-existing to recreate.[/yellow]")
                            skipped_count += 1
                            continue

                    # Create temporary table with all columns as TEXT (sanitized names)
                    temp_columns_def = [f"{column_mapping[h]} TEXT" for h in headers]
                    create_temp_sql = (
                        f"CREATE TABLE {temp_table_name} "
                        f"({', '.join(temp_columns_def)});"
                    )
                    cur.execute(create_temp_sql)

                    # Use COPY to bulk load CSV into temporary table
                    with open(csv_file, encoding=file_encoding) as f:
                        # Skip header row
                        next(f)

                        # Map Python encoding to PostgreSQL encoding name
                        pg_encoding_map = {
                            "utf-8": "UTF8",
                            "iso-8859-1": "LATIN1",
                            "windows-1252": "WIN1252",
                            "cp1252": "WIN1252",
                        }
                        pg_encoding = pg_encoding_map.get(file_encoding, "UTF8")

                        # Use psycopg3 copy API
                        copy_sql = f"""
                            COPY {temp_table_name}
                            FROM STDIN
                            WITH (FORMAT CSV, DELIMITER ';', NULL '–', ENCODING '{pg_encoding}')
                        """
                        with cur.copy(copy_sql) as copy:
                            while True:
                                data = f.read(8192)
                                if not data:
                                    break
                                copy.write(data)

                    # Get row count from temp table
                    cur.execute(f"SELECT COUNT(*) FROM {temp_table_name};")
                    rows_loaded = cur.fetchone()[0]

                    # Detect data types for columns
                    rprint("  [cyan]Detecting column types...[/cyan]")
                    column_types = {}
                    for header in headers:
                        col_name = column_mapping[header]
                        # Skip coordinate columns if we're creating geometry
                        if x_col and y_col and col_name in [x_col, y_col]:
                            continue
                        # Detect type for this column
                        detected_type = detect_column_type(cur, temp_table_name, col_name)
                        column_types[col_name] = detected_type

                    # Report detected types
                    numeric_cols = {
                        col: typ for col, typ in column_types.items() if typ != "TEXT"
                    }
                    if numeric_cols:
                        rprint(
                            f"  [green]Detected {len(numeric_cols)} numeric columns "
                            f"({sum(1 for t in numeric_cols.values() if t == 'INTEGER')} INTEGER, "
                            f"{sum(1 for t in numeric_cols.values() if t == 'DOUBLE PRECISION')} DOUBLE PRECISION)[/green]"
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
                        final_columns_def.append(f"geom GEOMETRY(Point, {srid})")

                    # Create the final table (we already checked if it exists above)
                    create_final_sql = (
                        f"CREATE TABLE {full_table_name} "
                        f"({', '.join(final_columns_def)});"
                    )
                    cur.execute(create_final_sql)

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
                            select_expr = (
                                f"NULLIF(REPLACE({col_name}, ',', '.'), '')::INTEGER"
                            )
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
                        # Use ST_SetSRID and ST_MakePoint for geometry creation
                        select_cols.append(
                            f"ST_SetSRID(ST_MakePoint("
                            f"NULLIF(REPLACE({x_col}, ',', '.'), '')::double precision, "
                            f"NULLIF(REPLACE({y_col}, ',', '.'), '')::double precision"
                            f"), {srid})"
                        )
                        insert_cols.append("geom")

                    # Insert from temp to final table with geometry transformation
                    insert_from_temp_sql = f"""
                        INSERT INTO {full_table_name} ({', '.join(insert_cols)})
                        SELECT {', '.join(select_cols)}
                        FROM {temp_table_name}
                    """
                    cur.execute(insert_from_temp_sql)

                    # Create spatial index if we have geometry
                    if x_col and y_col:
                        index_name = f"{table_name}_geom_idx"
                        cur.execute(
                            f"CREATE INDEX IF NOT EXISTS {index_name} "
                            f"ON {full_table_name} USING GIST (geom);"
                        )

                    # Drop temporary table
                    cur.execute(f"DROP TABLE {temp_table_name};")

                    conn.commit()
                    rprint(
                        f"  [green]✓ Imported {rows_loaded:,} rows to {full_table_name}"
                        f"{' (with geometry)' if x_col and y_col else ''}[/green]"
                    )
                    imported_count += 1

            except Exception as e:
                rprint(f"  [red]✗ Failed to import {csv_file.name}: {e!s}[/red]")
                failed_count += 1
                conn.rollback()
                continue

        rprint("\n[bold cyan]Import Summary:[/bold cyan]")
        rprint(f"  [green]✓ Imported: {imported_count}[/green]")
        rprint(f"  [yellow]⊘ Skipped: {skipped_count}[/yellow]")
        rprint(f"  [red]✗ Failed: {failed_count}[/red]")

    finally:
        conn.close()
        rprint("\n[cyan]Database connection closed[/cyan]")
