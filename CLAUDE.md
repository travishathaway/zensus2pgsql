# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

`zensus2pgsql` is a Python CLI tool that downloads German Census (Zensus 2022) Gitterdaten (grid data) files and imports them into a PostgreSQL database with PostGIS support. It handles 43 different datasets covering demographics, housing, buildings, and more.

## Development Commands

### Environment Setup

```bash
# Create and install virtual environment with all dependencies
uv sync --python 3.13 --all-extras

# Activate virtual environment
source .venv/bin/activate

# Install pre-commit hooks
pre-commit install --install-hooks
```

### Running the CLI

```bash
# Run CLI during development
uv run zensus2pgsql --help
uv run zensus2pgsql list
uv run zensus2pgsql create [dataset_name]

# Run with verbosity flags
uv run zensus2pgsql create -v           # INFO level
uv run zensus2pgsql create -vv          # DEBUG level
```

### Testing and Quality

```bash
# Run all available Poe tasks
poe

# Lint the codebase (runs pre-commit on all files)
poe lint

# Run tests with coverage
poe test

# Individual test commands
coverage run                # Run tests
coverage report             # Show coverage report
coverage xml                # Generate XML coverage report

# Type checking
mypy src
```

### Documentation

```bash
# Build documentation
poe docs

# Serve documentation locally with live reload
poe docs --serve
```

### Dependency Management

```bash
# Add a runtime dependency
uv add {package}

# Add a development dependency
uv add --dev {package}

# Upgrade all dependencies
uv sync --upgrade

# Upgrade only dev dependencies
uv sync --upgrade --only-dev
```

### Version Management

```bash
# Bump version, update CHANGELOG.md, and create git tag
cz bump

# Push changes with tags
git push origin main --tags
```

## Architecture

### CLI Structure

The application uses **Typer** for CLI interface with commands organized as:

```
zensus2pgsql
├── create      # Download, extract, and import datasets
├── list        # List available datasets
└── drop        # Drop tables from PostgreSQL schema (in clean.py)
```

Command functions are defined in `src/zensus2pgsql/commands/` and registered in `src/zensus2pgsql/cli.py`.

### Data Pipeline Architecture

The `create` command implements a **multi-stage async pipeline** with three worker types:

1. **Fetch Workers** (configurable, default 5): Download ZIP files from URLs
2. **Extract Worker** (1): Extract CSVs from ZIP files
3. **Database Workers** (configurable, default 5): Import CSVs into PostgreSQL concurrently

Workers communicate via `asyncio.Queue` with sentinel values (`None`) for graceful shutdown. A coordinator manages the pipeline lifecycle.

**Database Connection Pooling**: The application uses `asyncpg.create_pool()` to support multiple concurrent database workers. Each worker acquires its own connection from the pool (min_size=2, max_size=10), preventing the "cannot use Connection.transaction() in a manually started transaction" error that would occur if workers shared a single connection.

### Database Import Process

For each CSV file, the import follows these steps:

1. **Encoding Detection**: Auto-detect file encoding (UTF-8, ISO-8859-1, Windows-1252)
2. **Header Parsing**: Read CSV headers and sanitize column names for PostgreSQL
3. **Temporary Table**: Create temp table with all columns as TEXT
4. **Bulk Load**: Use `asyncpg.copy_to_table()` for fast COPY operation
5. **Type Detection**: Analyze data to detect INTEGER, DOUBLE PRECISION, or TEXT types
6. **Final Table**: Create final table with proper types
7. **Geometry Creation**: If coordinate columns (x_mp/y_mp) exist, create PostGIS Point geometry with SRID 3035
8. **Spatial Index**: Create GIST index on geometry column
9. **Cleanup**: Drop temporary table

### Key Technical Details

- **Coordinate Detection**: Looks for columns starting with `x_mp` or `y_mp` for spatial data
- **Column Sanitization**: Prefixes column names starting with digits with underscore
- **German Number Format**: Handles comma as decimal separator (e.g., "1,5" → 1.5)
- **NULL Handling**: Treats "-" as NULL value in CSV files
- **SRID**: Default spatial reference system is 3035 (ETRS89-extended / LAEA Europe)

### Logging System

Uses Python's `logging` module with **Rich** handler for colored output. Verbosity levels:

- **WARNING** (default): Errors only
- **INFO** (`-v`): Major operations (downloading, extracting, importing)
- **DEBUG** (`-vv`): Detailed information (encoding, column types, SQL operations)

Logger is configured in `src/zensus2pgsql/logging.py` and controlled by `configure_logging()` in `create.py`.

### File Organization

```
src/zensus2pgsql/
├── cli.py              # Main CLI entry point, registers commands
├── constants.py        # GITTERDATEN_FILES list with 43 datasets
├── cache.py            # Cache directory management (platformdirs)
├── logging.py          # Rich logging configuration
├── errors.py           # Custom exceptions
└── commands/
    ├── create.py       # Main create command with async pipeline
    ├── list.py         # List available datasets
    └── clean.py        # Drop tables command
```

### Database Schema Patterns

Tables are named by sanitizing the CSV filename:
- Prefix `zensus2022_` removed
- Suffix `_gitter` removed
- Hyphens/spaces converted to underscores
- Lowercased and truncated to 63 chars (PostgreSQL limit)

Example: `Zensus2022_Bevoelkerungszahl.csv` → `bevoelkerungszahl`

### Constants and Configuration

- **GITTERDATEN_FILES**: Tuple in `constants.py` with dataset `name` and `url`
- **CACHE**: Platform-specific cache directory via `platformdirs.user_cache_dir()`
- **CSV_FILE_MATCH_PATTERN**: `"*Gitter.csv"` for finding CSVs in extracted ZIPs

## Code Style

- Python 3.13+ (though should work on 3.10+)
- Uses **Ruff** for linting and formatting
- Line length: 100 characters
- Docstring convention: NumPy style
- Type hints encouraged but `mypy` configured with `ignore_missing_imports = true`
- Conventional Commits for commit messages

## Testing Notes

- Tests in `tests/` directory
- Uses pytest with `--exitfirst --failed-first` flags
- Test command runs via `coverage run` which executes pytest
- Coverage reports written to `reports/` directory
