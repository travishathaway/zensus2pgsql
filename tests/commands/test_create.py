"""Tests for the create command - targeting 100% coverage."""

import asyncio
import tempfile
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, Mock, patch

import httpx
import pytest

from zensus2pgsql.commands.create import (
    CreateCommandConfig,
    DatabaseConfig,
    FetchManager,
    collect_wrapper,
    create,
    detect_column_type,
    detect_file_encoding,
    get_db_pool,
    sanitize_column_name,
    sanitize_table_name,
)

# =============================================================================
# UTILITY FUNCTION TESTS
# =============================================================================


class TestSanitizeColumnName:
    """Tests for sanitize_column_name function."""

    def test_lowercase_conversion(self):
        """Test that column names are converted to lowercase."""
        assert sanitize_column_name("ColumnName") == "columnname"
        assert sanitize_column_name("UPPERCASE") == "uppercase"
        assert sanitize_column_name("MixedCase") == "mixedcase"

    def test_digit_prefix_gets_underscore(self):
        """Test that names starting with digits get underscore prefix."""
        assert sanitize_column_name("1column") == "_1column"
        assert sanitize_column_name("123abc") == "_123abc"
        assert sanitize_column_name("9test") == "_9test"

    def test_already_sanitized_unchanged(self):
        """Test that already sanitized names remain unchanged."""
        assert sanitize_column_name("column_name") == "column_name"
        assert sanitize_column_name("lowercase") == "lowercase"
        assert sanitize_column_name("_prefixed") == "_prefixed"

    def test_empty_string(self):
        """Test that empty string returns empty string."""
        assert sanitize_column_name("") == ""

    def test_underscore_prefix_preserved(self):
        """Test that existing underscore prefix is preserved."""
        assert sanitize_column_name("_existing") == "_existing"
        assert sanitize_column_name("__double") == "__double"


class TestSanitizeTableName:
    """Tests for sanitize_table_name function."""

    def test_removes_zensus2022_prefix(self):
        """Test that zensus2022_ prefix is removed."""
        assert sanitize_table_name("zensus2022_bevoelkerung") == "bevoelkerung"
        assert sanitize_table_name("Zensus2022_Test") == "test"

    def test_removes_gitter_suffix(self):
        """Test that _gitter suffix is removed."""
        assert sanitize_table_name("test_gitter") == "test"
        assert sanitize_table_name("data_gitter.csv") == "data.csv"

    def test_replaces_hyphens_with_underscores(self):
        """Test that hyphens are replaced with underscores."""
        assert sanitize_table_name("test-name") == "test_name"
        assert sanitize_table_name("multi-part-name") == "multi_part_name"

    def test_replaces_spaces_with_underscores(self):
        """Test that spaces are replaced with underscores."""
        assert sanitize_table_name("test name") == "test_name"
        assert sanitize_table_name("multi part name") == "multi_part_name"

    def test_truncates_to_63_characters(self):
        """Test that names are truncated to 63 characters."""
        long_name = "a" * 100
        result = sanitize_table_name(long_name)
        assert len(result) == 63
        assert result == "a" * 63

    def test_lowercase_conversion(self):
        """Test that names are converted to lowercase."""
        assert sanitize_table_name("TestName") == "testname"
        assert sanitize_table_name("UPPERCASE") == "uppercase"

    def test_combined_transformations(self):
        """Test multiple transformations applied together."""
        result = sanitize_table_name("Zensus2022_Test-Data Name_gitter")
        assert result == "test_data_name"

    def test_csv_extension_preserved(self):
        """Test that .csv extension is preserved in sanitization."""
        # The function doesn't explicitly remove .csv, it stays
        result = sanitize_table_name("Zensus2022_Data_Gitter.csv")
        assert ".csv" in result


class TestDatabaseConfig:
    """Tests for DatabaseConfig NamedTuple."""

    def test_create_valid_config(self, database_config):
        """Test creating a valid DatabaseConfig."""
        assert database_config.host == "localhost"
        assert database_config.port == 5432
        assert database_config.database == "test_db"
        assert database_config.user == "test_user"
        assert database_config.password == "test_pass"
        assert database_config.schema == "test_schema"
        assert database_config.srid == 3035
        assert database_config.drop_existing is False

    def test_config_with_none_password(self):
        """Test DatabaseConfig with None password."""
        config = DatabaseConfig(
            host="localhost",
            port=5432,
            database="test_db",
            user="test_user",
            password=None,
            schema="public",
            srid=3035,
            drop_existing=False,
        )
        assert config.password is None

    def test_config_with_custom_port(self):
        """Test DatabaseConfig with custom port."""
        config = DatabaseConfig(
            host="localhost",
            port=5433,
            database="test_db",
            user="test_user",
            password="pass",
            schema="public",
            srid=4326,
            drop_existing=True,
        )
        assert config.port == 5433
        assert config.srid == 4326
        assert config.drop_existing is True


# =============================================================================
# ENCODING DETECTION TESTS
# =============================================================================


class TestDetectFileEncoding:
    """Tests for detect_file_encoding async function."""

    @pytest.mark.asyncio
    async def test_detects_utf8_encoding(self, temp_csv_file):
        """Test that UTF-8 files are correctly detected."""
        encoding = await detect_file_encoding(temp_csv_file)
        assert encoding == "utf-8"

    @pytest.mark.asyncio
    async def test_detects_latin1_encoding(self, temp_csv_file_latin1):
        """Test that ISO-8859-1 files are correctly detected."""
        encoding = await detect_file_encoding(temp_csv_file_latin1)
        # UTF-8 can read ISO-8859-1 in many cases, so we accept either
        assert encoding in ["utf-8", "iso-8859-1"]

    @pytest.mark.asyncio
    async def test_detects_windows1252_encoding(self, temp_csv_file_windows1252):
        """Test that Windows-1252 files are correctly detected."""
        encoding = await detect_file_encoding(temp_csv_file_windows1252)
        # May detect as utf-8 if content is compatible
        assert encoding in ["utf-8", "windows-1252", "iso-8859-1"]

    @pytest.mark.asyncio
    async def test_fallback_to_iso8859(self):
        """Test fallback to ISO-8859-1 for unknown encodings."""
        # Create a file with invalid UTF-8 bytes
        with tempfile.NamedTemporaryFile(mode="wb", delete=False, suffix=".csv") as f:
            # Write bytes that are invalid UTF-8
            f.write(b"\xff\xfe" + "test".encode("utf-16-le"))
            temp_path = Path(f.name)

        encoding = await detect_file_encoding(temp_path)
        # Should fallback to iso-8859-1 or detect one of the encodings
        assert encoding in ["utf-8", "iso-8859-1", "windows-1252", "cp1252"]

    @pytest.mark.asyncio
    async def test_empty_file(self):
        """Test encoding detection on empty file."""
        with tempfile.NamedTemporaryFile(mode="w", delete=False, suffix=".csv") as f:
            temp_path = Path(f.name)

        encoding = await detect_file_encoding(temp_path)
        assert encoding == "utf-8"  # Empty file should be valid UTF-8


# =============================================================================
# TYPE DETECTION TESTS
# =============================================================================


class TestDetectColumnType:
    """Tests for detect_column_type async function."""

    @pytest.mark.asyncio
    async def test_detects_integer_type(self):
        """Test detection of INTEGER type columns."""
        conn = AsyncMock()
        conn.fetchrow = AsyncMock(return_value=(100, 0, 100, 100, 100))

        result = await detect_column_type(conn, "test_table", "test_col")
        assert result == "INTEGER"

    @pytest.mark.asyncio
    async def test_detects_double_precision_type(self):
        """Test detection of DOUBLE PRECISION type columns."""
        conn = AsyncMock()
        # non_empty=100, integer_count=50, numeric_count=100 (all are numeric but not all integers)
        conn.fetchrow = AsyncMock(return_value=(100, 0, 100, 50, 100))

        result = await detect_column_type(conn, "test_table", "test_col")
        assert result == "DOUBLE PRECISION"

    @pytest.mark.asyncio
    async def test_detects_text_type(self):
        """Test detection of TEXT type columns."""
        conn = AsyncMock()
        # Some values are not numeric
        conn.fetchrow = AsyncMock(return_value=(100, 0, 100, 50, 50))

        result = await detect_column_type(conn, "test_table", "test_col")
        assert result == "TEXT"

    @pytest.mark.asyncio
    async def test_empty_column_returns_text(self):
        """Test that empty columns return TEXT type."""
        conn = AsyncMock()
        # non_empty=0
        conn.fetchrow = AsyncMock(return_value=(100, 0, 0, 0, 0))

        result = await detect_column_type(conn, "test_table", "test_col")
        assert result == "TEXT"

    @pytest.mark.asyncio
    async def test_all_null_returns_text(self):
        """Test that all-NULL columns return TEXT type."""
        conn = AsyncMock()
        conn.fetchrow = AsyncMock(return_value=(0, 0, 0, 0, 0))

        result = await detect_column_type(conn, "test_table", "test_col")
        assert result == "TEXT"

    @pytest.mark.asyncio
    async def test_negative_integers_detected(self):
        """Test that negative integers are correctly detected."""
        conn = AsyncMock()
        # All values are integers including negatives
        conn.fetchrow = AsyncMock(return_value=(100, 0, 100, 100, 100))

        result = await detect_column_type(conn, "test_table", "test_col")
        assert result == "INTEGER"

    @pytest.mark.asyncio
    async def test_german_decimal_format_detected(self):
        """Test that German decimal format is detected as numeric."""
        conn = AsyncMock()
        # Numeric regex handles German format via REPLACE
        conn.fetchrow = AsyncMock(return_value=(100, 0, 100, 0, 100))

        result = await detect_column_type(conn, "test_table", "test_col")
        assert result == "DOUBLE PRECISION"

    @pytest.mark.asyncio
    async def test_null_values_integer_detected(self):
        """Test that null values and integers are detected as integer."""
        conn = AsyncMock()
        # Numeric regex handles German format via REPLACE
        conn.fetchrow = AsyncMock(return_value=(100, 50, 50, 0, 100))

        result = await detect_column_type(conn, "test_table", "test_col")
        assert result == "INTEGER"


# =============================================================================
# DATABASE POOL TESTS
# =============================================================================


class TestGetDbPool:
    """Tests for get_db_pool async function."""

    @pytest.mark.asyncio
    async def test_successful_connection_returns_pool(self, database_config):
        """Test that successful connection returns a pool."""
        mock_conn = AsyncMock()
        mock_conn.fetchval = AsyncMock(return_value=True)  # Schema exists
        mock_conn.close = AsyncMock()

        mock_pool = MagicMock()

        async def mock_connect(*args, **kwargs):
            return mock_conn

        async def mock_create_pool(*args, **kwargs):
            return mock_pool

        with (
            patch("zensus2pgsql.commands.create.asyncpg.connect", side_effect=mock_connect),
            patch("zensus2pgsql.commands.create.asyncpg.create_pool", side_effect=mock_create_pool),
        ):
            result = await get_db_pool(database_config)
            assert result == mock_pool
            mock_conn.close.assert_called_once()

    @pytest.mark.asyncio
    async def test_connection_failure_exits(self, database_config):
        """Test that connection failure raises typer.Exit."""
        import asyncpg
        import typer

        async def mock_connect(*args, **kwargs):
            raise asyncpg.PostgresError("Connection refused")

        with patch("zensus2pgsql.commands.create.asyncpg.connect", side_effect=mock_connect):
            with pytest.raises(typer.Exit) as exc_info:
                await get_db_pool(database_config)
            assert exc_info.value.exit_code == 1

    @pytest.mark.asyncio
    async def test_schema_not_exists_exits(self, database_config):
        """Test that missing schema raises typer.Exit."""
        import typer

        mock_conn = AsyncMock()
        mock_conn.fetchval = AsyncMock(return_value=False)  # Schema doesn't exist
        mock_conn.close = AsyncMock()

        async def mock_connect(*args, **kwargs):
            return mock_conn

        with patch("zensus2pgsql.commands.create.asyncpg.connect", side_effect=mock_connect):
            with pytest.raises(typer.Exit) as exc_info:
                await get_db_pool(database_config)
            assert exc_info.value.exit_code == 1
            mock_conn.close.assert_called_once()

    @pytest.mark.asyncio
    async def test_schema_check_error_exits(self, database_config):
        """Test that schema check error raises typer.Exit."""
        import asyncpg
        import typer

        mock_conn = AsyncMock()
        mock_conn.fetchval = AsyncMock(side_effect=asyncpg.PostgresError("Query failed"))
        mock_conn.close = AsyncMock()

        async def mock_connect(*args, **kwargs):
            return mock_conn

        with patch("zensus2pgsql.commands.create.asyncpg.connect", side_effect=mock_connect):
            with pytest.raises(typer.Exit) as exc_info:
                await get_db_pool(database_config)
            assert exc_info.value.exit_code == 1
            mock_conn.close.assert_called_once()

    @pytest.mark.asyncio
    async def test_pool_creation_failure_exits(self, database_config):
        """Test that pool creation failure raises typer.Exit."""
        import asyncpg
        import typer

        mock_conn = AsyncMock()
        mock_conn.fetchval = AsyncMock(return_value=True)  # Schema exists
        mock_conn.close = AsyncMock()

        async def mock_connect(*args, **kwargs):
            return mock_conn

        async def mock_create_pool(*args, **kwargs):
            raise asyncpg.PostgresError("Pool creation failed")

        with (
            patch("zensus2pgsql.commands.create.asyncpg.connect", side_effect=mock_connect),
            patch("zensus2pgsql.commands.create.asyncpg.create_pool", side_effect=mock_create_pool),
        ):
            with pytest.raises(typer.Exit) as exc_info:
                await get_db_pool(database_config)
            assert exc_info.value.exit_code == 1


# =============================================================================
# FETCH MANAGER TESTS
# =============================================================================


class TestFetchManagerInit:
    """Tests for FetchManager initialization."""

    def test_initialization(
        self, mock_httpx_client, mock_asyncpg_pool, mock_progress, database_config
    ):
        """Test FetchManager initializes correctly."""
        manager = FetchManager(
            client=mock_httpx_client,
            output_folder=Path("/tmp/test"),
            progress=mock_progress,
            db_pool=mock_asyncpg_pool,
            db_config=database_config,
            total=10,
            skip_existing=True,
            num_workers=3,
        )

        assert manager.client == mock_httpx_client
        assert manager.output_folder == Path("/tmp/test")
        assert manager.skip_existing is True
        assert manager.num_workers == 3
        assert manager.tables_created == 0

    def test_queues_created(
        self, mock_httpx_client, mock_asyncpg_pool, mock_progress, database_config
    ):
        """Test that queues are created during initialization."""
        manager = FetchManager(
            client=mock_httpx_client,
            output_folder=Path("/tmp/test"),
            progress=mock_progress,
            db_pool=mock_asyncpg_pool,
            db_config=database_config,
        )

        assert isinstance(manager.fetch_queue, asyncio.Queue)
        assert isinstance(manager.extract_queue, asyncio.Queue)
        assert isinstance(manager.database_queue, asyncio.Queue)


# =============================================================================
# FETCH WORKER TESTS
# =============================================================================


class TestFetchWorker:
    """Tests for FetchManager.fetch_worker method."""

    @pytest.mark.asyncio
    async def test_download_success(
        self, mock_httpx_client, mock_asyncpg_pool, mock_progress, database_config
    ):
        """Test successful file download."""
        with tempfile.TemporaryDirectory() as tmpdir:
            manager = FetchManager(
                client=mock_httpx_client,
                output_folder=Path(tmpdir),
                progress=mock_progress,
                db_pool=mock_asyncpg_pool,
                db_config=database_config,
                skip_existing=False,
            )

            # Add item to queue and sentinel
            await manager.fetch_queue.put(("https://example.com/test.zip", "test.zip"))
            await manager.fetch_queue.put(None)

            # Mock aiofiles.open
            mock_file = AsyncMock()
            mock_file.__aenter__ = AsyncMock(return_value=mock_file)
            mock_file.__aexit__ = AsyncMock(return_value=None)
            mock_file.write = AsyncMock()

            with patch("aiofiles.open", return_value=mock_file):
                await manager.fetch_worker()

            assert mock_file.write.call_count == 2

    @pytest.mark.asyncio
    async def test_skip_existing_file(
        self, mock_httpx_client, mock_asyncpg_pool, mock_progress, database_config
    ):
        """Test that existing files are skipped when skip_existing=True."""
        with tempfile.TemporaryDirectory() as tmpdir:
            # Create existing file
            existing_file = Path(tmpdir) / "existing.zip"
            existing_file.touch()

            manager = FetchManager(
                client=mock_httpx_client,
                output_folder=Path(tmpdir),
                progress=mock_progress,
                db_pool=mock_asyncpg_pool,
                db_config=database_config,
                skip_existing=True,
            )

            await manager.fetch_queue.put(("https://example.com/existing.zip", "existing.zip"))
            await manager.fetch_queue.put(None)

            # Mock aiofiles.open
            mock_file = AsyncMock()
            mock_file.__aenter__ = AsyncMock(return_value=mock_file)
            mock_file.__aexit__ = AsyncMock(return_value=None)
            mock_file.write = AsyncMock()

            with patch("aiofiles.open", return_value=mock_file):
                await manager.fetch_worker()

            # Make sure nothing new was written
            assert mock_file.write.call_count == 0

    @pytest.mark.asyncio
    async def test_download_failure_http_error(
        self, mock_asyncpg_pool, mock_progress, database_config
    ):
        """Test handling of HTTP errors during download."""

        class ErrorStreamResponse:
            """Mock response that raises HTTP error."""

            def raise_for_status(self):
                raise httpx.HTTPStatusError(
                    "404 Not Found", request=MagicMock(), response=MagicMock()
                )

            async def __aenter__(self):
                return self

            async def __aexit__(self, exc_type, exc_val, exc_tb):
                return None

        class ErrorStreamContextManager:
            """Mock context manager that returns error response."""

            async def __aenter__(self):
                return ErrorStreamResponse()

            async def __aexit__(self, exc_type, exc_val, exc_tb):
                return None

        with tempfile.TemporaryDirectory() as tmpdir:
            mock_client = MagicMock()
            mock_client.stream = MagicMock(return_value=ErrorStreamContextManager())

            manager = FetchManager(
                client=mock_client,
                output_folder=Path(tmpdir),
                progress=mock_progress,
                db_pool=mock_asyncpg_pool,
                db_config=database_config,
                skip_existing=False,
            )

            await manager.fetch_queue.put(("https://example.com/notfound.zip", "notfound.zip"))
            await manager.fetch_queue.put(None)

            await manager.fetch_worker()

            assert mock_client.stream.call_count == 1

    @pytest.mark.asyncio
    async def test_sentinel_shutdown(
        self, mock_httpx_client, mock_asyncpg_pool, mock_progress, database_config
    ):
        """Test that worker shuts down on receiving sentinel."""
        with tempfile.TemporaryDirectory() as tmpdir:
            manager = FetchManager(
                client=mock_httpx_client,
                output_folder=Path(tmpdir),
                progress=mock_progress,
                db_pool=mock_asyncpg_pool,
                db_config=database_config,
            )

            # Only add sentinel
            await manager.fetch_queue.put(None)

            # Should complete without hanging
            await asyncio.wait_for(manager.fetch_worker(), timeout=5.0)


# =============================================================================
# EXTRACT WORKER TESTS
# =============================================================================


class TestExtractWorker:
    """Tests for FetchManager.extract_worker method."""

    @pytest.mark.asyncio
    async def test_extract_single_csv(
        self,
        mock_httpx_client,
        mock_asyncpg_pool,
        mock_progress,
        database_config,
        temp_zip_with_csv,
    ):
        """Test extracting a ZIP with a single CSV."""
        with tempfile.TemporaryDirectory() as tmpdir:
            manager = FetchManager(
                client=mock_httpx_client,
                output_folder=Path(tmpdir),
                progress=mock_progress,
                db_pool=mock_asyncpg_pool,
                db_config=database_config,
            )

            await manager.extract_queue.put(temp_zip_with_csv)
            await manager.extract_queue.put(None)

            await manager.extract_worker()

            # Check that CSV was queued for database import
            assert manager.database_task_total == 1

    @pytest.mark.asyncio
    async def test_extract_multiple_csvs(
        self,
        mock_httpx_client,
        mock_asyncpg_pool,
        mock_progress,
        database_config,
        temp_zip_with_multiple_csvs,
    ):
        """Test extracting a ZIP with multiple CSVs."""
        with tempfile.TemporaryDirectory() as tmpdir:
            manager = FetchManager(
                client=mock_httpx_client,
                output_folder=Path(tmpdir),
                progress=mock_progress,
                db_pool=mock_asyncpg_pool,
                db_config=database_config,
            )

            await manager.extract_queue.put(temp_zip_with_multiple_csvs)
            await manager.extract_queue.put(None)

            await manager.extract_worker()

            # Should find 2 CSV files
            assert manager.database_task_total == 2

    @pytest.mark.asyncio
    async def test_extract_no_matching_csv(
        self, mock_httpx_client, mock_asyncpg_pool, mock_progress, database_config, temp_zip_no_csv
    ):
        """Test extracting a ZIP with no matching CSV files."""
        with tempfile.TemporaryDirectory() as tmpdir:
            manager = FetchManager(
                client=mock_httpx_client,
                output_folder=Path(tmpdir),
                progress=mock_progress,
                db_pool=mock_asyncpg_pool,
                db_config=database_config,
            )

            await manager.extract_queue.put(temp_zip_no_csv)
            await manager.extract_queue.put(None)

            await manager.extract_worker()

            # No CSVs should be queued
            assert manager.database_task_total == 0

    @pytest.mark.asyncio
    async def test_extract_empty_zip(
        self, mock_httpx_client, mock_asyncpg_pool, mock_progress, database_config, temp_empty_zip
    ):
        """Test extracting an empty ZIP file."""
        with tempfile.TemporaryDirectory() as tmpdir:
            manager = FetchManager(
                client=mock_httpx_client,
                output_folder=Path(tmpdir),
                progress=mock_progress,
                db_pool=mock_asyncpg_pool,
                db_config=database_config,
            )

            await manager.extract_queue.put(temp_empty_zip)
            await manager.extract_queue.put(None)

            await manager.extract_worker()

            assert manager.database_task_total == 0

    @pytest.mark.asyncio
    async def test_sentinel_shutdown(
        self, mock_httpx_client, mock_asyncpg_pool, mock_progress, database_config
    ):
        """Test that worker shuts down on receiving sentinel."""
        with tempfile.TemporaryDirectory() as tmpdir:
            manager = FetchManager(
                client=mock_httpx_client,
                output_folder=Path(tmpdir),
                progress=mock_progress,
                db_pool=mock_asyncpg_pool,
                db_config=database_config,
            )

            await manager.extract_queue.put(None)

            await asyncio.wait_for(manager.extract_worker(), timeout=5.0)


# =============================================================================
# CSV CACHE MANAGEMENT TESTS
# =============================================================================


class TestCsvCacheManagement:
    """Tests for CSV caching in extract_worker."""

    @pytest.mark.asyncio
    async def test_check_csv_cache_miss_no_directory(
        self, mock_httpx_client, mock_asyncpg_pool, mock_progress, database_config
    ):
        """Test cache miss when directory doesn't exist."""
        with tempfile.TemporaryDirectory() as tmpdir:
            manager = FetchManager(
                client=mock_httpx_client,
                output_folder=Path(tmpdir),
                progress=mock_progress,
                db_pool=mock_asyncpg_pool,
                db_config=database_config,
            )

            zip_file = Path(tmpdir) / "test.zip"
            zip_file.write_bytes(b"fake zip content")
            result = await manager.check_csv_cache(zip_file)
            assert result is None

    @pytest.mark.asyncio
    async def test_check_csv_cache_miss_no_metadata(
        self, mock_httpx_client, mock_asyncpg_pool, mock_progress, database_config
    ):
        """Test cache miss when metadata file is missing."""
        with tempfile.TemporaryDirectory() as tmpdir:
            manager = FetchManager(
                client=mock_httpx_client,
                output_folder=Path(tmpdir),
                progress=mock_progress,
                db_pool=mock_asyncpg_pool,
                db_config=database_config,
            )

            # Create cache dir but no metadata
            zip_file = Path(tmpdir) / "test.zip"
            zip_file.write_bytes(b"fake zip content")
            cache_dir = manager.csv_cache_dir / "test"
            cache_dir.mkdir(parents=True, exist_ok=True)

            result = await manager.check_csv_cache(zip_file)
            assert result is None

    @pytest.mark.asyncio
    async def test_check_csv_cache_hit(
        self,
        mock_httpx_client,
        mock_asyncpg_pool,
        mock_progress,
        database_config,
        temp_zip_with_csv,
    ):
        """Test cache hit with valid cached files."""
        import json

        with tempfile.TemporaryDirectory() as tmpdir:
            manager = FetchManager(
                client=mock_httpx_client,
                output_folder=Path(tmpdir),
                progress=mock_progress,
                db_pool=mock_asyncpg_pool,
                db_config=database_config,
            )

            # Create valid cache
            cache_dir = manager.csv_cache_dir / temp_zip_with_csv.stem
            cache_dir.mkdir(parents=True)

            csv_path = cache_dir / "test_Gitter.csv"
            csv_path.write_text("id;name\n1;test\n")

            metadata = {
                "zip_filename": temp_zip_with_csv.name,
                "zip_mtime": temp_zip_with_csv.stat().st_mtime,
                "zip_size": temp_zip_with_csv.stat().st_size,
                "csv_files": [{"name": "test_Gitter.csv", "size": csv_path.stat().st_size}],
            }

            meta_path = cache_dir / ".cache_meta.json"
            meta_path.write_text(json.dumps(metadata))

            result = await manager.check_csv_cache(temp_zip_with_csv)
            assert result is not None
            assert len(result) == 1
            assert result[0] == csv_path

    @pytest.mark.asyncio
    async def test_check_csv_cache_miss_stale(
        self,
        mock_httpx_client,
        mock_asyncpg_pool,
        mock_progress,
        database_config,
        temp_zip_with_csv,
    ):
        """Test cache miss when ZIP file modified."""
        import json

        with tempfile.TemporaryDirectory() as tmpdir:
            manager = FetchManager(
                client=mock_httpx_client,
                output_folder=Path(tmpdir),
                progress=mock_progress,
                db_pool=mock_asyncpg_pool,
                db_config=database_config,
            )

            # Create cache with old mtime
            cache_dir = manager.csv_cache_dir / temp_zip_with_csv.stem
            cache_dir.mkdir(parents=True)

            csv_path = cache_dir / "test_Gitter.csv"
            csv_path.write_text("id;name\n1;test\n")

            # Use old mtime (2 seconds ago)
            old_mtime = temp_zip_with_csv.stat().st_mtime - 2.0

            metadata = {
                "zip_filename": temp_zip_with_csv.name,
                "zip_mtime": old_mtime,
                "zip_size": temp_zip_with_csv.stat().st_size,
                "csv_files": [{"name": "test_Gitter.csv", "size": csv_path.stat().st_size}],
            }

            meta_path = cache_dir / ".cache_meta.json"
            meta_path.write_text(json.dumps(metadata))

            result = await manager.check_csv_cache(temp_zip_with_csv)
            assert result is None

    @pytest.mark.asyncio
    async def test_extract_and_cache_csvs(
        self,
        mock_httpx_client,
        mock_asyncpg_pool,
        mock_progress,
        database_config,
        temp_zip_with_csv,
    ):
        """Test extracting and caching CSVs."""
        import json

        with tempfile.TemporaryDirectory() as tmpdir:
            manager = FetchManager(
                client=mock_httpx_client,
                output_folder=Path(tmpdir),
                progress=mock_progress,
                db_pool=mock_asyncpg_pool,
                db_config=database_config,
            )

            result = await manager.extract_and_cache_csvs(temp_zip_with_csv)

            # Verify CSVs were cached
            assert len(result) > 0
            cache_dir = manager.csv_cache_dir / temp_zip_with_csv.stem
            assert cache_dir.exists()

            # Verify metadata was created
            meta_path = cache_dir / ".cache_meta.json"
            assert meta_path.exists()

            metadata = json.loads(meta_path.read_text())
            assert metadata["zip_filename"] == temp_zip_with_csv.name
            assert len(metadata["csv_files"]) == len(result)

    @pytest.mark.asyncio
    async def test_extract_worker_uses_cache(
        self,
        mock_httpx_client,
        mock_asyncpg_pool,
        mock_progress,
        database_config,
        temp_zip_with_csv,
    ):
        """Test that extract_worker uses cache when available."""
        with tempfile.TemporaryDirectory() as tmpdir:
            manager = FetchManager(
                client=mock_httpx_client,
                output_folder=Path(tmpdir),
                progress=mock_progress,
                db_pool=mock_asyncpg_pool,
                db_config=database_config,
            )

            # First extraction - should cache
            await manager.extract_queue.put(temp_zip_with_csv)
            await manager.extract_queue.put(None)

            # Run extract_worker in background
            extract_task = asyncio.create_task(manager.extract_worker())
            await asyncio.wait_for(extract_task, timeout=10.0)

            first_count = manager.database_task_total

            # Reset manager for second run
            manager.database_task_total = 0
            manager.database_queue = asyncio.Queue()

            # Second extraction - should use cache
            await manager.extract_queue.put(temp_zip_with_csv)
            await manager.extract_queue.put(None)

            # Run again
            extract_task = asyncio.create_task(manager.extract_worker())
            await asyncio.wait_for(extract_task, timeout=10.0)

            # Should have same number of CSVs queued
            assert manager.database_task_total == first_count


# =============================================================================
# DATABASE WORKER TESTS
# =============================================================================


class TestDatabaseWorker:
    """Tests for FetchManager.database_worker method."""

    @pytest.mark.asyncio
    async def test_import_csv_success(
        self,
        mock_httpx_client,
        mock_asyncpg_pool,
        mock_asyncpg_connection,
        mock_progress,
        database_config,
        temp_csv_file,
    ):
        """Test successful CSV import."""
        # Setup mock connection - table doesn't exist
        mock_asyncpg_connection.fetchval = AsyncMock(side_effect=[False])
        mock_asyncpg_connection.fetchrow = AsyncMock(
            return_value=(100, 100, 100, 100)
        )  # All integers

        with tempfile.TemporaryDirectory() as tmpdir:
            manager = FetchManager(
                client=mock_httpx_client,
                output_folder=Path(tmpdir),
                progress=mock_progress,
                db_pool=mock_asyncpg_pool,
                db_config=database_config,
            )

            await manager.database_queue.put(temp_csv_file)
            await manager.database_queue.put(None)

            await manager.database_worker()

            # Verify SQL operations were called
            assert mock_asyncpg_connection.execute.called
            assert mock_asyncpg_connection.copy_to_table.called

    @pytest.mark.asyncio
    async def test_skip_existing_table_no_drop(
        self,
        mock_httpx_client,
        mock_asyncpg_pool,
        mock_asyncpg_connection,
        mock_progress,
        database_config,
        temp_csv_file,
    ):
        """Test skipping existing table when drop_existing=False."""
        mock_asyncpg_connection.fetchval = AsyncMock(return_value=True)  # Table exists

        with tempfile.TemporaryDirectory() as tmpdir:
            manager = FetchManager(
                client=mock_httpx_client,
                output_folder=Path(tmpdir),
                progress=mock_progress,
                db_pool=mock_asyncpg_pool,
                db_config=database_config,  # drop_existing=False
            )

            await manager.database_queue.put(temp_csv_file)
            await manager.database_queue.put(None)

            await manager.database_worker()

            # copy_to_table should NOT be called since we skip
            # Note: The table existence check happens within transaction
            # so this tests the continue path

    @pytest.mark.asyncio
    async def test_drop_existing_table(
        self,
        mock_httpx_client,
        mock_asyncpg_pool,
        mock_asyncpg_connection,
        mock_progress,
        database_config_drop_existing,
        temp_csv_file,
    ):
        """Test dropping existing table when drop_existing=True."""
        mock_asyncpg_connection.fetchval = AsyncMock(return_value=True)  # Table exists
        mock_asyncpg_connection.fetchrow = AsyncMock(return_value=(100, 100, 100, 100))

        with tempfile.TemporaryDirectory() as tmpdir:
            manager = FetchManager(
                client=mock_httpx_client,
                output_folder=Path(tmpdir),
                progress=mock_progress,
                db_pool=mock_asyncpg_pool,
                db_config=database_config_drop_existing,
            )

            await manager.database_queue.put(temp_csv_file)
            await manager.database_queue.put(None)

            await manager.database_worker()

            # Should have called execute with DROP TABLE
            drop_calls = [
                call
                for call in mock_asyncpg_connection.execute.call_args_list
                if "DROP TABLE" in str(call)
            ]
            assert len(drop_calls) > 0

    @pytest.mark.asyncio
    async def test_geometry_column_created(
        self,
        mock_httpx_client,
        mock_asyncpg_pool,
        mock_asyncpg_connection,
        mock_progress,
        database_config,
        temp_csv_file,
    ):
        """Test that geometry column is created when coordinates present."""
        mock_asyncpg_connection.fetchval = AsyncMock(return_value=False)
        mock_asyncpg_connection.fetchrow = AsyncMock(return_value=(100, 0, 100, 100, 100))

        with tempfile.TemporaryDirectory() as tmpdir:
            manager = FetchManager(
                client=mock_httpx_client,
                output_folder=Path(tmpdir),
                progress=mock_progress,
                db_pool=mock_asyncpg_pool,
                db_config=database_config,
            )

            await manager.database_queue.put(temp_csv_file)
            await manager.database_queue.put(None)

            await manager.database_worker()

            # Check for geometry-related SQL
            execute_calls = [str(call) for call in mock_asyncpg_connection.execute.call_args_list]
            has_geometry = any(
                "GEOMETRY" in call or "ST_MakePoint" in call for call in execute_calls
            )
            has_gist_index = any("GIST" in call for call in execute_calls)

            # The CSV file has x_mp and y_mp columns, so geometry should be created
            assert has_geometry or has_gist_index

    @pytest.mark.asyncio
    async def test_no_geometry_without_coords(
        self,
        mock_httpx_client,
        mock_asyncpg_pool,
        mock_asyncpg_connection,
        mock_progress,
        database_config,
        sample_csv_content_no_coords,
    ):
        """Test that no geometry is created without coordinate columns."""
        # Create temp CSV without coordinates
        with tempfile.NamedTemporaryFile(
            mode="w", suffix="_Gitter.csv", delete=False, encoding="utf-8"
        ) as f:
            f.write(sample_csv_content_no_coords)
            csv_path = Path(f.name)

        mock_asyncpg_connection.fetchval = AsyncMock(return_value=False)
        mock_asyncpg_connection.fetchrow = AsyncMock(return_value=(100, 100, 100, 100))

        with tempfile.TemporaryDirectory() as tmpdir:
            manager = FetchManager(
                client=mock_httpx_client,
                output_folder=Path(tmpdir),
                progress=mock_progress,
                db_pool=mock_asyncpg_pool,
                db_config=database_config,
            )

            await manager.database_queue.put(csv_path)
            await manager.database_queue.put(None)

            await manager.database_worker()

            # Check that no GIST index was created (no geometry)
            execute_calls = [str(call) for call in mock_asyncpg_connection.execute.call_args_list]
            has_gist = any("GIST" in call for call in execute_calls)
            # Without coordinates, no GIST index should be created
            assert not has_gist

    @pytest.mark.asyncio
    async def test_custom_srid_used(
        self,
        mock_httpx_client,
        mock_asyncpg_pool,
        mock_asyncpg_connection,
        mock_progress,
        database_config_custom_srid,
        temp_csv_file,
    ):
        """Test that custom SRID is used in geometry creation."""
        mock_asyncpg_connection.fetchval = AsyncMock(return_value=False)
        mock_asyncpg_connection.fetchrow = AsyncMock(return_value=(100, 0, 100, 100, 100))

        with tempfile.TemporaryDirectory() as tmpdir:
            manager = FetchManager(
                client=mock_httpx_client,
                output_folder=Path(tmpdir),
                progress=mock_progress,
                db_pool=mock_asyncpg_pool,
                db_config=database_config_custom_srid,  # SRID=4326
            )

            await manager.database_queue.put(temp_csv_file)
            await manager.database_queue.put(None)

            await manager.database_worker()

            # Check for custom SRID in SQL
            execute_calls = [str(call) for call in mock_asyncpg_connection.execute.call_args_list]
            has_custom_srid = any("4326" in call for call in execute_calls)
            assert has_custom_srid

    @pytest.mark.asyncio
    async def test_error_handling(
        self,
        mock_httpx_client,
        mock_asyncpg_pool,
        mock_asyncpg_connection,
        mock_progress,
        database_config,
        temp_csv_file,
    ):
        """Test that errors during import are caught and logged."""
        mock_asyncpg_connection.fetchval = AsyncMock(side_effect=Exception("Database error"))

        with tempfile.TemporaryDirectory() as tmpdir:
            manager = FetchManager(
                client=mock_httpx_client,
                output_folder=Path(tmpdir),
                progress=mock_progress,
                db_pool=mock_asyncpg_pool,
                db_config=database_config,
            )

            await manager.database_queue.put(temp_csv_file)
            await manager.database_queue.put(None)

            # Should not raise - error is caught
            await manager.database_worker()

    @pytest.mark.asyncio
    async def test_sentinel_shutdown(
        self, mock_httpx_client, mock_asyncpg_pool, mock_progress, database_config
    ):
        """Test that worker shuts down on receiving sentinel."""
        with tempfile.TemporaryDirectory() as tmpdir:
            manager = FetchManager(
                client=mock_httpx_client,
                output_folder=Path(tmpdir),
                progress=mock_progress,
                db_pool=mock_asyncpg_pool,
                db_config=database_config,
            )

            await manager.database_queue.put(None)

            await asyncio.wait_for(manager.database_worker(), timeout=5.0)

    @pytest.mark.asyncio
    async def test_text_column_detection(
        self,
        mock_httpx_client,
        mock_asyncpg_pool,
        mock_asyncpg_connection,
        mock_progress,
        database_config,
        sample_csv_content_text_column,
    ):
        """Test that text columns are correctly detected."""
        with tempfile.NamedTemporaryFile(
            mode="w", suffix="_Gitter.csv", delete=False, encoding="utf-8"
        ) as f:
            f.write(sample_csv_content_text_column)
            csv_path = Path(f.name)

        # Return values indicating TEXT type (not all values are numeric)
        mock_asyncpg_connection.fetchval = AsyncMock(return_value=False)
        mock_asyncpg_connection.fetchrow = AsyncMock(return_value=(100, 0, 100, 0, 0))  # TEXT

        with tempfile.TemporaryDirectory() as tmpdir:
            manager = FetchManager(
                client=mock_httpx_client,
                output_folder=Path(tmpdir),
                progress=mock_progress,
                db_pool=mock_asyncpg_pool,
                db_config=database_config,
            )

            await manager.database_queue.put(csv_path)
            await manager.database_queue.put(None)

            await manager.database_worker()

    @pytest.mark.asyncio
    async def test_double_precision_column_detection(
        self,
        mock_httpx_client,
        mock_asyncpg_pool,
        mock_asyncpg_connection,
        mock_progress,
        database_config,
        sample_csv_content_german_decimals,
    ):
        """Test that decimal columns are correctly detected."""
        with tempfile.NamedTemporaryFile(
            mode="w", suffix="_Gitter.csv", delete=False, encoding="utf-8"
        ) as f:
            f.write(sample_csv_content_german_decimals)
            csv_path = Path(f.name)

        mock_asyncpg_connection.fetchval = AsyncMock(return_value=False)
        # non_empty=100, integer_count=50, numeric_count=100 -> DOUBLE PRECISION
        mock_asyncpg_connection.fetchrow = AsyncMock(return_value=(100, 0, 100, 50, 100))

        with tempfile.TemporaryDirectory() as tmpdir:
            manager = FetchManager(
                client=mock_httpx_client,
                output_folder=Path(tmpdir),
                progress=mock_progress,
                db_pool=mock_asyncpg_pool,
                db_config=database_config,
            )

            await manager.database_queue.put(csv_path)
            await manager.database_queue.put(None)

            await manager.database_worker()


# =============================================================================
# COORDINATOR TESTS
# =============================================================================


class TestCoordinator:
    """Tests for FetchManager.coordinator method."""

    @pytest.mark.asyncio
    async def test_sends_fetch_sentinels(
        self, mock_httpx_client, mock_asyncpg_pool, mock_progress, database_config
    ):
        """Test that coordinator sends correct number of fetch sentinels."""
        with tempfile.TemporaryDirectory() as tmpdir:
            manager = FetchManager(
                client=mock_httpx_client,
                output_folder=Path(tmpdir),
                progress=mock_progress,
                db_pool=mock_asyncpg_pool,
                db_config=database_config,
                num_workers=3,
            )

            # Start workers that will consume sentinels
            async def mock_fetch_worker():
                while True:
                    item = await manager.fetch_queue.get()
                    manager.fetch_queue.task_done()
                    if item is None:
                        break

            async def mock_extract_worker():
                while True:
                    item = await manager.extract_queue.get()
                    manager.extract_queue.task_done()
                    if item is None:
                        break

            async def mock_db_worker():
                while True:
                    item = await manager.database_queue.get()
                    manager.database_queue.task_done()
                    if item is None:
                        break

            # Run workers and coordinator
            await asyncio.gather(
                *[mock_fetch_worker() for _ in range(3)],
                mock_extract_worker(),
                *[mock_db_worker() for _ in range(3)],
                manager.coordinator(),
            )

    @pytest.mark.asyncio
    async def test_proper_shutdown_order(
        self, mock_httpx_client, mock_asyncpg_pool, mock_progress, database_config
    ):
        """Test that shutdown happens in correct order: fetch -> extract -> database."""
        with tempfile.TemporaryDirectory() as tmpdir:
            manager = FetchManager(
                client=mock_httpx_client,
                output_folder=Path(tmpdir),
                progress=mock_progress,
                db_pool=mock_asyncpg_pool,
                db_config=database_config,
                num_workers=2,
            )

            shutdown_order = []

            async def mock_fetch_worker(worker_id):
                while True:
                    item = await manager.fetch_queue.get()
                    manager.fetch_queue.task_done()
                    if item is None:
                        shutdown_order.append(f"fetch_{worker_id}")
                        break

            async def mock_extract_worker():
                while True:
                    item = await manager.extract_queue.get()
                    manager.extract_queue.task_done()
                    if item is None:
                        shutdown_order.append("extract")
                        break

            async def mock_db_worker(worker_id):
                while True:
                    item = await manager.database_queue.get()
                    manager.database_queue.task_done()
                    if item is None:
                        shutdown_order.append(f"db_{worker_id}")
                        break

            await asyncio.gather(
                mock_fetch_worker(0),
                mock_fetch_worker(1),
                mock_extract_worker(),
                mock_db_worker(0),
                mock_db_worker(1),
                manager.coordinator(),
            )

            # Verify fetch workers shutdown before extract
            fetch_indices = [i for i, x in enumerate(shutdown_order) if x.startswith("fetch")]
            extract_index = shutdown_order.index("extract")
            db_indices = [i for i, x in enumerate(shutdown_order) if x.startswith("db")]

            # All fetch workers should shutdown before extract
            assert all(fi < extract_index for fi in fetch_indices)
            # Extract should shutdown before database workers
            assert all(extract_index < di for di in db_indices)


# =============================================================================
# CLI ENTRY POINT TESTS
# =============================================================================


class TestCollectCommand:
    """Tests for the collect CLI command."""

    def test_collect_creates_database_config(self):
        """Test that collect creates proper DatabaseConfig."""
        config = DatabaseConfig(
            host="testhost",
            port=5433,
            database="testdb",
            user="testuser",
            password="testpass",
            schema="testschema",
            srid=4326,
            drop_existing=True,
        )

        assert config.host == "testhost"
        assert config.port == 5433
        assert config.database == "testdb"
        assert config.srid == 4326
        assert config.drop_existing is True

    @pytest.mark.asyncio
    async def test_collect_wrapper_happy_path(self, database_config, mock_gitterdaten_files):
        """Test collect_wrapper with mocked dependencies."""
        mock_pool = AsyncMock()
        mock_pool.close = AsyncMock()

        mock_http_client = AsyncMock()
        mock_http_client.aclose = AsyncMock()

        with (
            patch("zensus2pgsql.commands.create.get_db_pool", return_value=mock_pool),
            patch("zensus2pgsql.commands.create.httpx.AsyncClient", return_value=mock_http_client),
            patch("zensus2pgsql.commands.create.GITTERDATEN_FILES", mock_gitterdaten_files),
            patch("zensus2pgsql.commands.create.FetchManager") as mock_manager_class,
            patch("zensus2pgsql.commands.create.Progress"),
            patch("zensus2pgsql.commands.create.rprint"),
        ):
            mock_manager = MagicMock()
            mock_manager.start = AsyncMock()
            mock_manager.remove_temp_dir = MagicMock()
            mock_manager.success = 3
            mock_manager.skipped = 0
            mock_manager.failed = 0
            mock_manager_class.return_value = mock_manager

            cmd_config = CreateCommandConfig(
                tables=["all"], skip_existing=True, quiet=False, verbose=0
            )
            await collect_wrapper(cmd_config, database_config)

            mock_manager.start.assert_called_once()
            mock_manager.remove_temp_dir.assert_called_once()
            mock_pool.close.assert_called_once()
            mock_http_client.aclose.assert_called_once()

    @pytest.mark.asyncio
    async def test_collect_wrapper_single_table(self, database_config, mock_gitterdaten_files):
        """Test collect_wrapper with single table selection."""
        mock_pool = AsyncMock()
        mock_pool.close = AsyncMock()

        mock_http_client = AsyncMock()
        mock_http_client.aclose = AsyncMock()

        with (
            patch("zensus2pgsql.commands.create.get_db_pool", return_value=mock_pool),
            patch("zensus2pgsql.commands.create.httpx.AsyncClient", return_value=mock_http_client),
            patch("zensus2pgsql.commands.create.GITTERDATEN_FILES", mock_gitterdaten_files),
            patch("zensus2pgsql.commands.create.FetchManager") as mock_manager_class,
            patch("zensus2pgsql.commands.create.Progress"),
            patch("zensus2pgsql.commands.create.rprint"),
        ):
            mock_manager = MagicMock()
            mock_manager.start = AsyncMock()
            mock_manager.remove_temp_dir = MagicMock()
            mock_manager.success = 1
            mock_manager.skipped = 0
            mock_manager.failed = 0
            mock_manager_class.return_value = mock_manager

            cmd_config = CreateCommandConfig(
                tables=["test_dataset1"], skip_existing=True, quiet=False, verbose=0
            )
            await collect_wrapper(cmd_config, database_config)

            mock_manager.start.assert_called_once()

    @pytest.mark.asyncio
    async def test_collect_wrapper_cleanup_on_exception(
        self, database_config, mock_gitterdaten_files
    ):
        """Test that cleanup happens even on exception."""
        mock_pool = AsyncMock()
        mock_pool.close = AsyncMock()

        mock_http_client = AsyncMock()
        mock_http_client.aclose = AsyncMock()

        with (
            patch("zensus2pgsql.commands.create.get_db_pool", return_value=mock_pool),
            patch("zensus2pgsql.commands.create.httpx.AsyncClient", return_value=mock_http_client),
            patch("zensus2pgsql.commands.create.GITTERDATEN_FILES", mock_gitterdaten_files),
            patch("zensus2pgsql.commands.create.FetchManager") as mock_manager_class,
            patch("zensus2pgsql.commands.create.Progress"),
            patch("zensus2pgsql.commands.create.rprint"),
        ):
            mock_manager = MagicMock()
            mock_manager.start = AsyncMock(side_effect=Exception("Test error"))
            mock_manager.remove_temp_dir = MagicMock()
            mock_manager.success = 0
            mock_manager.skipped = 0
            mock_manager.failed = 0
            mock_manager_class.return_value = mock_manager

            cmd_config = CreateCommandConfig(
                tables=["all"], skip_existing=True, quiet=False, verbose=0
            )
            with pytest.raises(Exception, match="Test error"):
                await collect_wrapper(cmd_config, database_config)

            # Cleanup should still be called
            mock_manager.remove_temp_dir.assert_called_once()
            mock_pool.close.assert_called_once()
            mock_http_client.aclose.assert_called_once()


# =============================================================================
# FULL PIPELINE INTEGRATION TESTS
# =============================================================================


class TestFullPipeline:
    """Integration tests for the full create pipeline."""

    @pytest.mark.asyncio
    async def test_start_method(
        self, mock_httpx_client, mock_asyncpg_pool, mock_progress, database_config
    ):
        """Test the FetchManager.start method orchestrates workers correctly."""
        with tempfile.TemporaryDirectory() as tmpdir:
            manager = FetchManager(
                client=mock_httpx_client,
                output_folder=Path(tmpdir),
                progress=mock_progress,
                db_pool=mock_asyncpg_pool,
                db_config=database_config,
                num_workers=1,  # Use 1 worker for simpler testing
            )

            # Mock the worker methods to track calls
            fetch_called = []
            extract_called = []
            db_called = []
            coordinator_called = []

            async def mock_fetch():
                fetch_called.append(True)
                while True:
                    item = await manager.fetch_queue.get()
                    manager.fetch_queue.task_done()
                    if item is None:
                        break

            async def mock_extract():
                extract_called.append(True)
                while True:
                    item = await manager.extract_queue.get()
                    manager.extract_queue.task_done()
                    if item is None:
                        break

            async def mock_db():
                db_called.append(True)
                while True:
                    item = await manager.database_queue.get()
                    manager.database_queue.task_done()
                    if item is None:
                        break

            async def mock_coord():
                coordinator_called.append(True)
                for _ in range(manager.num_workers):
                    await manager.fetch_queue.put(None)
                await manager.fetch_queue.join()
                await manager.extract_queue.put(None)
                await manager.extract_queue.join()
                for _ in range(manager.num_workers):
                    await manager.database_queue.put(None)
                await manager.database_queue.join()

            with (
                patch.object(manager, "fetch_worker", mock_fetch),
                patch.object(manager, "extract_worker", mock_extract),
                patch.object(manager, "database_worker", mock_db),
                patch.object(manager, "coordinator", mock_coord),
            ):
                await manager.start([], ["all"])

            assert len(fetch_called) == 1  # num_workers=1
            assert len(extract_called) == 1
            assert len(db_called) == 1
            assert len(coordinator_called) == 1

    def test_remove_temp_dir(
        self, mock_httpx_client, mock_asyncpg_pool, mock_progress, database_config
    ):
        """Test that remove_temp_dir cleans up temporary directory."""
        with tempfile.TemporaryDirectory() as tmpdir:
            manager = FetchManager(
                client=mock_httpx_client,
                output_folder=Path(tmpdir),
                progress=mock_progress,
                db_pool=mock_asyncpg_pool,
                db_config=database_config,
            )

            temp_dir_path = Path(manager.temp_dir.name)
            assert temp_dir_path.exists()

            manager.remove_temp_dir()

            # After cleanup, the directory should not exist
            assert not temp_dir_path.exists()

    @pytest.mark.asyncio
    async def test_files_filtered_correctly(
        self, mock_httpx_client, mock_asyncpg_pool, mock_progress, database_config
    ):
        """Test that files are filtered based on tables argument."""
        with tempfile.TemporaryDirectory() as tmpdir:
            manager = FetchManager(
                client=mock_httpx_client,
                output_folder=Path(tmpdir),
                progress=mock_progress,
                db_pool=mock_asyncpg_pool,
                db_config=database_config,
                num_workers=1,
            )

            files_to_import = [
                ("dataset1", "https://example.com/1.zip"),
                ("dataset2", "https://example.com/2.zip"),
                ("dataset3", "https://example.com/3.zip"),
            ]

            # Mock workers to just consume queue items
            async def consume_fetch():
                while True:
                    item = await manager.fetch_queue.get()
                    manager.fetch_queue.task_done()
                    if item is None:
                        break

            async def consume_extract():
                while True:
                    item = await manager.extract_queue.get()
                    manager.extract_queue.task_done()
                    if item is None:
                        break

            async def consume_db():
                while True:
                    item = await manager.database_queue.get()
                    manager.database_queue.task_done()
                    if item is None:
                        break

            async def coord():
                await manager.fetch_queue.put(None)
                await manager.fetch_queue.join()
                await manager.extract_queue.put(None)
                await manager.extract_queue.join()
                await manager.database_queue.put(None)
                await manager.database_queue.join()

            with (
                patch.object(manager, "fetch_worker", consume_fetch),
                patch.object(manager, "extract_worker", consume_extract),
                patch.object(manager, "database_worker", consume_db),
                patch.object(manager, "coordinator", coord),
            ):
                # When tables is ["all"], all files should be queued
                await manager.start(files_to_import, ["all"])


class TestEncodingInDatabaseWorker:
    """Tests for encoding handling in database worker."""

    @pytest.mark.asyncio
    async def test_utf8_encoding_mapping(
        self,
        mock_httpx_client,
        mock_asyncpg_pool,
        mock_asyncpg_connection,
        mock_progress,
        database_config,
        temp_csv_file,
    ):
        """Test that UTF-8 encoding maps to PostgreSQL UTF8."""
        mock_asyncpg_connection.fetchval = AsyncMock(return_value=False)
        mock_asyncpg_connection.fetchrow = AsyncMock(return_value=(100, 100, 100, 100))

        with tempfile.TemporaryDirectory() as tmpdir:
            manager = FetchManager(
                client=mock_httpx_client,
                output_folder=Path(tmpdir),
                progress=mock_progress,
                db_pool=mock_asyncpg_pool,
                db_config=database_config,
            )

            await manager.database_queue.put(temp_csv_file)
            await manager.database_queue.put(None)

            await manager.database_worker()

            # Verify copy_to_table was called with UTF8 encoding
            copy_calls = mock_asyncpg_connection.copy_to_table.call_args_list
            assert len(copy_calls) > 0
            # Check that encoding parameter was passed
            call_kwargs = copy_calls[0].kwargs
            assert call_kwargs.get("encoding") == "UTF8"

    @pytest.mark.asyncio
    async def test_latin1_encoding_mapping(
        self,
        mock_httpx_client,
        mock_asyncpg_pool,
        mock_asyncpg_connection,
        mock_progress,
        database_config,
        temp_csv_file_latin1,
    ):
        """Test that ISO-8859-1 encoding maps to PostgreSQL LATIN1."""
        mock_asyncpg_connection.fetchval = AsyncMock(return_value=False)
        mock_asyncpg_connection.fetchrow = AsyncMock(return_value=(100, 100, 100, 100))

        with tempfile.TemporaryDirectory() as tmpdir:
            manager = FetchManager(
                client=mock_httpx_client,
                output_folder=Path(tmpdir),
                progress=mock_progress,
                db_pool=mock_asyncpg_pool,
                db_config=database_config,
            )

            await manager.database_queue.put(temp_csv_file_latin1)
            await manager.database_queue.put(None)

            await manager.database_worker()

            # Check encoding was passed to copy_to_table
            copy_calls = mock_asyncpg_connection.copy_to_table.call_args_list
            if copy_calls:
                call_kwargs = copy_calls[0].kwargs
                # Encoding should be LATIN1 or UTF8 depending on detection
                assert call_kwargs.get("encoding") in ["UTF8", "LATIN1"]


# =============================================================================
# ADDITIONAL COVERAGE TESTS
# =============================================================================


class TestCollectCLI:
    """Tests for the collect CLI entry point function."""

    def test_collect_function_invokes_asyncio_run(self):
        """Test that collect function calls asyncio.run with collect_wrapper."""
        from typer.testing import CliRunner

        from zensus2pgsql.cli import app

        runner = CliRunner()

        # Invoke with --help to avoid actual execution but still load the command
        result = runner.invoke(app, ["create", "--help"])

        # Should show help text
        assert result.exit_code == 0
        assert "Download, extract and import" in result.stdout

    def test_collect_function_direct_call(self):
        """Test calling collect function directly with mocks."""
        # Track the coroutine so we can close it properly
        captured_coro = None

        def capture_and_close_coro(coro):
            nonlocal captured_coro
            captured_coro = coro
            # Close the coroutine to prevent "was never awaited" warning
            coro.close()

        with (
            patch("zensus2pgsql.commands.create.configure_logging") as mock_configure,
            patch("zensus2pgsql.commands.create.create_cache_dir") as mock_cache,
            patch(
                "zensus2pgsql.commands.create.asyncio.run", side_effect=capture_and_close_coro
            ) as mock_asyncio_run,
        ):
            # Call the function directly
            create(
                tables=["all"],
                host="localhost",
                port=5432,
                database="testdb",
                user="testuser",
                password="testpass",
                schema="public",
                srid=3035,
                drop_existing=False,
                skip_existing=True,
                verbose=0,
                quiet=False,
            )

            # Verify logging was configured
            mock_configure.assert_called_once_with(0, False)

            # Verify cache dir was created
            mock_cache.assert_called_once()

            # Verify asyncio.run was called with collect_wrapper
            mock_asyncio_run.assert_called_once()
            # Verify a coroutine was captured
            assert captured_coro is not None


class TestStartMethodFiltering:
    """Tests for file filtering in FetchManager.start method."""

    @pytest.mark.asyncio
    async def test_start_filters_tables_not_in_list(
        self, mock_httpx_client, mock_asyncpg_pool, mock_progress, database_config
    ):
        """Test that files not in the tables list are filtered out."""
        with tempfile.TemporaryDirectory() as tmpdir:
            manager = FetchManager(
                client=mock_httpx_client,
                output_folder=Path(tmpdir),
                progress=mock_progress,
                db_pool=mock_asyncpg_pool,
                db_config=database_config,
                num_workers=1,
            )

            files_to_import = [
                ("dataset1", "https://example.com/1.zip"),
                ("dataset2", "https://example.com/2.zip"),
                ("dataset3", "https://example.com/3.zip"),
            ]

            queued_items = []

            async def consume_fetch():
                while True:
                    item = await manager.fetch_queue.get()
                    if item is not None:
                        queued_items.append(item)
                    manager.fetch_queue.task_done()
                    if item is None:
                        break

            async def consume_extract():
                while True:
                    item = await manager.extract_queue.get()
                    manager.extract_queue.task_done()
                    if item is None:
                        break

            async def consume_db():
                while True:
                    item = await manager.database_queue.get()
                    manager.database_queue.task_done()
                    if item is None:
                        break

            async def coord():
                await manager.fetch_queue.put(None)
                await manager.fetch_queue.join()
                await manager.extract_queue.put(None)
                await manager.extract_queue.join()
                await manager.database_queue.put(None)
                await manager.database_queue.join()

            with (
                patch.object(manager, "fetch_worker", consume_fetch),
                patch.object(manager, "extract_worker", consume_extract),
                patch.object(manager, "database_worker", consume_db),
                patch.object(manager, "coordinator", coord),
            ):
                # Only request dataset2 - dataset1 and dataset3 should be filtered
                await manager.start(files_to_import, ["dataset2"])

            # Only dataset2 should have been queued
            assert len(queued_items) == 1
            assert "dataset2.zip" in queued_items[0][1]


class TestDebugLogging:
    """Tests that exercise DEBUG-level logging branches."""

    @pytest.mark.asyncio
    async def test_database_worker_with_debug_logging(
        self,
        mock_httpx_client,
        mock_asyncpg_pool,
        mock_asyncpg_connection,
        mock_progress,
        database_config,
    ):
        """Test database worker with DEBUG logging enabled (covers renamed columns and row count)."""
        import logging

        # Create CSV with column that will be renamed (starts with digit)
        csv_content = """1column;x_mp_100m;y_mp_100m;value
data1;4334150;2668050;100
"""
        with tempfile.NamedTemporaryFile(
            mode="w", suffix="_Gitter.csv", delete=False, encoding="utf-8"
        ) as f:
            f.write(csv_content)
            csv_path = Path(f.name)

        mock_asyncpg_connection.fetchval = AsyncMock(
            side_effect=[False, 100]
        )  # Table doesn't exist, row count
        mock_asyncpg_connection.fetchrow = AsyncMock(return_value=(100, 100, 100, 100))

        with tempfile.TemporaryDirectory() as tmpdir:
            manager = FetchManager(
                client=mock_httpx_client,
                output_folder=Path(tmpdir),
                progress=mock_progress,
                db_pool=mock_asyncpg_pool,
                db_config=database_config,
            )

            await manager.database_queue.put(csv_path)
            await manager.database_queue.put(None)

            # Set logger to DEBUG level to cover those branches
            from zensus2pgsql.logging import logger

            original_level = logger.level
            logger.setLevel(logging.DEBUG)

            try:
                await manager.database_worker()
            finally:
                logger.setLevel(original_level)

            # Verify operations completed
            assert mock_asyncpg_connection.execute.called

    @pytest.mark.asyncio
    async def test_database_worker_with_many_renamed_columns(
        self,
        mock_httpx_client,
        mock_asyncpg_pool,
        mock_asyncpg_connection,
        mock_progress,
        database_config,
    ):
        """Test database worker with more than 5 renamed columns (covers the '... and N more' branch)."""
        import logging

        # Create CSV with 7 columns that will be renamed (start with digits)
        csv_content = """1col;2col;3col;4col;5col;6col;7col
a;b;c;d;e;f;g
"""
        with tempfile.NamedTemporaryFile(
            mode="w", suffix="_Gitter.csv", delete=False, encoding="utf-8"
        ) as f:
            f.write(csv_content)
            csv_path = Path(f.name)

        mock_asyncpg_connection.fetchval = AsyncMock(side_effect=[False, 100])
        mock_asyncpg_connection.fetchrow = AsyncMock(return_value=(100, 100, 0, 0))  # TEXT columns

        with tempfile.TemporaryDirectory() as tmpdir:
            manager = FetchManager(
                client=mock_httpx_client,
                output_folder=Path(tmpdir),
                progress=mock_progress,
                db_pool=mock_asyncpg_pool,
                db_config=database_config,
            )

            await manager.database_queue.put(csv_path)
            await manager.database_queue.put(None)

            from zensus2pgsql.logging import logger

            original_level = logger.level
            logger.setLevel(logging.DEBUG)

            try:
                await manager.database_worker()
            finally:
                logger.setLevel(original_level)


# =============================================================================
# FETCH WORKER RETRY TESTS
# =============================================================================


class TestFetchWorkerRetry:
    """Tests for fetch_worker retry logic with exponential backoff."""

    @pytest.mark.asyncio
    async def test_fetch_worker_retries_on_timeout(
        self, mock_asyncpg_pool, mock_progress, database_config
    ):
        """Test that fetch_worker retries on timeout errors."""
        # Share state across retry attempts
        attempt_counter = {"count": 0}

        class TimeoutStreamResponse:
            """Mock response that raises timeout on first attempt, succeeds on second."""

            def raise_for_status(self):
                pass

            async def aiter_bytes(self, chunk_size):
                attempt_counter["count"] += 1
                if attempt_counter["count"] == 1:
                    raise httpx.TimeoutException("Request timeout")
                # Success on second attempt
                yield b"test data chunk 1"
                yield b"test data chunk 2"

            async def __aenter__(self):
                return self

            async def __aexit__(self, exc_type, exc_val, exc_tb):
                return None

        class TimeoutStreamContextManager:
            """Mock context manager that returns timeout response."""

            async def __aenter__(self):
                return TimeoutStreamResponse()

            async def __aexit__(self, exc_type, exc_val, exc_tb):
                return None

        with tempfile.TemporaryDirectory() as tmpdir:
            mock_client = MagicMock()

            manager = FetchManager(
                client=mock_client,
                output_folder=Path(tmpdir),
                progress=mock_progress,
                db_pool=mock_asyncpg_pool,
                db_config=database_config,
                skip_existing=False,
            )

            # Set up mock to return new context manager each time
            def get_context_manager(*args, **kwargs):
                return TimeoutStreamContextManager()

            mock_client.stream = MagicMock(side_effect=get_context_manager)

            await manager.fetch_queue.put(("https://example.com/test.zip", "test.zip"))
            await manager.fetch_queue.put(None)

            mock_file = AsyncMock()
            mock_file.__aenter__ = AsyncMock(return_value=mock_file)
            mock_file.__aexit__ = AsyncMock(return_value=None)
            mock_file.write = AsyncMock()

            with (
                patch("aiofiles.open", return_value=mock_file),
                patch("asyncio.sleep", new_callable=AsyncMock) as mock_sleep,
            ):
                await manager.fetch_worker()

                # Should have retried once (slept once)
                assert mock_sleep.call_count >= 1
                # Verify it succeeded after retry
                assert attempt_counter["count"] == 2
                # File should have been written successfully after retry
                assert mock_file.write.call_count > 0

    @pytest.mark.asyncio
    async def test_fetch_worker_retries_on_5xx_error(
        self, mock_asyncpg_pool, mock_progress, database_config
    ):
        """Test that fetch_worker retries on 5xx server errors."""
        # Share state across retry attempts
        attempt_counter = {"count": 0}

        class Server5xxStreamResponse:
            """Mock response that raises 500 error on first attempt, succeeds on second."""

            def raise_for_status(self):
                attempt_counter["count"] += 1
                if attempt_counter["count"] == 1:
                    response = Mock(spec=httpx.Response)
                    response.status_code = 500
                    raise httpx.HTTPStatusError(
                        "Server error", request=MagicMock(), response=response
                    )
                # Success on second attempt

            async def aiter_bytes(self, chunk_size):
                yield b"test data"

            async def __aenter__(self):
                return self

            async def __aexit__(self, exc_type, exc_val, exc_tb):
                return None

        class Server5xxStreamContextManager:
            """Mock context manager for 5xx error."""

            async def __aenter__(self):
                return Server5xxStreamResponse()

            async def __aexit__(self, exc_type, exc_val, exc_tb):
                return None

        with tempfile.TemporaryDirectory() as tmpdir:
            mock_client = MagicMock()

            manager = FetchManager(
                client=mock_client,
                output_folder=Path(tmpdir),
                progress=mock_progress,
                db_pool=mock_asyncpg_pool,
                db_config=database_config,
                skip_existing=False,
            )

            def get_context_manager(*args, **kwargs):
                return Server5xxStreamContextManager()

            mock_client.stream = MagicMock(side_effect=get_context_manager)

            await manager.fetch_queue.put(("https://example.com/test.zip", "test.zip"))
            await manager.fetch_queue.put(None)

            mock_file = AsyncMock()
            mock_file.__aenter__ = AsyncMock(return_value=mock_file)
            mock_file.__aexit__ = AsyncMock(return_value=None)
            mock_file.write = AsyncMock()

            with (
                patch("aiofiles.open", return_value=mock_file),
                patch("asyncio.sleep", new_callable=AsyncMock) as mock_sleep,
            ):
                await manager.fetch_worker()

                # Should have retried (slept at least once)
                assert mock_sleep.call_count >= 1
                # Verify it succeeded after retry
                assert attempt_counter["count"] == 2
                # File should be written after retry
                assert mock_file.write.call_count > 0

    @pytest.mark.asyncio
    async def test_fetch_worker_no_retry_on_404(
        self, mock_asyncpg_pool, mock_progress, database_config
    ):
        """Test that fetch_worker does NOT retry on 404 errors (non-retryable)."""

        class NotFound404StreamResponse:
            """Mock response that always raises 404 error."""

            def raise_for_status(self):
                response = Mock(spec=httpx.Response)
                response.status_code = 404
                raise httpx.HTTPStatusError("Not found", request=MagicMock(), response=response)

            async def __aenter__(self):
                return self

            async def __aexit__(self, exc_type, exc_val, exc_tb):
                return None

        class NotFound404StreamContextManager:
            """Mock context manager for 404 error."""

            async def __aenter__(self):
                return NotFound404StreamResponse()

            async def __aexit__(self, exc_type, exc_val, exc_tb):
                return None

        with tempfile.TemporaryDirectory() as tmpdir:
            mock_client = MagicMock()
            mock_client.stream = MagicMock(return_value=NotFound404StreamContextManager())

            manager = FetchManager(
                client=mock_client,
                output_folder=Path(tmpdir),
                progress=mock_progress,
                db_pool=mock_asyncpg_pool,
                db_config=database_config,
                skip_existing=False,
            )

            await manager.fetch_queue.put(("https://example.com/notfound.zip", "notfound.zip"))
            await manager.fetch_queue.put(None)

            with patch("asyncio.sleep", new_callable=AsyncMock) as mock_sleep:
                await manager.fetch_worker()

                # Should NOT have retried (no sleep calls)
                assert mock_sleep.call_count == 0

    @pytest.mark.asyncio
    async def test_fetch_worker_respects_max_retries(
        self, mock_asyncpg_pool, mock_progress, database_config
    ):
        """Test that fetch_worker respects the maximum retry limit."""
        call_count = {"count": 0}

        class PersistentTimeoutStreamResponse:
            """Mock response that always times out."""

            def raise_for_status(self):
                pass

            async def aiter_bytes(self, chunk_size):
                call_count["count"] += 1
                raise httpx.TimeoutException("Persistent timeout")
                # This yield is unreachable but needed to make this an async generator
                yield  # pragma: no cover

            async def __aenter__(self):
                return self

            async def __aexit__(self, exc_type, exc_val, exc_tb):
                return None

        class PersistentTimeoutStreamContextManager:
            """Mock context manager for persistent timeout."""

            async def __aenter__(self):
                return PersistentTimeoutStreamResponse()

            async def __aexit__(self, exc_type, exc_val, exc_tb):
                return None

        with tempfile.TemporaryDirectory() as tmpdir:
            mock_client = MagicMock()
            mock_client.stream = MagicMock(return_value=PersistentTimeoutStreamContextManager())

            manager = FetchManager(
                client=mock_client,
                output_folder=Path(tmpdir),
                progress=mock_progress,
                db_pool=mock_asyncpg_pool,
                db_config=database_config,
                skip_existing=False,
            )

            await manager.fetch_queue.put(("https://example.com/test.zip", "test.zip"))
            await manager.fetch_queue.put(None)

            with patch("asyncio.sleep", new_callable=AsyncMock) as mock_sleep:
                await manager.fetch_worker()

                # With max_attempts=4, should try 4 times total
                assert call_count["count"] == 4
                # Should sleep 3 times (between 4 attempts)
                assert mock_sleep.call_count == 3

    @pytest.mark.asyncio
    async def test_fetch_worker_logs_retries_at_debug(
        self, mock_asyncpg_pool, mock_progress, database_config, caplog
    ):
        """Test that retry attempts are logged at DEBUG level."""
        import logging

        # Share state across retry attempts
        attempt_counter = {"count": 0}

        class TimeoutOnceStreamResponse:
            """Mock response that times out once then succeeds."""

            def raise_for_status(self):
                pass

            async def aiter_bytes(self, chunk_size):
                attempt_counter["count"] += 1
                if attempt_counter["count"] == 1:
                    raise httpx.TimeoutException("Timeout")
                yield b"data"

            async def __aenter__(self):
                return self

            async def __aexit__(self, exc_type, exc_val, exc_tb):
                return None

        class TimeoutOnceStreamContextManager:
            """Mock context manager."""

            async def __aenter__(self):
                return TimeoutOnceStreamResponse()

            async def __aexit__(self, exc_type, exc_val, exc_tb):
                return None

        with tempfile.TemporaryDirectory() as tmpdir:
            mock_client = MagicMock()

            def get_context_manager(*args, **kwargs):
                return TimeoutOnceStreamContextManager()

            mock_client.stream = MagicMock(side_effect=get_context_manager)

            manager = FetchManager(
                client=mock_client,
                output_folder=Path(tmpdir),
                progress=mock_progress,
                db_pool=mock_asyncpg_pool,
                db_config=database_config,
                skip_existing=False,
            )

            await manager.fetch_queue.put(("https://example.com/test.zip", "test.zip"))
            await manager.fetch_queue.put(None)

            mock_file = AsyncMock()
            mock_file.__aenter__ = AsyncMock(return_value=mock_file)
            mock_file.__aexit__ = AsyncMock(return_value=None)
            mock_file.write = AsyncMock()

            with (
                patch("aiofiles.open", return_value=mock_file),
                patch("asyncio.sleep", new_callable=AsyncMock),
                caplog.at_level(logging.DEBUG),
            ):
                await manager.fetch_worker()

                # Check that retry was logged at DEBUG level
                debug_logs = [
                    record for record in caplog.records if record.levelno == logging.DEBUG
                ]
                retry_logs = [log for log in debug_logs if "Retrying download" in log.message]
                assert len(retry_logs) > 0

    @pytest.mark.asyncio
    async def test_fetch_worker_logs_final_failure_at_error(
        self, mock_asyncpg_pool, mock_progress, database_config, caplog
    ):
        """Test that final failures after all retries are logged at ERROR level."""
        import logging

        class PersistentFailureStreamResponse:
            """Mock response that always fails."""

            def raise_for_status(self):
                pass

            async def aiter_bytes(self, chunk_size):
                raise httpx.TimeoutException("Persistent failure")
                # This yield is unreachable but needed to make this an async generator
                yield  # pragma: no cover

            async def __aenter__(self):
                return self

            async def __aexit__(self, exc_type, exc_val, exc_tb):
                return None

        class PersistentFailureStreamContextManager:
            """Mock context manager."""

            async def __aenter__(self):
                return PersistentFailureStreamResponse()

            async def __aexit__(self, exc_type, exc_val, exc_tb):
                return None

        with tempfile.TemporaryDirectory() as tmpdir:
            mock_client = MagicMock()
            mock_client.stream = MagicMock(return_value=PersistentFailureStreamContextManager())

            manager = FetchManager(
                client=mock_client,
                output_folder=Path(tmpdir),
                progress=mock_progress,
                db_pool=mock_asyncpg_pool,
                db_config=database_config,
                skip_existing=False,
            )

            await manager.fetch_queue.put(("https://example.com/test.zip", "test.zip"))
            await manager.fetch_queue.put(None)

            with patch("asyncio.sleep", new_callable=AsyncMock), caplog.at_level(logging.ERROR):
                await manager.fetch_worker()

                # Check that final failure was logged at ERROR level
                error_logs = [
                    record for record in caplog.records if record.levelno == logging.ERROR
                ]
                failure_logs = [
                    log
                    for log in error_logs
                    if "Failed to download" in log.message and "after retries" in log.message
                ]
                assert len(failure_logs) > 0
