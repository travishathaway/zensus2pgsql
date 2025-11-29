"""Shared fixtures and mocks for zensus2pgsql tests."""

import asyncio
import tempfile
import zipfile
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock

import pytest
from typer.testing import CliRunner

from zensus2pgsql.commands.create import DatabaseConfig


@pytest.fixture
def cli_runner():
    """Mock zensus2pgsql cli."""
    runner = CliRunner()

    return runner


@pytest.fixture
def database_config() -> DatabaseConfig:
    """Create a test DatabaseConfig instance."""
    return DatabaseConfig(
        host="localhost",
        port=5432,
        database="test_db",
        user="test_user",
        password="test_pass",
        schema="test_schema",
        srid=3035,
        drop_existing=False,
    )


@pytest.fixture
def database_config_drop_existing() -> DatabaseConfig:
    """Create a test DatabaseConfig with drop_existing=True."""
    return DatabaseConfig(
        host="localhost",
        port=5432,
        database="test_db",
        user="test_user",
        password="test_pass",
        schema="test_schema",
        srid=3035,
        drop_existing=True,
    )


@pytest.fixture
def database_config_custom_srid() -> DatabaseConfig:
    """Create a test DatabaseConfig with custom SRID."""
    return DatabaseConfig(
        host="localhost",
        port=5432,
        database="test_db",
        user="test_user",
        password="test_pass",
        schema="test_schema",
        srid=4326,
        drop_existing=False,
    )


class MockTransaction:
    """Mock asyncpg transaction context manager."""

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        return None


class MockAsyncpgConnection:
    """Mock asyncpg connection with proper async methods."""

    def __init__(self):
        self.execute = AsyncMock(return_value=None)
        self.fetchval = AsyncMock(return_value=True)
        self.fetchrow = AsyncMock(return_value=(100, 100, 100, 0))  # All integers
        self.copy_to_table = AsyncMock(return_value=None)
        self.close = AsyncMock(return_value=None)

    def transaction(self):
        return MockTransaction()


class MockPoolAcquireContext:
    """Mock context manager for pool.acquire()."""

    def __init__(self, connection):
        self.connection = connection

    async def __aenter__(self):
        return self.connection

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        return None


class MockAsyncpgPool:
    """Mock asyncpg pool with proper acquire context manager."""

    def __init__(self, connection):
        self.connection = connection
        self.close = AsyncMock(return_value=None)

    def acquire(self):
        return MockPoolAcquireContext(self.connection)


@pytest.fixture
def mock_asyncpg_connection() -> MockAsyncpgConnection:
    """Create a mocked asyncpg connection."""
    return MockAsyncpgConnection()


@pytest.fixture
def mock_asyncpg_pool(mock_asyncpg_connection: MockAsyncpgConnection) -> MockAsyncpgPool:
    """Create a mocked asyncpg connection pool."""
    return MockAsyncpgPool(mock_asyncpg_connection)


class MockAsyncIterator:
    """Mock async iterator for streaming bytes."""

    def __init__(self, chunks):
        self.chunks = chunks
        self.index = 0

    def __aiter__(self):
        return self

    async def __anext__(self):
        if self.index >= len(self.chunks):
            raise StopAsyncIteration
        chunk = self.chunks[self.index]
        self.index += 1
        return chunk


class MockStreamResponse:
    """Mock httpx streaming response with proper async context manager."""

    def __init__(self):
        self.chunks = [b"PK\x03\x04", b"\x00" * 100]

    def raise_for_status(self):
        pass

    def aiter_bytes(self, chunk_size: int = 8192):
        return MockAsyncIterator(self.chunks)

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        return None


class MockStreamContextManager:
    """Mock context manager for httpx client.stream()."""

    def __init__(self):
        self.response = MockStreamResponse()

    async def __aenter__(self):
        return self.response

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        return None


@pytest.fixture
def mock_httpx_response() -> MockStreamResponse:
    """Create a mocked httpx streaming response."""
    return MockStreamResponse()


@pytest.fixture
def mock_httpx_client() -> MagicMock:
    """Create a mocked httpx AsyncClient."""
    client = MagicMock()

    # Mock stream to return our async context manager
    def stream_side_effect(*args, **kwargs):
        return MockStreamContextManager()

    client.stream = MagicMock(side_effect=stream_side_effect)
    client.aclose = AsyncMock(return_value=None)

    return client


@pytest.fixture
def mock_progress() -> MagicMock:
    """Create a mocked Rich Progress bar."""
    progress = MagicMock()
    progress.add_task = MagicMock(return_value=1)
    progress.update = MagicMock()
    progress.__enter__ = MagicMock(return_value=progress)
    progress.__exit__ = MagicMock(return_value=None)
    return progress


@pytest.fixture
def sample_csv_content() -> str:
    """Sample CSV content with German census data format."""
    return """Gitter_ID_100m;x_mp_100m;y_mp_100m;Einwohner;Alter
100mN26680E43341;4334150;2668050;42;35
100mN26680E43342;4334250;2668050;38;42
100mN26680E43343;4334350;2668050;-;28
"""


@pytest.fixture
def sample_csv_content_german_decimals() -> str:
    """Sample CSV content with German decimal format."""
    return """id;wert;name
1;1,5;Test1
2;2,75;Test2
3;-;Test3
"""


@pytest.fixture
def sample_csv_content_no_coords() -> str:
    """Sample CSV content without coordinate columns."""
    return """id;name;value
1;Test1;100
2;Test2;200
3;Test3;-
"""


@pytest.fixture
def sample_csv_content_text_column() -> str:
    """Sample CSV content with text column."""
    return """id;name;category
1;Test1;CategoryA
2;Test2;CategoryB
3;Test3;CategoryC
"""


@pytest.fixture
def temp_csv_file(sample_csv_content: str) -> Path:
    """Create a temporary CSV file with sample content."""
    with tempfile.NamedTemporaryFile(
        mode="w", suffix="_Gitter.csv", delete=False, encoding="utf-8"
    ) as f:
        f.write(sample_csv_content)
        return Path(f.name)


@pytest.fixture
def temp_csv_file_latin1() -> Path:
    """Create a temporary CSV file with ISO-8859-1 encoding."""
    content = "id;name;stadt\n1;Müller;München\n2;Größe;Köln\n"
    with tempfile.NamedTemporaryFile(mode="wb", suffix="_Gitter.csv", delete=False) as f:
        f.write(content.encode("iso-8859-1"))
        return Path(f.name)


@pytest.fixture
def temp_csv_file_windows1252() -> Path:
    """Create a temporary CSV file with Windows-1252 encoding."""
    content = "id;name;info\n1;Test;Special chars: €™\n"
    with tempfile.NamedTemporaryFile(mode="wb", suffix="_Gitter.csv", delete=False) as f:
        f.write(content.encode("windows-1252"))
        return Path(f.name)


@pytest.fixture
def temp_zip_with_csv(sample_csv_content: str) -> Path:
    """Create a temporary ZIP file containing a CSV file."""
    with tempfile.NamedTemporaryFile(suffix=".zip", delete=False) as zip_tmp:
        zip_path = Path(zip_tmp.name)

    with zipfile.ZipFile(zip_path, "w") as zf:
        zf.writestr("Zensus2022_TestData_Gitter.csv", sample_csv_content)

    return zip_path


@pytest.fixture
def temp_zip_with_multiple_csvs(sample_csv_content: str) -> Path:
    """Create a temporary ZIP file containing multiple CSV files."""
    with tempfile.NamedTemporaryFile(suffix=".zip", delete=False) as zip_tmp:
        zip_path = Path(zip_tmp.name)

    with zipfile.ZipFile(zip_path, "w") as zf:
        zf.writestr("Zensus2022_Data1_Gitter.csv", sample_csv_content)
        zf.writestr("Zensus2022_Data2_Gitter.csv", sample_csv_content)
        zf.writestr("Other_File.txt", "Not a CSV")

    return zip_path


@pytest.fixture
def temp_zip_no_csv() -> Path:
    """Create a temporary ZIP file with no matching CSV files."""
    with tempfile.NamedTemporaryFile(suffix=".zip", delete=False) as zip_tmp:
        zip_path = Path(zip_tmp.name)

    with zipfile.ZipFile(zip_path, "w") as zf:
        zf.writestr("readme.txt", "No CSV files here")
        zf.writestr("data.json", '{"key": "value"}')

    return zip_path


@pytest.fixture
def temp_empty_zip() -> Path:
    """Create an empty ZIP file."""
    with tempfile.NamedTemporaryFile(suffix=".zip", delete=False) as zip_tmp:
        zip_path = Path(zip_tmp.name)

    with zipfile.ZipFile(zip_path, "w"):
        pass  # Create empty ZIP

    return zip_path


@pytest.fixture
def mock_gitterdaten_files() -> tuple[dict[str, str], ...]:
    """Create mock GITTERDATEN_FILES for testing."""
    return (
        {"name": "test_dataset1", "url": "https://example.com/test1.zip"},
        {"name": "test_dataset2", "url": "https://example.com/test2.zip"},
        {"name": "test_dataset3", "url": "https://example.com/test3.zip"},
    )


def create_type_detection_fetchrow(
    total: int = 100, non_empty: int = 100, integer_count: int = 0, numeric_count: int = 0
) -> tuple[int, int, int, int]:
    """Helper to create fetchrow return value for type detection tests."""
    return (total, non_empty, integer_count, numeric_count)


# Async test event loop configuration
@pytest.fixture
def event_loop():
    """Create an instance of the default event loop for each test case."""
    loop = asyncio.new_event_loop()
    yield loop
    loop.close()
