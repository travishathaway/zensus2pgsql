"""Tests for cache management functionality."""

from zensus2pgsql.cache import clear_csv_cache, format_bytes, get_cache_size


class TestGetCacheSize:
    """Tests for get_cache_size function."""

    def test_empty_cache(self, tmp_path, monkeypatch):
        """Test cache size calculation with empty cache."""
        monkeypatch.setattr("zensus2pgsql.cache.CACHE", str(tmp_path))
        sizes = get_cache_size()
        assert sizes["zip_bytes"] == 0
        assert sizes["csv_bytes"] == 0
        assert sizes["total_bytes"] == 0

    def test_zip_only_cache(self, tmp_path, monkeypatch):
        """Test cache size with only ZIP files."""
        monkeypatch.setattr("zensus2pgsql.cache.CACHE", str(tmp_path))

        # Create test ZIP file
        zip_file = tmp_path / "test.zip"
        zip_file.write_bytes(b"0" * 1000)  # 1KB

        sizes = get_cache_size()
        assert sizes["zip_bytes"] == 1000
        assert sizes["csv_bytes"] == 0
        assert sizes["total_bytes"] == 1000

    def test_csv_cache(self, tmp_path, monkeypatch):
        """Test cache size with extracted CSVs."""
        monkeypatch.setattr("zensus2pgsql.cache.CACHE", str(tmp_path))

        # Create extracted cache
        csv_cache = tmp_path / "extracted" / "dataset1"
        csv_cache.mkdir(parents=True)
        (csv_cache / "test.csv").write_bytes(b"0" * 2000)  # 2KB

        sizes = get_cache_size()
        assert sizes["csv_bytes"] == 2000
        assert sizes["total_bytes"] == 2000

    def test_both_caches(self, tmp_path, monkeypatch):
        """Test cache size with both ZIP and CSV files."""
        monkeypatch.setattr("zensus2pgsql.cache.CACHE", str(tmp_path))

        # Create ZIP file
        zip_file = tmp_path / "test.zip"
        zip_file.write_bytes(b"0" * 1000)

        # Create CSV cache
        csv_cache = tmp_path / "extracted" / "dataset1"
        csv_cache.mkdir(parents=True)
        (csv_cache / "test.csv").write_bytes(b"0" * 2000)

        sizes = get_cache_size()
        assert sizes["zip_bytes"] == 1000
        assert sizes["csv_bytes"] == 2000
        assert sizes["total_bytes"] == 3000

    def test_multiple_csv_datasets(self, tmp_path, monkeypatch):
        """Test cache size with multiple CSV datasets."""
        monkeypatch.setattr("zensus2pgsql.cache.CACHE", str(tmp_path))

        # Create multiple dataset caches
        for i in range(3):
            csv_cache = tmp_path / "extracted" / f"dataset{i}"
            csv_cache.mkdir(parents=True)
            (csv_cache / "test.csv").write_bytes(b"0" * 1000)
            (csv_cache / ".cache_meta.json").write_bytes(b"0" * 100)

        sizes = get_cache_size()
        # 3 datasets * (1000 + 100) bytes
        assert sizes["csv_bytes"] == 3300
        assert sizes["total_bytes"] == 3300


class TestFormatBytes:
    """Tests for format_bytes function."""

    def test_bytes(self):
        """Test formatting bytes."""
        assert format_bytes(100) == "100.0 B"
        assert format_bytes(500) == "500.0 B"

    def test_kilobytes(self):
        """Test formatting kilobytes."""
        assert format_bytes(1024) == "1.0 KB"
        assert format_bytes(1536) == "1.5 KB"
        assert format_bytes(2048) == "2.0 KB"

    def test_megabytes(self):
        """Test formatting megabytes."""
        assert format_bytes(1_048_576) == "1.0 MB"
        assert format_bytes(5_242_880) == "5.0 MB"

    def test_gigabytes(self):
        """Test formatting gigabytes."""
        assert format_bytes(1_073_741_824) == "1.0 GB"
        assert format_bytes(3_221_225_472) == "3.0 GB"

    def test_zero(self):
        """Test formatting zero bytes."""
        assert format_bytes(0) == "0.0 B"


class TestClearCsvCache:
    """Tests for clear_csv_cache function."""

    def test_clear_all(self, tmp_path, monkeypatch):
        """Test clearing entire CSV cache."""
        monkeypatch.setattr("zensus2pgsql.cache.CACHE", str(tmp_path))

        # Create cache structure
        csv_cache = tmp_path / "extracted"
        dataset1 = csv_cache / "dataset1"
        dataset2 = csv_cache / "dataset2"
        dataset1.mkdir(parents=True)
        dataset2.mkdir(parents=True)

        (dataset1 / "test1.csv").write_bytes(b"0" * 1000)
        (dataset2 / "test2.csv").write_bytes(b"0" * 2000)

        bytes_freed = clear_csv_cache()
        assert bytes_freed == 3000
        assert not list(csv_cache.iterdir())

    def test_clear_specific_dataset(self, tmp_path, monkeypatch):
        """Test clearing specific dataset cache."""
        monkeypatch.setattr("zensus2pgsql.cache.CACHE", str(tmp_path))

        csv_cache = tmp_path / "extracted"
        dataset1 = csv_cache / "dataset1"
        dataset2 = csv_cache / "dataset2"
        dataset1.mkdir(parents=True)
        dataset2.mkdir(parents=True)

        (dataset1 / "test1.csv").write_bytes(b"0" * 1000)
        (dataset2 / "test2.csv").write_bytes(b"0" * 2000)

        bytes_freed = clear_csv_cache("dataset1")
        assert bytes_freed == 1000
        assert not dataset1.exists()
        assert dataset2.exists()

    def test_clear_nonexistent_cache(self, tmp_path, monkeypatch):
        """Test clearing cache when it doesn't exist."""
        monkeypatch.setattr("zensus2pgsql.cache.CACHE", str(tmp_path))

        bytes_freed = clear_csv_cache()
        assert bytes_freed == 0

    def test_clear_nonexistent_dataset(self, tmp_path, monkeypatch):
        """Test clearing specific dataset that doesn't exist."""
        monkeypatch.setattr("zensus2pgsql.cache.CACHE", str(tmp_path))

        csv_cache = tmp_path / "extracted"
        csv_cache.mkdir(parents=True)

        bytes_freed = clear_csv_cache("nonexistent")
        assert bytes_freed == 0

    def test_clear_with_subdirectories(self, tmp_path, monkeypatch):
        """Test clearing cache with nested subdirectories."""
        monkeypatch.setattr("zensus2pgsql.cache.CACHE", str(tmp_path))

        csv_cache = tmp_path / "extracted"
        dataset1 = csv_cache / "dataset1"
        subdir = dataset1 / "subdir"
        subdir.mkdir(parents=True)

        (dataset1 / "test1.csv").write_bytes(b"0" * 1000)
        (subdir / "test2.csv").write_bytes(b"0" * 500)

        bytes_freed = clear_csv_cache()
        assert bytes_freed == 1500
        assert not list(csv_cache.iterdir())
