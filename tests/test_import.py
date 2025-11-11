"""Test Zensus2pgsql."""

import zensus2pgsql


def test_import() -> None:
    """Test that the app can be imported."""
    assert isinstance(zensus2pgsql.__name__, str)
