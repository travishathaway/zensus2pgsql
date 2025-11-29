"""Test Zensus2pgsql CLI."""

from zensus2pgsql import __version__
from zensus2pgsql.cli import app


def test_cli_help(cli_runner) -> None:
    """Test that the CLI help command works."""
    result = cli_runner.invoke(app, ["--help"])
    assert result.exit_code == 0
    assert "zensus2pgsql" in result.stdout
    assert "create" in result.stdout
    assert "list" in result.stdout
    assert "drop" in result.stdout


def test_cli_version(cli_runner) -> None:
    """
    Ensure version string is printed.
    """
    result = cli_runner.invoke(app, ["--version"])

    assert result.exit_code == 0
    assert result.stdout == f"zensus2pgsql version: {__version__}\n"
