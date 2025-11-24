"""Test Zensus2pgsql CLI."""

from typer.testing import CliRunner

from zensus2pgsql.cli import app


def test_cli_help() -> None:
    """Test that the CLI help command works."""
    runner = CliRunner()
    result = runner.invoke(app, ["--help"])
    assert result.exit_code == 0
    assert "zensus2pgsql" in result.stdout
    assert "create" in result.stdout
    assert "list" in result.stdout
    assert "drop" in result.stdout
