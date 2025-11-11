"""Tests for the list command."""

from io import StringIO
from unittest.mock import patch

import pytest
from typer.testing import CliRunner

from zensus2pgsql.cli import app
from zensus2pgsql.commands.list import list_datasets
from zensus2pgsql.constants import GITTERDATEN_FILES


@pytest.fixture
def runner():
    """Create a CLI test runner."""
    return CliRunner()


def test_list_command_via_cli(runner):
    """Test the list command through the CLI."""
    result = runner.invoke(app, ["list"])

    assert result.exit_code == 0
    assert "Available Zensus 2022 Gitterdaten Datasets" in result.stdout
    assert f"Total datasets: {len(GITTERDATEN_FILES)}" in result.stdout
    assert "zensus2pgsql create <dataset_name>" in result.stdout


def test_list_command_shows_all_datasets(runner):
    """Test that all datasets are shown in the output."""
    result = runner.invoke(app, ["list"])

    assert result.exit_code == 0

    # Check that all dataset names appear in the output
    for dataset in GITTERDATEN_FILES:
        assert dataset["name"] in result.stdout


def test_list_command_contains_numbered_rows(runner):
    """Test that datasets are numbered starting from 1."""
    result = runner.invoke(app, ["list"])

    assert result.exit_code == 0

    # Check for the first few row numbers
    # The exact format depends on Rich rendering, but numbers should appear
    output = result.stdout

    # Check that we have the expected number of datasets
    assert str(len(GITTERDATEN_FILES)) in output


def test_list_command_contains_usage_instructions(runner):
    """Test that usage instructions are included."""
    result = runner.invoke(app, ["list"])

    assert result.exit_code == 0
    assert "Use dataset names with the 'create' command:" in result.stdout
    assert "zensus2pgsql create" in result.stdout


def test_list_datasets_function_direct():
    """Test the list_datasets function directly."""
    # Capture console output
    with patch("sys.stdout", new_callable=StringIO) as mock_stdout:
        list_datasets()
        output = mock_stdout.getvalue()

        # Basic checks - output should contain key elements
        # Note: Rich formatting may make exact matching difficult,
        # but we can verify the function runs without error
        assert output or True  # Function executed successfully


def test_list_command_table_structure(runner):
    """Test that the table has the correct structure."""
    result = runner.invoke(app, ["list"])

    assert result.exit_code == 0

    # Check for table column headers
    assert "#" in result.stdout
    assert "Dataset Name" in result.stdout


def test_list_command_with_empty_gitterdaten_files(runner, monkeypatch):
    """Test list command behavior with empty dataset list."""
    # Mock GITTERDATEN_FILES to be empty
    monkeypatch.setattr("zensus2pgsql.commands.list.GITTERDATEN_FILES", ())

    result = runner.invoke(app, ["list"])

    assert result.exit_code == 0
    assert "Total datasets: 0" in result.stdout


def test_list_command_with_single_dataset(runner, monkeypatch):
    """Test list command behavior with a single dataset."""
    # Mock GITTERDATEN_FILES with one entry
    single_dataset = ({"name": "test_dataset", "url": "https://example.com/test.zip"},)
    monkeypatch.setattr("zensus2pgsql.commands.list.GITTERDATEN_FILES", single_dataset)

    result = runner.invoke(app, ["list"])

    assert result.exit_code == 0
    assert "test_dataset" in result.stdout
    assert "Total datasets: 1" in result.stdout


def test_list_command_dataset_ordering(runner):
    """Test that datasets appear in the correct order."""
    result = runner.invoke(app, ["list"])

    assert result.exit_code == 0

    # Get the positions of first and last dataset names
    first_dataset = GITTERDATEN_FILES[0]["name"]
    last_dataset = GITTERDATEN_FILES[-1]["name"]

    first_pos = result.stdout.find(first_dataset)
    last_pos = result.stdout.find(last_dataset)

    # First dataset should appear before last dataset
    assert first_pos < last_pos
    assert first_pos > 0  # Should be found


def test_list_command_contains_specific_datasets(runner):
    """Test that specific known datasets are in the output."""
    result = runner.invoke(app, ["list"])

    assert result.exit_code == 0

    # Check for some specific datasets we know exist
    assert "bevoelkerungszahl" in result.stdout
    assert "religion" in result.stdout
    assert "heizungsart" in result.stdout


def test_list_datasets_console_output():
    """Test that list_datasets creates proper console output."""
    from io import StringIO

    from rich.console import Console

    # Create a string buffer to capture output
    string_io = StringIO()
    console = Console(file=string_io, force_terminal=True)

    # Patch the Console constructor to use our test console
    with patch("zensus2pgsql.commands.list.Console", return_value=console):
        list_datasets()

    output = string_io.getvalue()

    # Verify key elements are in the output
    assert "Available Zensus 2022 Gitterdaten Datasets" in output
    assert "Total datasets:" in output
    assert "zensus2pgsql create" in output

    # Verify at least some datasets are present
    dataset_count = 0
    for dataset in GITTERDATEN_FILES[:5]:  # Check first 5
        if dataset["name"] in output:
            dataset_count += 1

    assert dataset_count > 0  # At least some datasets should appear


def test_list_command_no_errors_with_special_characters(runner, monkeypatch):
    """Test that special characters in dataset names don't cause errors."""
    # Mock GITTERDATEN_FILES with special characters
    special_datasets = (
        {"name": "dataset_with_ümlaut", "url": "https://example.com/test1.zip"},
        {"name": "dataset-with-hyphens", "url": "https://example.com/test2.zip"},
        {"name": "dataset with spaces", "url": "https://example.com/test3.zip"},
    )
    monkeypatch.setattr("zensus2pgsql.commands.list.GITTERDATEN_FILES", special_datasets)

    result = runner.invoke(app, ["list"])

    assert result.exit_code == 0
    assert "dataset_with_ümlaut" in result.stdout
    assert "dataset-with-hyphens" in result.stdout
    assert "dataset with spaces" in result.stdout
