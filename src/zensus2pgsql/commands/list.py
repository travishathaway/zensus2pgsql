"""
zensus2pgsql list command

Lists all available Zensus 2022 Gitterdaten datasets that can be downloaded and imported.
"""

from rich.console import Console
from rich.table import Table

from ..constants import GITTERDATEN_FILES


def list_datasets() -> None:
    """List all available Zensus 2022 Gitterdaten datasets."""
    console = Console()

    # Create a table with rich
    table = Table(
        title="Available Zensus 2022 Gitterdaten Datasets",
        show_header=True,
        header_style="bold cyan",
        show_lines=False,
        expand=True,
    )

    table.add_column("#", style="dim", width=4, justify="right")
    table.add_column("Dataset Name", style="green", no_wrap=False)

    # Add all datasets to the table
    for idx, dataset in enumerate(GITTERDATEN_FILES, start=1):
        table.add_row(str(idx), dataset["name"])

    # Print the table
    console.print()
    console.print(table)
    console.print()
    console.print(f"[bold cyan]Total datasets:[/bold cyan] {len(GITTERDATEN_FILES)}")
    console.print()
    console.print(
        "[dim]Use dataset names with the 'create' command:[/dim]\n"
        "  [cyan]zensus2pgsql create <dataset_name>[/cyan]"
    )
    console.print()
