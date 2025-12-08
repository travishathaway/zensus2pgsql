"""
zensus2pgsql cache command

Manage cached ZIP and CSV files.
"""

import typer
from rich.console import Console
from rich.table import Table

from ..cache import CACHE, clear_csv_cache, format_bytes, get_cache_size


def cache_command(
    clear: bool = typer.Option(False, "--clear", help="Clear CSV cache"),
    dataset: str | None = typer.Option(None, "--dataset", help="Specific dataset to clear"),
) -> None:
    """Show cache information and optionally clear cache."""
    console = Console()

    # Get cache sizes
    cache_sizes = get_cache_size()

    # Display cache information
    table = Table(title="Cache Information", show_header=True, header_style="bold cyan")
    table.add_column("Type", style="cyan")
    table.add_column("Size", justify="right", style="green")

    table.add_row("ZIP files", format_bytes(cache_sizes["zip_bytes"]))
    table.add_row("Extracted CSVs", format_bytes(cache_sizes["csv_bytes"]))
    table.add_row("[bold]Total[/bold]", f"[bold]{format_bytes(cache_sizes['total_bytes'])}[/bold]")

    console.print()
    console.print(table)
    console.print()
    console.print(f"[dim]Cache location: {CACHE}[/dim]")
    console.print()

    # Clear cache if requested
    if clear:
        if dataset:
            console.print(f"[yellow]Clearing CSV cache for dataset: {dataset}[/yellow]")
        else:
            console.print("[yellow]Clearing entire CSV cache...[/yellow]")

        bytes_freed = clear_csv_cache(dataset)
        console.print(f"[green]✓ Freed {format_bytes(bytes_freed)}[/green]")
