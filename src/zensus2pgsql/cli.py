"""Zensus Collector CLI."""

import typer

from .commands.create import collect
from .commands.drop import drop
from .commands.list import list_datasets

app = typer.Typer(help="Zensus 2022 Gitterdaten PostgreSQL importer", no_args_is_help=True)

# Add commands directly to the main app
app.command(name="create")(collect)
app.command(name="list")(list_datasets)
app.command(name="drop")(drop)
