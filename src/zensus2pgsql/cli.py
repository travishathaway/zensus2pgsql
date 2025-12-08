"""zensus2pgsql CLI application entry point."""

import typer

from . import __version__
from .commands.cache import cache_command
from .commands.create import create
from .commands.drop import drop
from .commands.list import list_datasets


def _version_callback(value: bool):
    if value:
        typer.echo(f"zensus2pgsql version: {__version__}")
        raise typer.Exit()


app = typer.Typer(
    help=f"zensus2pgsql [{__version__}]  A CLI program for importing geographic German census data into PostgreSQL ",
    no_args_is_help=True,
)


@app.callback()
def main(
    version: bool = typer.Option(
        None,
        "--version",
        "-v",
        help="Show the version and exit.",
        is_eager=True,
        callback=_version_callback,
    ),
):
    f"""zensus2pgsql [{__version__}]  A CLI program for importing geographic German census data into PostgreSQL """


# Add commands directly to the main app
app.command(name="cache")(cache_command)
app.command(name="create")(create)
app.command(name="list")(list_datasets)
app.command(name="drop")(drop)
