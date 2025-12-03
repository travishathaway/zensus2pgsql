"""
Contains base types that are common to multiple commands
"""

import typer

#: Option for controlling output verbosity
VerboseOption = typer.Option(
    0, "--verbose", "-v", count=True, help="Increase verbosity (-v for INFO, -vv for DEBUG)"
)

#: Option for suppressing out to terminal
QuietOption = typer.Option(False, "--quiet", "-q", help="Supress all terminal output")

#: Option for PostgreSQL host
DatabaseHost = typer.Option("localhost", "--host", "-h", help="PostgreSQL host")

#: Option for PostgreSQL port
DatabasePort = typer.Option(5432, "--port", "-p", help="PostgreSQL port")

#: Option for PostgreSQL database name
DatabaseName = typer.Option("zensus", "--database", "-d", help="PostgreSQL database name")

#: Option for PostgreSQL user
DatabaseUser = typer.Option("postgres", "--user", "-u", help="PostgreSQL user")

#: Option for PostgreSQL password
DatabasePassword = typer.Option(
    None,
    "--password",
    help="PostgreSQL password (will prompt if not provided)",
    prompt=True,
    hide_input=True,
)

#: Option for PostgreSQL schema name
DatabaseSchema = typer.Option("zensus", "--schema", "-s", help="PostgreSQL schema name")
