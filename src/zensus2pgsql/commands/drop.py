"""
zensus2pgsql drop command.

This command drops all tables from a specified PostgreSQL schema.
"""

import asyncio

import asyncpg
import typer
from rich import print as rprint

from zensus2pgsql.commands._base import (
    DatabaseHost,
    DatabaseName,
    DatabasePassword,
    DatabasePort,
    DatabaseSchema,
    DatabaseUser,
)
from zensus2pgsql.logging import logger


def drop(
    host: str = DatabaseHost,
    port: int = DatabasePort,
    database: str = DatabaseName,
    user: str = DatabaseUser,
    password: str | None = DatabasePassword,
    schema: str = DatabaseSchema,
    confirm: bool = typer.Option(False, "--confirm", "-y", help="Skip confirmation prompt"),
) -> None:
    """Drop all tables in a PostgreSQL schema."""
    asyncio.run(drop_async(host, port, database, user, password, schema, confirm))


async def drop_async(
    host: str, port: int, database: str, user: str, password: str | None, schema: str, confirm: bool
) -> None:
    """Async implementation of drop command."""
    # Connect to PostgreSQL
    try:
        conn = await asyncpg.connect(
            user=user, password=password, database=database, host=host, port=port
        )
        logger.info(f"Connected to PostgreSQL database '{database}'")
        rprint(f"[green]✓ Connected to PostgreSQL database '{database}'[/green]")
    except asyncpg.PostgresError as e:
        logger.error(f"Error connecting to PostgreSQL: {e!s}")
        rprint(f"[red]Error connecting to PostgreSQL: {e!s}[/red]")
        raise typer.Exit(1)

    try:
        # Get all tables in the schema
        tables = await conn.fetch(
            """
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = $1
            AND table_type = 'BASE TABLE'
            ORDER BY table_name
            """,
            schema,
        )
        table_names = [row["table_name"] for row in tables]

        if not table_names:
            logger.info(f"No tables found in schema '{schema}'")
            rprint(f"[yellow]No tables found in schema '{schema}'[/yellow]")
            raise typer.Exit(0)

        rprint(f"\n[cyan]Found {len(table_names)} tables in schema '{schema}':[/cyan]")
        for table in table_names[:10]:  # Show first 10
            rprint(f"  • {table}")
        if len(table_names) > 10:
            rprint(f"  ... and {len(table_names) - 10} more")

        # Confirm deletion
        if not confirm:
            rprint(
                f"\n[bold red]Warning: This will drop all {len(table_names)} tables "
                f"in schema '{schema}'![/bold red]"
            )
            confirmed = typer.confirm("Are you sure you want to continue?")
            if not confirmed:
                logger.info("Drop operation aborted by user")
                rprint("[yellow]Aborted[/yellow]")
                raise typer.Exit(0)

        # Drop all tables
        rprint(f"\n[cyan]Dropping {len(table_names)} tables...[/cyan]")

        async with conn.transaction():
            for table_name in table_names:
                full_table_name = f"{schema}.{table_name}"
                await conn.execute(f"DROP TABLE IF EXISTS {full_table_name} CASCADE;")
                rprint(f"  [green]✓ Dropped {full_table_name}[/green]")

        logger.info(f"Successfully dropped {len(table_names)} tables in schema '{schema}'")
        rprint(f"\n[bold green]Successfully dropped all tables in schema '{schema}'[/bold green]")

    except typer.Exit:
        # Re-raise typer.Exit to preserve exit codes
        raise
    except asyncpg.PostgresError as e:
        logger.error(f"PostgreSQL error: {e!s}")
        rprint(f"[red]Error: {e!s}[/red]")
        raise typer.Exit(1)
    except Exception as e:
        logger.error(f"Error: {e!s}")
        rprint(f"[red]Error: {e!s}[/red]")
        raise typer.Exit(1)
    finally:
        await conn.close()
