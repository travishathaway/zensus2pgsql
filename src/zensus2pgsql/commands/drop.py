import psycopg
import typer
from rich import print as rprint

from ._base import (
    DatabaseHost,
    DatabaseName,
    DatabasePassword,
    DatabasePort,
    DatabaseSchema,
    DatabaseUser,
)


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
    # Connect to PostgreSQL
    try:
        conn_str = f"host={host} port={port} dbname={database} user={user} password={password}"
        conn = psycopg.connect(conn_str)
        rprint(f"[green]✓ Connected to PostgreSQL database '{database}'[/green]")
    except psycopg.Error as e:
        rprint(f"[red]Error connecting to PostgreSQL: {e!s}[/red]")
        raise typer.Exit(1)

    try:
        with conn.cursor() as cur:
            # Get all tables in the schema
            cur.execute(
                """
                SELECT table_name
                FROM information_schema.tables
                WHERE table_schema = %s
                AND table_type = 'BASE TABLE'
                ORDER BY table_name
                """,
                (schema,),
            )
            tables = [row[0] for row in cur.fetchall()]

            if not tables:
                rprint(f"[yellow]No tables found in schema '{schema}'[/yellow]")
                raise typer.Exit(0)

            rprint(f"\n[cyan]Found {len(tables)} tables in schema '{schema}':[/cyan]")
            for table in tables[:10]:  # Show first 10
                rprint(f"  • {table}")
            if len(tables) > 10:
                rprint(f"  ... and {len(tables) - 10} more")

            # Confirm deletion
            if not confirm:
                rprint(
                    f"\n[bold red]Warning: This will drop all {len(tables)} tables "
                    f"in schema '{schema}'![/bold red]"
                )
                confirmed = typer.confirm("Are you sure you want to continue?")
                if not confirmed:
                    rprint("[yellow]Aborted[/yellow]")
                    raise typer.Exit(0)

            # Drop all tables
            rprint(f"\n[cyan]Dropping {len(tables)} tables...[/cyan]")
            for table in tables:
                full_table_name = f"{schema}.{table}"
                cur.execute(f"DROP TABLE IF EXISTS {full_table_name} CASCADE;")
                rprint(f"  [green]✓ Dropped {full_table_name}[/green]")

            conn.commit()
            rprint(
                f"\n[bold green]Successfully dropped all tables in schema '{schema}'[/bold green]"
            )

    except Exception as e:
        rprint(f"[red]Error: {e!s}[/red]")
        conn.rollback()
        raise typer.Exit(1)
    finally:
        conn.close()
