"""Tests for the drop command."""

from unittest.mock import AsyncMock, MagicMock, patch

import asyncpg
import pytest
import typer

from tests.conftest import MockTransaction
from zensus2pgsql.commands.drop import drop, drop_async


class TestConnectionHandling:
    """Tests for database connection establishment and errors."""

    @pytest.mark.asyncio
    async def test_successful_connection(self):
        """Test successful database connection logs and prints correctly."""
        mock_conn = AsyncMock()
        mock_conn.fetch = AsyncMock(return_value=[])  # No tables
        mock_conn.close = AsyncMock()

        with patch("zensus2pgsql.commands.drop.asyncpg.connect", return_value=mock_conn):
            with patch("zensus2pgsql.commands.drop.rprint") as mock_rprint:
                with patch("zensus2pgsql.commands.drop.logger") as mock_logger:
                    with pytest.raises(typer.Exit) as exc_info:
                        await drop_async(
                            "localhost", 5432, "testdb", "testuser", "testpass", "public", True
                        )

                    # Should exit with 0 (no tables)
                    assert exc_info.value.exit_code == 0

                    # Verify connection logging
                    mock_logger.info.assert_any_call("Connected to PostgreSQL database 'testdb'")

                    # Verify rich print
                    assert any("[green]" in str(call) for call in mock_rprint.call_args_list)

    @pytest.mark.asyncio
    async def test_connection_failure_postgres_error(self):
        """Test handling of PostgreSQL connection errors."""

        async def mock_connect_fail(*args, **kwargs):
            raise asyncpg.PostgresConnectionError("Connection refused")

        with patch("zensus2pgsql.commands.drop.asyncpg.connect", side_effect=mock_connect_fail):
            with patch("zensus2pgsql.commands.drop.rprint") as mock_rprint:
                with patch("zensus2pgsql.commands.drop.logger") as mock_logger:
                    with pytest.raises(typer.Exit) as exc_info:
                        await drop_async(
                            "localhost", 5432, "testdb", "testuser", "testpass", "public", True
                        )

                    # Should exit with 1
                    assert exc_info.value.exit_code == 1

                    # Verify error logging
                    mock_logger.error.assert_called()
                    error_call = str(mock_logger.error.call_args)
                    assert "Connection refused" in error_call

    @pytest.mark.asyncio
    async def test_connection_with_none_password(self):
        """Test connection when password is None."""
        mock_conn = AsyncMock()
        mock_conn.fetch = AsyncMock(return_value=[])
        mock_conn.close = AsyncMock()

        with patch(
            "zensus2pgsql.commands.drop.asyncpg.connect", return_value=mock_conn
        ) as mock_connect:
            with patch("zensus2pgsql.commands.drop.rprint"):
                with patch("zensus2pgsql.commands.drop.logger"):
                    with pytest.raises(typer.Exit) as exc_info:
                        await drop_async(
                            "localhost", 5432, "testdb", "testuser", None, "public", True
                        )

            # Verifyassert exc_info.value.exit_code == 0

            # Verify connect was called with password=None
            call_kwargs = mock_connect.call_args.kwargs
            assert call_kwargs["password"] is None


class TestTableDiscovery:
    """Tests for fetching and listing tables from schema."""

    @pytest.mark.asyncio
    async def test_fetch_tables_empty_schema(self):
        """Test behavior when schema has no tables."""
        mock_conn = AsyncMock()
        mock_conn.fetch = AsyncMock(return_value=[])  # No tables
        mock_conn.close = AsyncMock()

        with patch("zensus2pgsql.commands.drop.asyncpg.connect", return_value=mock_conn):
            with patch("zensus2pgsql.commands.drop.rprint") as mock_rprint:
                with patch("zensus2pgsql.commands.drop.logger") as mock_logger:
                    with pytest.raises(typer.Exit) as exc_info:
                        await drop_async(
                            "localhost", 5432, "testdb", "testuser", "testpass", "public", True
                        )

                    # Should exit with 0
                    assert exc_info.value.exit_code == 0

                    # Verify "no tables found" message
                    mock_logger.info.assert_any_call("No tables found in schema 'public'")
                    assert any(
                        "[yellow]" in str(call) and "No tables found" in str(call)
                        for call in mock_rprint.call_args_list
                    )

    @pytest.mark.asyncio
    async def test_fetch_tables_single_table(self):
        """Test discovering a single table in schema."""
        mock_conn = AsyncMock()
        mock_conn.fetch = AsyncMock(return_value=[{"table_name": "test_table"}])
        mock_conn.close = AsyncMock()
        mock_conn.execute = AsyncMock()
        mock_conn.transaction = MagicMock(return_value=MockTransaction())

        with patch("zensus2pgsql.commands.drop.asyncpg.connect", return_value=mock_conn):
            with patch("zensus2pgsql.commands.drop.rprint") as mock_rprint:
                with patch("zensus2pgsql.commands.drop.logger"):
                    # Should complete successfully without raising
                    await drop_async(
                        "localhost", 5432, "testdb", "testuser", "testpass", "public", True
                    )

            # Verify table count displayed
            assert any("Found 1 tables" in str(call) for call in mock_rprint.call_args_list)

    @pytest.mark.asyncio
    async def test_fetch_tables_shows_first_10(self):
        """Test that only first 10 tables are displayed when many exist."""
        # Create 15 mock tables
        tables = [{"table_name": f"table_{i:02d}"} for i in range(15)]

        mock_conn = AsyncMock()
        mock_conn.fetch = AsyncMock(return_value=tables)
        mock_conn.close = AsyncMock()
        mock_conn.execute = AsyncMock()
        mock_conn.transaction = MagicMock(return_value=MockTransaction())

        with patch("zensus2pgsql.commands.drop.asyncpg.connect", return_value=mock_conn):
            with patch("zensus2pgsql.commands.drop.rprint") as mock_rprint:
                with patch("zensus2pgsql.commands.drop.logger"):
                    # Should complete successfully without raising
                    await drop_async(
                        "localhost", 5432, "testdb", "testuser", "testpass", "public", True
                    )

            # Verify "... and 5 more" message
            assert any("and 5 more" in str(call) for call in mock_rprint.call_args_list)

            # Verify first table shown
            assert any("table_00" in str(call) for call in mock_rprint.call_args_list)

    @pytest.mark.asyncio
    async def test_fetch_tables_uses_parameterized_query(self):
        """Test that query uses parameterized schema and ORDER BY."""
        mock_conn = AsyncMock()
        mock_conn.fetch = AsyncMock(return_value=[{"table_name": "test"}])
        mock_conn.close = AsyncMock()
        mock_conn.execute = AsyncMock()
        mock_conn.transaction = MagicMock(return_value=MockTransaction())

        with patch("zensus2pgsql.commands.drop.asyncpg.connect", return_value=mock_conn):
            with patch("zensus2pgsql.commands.drop.rprint"):
                with patch("zensus2pgsql.commands.drop.logger"):
                    # Should complete successfully without raising
                    await drop_async(
                        "localhost", 5432, "testdb", "testuser", "testpass", "myschema", True
                    )

        # Verify query includes ORDER BY
        fetch_call = mock_conn.fetch.call_args
        query = fetch_call[0][0]
        assert "ORDER BY table_name" in query
        assert "$1" in query

        # Verify schema passed as parameter
        assert fetch_call[0][1] == "myschema"


class TestUserConfirmation:
    """Tests for confirmation prompts and --confirm flag."""

    @pytest.mark.asyncio
    async def test_confirmation_prompt_when_confirm_false(self):
        """Test that typer.confirm is called when confirm=False."""
        mock_conn = AsyncMock()
        mock_conn.fetch = AsyncMock(return_value=[{"table_name": "test_table"}])
        mock_conn.close = AsyncMock()

        with patch("zensus2pgsql.commands.drop.asyncpg.connect", return_value=mock_conn):
            with patch(
                "zensus2pgsql.commands.drop.typer.confirm", return_value=False
            ) as mock_confirm:
                with patch("zensus2pgsql.commands.drop.rprint"):
                    with patch("zensus2pgsql.commands.drop.logger"):
                        with pytest.raises(typer.Exit) as exc_info:
                            await drop_async(
                                "localhost", 5432, "testdb", "testuser", "testpass", "public", False
                            )

                        # Should exit with 0 (user cancelled)
                        assert exc_info.value.exit_code == 0

                        # Verify confirm was called
                        mock_confirm.assert_called_once()
                        assert "Are you sure" in str(mock_confirm.call_args)

    @pytest.mark.asyncio
    async def test_user_confirms_yes(self):
        """Test successful drop when user confirms."""
        mock_conn = AsyncMock()
        mock_conn.fetch = AsyncMock(return_value=[{"table_name": "test_table"}])
        mock_conn.close = AsyncMock()
        mock_conn.execute = AsyncMock()
        mock_conn.transaction = MagicMock(return_value=MockTransaction())

        with patch("zensus2pgsql.commands.drop.asyncpg.connect", return_value=mock_conn):
            with patch("zensus2pgsql.commands.drop.typer.confirm", return_value=True):
                with patch("zensus2pgsql.commands.drop.rprint"):
                    with patch("zensus2pgsql.commands.drop.logger"):
                        # Should complete successfully
                        await drop_async(
                            "localhost", 5432, "testdb", "testuser", "testpass", "public", False
                        )

                        # Verify DROP was executed
                        mock_conn.execute.assert_called()

    @pytest.mark.asyncio
    async def test_user_confirms_no_abort(self):
        """Test abort when user declines confirmation."""
        mock_conn = AsyncMock()
        mock_conn.fetch = AsyncMock(return_value=[{"table_name": "test_table"}])
        mock_conn.close = AsyncMock()
        mock_conn.execute = AsyncMock()

        with patch("zensus2pgsql.commands.drop.asyncpg.connect", return_value=mock_conn):
            with patch("zensus2pgsql.commands.drop.typer.confirm", return_value=False):
                with patch("zensus2pgsql.commands.drop.rprint") as mock_rprint:
                    with patch("zensus2pgsql.commands.drop.logger") as mock_logger:
                        with pytest.raises(typer.Exit) as exc_info:
                            await drop_async(
                                "localhost", 5432, "testdb", "testuser", "testpass", "public", False
                            )

                        # Should exit with 0
                        assert exc_info.value.exit_code == 0

                        # Verify abort message
                        mock_logger.info.assert_any_call("Drop operation aborted by user")
                        assert any(
                            "[yellow]Aborted[/yellow]" in str(call)
                            for call in mock_rprint.call_args_list
                        )

                        # Verify no DROP was executed
                        mock_conn.execute.assert_not_called()

    @pytest.mark.asyncio
    async def test_skip_confirmation_with_confirm_flag(self):
        """Test that --confirm flag skips the prompt."""
        mock_conn = AsyncMock()
        mock_conn.fetch = AsyncMock(return_value=[{"table_name": "test_table"}])
        mock_conn.close = AsyncMock()
        mock_conn.execute = AsyncMock()
        mock_conn.transaction = MagicMock(return_value=MockTransaction())

        with patch("zensus2pgsql.commands.drop.asyncpg.connect", return_value=mock_conn):
            with patch("zensus2pgsql.commands.drop.typer.confirm") as mock_confirm:
                with patch("zensus2pgsql.commands.drop.rprint"):
                    with patch("zensus2pgsql.commands.drop.logger"):
                        # Should complete successfully
                        await drop_async(
                            "localhost", 5432, "testdb", "testuser", "testpass", "public", True
                        )

                        # Should NOT call confirm
                        mock_confirm.assert_not_called()

                        # Should execute DROP
                        mock_conn.execute.assert_called()


class TestDropOperations:
    """Tests for actual table dropping within transaction."""

    @pytest.mark.asyncio
    async def test_drop_single_table_in_transaction(self):
        """Test dropping a single table within transaction."""
        mock_conn = AsyncMock()
        mock_conn.fetch = AsyncMock(return_value=[{"table_name": "my_table"}])
        mock_conn.close = AsyncMock()
        mock_conn.execute = AsyncMock()

        mock_transaction = MockTransaction()
        mock_conn.transaction = MagicMock(return_value=mock_transaction)

        with patch("zensus2pgsql.commands.drop.asyncpg.connect", return_value=mock_conn):
            with patch("zensus2pgsql.commands.drop.rprint"):
                with patch("zensus2pgsql.commands.drop.logger"):
                    # Should complete successfully
                    await drop_async(
                        "localhost", 5432, "testdb", "testuser", "testpass", "test_schema", True
                    )

        # Verify transaction was used
        mock_conn.transaction.assert_called_once()

        # Verify DROP TABLE was executed
        mock_conn.execute.assert_called_once()
        drop_call = str(mock_conn.execute.call_args)
        assert "DROP TABLE IF EXISTS test_schema.my_table CASCADE" in drop_call

    @pytest.mark.asyncio
    async def test_drop_multiple_tables(self):
        """Test dropping multiple tables in one transaction."""
        tables = [{"table_name": "table_a"}, {"table_name": "table_b"}, {"table_name": "table_c"}]

        mock_conn = AsyncMock()
        mock_conn.fetch = AsyncMock(return_value=tables)
        mock_conn.close = AsyncMock()
        mock_conn.execute = AsyncMock()
        mock_conn.transaction = MagicMock(return_value=MockTransaction())

        with patch("zensus2pgsql.commands.drop.asyncpg.connect", return_value=mock_conn):
            with patch("zensus2pgsql.commands.drop.rprint") as mock_rprint:
                with patch("zensus2pgsql.commands.drop.logger"):
                    # Should complete successfully
                    await drop_async(
                        "localhost", 5432, "testdb", "testuser", "testpass", "public", True
                    )

        # Verify execute called 3 times (once per table)
        assert mock_conn.execute.call_count == 3

        # Verify all tables were dropped
        execute_calls = [str(call) for call in mock_conn.execute.call_args_list]
        assert any("table_a" in call for call in execute_calls)
        assert any("table_b" in call for call in execute_calls)
        assert any("table_c" in call for call in execute_calls)

        # Verify success message shown for each table
        rprint_calls = [str(call) for call in mock_rprint.call_args_list]
        assert sum("Dropped" in call for call in rprint_calls) == 3

    @pytest.mark.asyncio
    async def test_drop_uses_cascade(self):
        """Test that DROP TABLE uses CASCADE option."""
        mock_conn = AsyncMock()
        mock_conn.fetch = AsyncMock(return_value=[{"table_name": "parent_table"}])
        mock_conn.close = AsyncMock()
        mock_conn.execute = AsyncMock()
        mock_conn.transaction = MagicMock(return_value=MockTransaction())

        with patch("zensus2pgsql.commands.drop.asyncpg.connect", return_value=mock_conn):
            with patch("zensus2pgsql.commands.drop.rprint"):
                with patch("zensus2pgsql.commands.drop.logger"):
                    # Should complete successfully
                    await drop_async(
                        "localhost", 5432, "testdb", "testuser", "testpass", "public", True
                    )

        # Verify CASCADE is in the DROP statement
        drop_call = str(mock_conn.execute.call_args)
        assert "CASCADE" in drop_call

    @pytest.mark.asyncio
    async def test_drop_uses_if_exists(self):
        """Test that DROP TABLE uses IF EXISTS."""
        mock_conn = AsyncMock()
        mock_conn.fetch = AsyncMock(return_value=[{"table_name": "some_table"}])
        mock_conn.close = AsyncMock()
        mock_conn.execute = AsyncMock()
        mock_conn.transaction = MagicMock(return_value=MockTransaction())

        with patch("zensus2pgsql.commands.drop.asyncpg.connect", return_value=mock_conn):
            with patch("zensus2pgsql.commands.drop.rprint"):
                with patch("zensus2pgsql.commands.drop.logger"):
                    # Should complete successfully
                    await drop_async(
                        "localhost", 5432, "testdb", "testuser", "testpass", "public", True
                    )

        # Verify IF EXISTS is in the DROP statement
        drop_call = str(mock_conn.execute.call_args)
        assert "IF EXISTS" in drop_call

    @pytest.mark.asyncio
    async def test_drop_qualified_table_names(self):
        """Test that table names are properly qualified with schema."""
        mock_conn = AsyncMock()
        mock_conn.fetch = AsyncMock(return_value=[{"table_name": "users"}])
        mock_conn.close = AsyncMock()
        mock_conn.execute = AsyncMock()
        mock_conn.transaction = MagicMock(return_value=MockTransaction())

        with patch("zensus2pgsql.commands.drop.asyncpg.connect", return_value=mock_conn):
            with patch("zensus2pgsql.commands.drop.rprint"):
                with patch("zensus2pgsql.commands.drop.logger"):
                    # Should complete successfully
                    await drop_async(
                        "localhost", 5432, "testdb", "testuser", "testpass", "myschema", True
                    )

        # Verify schema.table format
        drop_call = str(mock_conn.execute.call_args)
        assert "myschema.users" in drop_call


class TestErrorHandling:
    """Tests for error conditions and proper exit codes."""

    @pytest.mark.asyncio
    async def test_postgres_error_during_fetch(self):
        """Test handling of PostgreSQL errors during table fetch."""
        mock_conn = AsyncMock()
        mock_conn.fetch = AsyncMock(side_effect=asyncpg.PostgresError("Query failed"))
        mock_conn.close = AsyncMock()

        with patch("zensus2pgsql.commands.drop.asyncpg.connect", return_value=mock_conn):
            with patch("zensus2pgsql.commands.drop.rprint") as mock_rprint:
                with patch("zensus2pgsql.commands.drop.logger") as mock_logger:
                    with pytest.raises(typer.Exit) as exc_info:
                        await drop_async(
                            "localhost", 5432, "testdb", "testuser", "testpass", "public", True
                        )

                    # Should exit with 1
                    assert exc_info.value.exit_code == 1

                    # Verify error logged
                    mock_logger.error.assert_called()
                    assert any(
                        "PostgreSQL error" in str(call) for call in mock_logger.error.call_args_list
                    )

    @pytest.mark.asyncio
    async def test_postgres_error_during_drop(self):
        """Test handling of PostgreSQL errors during DROP operation."""
        mock_conn = AsyncMock()
        mock_conn.fetch = AsyncMock(return_value=[{"table_name": "locked_table"}])
        mock_conn.close = AsyncMock()
        mock_conn.execute = AsyncMock(side_effect=asyncpg.PostgresError("Table is locked"))
        mock_conn.transaction = MagicMock(return_value=MockTransaction())

        with patch("zensus2pgsql.commands.drop.asyncpg.connect", return_value=mock_conn):
            with patch("zensus2pgsql.commands.drop.rprint"):
                with patch("zensus2pgsql.commands.drop.logger") as mock_logger:
                    with pytest.raises(typer.Exit) as exc_info:
                        await drop_async(
                            "localhost", 5432, "testdb", "testuser", "testpass", "public", True
                        )

                    # Should exit with 1
                    assert exc_info.value.exit_code == 1

                    # Verify error logged
                    assert any(
                        "Table is locked" in str(call) for call in mock_logger.error.call_args_list
                    )

    @pytest.mark.asyncio
    async def test_generic_exception_handling(self):
        """Test handling of non-PostgreSQL exceptions."""
        mock_conn = AsyncMock()
        mock_conn.fetch = AsyncMock(side_effect=RuntimeError("Unexpected error"))
        mock_conn.close = AsyncMock()

        with patch("zensus2pgsql.commands.drop.asyncpg.connect", return_value=mock_conn):
            with patch("zensus2pgsql.commands.drop.rprint"):
                with patch("zensus2pgsql.commands.drop.logger") as mock_logger:
                    with pytest.raises(typer.Exit) as exc_info:
                        await drop_async(
                            "localhost", 5432, "testdb", "testuser", "testpass", "public", True
                        )

                    # Should exit with 1
                    assert exc_info.value.exit_code == 1

                    # Verify generic error handler caught it
                    assert any("Error:" in str(call) for call in mock_logger.error.call_args_list)


class TestCleanup:
    """Tests for connection cleanup in finally block."""

    @pytest.mark.asyncio
    async def test_connection_closed_on_success(self):
        """Test that connection is closed after successful drop."""
        mock_conn = AsyncMock()
        mock_conn.fetch = AsyncMock(return_value=[])
        mock_conn.close = AsyncMock()

        with patch("zensus2pgsql.commands.drop.asyncpg.connect", return_value=mock_conn):
            with patch("zensus2pgsql.commands.drop.rprint"):
                with patch("zensus2pgsql.commands.drop.logger"):
                    # Should complete successfully
                    with pytest.raises(typer.Exit) as exc_info:
                        await drop_async(
                            "localhost", 5432, "testdb", "testuser", "testpass", "public", True
                        )

        # Should exit with 0
        assert exc_info.value.exit_code == 0

        # Verify close was called
        mock_conn.close.assert_called_once()

    @pytest.mark.asyncio
    async def test_connection_closed_on_error(self):
        """Test that connection is closed even when errors occur."""
        mock_conn = AsyncMock()
        mock_conn.fetch = AsyncMock(side_effect=asyncpg.PostgresError("Error"))
        mock_conn.close = AsyncMock()

        with patch("zensus2pgsql.commands.drop.asyncpg.connect", return_value=mock_conn):
            with patch("zensus2pgsql.commands.drop.rprint"):
                with patch("zensus2pgsql.commands.drop.logger"):
                    with pytest.raises(typer.Exit) as exc_info:
                        await drop_async(
                            "localhost", 5432, "testdb", "testuser", "testpass", "public", True
                        )

        # Should exit with 1
        assert exc_info.value.exit_code == 1

        # Verify close was still called
        mock_conn.close.assert_called_once()

    @pytest.mark.asyncio
    async def test_connection_closed_on_user_abort(self):
        """Test that connection is closed when user cancels."""
        mock_conn = AsyncMock()
        mock_conn.fetch = AsyncMock(return_value=[{"table_name": "test"}])
        mock_conn.close = AsyncMock()

        with patch("zensus2pgsql.commands.drop.asyncpg.connect", return_value=mock_conn):
            with patch("zensus2pgsql.commands.drop.typer.confirm", return_value=False):
                with patch("zensus2pgsql.commands.drop.rprint"):
                    with patch("zensus2pgsql.commands.drop.logger"):
                        with pytest.raises(typer.Exit) as exc_info:
                            await drop_async(
                                "localhost", 5432, "testdb", "testuser", "testpass", "public", False
                            )

        # Verify close was calledassert exc_info.value.exit_code == 0

        # Verify close was called
        mock_conn.close.assert_called_once()


class TestCLIIntegration:
    """Tests for CLI entry point and sync wrapper."""

    def test_drop_sync_wrapper_calls_asyncio_run(self):
        """Test that drop() calls asyncio.run with drop_async()."""
        with patch("zensus2pgsql.commands.drop.asyncio.run") as mock_run:
            # Create a mock coroutine and close it to avoid warnings
            async def mock_coro():
                pass

            def capture_coro(coro):
                coro.close()

            mock_run.side_effect = capture_coro

            drop(
                host="localhost",
                port=5432,
                database="test",
                user="user",
                password="pass",
                schema="public",
                confirm=True,
            )

            # Verify asyncio.run was called
            mock_run.assert_called_once()

    def test_drop_help_command(self, cli_runner):
        """Test that drop --help works."""
        from zensus2pgsql.cli import app

        result = cli_runner.invoke(app, ["drop", "--help"])

        assert result.exit_code == 0
        assert "Drop all tables in a PostgreSQL schema" in result.stdout
        assert "--confirm" in result.stdout
        assert "--schema" in result.stdout

    def test_drop_with_confirm_flag(self):
        """Test drop command with --confirm flag skips prompt."""
        mock_conn = AsyncMock()
        mock_conn.fetch = AsyncMock(return_value=[])
        mock_conn.close = AsyncMock()

        async def mock_drop_async(*args, **kwargs):
            pass

        with patch("zensus2pgsql.commands.drop.asyncio.run") as mock_run:
            with patch("zensus2pgsql.commands.drop.drop_async", side_effect=mock_drop_async):
                drop(
                    host="localhost",
                    port=5432,
                    database="test",
                    user="user",
                    password="pass",
                    schema="public",
                    confirm=True,
                )

                # Verify asyncio.run was called
                assert mock_run.called
