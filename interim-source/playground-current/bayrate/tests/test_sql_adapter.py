import unittest

from bayrate import sql_adapter


class SqlAdapterTest(unittest.TestCase):
    """Represent SQL adapter tests."""

    def test_query_rows_via_odbc_commits_after_fetch(self):
        """Verify that row-returning calls commit after fetch."""
        fake_pyodbc = _FakePyodbc()
        original_pyodbc = sql_adapter.pyodbc
        try:
            sql_adapter.pyodbc = fake_pyodbc

            rows = sql_adapter._query_rows_via_odbc("conn", "EXEC dbo.write_and_return @Value = ?", ("value",))
        finally:
            sql_adapter.pyodbc = original_pyodbc

        self.assertEqual(rows, [{"Result": "ok"}])
        self.assertEqual(fake_pyodbc.connection.cursor_obj.executed, [("EXEC dbo.write_and_return @Value = ?", ("value",))])
        self.assertTrue(fake_pyodbc.connection.committed)
        self.assertFalse(fake_pyodbc.connection.rolled_back)
        self.assertTrue(fake_pyodbc.connection.closed)
        self.assertTrue(fake_pyodbc.connection.cursor_obj.closed)


class _FakePyodbc:
    """Represent fake pyodbc module."""

    def __init__(self):
        """Initialize the fake pyodbc module."""
        self.connection = _FakeSqlConnection()

    def connect(self, conn_str):
        """Connect to fake SQL."""
        self.connection.conn_str = conn_str
        return self.connection


class _FakeSqlConnection:
    """Represent fake SQL connection."""

    def __init__(self):
        """Initialize the fake SQL connection."""
        self.conn_str = None
        self.cursor_obj = _FakeSqlCursor()
        self.committed = False
        self.rolled_back = False
        self.closed = False

    def cursor(self):
        """Return fake cursor."""
        return self.cursor_obj

    def commit(self):
        """Commit fake transaction."""
        self.committed = True

    def rollback(self):
        """Rollback fake transaction."""
        self.rolled_back = True

    def close(self):
        """Close fake connection."""
        self.closed = True


class _FakeSqlCursor:
    """Represent fake SQL cursor."""

    description = [("Result",)]

    def __init__(self):
        """Initialize fake SQL cursor."""
        self.executed = []
        self.closed = False

    def execute(self, query, *params):
        """Execute fake SQL."""
        self.executed.append((query, tuple(params)))

    def fetchall(self):
        """Fetch fake rows."""
        return [("ok",)]

    def close(self):
        """Close fake cursor."""
        self.closed = True


if __name__ == "__main__":
    unittest.main()
