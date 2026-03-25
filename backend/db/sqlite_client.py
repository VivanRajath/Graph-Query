"""SQLite connection and query execution."""

import sqlite3
import os

DB_PATH = os.path.join(os.path.dirname(os.path.dirname(__file__)), "data.db")


def get_connection() -> sqlite3.Connection:
    """Return a connection to the SQLite database (read-only on Vercel)."""
    # Use URI mode for read-only access on serverless (read-only filesystem)
    uri = f"file:{DB_PATH}?mode=ro"
    conn = sqlite3.connect(uri, uri=True)
    conn.row_factory = sqlite3.Row
    return conn


def execute_query(sql: str, params: tuple = ()) -> list[dict]:
    """Execute a parameterized SQL query and return rows as dicts."""
    conn = get_connection()
    result = []
    try:
        cursor = conn.cursor()
        cursor.execute(sql, params)
        columns = [desc[0] for desc in cursor.description] if cursor.description else []
        result = [dict(zip(columns, row)) for row in cursor.fetchall()]
    except Exception as e:
        print(f"[DB] Error executing query: {e}")
    finally:
        conn.close()
    return result


def get_tables() -> list[str]:
    """Return a list of table names in the database."""
    conn = get_connection()
    result = []
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
        result = [row[0] for row in cursor.fetchall()]
    except Exception as e:
        print(f"[DB] Error fetching tables: {e}")
    finally:
        conn.close()
    return result


def get_table_columns(table_name: str) -> list[str]:
    """Return column names for a given table."""
    conn = get_connection()
    result = []
    try:
        cursor = conn.cursor()
        # Prevent SQL injection by checking against existing tables
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
        valid_tables = {row[0] for row in cursor.fetchall()}
        
        if table_name in valid_tables:
            cursor.execute(f"PRAGMA table_info({table_name})")
            result = [row[1] for row in cursor.fetchall()]
    except Exception as e:
        print(f"[DB] Error fetching table columns: {e}")
    finally:
        conn.close()
    return result
