"""SQLite database connection and schema management."""

import sqlite3
from contextlib import contextmanager
from pathlib import Path
from typing import Iterator

from schema_travels.config import get_settings


class Database:
    """
    SQLite database connection manager.

    Handles connection pooling and schema initialization.
    """

    SCHEMA_VERSION = 1

    SCHEMA_SQL = """
    -- Analysis runs
    CREATE TABLE IF NOT EXISTS analyses (
        id TEXT PRIMARY KEY,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        source_db_type TEXT NOT NULL,
        target_db_type TEXT NOT NULL,
        logs_dir TEXT,
        schema_file TEXT,
        total_queries INTEGER DEFAULT 0,
        tables_analyzed INTEGER DEFAULT 0,
        status TEXT DEFAULT 'pending'
    );

    -- Analysis results (JSON storage)
    CREATE TABLE IF NOT EXISTS analysis_results (
        analysis_id TEXT PRIMARY KEY,
        join_patterns_json TEXT,
        mutation_patterns_json TEXT,
        access_patterns_json TEXT,
        table_statistics_json TEXT,
        FOREIGN KEY (analysis_id) REFERENCES analyses(id)
    );

    -- Recommendations
    CREATE TABLE IF NOT EXISTS recommendations (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        analysis_id TEXT NOT NULL,
        parent_table TEXT NOT NULL,
        child_table TEXT NOT NULL,
        decision TEXT NOT NULL,
        confidence REAL,
        reasoning_json TEXT,
        warnings_json TEXT,
        FOREIGN KEY (analysis_id) REFERENCES analyses(id)
    );

    -- Target schemas
    CREATE TABLE IF NOT EXISTS target_schemas (
        analysis_id TEXT PRIMARY KEY,
        target_type TEXT NOT NULL,
        schema_json TEXT NOT NULL,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (analysis_id) REFERENCES analyses(id)
    );

    -- Simulation results
    CREATE TABLE IF NOT EXISTS simulations (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        analysis_id TEXT NOT NULL,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        result_json TEXT NOT NULL,
        FOREIGN KEY (analysis_id) REFERENCES analyses(id)
    );

    -- Schema version tracking
    CREATE TABLE IF NOT EXISTS schema_version (
        version INTEGER PRIMARY KEY
    );

    -- Indexes
    CREATE INDEX IF NOT EXISTS idx_analyses_created_at ON analyses(created_at);
    CREATE INDEX IF NOT EXISTS idx_recommendations_analysis ON recommendations(analysis_id);
    """

    def __init__(self, db_path: Path | str | None = None):
        """
        Initialize database connection.

        Args:
            db_path: Path to SQLite database file (defaults to config)
        """
        if db_path is None:
            settings = get_settings()
            db_path = settings.db_path

        self.db_path = Path(db_path)
        self._ensure_directory()
        self._init_schema()

    def _ensure_directory(self) -> None:
        """Ensure database directory exists."""
        self.db_path.parent.mkdir(parents=True, exist_ok=True)

    def _init_schema(self) -> None:
        """Initialize database schema."""
        with self.connection() as conn:
            cursor = conn.cursor()
            cursor.executescript(self.SCHEMA_SQL)

            # Check/update schema version
            cursor.execute(
                "INSERT OR IGNORE INTO schema_version (version) VALUES (?)",
                (self.SCHEMA_VERSION,),
            )
            conn.commit()

    @contextmanager
    def connection(self) -> Iterator[sqlite3.Connection]:
        """
        Get a database connection.

        Yields:
            SQLite connection with row factory enabled
        """
        conn = sqlite3.connect(
            self.db_path,
            detect_types=sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES,
        )
        conn.row_factory = sqlite3.Row

        try:
            yield conn
        finally:
            conn.close()

    @contextmanager
    def transaction(self) -> Iterator[sqlite3.Connection]:
        """
        Get a database connection with automatic transaction handling.

        Yields:
            SQLite connection that will commit on success or rollback on error
        """
        with self.connection() as conn:
            try:
                yield conn
                conn.commit()
            except Exception:
                conn.rollback()
                raise

    def execute(
        self,
        sql: str,
        params: tuple | dict | None = None,
    ) -> sqlite3.Cursor:
        """
        Execute a SQL statement.

        Args:
            sql: SQL statement
            params: Query parameters

        Returns:
            Cursor with results
        """
        with self.connection() as conn:
            cursor = conn.cursor()
            if params:
                cursor.execute(sql, params)
            else:
                cursor.execute(sql)
            conn.commit()
            return cursor

    def fetch_one(
        self,
        sql: str,
        params: tuple | dict | None = None,
    ) -> sqlite3.Row | None:
        """
        Fetch a single row.

        Args:
            sql: SQL query
            params: Query parameters

        Returns:
            Row or None if not found
        """
        with self.connection() as conn:
            cursor = conn.cursor()
            if params:
                cursor.execute(sql, params)
            else:
                cursor.execute(sql)
            return cursor.fetchone()

    def fetch_all(
        self,
        sql: str,
        params: tuple | dict | None = None,
    ) -> list[sqlite3.Row]:
        """
        Fetch all rows.

        Args:
            sql: SQL query
            params: Query parameters

        Returns:
            List of rows
        """
        with self.connection() as conn:
            cursor = conn.cursor()
            if params:
                cursor.execute(sql, params)
            else:
                cursor.execute(sql)
            return cursor.fetchall()

    def clear_all(self) -> None:
        """Clear all data from database (for testing)."""
        with self.transaction() as conn:
            cursor = conn.cursor()
            cursor.execute("DELETE FROM simulations")
            cursor.execute("DELETE FROM target_schemas")
            cursor.execute("DELETE FROM recommendations")
            cursor.execute("DELETE FROM analysis_results")
            cursor.execute("DELETE FROM analyses")
