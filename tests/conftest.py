"""Pytest configuration and fixtures."""

import pytest
from pathlib import Path
import tempfile
import os


@pytest.fixture
def temp_dir():
    """Create a temporary directory for tests."""
    with tempfile.TemporaryDirectory() as tmpdir:
        yield Path(tmpdir)


@pytest.fixture
def sample_schema_sql():
    """Sample SQL schema for testing."""
    return """
    CREATE TABLE users (
        id SERIAL PRIMARY KEY,
        email VARCHAR(255) NOT NULL,
        name VARCHAR(100),
        created_at TIMESTAMP DEFAULT NOW()
    );

    CREATE TABLE orders (
        id SERIAL PRIMARY KEY,
        user_id INTEGER REFERENCES users(id),
        total DECIMAL(10,2),
        status VARCHAR(50) DEFAULT 'pending',
        created_at TIMESTAMP DEFAULT NOW()
    );

    CREATE TABLE order_items (
        id SERIAL PRIMARY KEY,
        order_id INTEGER REFERENCES orders(id),
        product_id INTEGER,
        quantity INTEGER DEFAULT 1,
        price DECIMAL(10,2)
    );
    """


@pytest.fixture
def sample_postgres_log():
    """Sample PostgreSQL log content for testing."""
    return """2024-01-15 10:30:45.123 UTC [12345] app@testdb LOG:  statement: SELECT * FROM users WHERE id = 1
2024-01-15 10:30:45.125 UTC [12345] app@testdb LOG:  duration: 2.345 ms
2024-01-15 10:30:46.000 UTC [12345] app@testdb LOG:  statement: SELECT u.name, o.total FROM users u JOIN orders o ON u.id = o.user_id WHERE u.id = 1
2024-01-15 10:30:46.015 UTC [12345] app@testdb LOG:  duration: 15.234 ms
"""


@pytest.fixture
def sample_mysql_log():
    """Sample MySQL slow query log content for testing."""
    return """# Time: 2024-01-15T10:30:45.123456Z
# User@Host: app[app] @ localhost []
# Query_time: 0.002345  Lock_time: 0.000000 Rows_sent: 1  Rows_examined: 1
SELECT * FROM users WHERE id = 1;
# Time: 2024-01-15T10:30:46.000000Z
# User@Host: app[app] @ localhost []
# Query_time: 0.015234  Lock_time: 0.000000 Rows_sent: 5  Rows_examined: 100
SELECT u.name, o.total FROM users u JOIN orders o ON u.id = o.user_id WHERE u.id = 1;
"""


@pytest.fixture
def mock_env_vars(monkeypatch):
    """Set up mock environment variables for testing."""
    monkeypatch.setenv("ANTHROPIC_API_KEY", "test-api-key")
    monkeypatch.setenv("ANTHROPIC_MODEL", "claude-sonnet-4-20250514")
    monkeypatch.setenv("DATABASE_PATH", ":memory:")


@pytest.fixture
def test_database(temp_dir):
    """Create a test database."""
    from schema_travels.persistence.database import Database
    
    db_path = temp_dir / "test.db"
    db = Database(db_path)
    yield db
    # Cleanup happens automatically with temp_dir


@pytest.fixture
def test_repository(test_database):
    """Create a test repository."""
    from schema_travels.persistence.repository import AnalysisRepository
    
    return AnalysisRepository(test_database)
