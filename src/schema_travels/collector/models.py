"""Data models for the collector module."""

from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any


class QueryType(Enum):
    """Type of SQL query."""
    SELECT = "SELECT"
    INSERT = "INSERT"
    UPDATE = "UPDATE"
    DELETE = "DELETE"
    OTHER = "OTHER"


@dataclass
class QueryLog:
    """Represents a parsed query from database logs."""
    
    sql: str
    timestamp: datetime | None = None
    duration_ms: float | None = None
    rows_affected: int | None = None
    user: str | None = None
    database: str | None = None
    
    # Computed fields
    query_type: QueryType = field(default=QueryType.OTHER)
    normalized_sql: str = ""
    
    def __post_init__(self):
        """Compute derived fields."""
        sql_upper = self.sql.strip().upper()
        if sql_upper.startswith("SELECT"):
            self.query_type = QueryType.SELECT
        elif sql_upper.startswith("INSERT"):
            self.query_type = QueryType.INSERT
        elif sql_upper.startswith("UPDATE"):
            self.query_type = QueryType.UPDATE
        elif sql_upper.startswith("DELETE"):
            self.query_type = QueryType.DELETE
        
        # Basic normalization (replace literals with placeholders)
        self.normalized_sql = self._normalize_sql(self.sql)
    
    def _normalize_sql(self, sql: str) -> str:
        """Normalize SQL by replacing literals with placeholders."""
        import re
        
        # Replace string literals
        normalized = re.sub(r"'[^']*'", "'?'", sql)
        # Replace numeric literals
        normalized = re.sub(r"\b\d+\b", "?", normalized)
        # Normalize whitespace
        normalized = " ".join(normalized.split())
        
        return normalized


@dataclass
class ColumnDefinition:
    """Represents a column in a table."""
    
    name: str
    data_type: str
    nullable: bool = True
    default: str | None = None
    is_primary_key: bool = False


@dataclass
class ForeignKeyDefinition:
    """Represents a foreign key relationship."""
    
    constraint_name: str
    from_table: str
    from_columns: list[str]
    to_table: str
    to_columns: list[str]


@dataclass
class IndexDefinition:
    """Represents an index on a table."""
    
    name: str
    table: str
    columns: list[str]
    is_unique: bool = False
    is_primary: bool = False


@dataclass
class TableDefinition:
    """Represents a table definition."""
    
    name: str
    columns: list[ColumnDefinition] = field(default_factory=list)
    primary_key: list[str] = field(default_factory=list)
    indexes: list[IndexDefinition] = field(default_factory=list)
    
    def get_column(self, name: str) -> ColumnDefinition | None:
        """Get column by name."""
        for col in self.columns:
            if col.name.lower() == name.lower():
                return col
        return None


@dataclass
class SchemaDefinition:
    """Represents a complete database schema."""
    
    tables: list[TableDefinition] = field(default_factory=list)
    foreign_keys: list[ForeignKeyDefinition] = field(default_factory=list)
    source_file: str | None = None
    
    def get_table(self, name: str) -> TableDefinition | None:
        """Get table by name."""
        for table in self.tables:
            if table.name.lower() == name.lower():
                return table
        return None
    
    def get_relationships(self, table_name: str) -> list[ForeignKeyDefinition]:
        """Get all foreign keys involving a table."""
        return [
            fk for fk in self.foreign_keys
            if fk.from_table.lower() == table_name.lower()
            or fk.to_table.lower() == table_name.lower()
        ]
    
    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for serialization."""
        return {
            "tables": [
                {
                    "name": t.name,
                    "columns": [
                        {
                            "name": c.name,
                            "type": c.data_type,
                            "nullable": c.nullable,
                            "default": c.default,
                            "is_primary_key": c.is_primary_key,
                        }
                        for c in t.columns
                    ],
                    "primary_key": t.primary_key,
                }
                for t in self.tables
            ],
            "foreign_keys": [
                {
                    "name": fk.constraint_name,
                    "from_table": fk.from_table,
                    "from_columns": fk.from_columns,
                    "to_table": fk.to_table,
                    "to_columns": fk.to_columns,
                }
                for fk in self.foreign_keys
            ],
        }


@dataclass
class CollectedData:
    """Complete collected data from a database."""
    
    schema: SchemaDefinition
    queries: list[QueryLog]
    statistics: dict[str, Any] = field(default_factory=dict)
    metadata: dict[str, Any] = field(default_factory=dict)
