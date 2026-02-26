"""Data models for the analyzer module."""

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any


@dataclass
class JoinPattern:
    """Represents a frequently occurring JOIN pattern."""

    left_table: str
    right_table: str
    join_type: str = "INNER"
    join_columns: tuple[str, str] = ("", "")
    frequency: int = 0
    total_time_ms: float = 0.0
    avg_time_ms: float = 0.0

    @property
    def cost_score(self) -> float:
        """Higher score = more impactful to optimize."""
        return self.frequency * self.avg_time_ms

    @property
    def table_pair(self) -> tuple[str, str]:
        """Get sorted table pair for consistent comparison."""
        return tuple(sorted([self.left_table, self.right_table]))

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary."""
        return {
            "left_table": self.left_table,
            "right_table": self.right_table,
            "join_type": self.join_type,
            "join_columns": list(self.join_columns),
            "frequency": self.frequency,
            "total_time_ms": self.total_time_ms,
            "avg_time_ms": self.avg_time_ms,
            "cost_score": self.cost_score,
        }


@dataclass
class MutationPattern:
    """Represents read/write patterns for a table."""

    table: str
    select_count: int = 0
    insert_count: int = 0
    update_count: int = 0
    delete_count: int = 0
    total_time_ms: float = 0.0

    @property
    def total_writes(self) -> int:
        """Total number of write operations."""
        return self.insert_count + self.update_count + self.delete_count

    @property
    def total_operations(self) -> int:
        """Total number of all operations."""
        return self.select_count + self.total_writes

    @property
    def write_ratio(self) -> float:
        """Ratio of writes to total operations."""
        total = self.total_operations
        return self.total_writes / max(total, 1)

    @property
    def read_ratio(self) -> float:
        """Ratio of reads to total operations."""
        return 1.0 - self.write_ratio

    @property
    def is_read_heavy(self) -> bool:
        """Check if table is read-heavy (>70% reads)."""
        return self.read_ratio > 0.7

    @property
    def is_write_heavy(self) -> bool:
        """Check if table is write-heavy (>50% writes)."""
        return self.write_ratio > 0.5

    @property
    def is_update_heavy(self) -> bool:
        """Check if updates dominate writes."""
        return self.update_count > self.insert_count * 2

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary."""
        return {
            "table": self.table,
            "select_count": self.select_count,
            "insert_count": self.insert_count,
            "update_count": self.update_count,
            "delete_count": self.delete_count,
            "total_time_ms": self.total_time_ms,
            "write_ratio": self.write_ratio,
            "read_ratio": self.read_ratio,
            "is_read_heavy": self.is_read_heavy,
            "is_write_heavy": self.is_write_heavy,
        }


@dataclass
class AccessPattern:
    """Represents co-access patterns between tables."""

    table_a: str
    table_b: str
    co_access_count: int = 0
    table_a_solo_count: int = 0
    table_b_solo_count: int = 0

    @property
    def table_pair(self) -> tuple[str, str]:
        """Get sorted table pair."""
        return tuple(sorted([self.table_a, self.table_b]))

    @property
    def co_access_ratio(self) -> float:
        """Ratio of co-accesses to total accesses of either table."""
        total_a = self.co_access_count + self.table_a_solo_count
        total_b = self.co_access_count + self.table_b_solo_count
        min_total = min(total_a, total_b)
        return self.co_access_count / max(min_total, 1)

    @property
    def table_a_independence(self) -> float:
        """How often table_a is accessed alone."""
        total = self.co_access_count + self.table_a_solo_count
        return self.table_a_solo_count / max(total, 1)

    @property
    def table_b_independence(self) -> float:
        """How often table_b is accessed alone."""
        total = self.co_access_count + self.table_b_solo_count
        return self.table_b_solo_count / max(total, 1)

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary."""
        return {
            "table_a": self.table_a,
            "table_b": self.table_b,
            "co_access_count": self.co_access_count,
            "table_a_solo_count": self.table_a_solo_count,
            "table_b_solo_count": self.table_b_solo_count,
            "co_access_ratio": self.co_access_ratio,
            "table_a_independence": self.table_a_independence,
            "table_b_independence": self.table_b_independence,
        }


@dataclass
class TableStatistics:
    """Statistics about a table's usage."""

    table: str
    total_accesses: int = 0
    solo_accesses: int = 0
    joined_accesses: int = 0
    total_time_ms: float = 0.0
    frequently_filtered_columns: list[str] = field(default_factory=list)
    frequently_updated_columns: list[str] = field(default_factory=list)

    @property
    def solo_ratio(self) -> float:
        """Ratio of solo accesses to total."""
        return self.solo_accesses / max(self.total_accesses, 1)

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary."""
        return {
            "table": self.table,
            "total_accesses": self.total_accesses,
            "solo_accesses": self.solo_accesses,
            "joined_accesses": self.joined_accesses,
            "solo_ratio": self.solo_ratio,
            "total_time_ms": self.total_time_ms,
            "frequently_filtered_columns": self.frequently_filtered_columns,
            "frequently_updated_columns": self.frequently_updated_columns,
        }


@dataclass
class AnalysisResult:
    """Complete result of query pattern analysis."""

    # Metadata
    analysis_id: str = ""
    created_at: datetime = field(default_factory=datetime.now)
    source_db_type: str = ""
    total_queries_analyzed: int = 0

    # Patterns
    join_patterns: list[JoinPattern] = field(default_factory=list)
    mutation_patterns: list[MutationPattern] = field(default_factory=list)
    access_patterns: list[AccessPattern] = field(default_factory=list)
    table_statistics: list[TableStatistics] = field(default_factory=list)

    # Summary metrics
    tables_analyzed: list[str] = field(default_factory=list)
    hot_joins_count: int = 0
    embedding_candidates_count: int = 0

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for serialization."""
        return {
            "analysis_id": self.analysis_id,
            "created_at": self.created_at.isoformat(),
            "source_db_type": self.source_db_type,
            "total_queries_analyzed": self.total_queries_analyzed,
            "tables_analyzed": self.tables_analyzed,
            "hot_joins_count": self.hot_joins_count,
            "embedding_candidates_count": self.embedding_candidates_count,
            "join_patterns": [jp.to_dict() for jp in self.join_patterns],
            "mutation_patterns": [mp.to_dict() for mp in self.mutation_patterns],
            "access_patterns": [ap.to_dict() for ap in self.access_patterns],
            "table_statistics": [ts.to_dict() for ts in self.table_statistics],
        }
