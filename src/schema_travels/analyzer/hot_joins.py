"""Hot joins analyzer - identifies frequently executed, expensive JOINs."""

from collections import defaultdict
from typing import Any

import sqlglot
from sqlglot import exp

from schema_travels.collector.models import QueryLog
from schema_travels.analyzer.models import JoinPattern, TableStatistics


class HotJoinAnalyzer:
    """Analyzes query logs to identify hot (frequent + expensive) JOINs."""

    def __init__(self):
        """Initialize the analyzer."""
        self.join_patterns: dict[tuple[str, str], JoinPattern] = {}
        self.table_stats: dict[str, TableStatistics] = {}
        self._queries_processed = 0

    def analyze(self, queries: list[QueryLog]) -> list[JoinPattern]:
        """
        Analyze queries and return hot join patterns.

        Args:
            queries: List of query logs to analyze

        Returns:
            List of join patterns sorted by cost score (descending)
        """
        for query in queries:
            self._process_query(query)

        # Sort by cost score
        hot_joins = sorted(
            self.join_patterns.values(),
            key=lambda j: j.cost_score,
            reverse=True,
        )

        return hot_joins

    def _process_query(self, query: QueryLog) -> None:
        """Process a single query and extract join patterns."""
        self._queries_processed += 1

        try:
            parsed = sqlglot.parse_one(query.sql)
        except Exception:
            return  # Skip unparseable queries

        # Extract tables
        tables = self._extract_tables(parsed)

        # Update table statistics
        duration = query.duration_ms or 0

        if len(tables) == 1:
            # Solo access
            table = tables[0]
            self._ensure_table_stats(table)
            self.table_stats[table].solo_accesses += 1
            self.table_stats[table].total_accesses += 1
            self.table_stats[table].total_time_ms += duration
        else:
            # Joined access
            for table in tables:
                self._ensure_table_stats(table)
                self.table_stats[table].joined_accesses += 1
                self.table_stats[table].total_accesses += 1
                self.table_stats[table].total_time_ms += duration / len(tables)

        # Extract join patterns
        joins = self._extract_joins(parsed)
        for join_info in joins:
            self._record_join(join_info, duration)

    def _extract_tables(self, parsed: exp.Expression) -> list[str]:
        """Extract all table names from a parsed query."""
        tables = []
        for table_expr in parsed.find_all(exp.Table):
            if hasattr(table_expr, "name") and table_expr.name:
                # Skip common aliases and subqueries
                name = table_expr.name.lower()
                if name not in ("dual", "sysibm.sysdummy1"):
                    tables.append(name)
        return list(set(tables))

    def _extract_joins(self, parsed: exp.Expression) -> list[dict[str, Any]]:
        """Extract join information from parsed query."""
        joins = []

        for join_expr in parsed.find_all(exp.Join):
            join_info = self._parse_join(join_expr, parsed)
            if join_info:
                joins.append(join_info)

        return joins

    def _parse_join(
        self, join_expr: exp.Join, full_query: exp.Expression
    ) -> dict[str, Any] | None:
        """Parse a JOIN expression to extract details."""
        try:
            # Get join type
            join_kind = join_expr.kind or "INNER"
            if isinstance(join_kind, str):
                join_type = join_kind.upper()
            else:
                join_type = "INNER"

            # Get right table
            right_table = None
            if hasattr(join_expr.this, "name"):
                right_table = join_expr.this.name.lower()
            elif hasattr(join_expr.this, "alias"):
                # Subquery with alias
                return None

            if not right_table:
                return None

            # Try to find left table from ON condition
            on_condition = join_expr.args.get("on")
            left_table = None
            left_col = ""
            right_col = ""

            if on_condition:
                columns = list(on_condition.find_all(exp.Column))
                if len(columns) >= 2:
                    col1 = columns[0]
                    col2 = columns[1]

                    # Determine which column belongs to which table
                    col1_table = col1.table.lower() if col1.table else ""
                    col2_table = col2.table.lower() if col2.table else ""

                    if col1_table == right_table:
                        right_col = col1.name
                        left_table = col2_table
                        left_col = col2.name
                    elif col2_table == right_table:
                        right_col = col2.name
                        left_table = col1_table
                        left_col = col1.name
                    else:
                        # Can't determine - use first as left
                        left_table = col1_table
                        left_col = col1.name
                        right_col = col2.name

            # If we couldn't find left table, try to infer from FROM clause
            if not left_table:
                all_tables = self._extract_tables(full_query)
                other_tables = [t for t in all_tables if t != right_table]
                if other_tables:
                    left_table = other_tables[0]

            if not left_table:
                return None

            return {
                "left_table": left_table,
                "right_table": right_table,
                "join_type": join_type,
                "left_col": left_col,
                "right_col": right_col,
            }

        except Exception:
            return None

    def _record_join(self, join_info: dict[str, Any], duration_ms: float) -> None:
        """Record a join occurrence."""
        # Use sorted tuple as key for consistent lookup
        key = tuple(sorted([join_info["left_table"], join_info["right_table"]]))

        if key not in self.join_patterns:
            self.join_patterns[key] = JoinPattern(
                left_table=key[0],
                right_table=key[1],
                join_type=join_info["join_type"],
                join_columns=(join_info["left_col"], join_info["right_col"]),
            )

        pattern = self.join_patterns[key]
        pattern.frequency += 1
        pattern.total_time_ms += duration_ms
        pattern.avg_time_ms = pattern.total_time_ms / pattern.frequency

    def _ensure_table_stats(self, table: str) -> None:
        """Ensure table statistics entry exists."""
        if table not in self.table_stats:
            self.table_stats[table] = TableStatistics(table=table)

    def get_table_statistics(self) -> list[TableStatistics]:
        """Get statistics for all tables."""
        return list(self.table_stats.values())

    def get_hot_joins(self, top_n: int = 20) -> list[JoinPattern]:
        """Get top N hot joins by cost score."""
        sorted_joins = sorted(
            self.join_patterns.values(),
            key=lambda j: j.cost_score,
            reverse=True,
        )
        return sorted_joins[:top_n]

    def get_co_access_matrix(self) -> dict[tuple[str, str], int]:
        """Get co-access frequency matrix for all table pairs."""
        return {key: pattern.frequency for key, pattern in self.join_patterns.items()}

    @property
    def queries_processed(self) -> int:
        """Number of queries processed."""
        return self._queries_processed
