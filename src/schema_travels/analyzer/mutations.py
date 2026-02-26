"""Mutation analyzer - tracks read/write patterns per table."""

from collections import defaultdict

import sqlglot
from sqlglot import exp

from schema_travels.collector.models import QueryLog, QueryType
from schema_travels.analyzer.models import MutationPattern


class MutationAnalyzer:
    """Analyzes query logs to track read/write patterns per table."""

    def __init__(self):
        """Initialize the analyzer."""
        self.patterns: dict[str, MutationPattern] = {}
        self.updated_columns: dict[str, dict[str, int]] = defaultdict(lambda: defaultdict(int))
        self.filtered_columns: dict[str, dict[str, int]] = defaultdict(lambda: defaultdict(int))
        self._queries_processed = 0

    def analyze(self, queries: list[QueryLog]) -> dict[str, MutationPattern]:
        """
        Analyze queries and return mutation patterns per table.

        Args:
            queries: List of query logs to analyze

        Returns:
            Dictionary mapping table names to mutation patterns
        """
        for query in queries:
            self._process_query(query)

        return self.patterns

    def _process_query(self, query: QueryLog) -> None:
        """Process a single query and update mutation patterns."""
        self._queries_processed += 1
        duration = query.duration_ms or 0

        try:
            parsed = sqlglot.parse_one(query.sql)
        except Exception:
            return  # Skip unparseable queries

        if parsed is None:
            return

        # Dispatch based on statement type
        stmt_type = type(parsed).__name__

        if isinstance(parsed, exp.Select):
            self._process_select(parsed, duration)
        elif isinstance(parsed, exp.Insert):
            self._process_insert(parsed, duration)
        elif isinstance(parsed, exp.Update):
            self._process_update(parsed, duration)
        elif isinstance(parsed, exp.Delete):
            self._process_delete(parsed, duration)

    def _ensure_pattern(self, table: str) -> None:
        """Ensure mutation pattern exists for table."""
        table = table.lower()
        if table not in self.patterns:
            self.patterns[table] = MutationPattern(table=table)

    def _get_table_name(self, expr) -> str | None:
        """Extract table name from various expression types."""
        if expr is None:
            return None
            
        # Direct name attribute
        if hasattr(expr, "name") and expr.name:
            return expr.name.lower()
        
        # Table expression
        if isinstance(expr, exp.Table):
            if hasattr(expr, "name") and expr.name:
                return expr.name.lower()
        
        # Check .this attribute
        if hasattr(expr, "this"):
            if hasattr(expr.this, "name") and expr.this.name:
                return expr.this.name.lower()
            if isinstance(expr.this, exp.Table):
                return self._get_table_name(expr.this)
            if isinstance(expr.this, str):
                return expr.this.lower()
        
        # String conversion as last resort
        try:
            name = str(expr).lower().strip("`\"'")
            if name and not name.startswith("("):
                return name
        except Exception:
            pass
            
        return None

    def _process_select(self, parsed: exp.Select, duration: float) -> None:
        """Process a SELECT query."""
        tables = self._extract_tables(parsed)

        for table in tables:
            self._ensure_pattern(table)
            self.patterns[table].select_count += 1
            self.patterns[table].total_time_ms += duration / len(tables)

        # Track filtered columns (from WHERE clause)
        self._extract_filtered_columns(parsed, tables)

    def _process_insert(self, parsed: exp.Insert, duration: float) -> None:
        """Process an INSERT query."""
        # For INSERT, the table is in parsed.this
        table = self._get_table_name(parsed.this)
        
        # Try alternative: look for Table expressions
        if not table:
            for tbl in parsed.find_all(exp.Table):
                table = self._get_table_name(tbl)
                if table:
                    break
        
        if table:
            self._ensure_pattern(table)
            self.patterns[table].insert_count += 1
            self.patterns[table].total_time_ms += duration

    def _process_update(self, parsed: exp.Update, duration: float) -> None:
        """Process an UPDATE query."""
        table = self._get_table_name(parsed.this)
        
        # Try alternative: look for Table expressions
        if not table:
            for tbl in parsed.find_all(exp.Table):
                table = self._get_table_name(tbl)
                if table:
                    break
        
        if table:
            self._ensure_pattern(table)
            self.patterns[table].update_count += 1
            self.patterns[table].total_time_ms += duration

            # Track updated columns
            self._extract_updated_columns(parsed, table)

            # Track filtered columns
            self._extract_filtered_columns(parsed, [table])

    def _process_delete(self, parsed: exp.Delete, duration: float) -> None:
        """Process a DELETE query."""
        table = self._get_table_name(parsed.this)
        
        # Try alternative: look for Table expressions
        if not table:
            for tbl in parsed.find_all(exp.Table):
                table = self._get_table_name(tbl)
                if table:
                    break
        
        if table:
            self._ensure_pattern(table)
            self.patterns[table].delete_count += 1
            self.patterns[table].total_time_ms += duration

            # Track filtered columns
            self._extract_filtered_columns(parsed, [table])

    def _extract_tables(self, parsed: exp.Expression) -> list[str]:
        """Extract all table names from a query."""
        tables = []
        for table_expr in parsed.find_all(exp.Table):
            name = self._get_table_name(table_expr)
            if name:
                tables.append(name)
        return list(set(tables))

    def _extract_updated_columns(self, parsed: exp.Update, table: str) -> None:
        """Extract columns being updated."""
        # Find SET expressions
        for eq in parsed.find_all(exp.EQ):
            # Left side of EQ in SET clause is the column being updated
            if hasattr(eq.this, "name"):
                col_name = eq.this.name.lower()
                self.updated_columns[table][col_name] += 1

    def _extract_filtered_columns(
        self, parsed: exp.Expression, tables: list[str]
    ) -> None:
        """Extract columns used in WHERE clauses."""
        where_clause = parsed.find(exp.Where)
        if not where_clause:
            return

        for column in where_clause.find_all(exp.Column):
            col_name = column.name.lower() if hasattr(column, "name") else None
            if not col_name:
                continue

            # Try to determine which table the column belongs to
            col_table = column.table.lower() if column.table else None

            if col_table and col_table in tables:
                self.filtered_columns[col_table][col_name] += 1
            elif len(tables) == 1:
                # If only one table, assume column belongs to it
                self.filtered_columns[tables[0]][col_name] += 1

    def get_mutation_report(self) -> dict:
        """Generate a mutation analysis report."""
        report = {
            "tables": [],
            "embedding_warnings": [],
            "index_recommendations": [],
        }

        for table, pattern in sorted(
            self.patterns.items(),
            key=lambda x: x[1].total_operations,
            reverse=True,
        ):
            # Get top updated columns
            top_updated = sorted(
                self.updated_columns.get(table, {}).items(),
                key=lambda x: x[1],
                reverse=True,
            )[:5]

            # Get top filtered columns
            top_filtered = sorted(
                self.filtered_columns.get(table, {}).items(),
                key=lambda x: x[1],
                reverse=True,
            )[:5]

            table_report = {
                "table": table,
                "reads": pattern.select_count,
                "inserts": pattern.insert_count,
                "updates": pattern.update_count,
                "deletes": pattern.delete_count,
                "total_operations": pattern.total_operations,
                "write_ratio": f"{pattern.write_ratio:.1%}",
                "read_ratio": f"{pattern.read_ratio:.1%}",
                "is_read_heavy": pattern.is_read_heavy,
                "is_write_heavy": pattern.is_write_heavy,
                "frequently_updated_columns": [col for col, _ in top_updated],
                "frequently_filtered_columns": [col for col, _ in top_filtered],
            }
            report["tables"].append(table_report)

            # Generate warnings
            if pattern.is_write_heavy:
                report["embedding_warnings"].append({
                    "table": table,
                    "warning": f"High write ratio ({pattern.write_ratio:.0%}). "
                               f"Embedding this table may cause update complexity.",
                    "severity": "high" if pattern.write_ratio > 0.7 else "medium",
                })

            if pattern.is_update_heavy:
                report["embedding_warnings"].append({
                    "table": table,
                    "warning": f"Update-heavy table ({pattern.update_count} updates vs "
                               f"{pattern.insert_count} inserts). Consider keeping separate.",
                    "severity": "medium",
                })

            # Index recommendations
            if top_filtered:
                report["index_recommendations"].append({
                    "table": table,
                    "columns": [col for col, _ in top_filtered[:3]],
                    "reason": "Frequently used in WHERE clauses",
                })

        return report

    def get_write_heavy_tables(self, threshold: float = 0.5) -> list[str]:
        """Get tables with write ratio above threshold."""
        return [
            table for table, pattern in self.patterns.items()
            if pattern.write_ratio > threshold
        ]

    def get_update_heavy_tables(self) -> list[str]:
        """Get tables where updates dominate writes."""
        return [
            table for table, pattern in self.patterns.items()
            if pattern.is_update_heavy
        ]

    @property
    def queries_processed(self) -> int:
        """Number of queries processed."""
        return self._queries_processed
