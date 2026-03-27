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
        # v2.0.0: Track columns in SELECT clauses for GSI projection optimization
        self.selected_columns: dict[str, dict[str, int]] = defaultdict(lambda: defaultdict(int))
        # v2.0.0: Track SELECT * usage per table
        self.select_star_tables: set[str] = set()
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
        tables, alias_map = self._extract_tables_with_aliases(parsed)

        for table in tables:
            self._ensure_pattern(table)
            self.patterns[table].select_count += 1
            self.patterns[table].total_time_ms += duration / len(tables)

        # Track filtered columns (from WHERE clause)
        self._extract_filtered_columns(parsed, tables, alias_map)
        
        # v2.0.0: Track selected columns (from SELECT clause)
        self._extract_selected_columns(parsed, tables, alias_map)

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

    def _extract_tables_with_aliases(self, parsed: exp.Expression) -> tuple[list[str], dict[str, str]]:
        """
        Extract all table names and their aliases from a query.
        
        Returns:
            Tuple of (table_names, alias_to_table_mapping)
            e.g., (['users', 'orders'], {'u': 'users', 'o': 'orders'})
        """
        tables = []
        alias_map: dict[str, str] = {}
        
        for table_expr in parsed.find_all(exp.Table):
            name = self._get_table_name(table_expr)
            if name:
                tables.append(name)
                # Check for alias
                if hasattr(table_expr, 'alias') and table_expr.alias:
                    alias = table_expr.alias.lower()
                    alias_map[alias] = name
                # Also map table name to itself for consistent lookup
                alias_map[name] = name
        
        return list(set(tables)), alias_map

    def _extract_updated_columns(self, parsed: exp.Update, table: str) -> None:
        """Extract columns being updated."""
        # Find SET expressions
        for eq in parsed.find_all(exp.EQ):
            # Left side of EQ in SET clause is the column being updated
            if hasattr(eq.this, "name"):
                col_name = eq.this.name.lower()
                self.updated_columns[table][col_name] += 1

    def _extract_filtered_columns(
        self, parsed: exp.Expression, tables: list[str], alias_map: dict[str, str] | None = None
    ) -> None:
        """Extract columns used in WHERE clauses."""
        where_clause = parsed.find(exp.Where)
        if not where_clause:
            return
        
        alias_map = alias_map or {}

        for column in where_clause.find_all(exp.Column):
            col_name = column.name.lower() if hasattr(column, "name") else None
            if not col_name:
                continue

            # Try to determine which table the column belongs to
            col_table = column.table.lower() if column.table else None

            if col_table:
                # Resolve alias to actual table name
                actual_table = alias_map.get(col_table, col_table)
                if actual_table in tables:
                    self.filtered_columns[actual_table][col_name] += 1
            elif len(tables) == 1:
                # If only one table, assume column belongs to it
                self.filtered_columns[tables[0]][col_name] += 1

    def _extract_selected_columns(
        self, parsed: exp.Select, tables: list[str], alias_map: dict[str, str] | None = None
    ) -> None:
        """
        Extract columns from SELECT clause.
        
        v2.0.0: This drives GSI projection decisions in DynamoDB:
        - If only 5 of 50 columns are ever SELECTed, use INCLUDE projection
        - If SELECT * is common, use ALL projection
        - If only keys needed, use KEYS_ONLY projection
        
        Args:
            parsed: Parsed SELECT statement
            tables: List of tables involved in the query
            alias_map: Mapping from aliases to actual table names
        """
        if not parsed.expressions:
            return
        
        alias_map = alias_map or {}
            
        for expr in parsed.expressions:
            # Handle SELECT *
            if isinstance(expr, exp.Star):
                # Mark all tables as having SELECT *
                for table in tables:
                    self.select_star_tables.add(table)
                continue
            
            # Handle table.* (e.g., SELECT users.* or SELECT u.*)
            if isinstance(expr, exp.Column) and isinstance(expr.this, exp.Star):
                table_ref = expr.table.lower() if expr.table else None
                if table_ref:
                    # Resolve alias to actual table name
                    actual_table = alias_map.get(table_ref, table_ref)
                    if actual_table in tables:
                        self.select_star_tables.add(actual_table)
                continue
                
            # Handle regular columns
            if isinstance(expr, exp.Column):
                col_name = expr.name.lower() if hasattr(expr, "name") and expr.name else None
                if not col_name:
                    continue
                    
                # Determine which table the column belongs to
                col_table = expr.table.lower() if expr.table else None
                
                if col_table:
                    # Resolve alias to actual table name
                    actual_table = alias_map.get(col_table, col_table)
                    if actual_table in tables:
                        self.selected_columns[actual_table][col_name] += 1
                elif len(tables) == 1:
                    # Single table query - attribute to that table
                    self.selected_columns[tables[0]][col_name] += 1
                else:
                    # Multi-table query without explicit table prefix
                    # Try to match column to a table (best effort)
                    # For now, skip ambiguous columns
                    pass
            
            # Handle aliased columns (e.g., SELECT u.name AS user_name)
            elif isinstance(expr, exp.Alias):
                inner = expr.this
                if isinstance(inner, exp.Column):
                    col_name = inner.name.lower() if hasattr(inner, "name") and inner.name else None
                    if not col_name:
                        continue
                    
                    col_table = inner.table.lower() if inner.table else None
                    
                    if col_table:
                        # Resolve alias to actual table name
                        actual_table = alias_map.get(col_table, col_table)
                        if actual_table in tables:
                            self.selected_columns[actual_table][col_name] += 1
                    elif len(tables) == 1:
                        self.selected_columns[tables[0]][col_name] += 1
            
            # Handle function calls like COUNT(*), SUM(column), etc.
            elif isinstance(expr, (exp.Func, exp.AggFunc)):
                # Extract columns from function arguments
                for col in expr.find_all(exp.Column):
                    col_name = col.name.lower() if hasattr(col, "name") and col.name else None
                    if not col_name:
                        continue
                    
                    col_table = col.table.lower() if col.table else None
                    
                    if col_table:
                        # Resolve alias to actual table name
                        actual_table = alias_map.get(col_table, col_table)
                        if actual_table in tables:
                            self.selected_columns[actual_table][col_name] += 1
                    elif len(tables) == 1:
                        self.selected_columns[tables[0]][col_name] += 1

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
            
            # v2.0.0: Get top selected columns
            top_selected = sorted(
                self.selected_columns.get(table, {}).items(),
                key=lambda x: x[1],
                reverse=True,
            )[:10]

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
                "frequently_selected_columns": [col for col, _ in top_selected],
                "has_select_star": table in self.select_star_tables,
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

    def get_frequently_selected_columns(self, table: str, top_n: int = 10) -> list[str]:
        """
        Get most frequently selected columns for a table.
        
        v2.0.0: Used for GSI projection optimization in DynamoDB.
        
        Args:
            table: Table name
            top_n: Number of columns to return
            
        Returns:
            List of column names sorted by selection frequency
        """
        table = table.lower()
        columns = self.selected_columns.get(table, {})
        sorted_cols = sorted(columns.items(), key=lambda x: x[1], reverse=True)
        return [col for col, _ in sorted_cols[:top_n]]

    def has_select_star(self, table: str) -> bool:
        """
        Check if SELECT * is used for a table.
        
        v2.0.0: If SELECT * is common, GSI should use ALL projection.
        
        Args:
            table: Table name
            
        Returns:
            True if SELECT * has been used for this table
        """
        return table.lower() in self.select_star_tables

    def get_projection_recommendation(self, table: str) -> str:
        """
        Recommend GSI projection type based on SELECT patterns.
        
        v2.0.0: DynamoDB GSI projection types:
        - KEYS_ONLY: Only key attributes (cheapest, smallest)
        - INCLUDE: Keys + specified attributes
        - ALL: All attributes (most expensive, largest)
        
        Args:
            table: Table name
            
        Returns:
            One of: "KEYS_ONLY", "INCLUDE", "ALL"
        """
        table = table.lower()
        
        # If SELECT * is used, recommend ALL
        if table in self.select_star_tables:
            return "ALL"
        
        # Get selected columns
        selected = self.selected_columns.get(table, {})
        
        # If no columns tracked or very few, might be KEYS_ONLY
        if len(selected) == 0:
            return "KEYS_ONLY"
        
        # If many columns selected, recommend ALL
        if len(selected) > 10:
            return "ALL"
        
        # Otherwise, INCLUDE specific columns
        return "INCLUDE"

    @property
    def queries_processed(self) -> int:
        """Number of queries processed."""
        return self._queries_processed
