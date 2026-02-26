"""Schema parser module for extracting schema definitions from SQL DDL files."""

import re
from pathlib import Path

import sqlglot
from sqlglot import exp

from schema_travels.collector.models import (
    ColumnDefinition,
    ForeignKeyDefinition,
    IndexDefinition,
    SchemaDefinition,
    TableDefinition,
)


class SchemaParser:
    """Parser for SQL DDL schema files."""

    def __init__(self, dialect: str = "postgres"):
        """Initialize parser with SQL dialect."""
        self.dialect = dialect

    def parse_file(self, schema_file: Path | str) -> SchemaDefinition:
        """Parse a SQL schema file."""
        schema_file = Path(schema_file)
        if not schema_file.exists():
            raise FileNotFoundError(f"Schema file not found: {schema_file}")

        with open(schema_file, "r", encoding="utf-8") as f:
            sql_content = f.read()

        return self.parse_sql(sql_content, source_file=str(schema_file))

    def parse_sql(self, sql_content: str, source_file: str | None = None) -> SchemaDefinition:
        """Parse SQL DDL content."""
        tables: list[TableDefinition] = []
        foreign_keys: list[ForeignKeyDefinition] = []
        indexes: list[IndexDefinition] = []

        # Parse using sqlglot
        try:
            statements = sqlglot.parse(sql_content, dialect=self.dialect)
        except Exception:
            # Fall back to regex parsing if sqlglot fails
            return self._parse_with_regex(sql_content, source_file)

        for stmt in statements:
            if stmt is None:
                continue

            if isinstance(stmt, exp.Create):
                table_def = self._parse_create_table(stmt)
                if table_def and table_def.name:  # Only add if we got a valid name
                    tables.append(table_def)

                    # Extract inline foreign keys
                    inline_fks = self._extract_inline_foreign_keys(stmt, table_def.name)
                    foreign_keys.extend(inline_fks)

            elif isinstance(stmt, exp.Index):
                # Handle CREATE INDEX
                idx = self._parse_create_index(stmt)
                if idx:
                    indexes.append(idx)

        # If sqlglot parsing didn't find tables, try regex
        if not tables:
            return self._parse_with_regex(sql_content, source_file)

        # Attach indexes to tables
        for idx in indexes:
            for table in tables:
                if table.name.lower() == idx.table.lower():
                    table.indexes.append(idx)
                    break

        return SchemaDefinition(
            tables=tables,
            foreign_keys=foreign_keys,
            source_file=source_file,
        )

    def _get_table_name(self, table_expr) -> str:
        """Extract table name from various sqlglot expression types."""
        if table_expr is None:
            return ""
        
        # Try different ways to get the name
        if hasattr(table_expr, "name") and table_expr.name:
            return table_expr.name
        
        if hasattr(table_expr, "this"):
            if hasattr(table_expr.this, "name") and table_expr.this.name:
                return table_expr.this.name
            if isinstance(table_expr.this, str):
                return table_expr.this
        
        # Try to get from Table expression
        if isinstance(table_expr, exp.Table):
            if hasattr(table_expr, "name") and table_expr.name:
                return table_expr.name
        
        # Last resort: convert to string and extract
        table_str = str(table_expr)
        # Remove schema prefix if present (e.g., "public.users" -> "users")
        if "." in table_str:
            table_str = table_str.split(".")[-1]
        # Remove quotes
        table_str = table_str.strip("`\"'")
        return table_str

    def _parse_create_table(self, stmt: exp.Create) -> TableDefinition | None:
        """Parse a CREATE TABLE statement."""
        if not stmt.this:
            return None

        # Get table name using robust extraction
        table_name = self._get_table_name(stmt.this)
        if not table_name:
            return None

        columns: list[ColumnDefinition] = []
        primary_key: list[str] = []

        # Get table expression (Schema contains the column definitions)
        table_expr = stmt.find(exp.Schema)
        if not table_expr:
            return TableDefinition(name=table_name, columns=[], primary_key=[])

        for col_expr in table_expr.expressions:
            if isinstance(col_expr, exp.ColumnDef):
                col_def = self._parse_column_def(col_expr)
                if col_def:
                    columns.append(col_def)
                    if col_def.is_primary_key:
                        primary_key.append(col_def.name)

            elif isinstance(col_expr, exp.PrimaryKey):
                # Table-level PRIMARY KEY constraint
                for col in col_expr.expressions:
                    col_name = col.name if hasattr(col, "name") else str(col)
                    if col_name not in primary_key:
                        primary_key.append(col_name)

        return TableDefinition(
            name=table_name,
            columns=columns,
            primary_key=primary_key,
        )

    def _parse_column_def(self, col_expr: exp.ColumnDef) -> ColumnDefinition | None:
        """Parse a column definition."""
        col_name = col_expr.name if hasattr(col_expr, "name") else None
        if not col_name:
            return None

        # Get data type
        data_type = "TEXT"  # Default
        kind = col_expr.args.get("kind")
        if kind:
            data_type = kind.sql(dialect=self.dialect)

        # Check constraints
        nullable = True
        is_primary_key = False
        default = None

        for constraint in col_expr.args.get("constraints", []):
            if isinstance(constraint, exp.ColumnConstraint):
                constraint_kind = constraint.kind
                if isinstance(constraint_kind, exp.NotNullColumnConstraint):
                    nullable = False
                elif isinstance(constraint_kind, exp.PrimaryKeyColumnConstraint):
                    is_primary_key = True
                    nullable = False
                elif isinstance(constraint_kind, exp.DefaultColumnConstraint):
                    default = str(constraint_kind.this) if constraint_kind.this else None

        return ColumnDefinition(
            name=col_name,
            data_type=data_type,
            nullable=nullable,
            default=default,
            is_primary_key=is_primary_key,
        )

    def _extract_inline_foreign_keys(self, stmt: exp.Create, table_name: str) -> list[ForeignKeyDefinition]:
        """Extract inline foreign key definitions from CREATE TABLE."""
        foreign_keys = []

        # Look for REFERENCES in column constraints (inline FK)
        for col_def in stmt.find_all(exp.ColumnDef):
            col_name = col_def.name if hasattr(col_def, "name") else None
            if not col_name:
                continue
            
            for constraint in col_def.args.get("constraints", []):
                if isinstance(constraint, exp.ColumnConstraint):
                    # Check for Reference constraint
                    ref = constraint.args.get("kind")
                    if ref and hasattr(ref, "this") and isinstance(ref.this, exp.Table):
                        ref_table = self._get_table_name(ref.this)
                        ref_cols = []
                        if hasattr(ref, "expressions") and ref.expressions:
                            ref_cols = [c.name if hasattr(c, "name") else str(c) for c in ref.expressions]
                        if not ref_cols:
                            ref_cols = [col_name]  # Assume same column name
                        
                        foreign_keys.append(ForeignKeyDefinition(
                            constraint_name=f"fk_{table_name}_{ref_table}",
                            from_table=table_name,
                            from_columns=[col_name],
                            to_table=ref_table,
                            to_columns=ref_cols,
                        ))

        # Look for table-level FOREIGN KEY constraints
        for fk_expr in stmt.find_all(exp.ForeignKey):
            fk_def = self._parse_foreign_key(fk_expr, table_name)
            if fk_def:
                foreign_keys.append(fk_def)

        return foreign_keys

    def _parse_foreign_key(
        self, fk_expr: exp.ForeignKey, from_table: str
    ) -> ForeignKeyDefinition | None:
        """Parse a FOREIGN KEY constraint."""
        # Get from columns
        from_columns = []
        for col in fk_expr.expressions:
            col_name = col.name if hasattr(col, "name") else str(col)
            from_columns.append(col_name)

        # Get reference
        ref = fk_expr.args.get("reference")
        if not ref:
            return None

        to_table = self._get_table_name(ref.this)
        if not to_table:
            return None

        to_columns = []
        if ref.expressions:
            for col in ref.expressions:
                col_name = col.name if hasattr(col, "name") else str(col)
                to_columns.append(col_name)
        else:
            # Assume same column names if not specified
            to_columns = from_columns.copy()

        return ForeignKeyDefinition(
            constraint_name=f"fk_{from_table}_{to_table}",
            from_table=from_table,
            from_columns=from_columns,
            to_table=to_table,
            to_columns=to_columns,
        )

    def _parse_create_index(self, stmt: exp.Index) -> IndexDefinition | None:
        """Parse CREATE INDEX statement."""
        return None

    def _parse_with_regex(
        self, sql_content: str, source_file: str | None = None
    ) -> SchemaDefinition:
        """Fallback regex-based parsing for when sqlglot fails."""
        tables: list[TableDefinition] = []
        foreign_keys: list[ForeignKeyDefinition] = []

        # Simple CREATE TABLE pattern
        create_table_pattern = re.compile(
            r"CREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?[`\"]?(\w+)[`\"]?\s*\((.*?)\)\s*;",
            re.IGNORECASE | re.DOTALL,
        )

        for match in create_table_pattern.finditer(sql_content):
            table_name = match.group(1)
            columns_str = match.group(2)

            columns = self._parse_columns_regex(columns_str)
            primary_key = [c.name for c in columns if c.is_primary_key]

            tables.append(
                TableDefinition(
                    name=table_name,
                    columns=columns,
                    primary_key=primary_key,
                )
            )

            # Extract inline REFERENCES in column definitions
            ref_pattern = re.compile(
                r"(\w+)\s+\w+.*?REFERENCES\s+[`\"]?(\w+)[`\"]?\s*\(([^)]+)\)",
                re.IGNORECASE,
            )
            for ref_match in ref_pattern.finditer(columns_str):
                from_col = ref_match.group(1)
                to_table = ref_match.group(2)
                to_cols = [c.strip().strip("`\"") for c in ref_match.group(3).split(",")]

                foreign_keys.append(
                    ForeignKeyDefinition(
                        constraint_name=f"fk_{table_name}_{to_table}",
                        from_table=table_name,
                        from_columns=[from_col],
                        to_table=to_table,
                        to_columns=to_cols,
                    )
                )

            # Extract table-level FOREIGN KEY constraints
            fk_pattern = re.compile(
                r"FOREIGN\s+KEY\s*\(([^)]+)\)\s*REFERENCES\s+[`\"]?(\w+)[`\"]?\s*\(([^)]+)\)",
                re.IGNORECASE,
            )
            for fk_match in fk_pattern.finditer(columns_str):
                from_cols = [c.strip().strip("`\"") for c in fk_match.group(1).split(",")]
                to_table = fk_match.group(2)
                to_cols = [c.strip().strip("`\"") for c in fk_match.group(3).split(",")]

                foreign_keys.append(
                    ForeignKeyDefinition(
                        constraint_name=f"fk_{table_name}_{to_table}",
                        from_table=table_name,
                        from_columns=from_cols,
                        to_table=to_table,
                        to_columns=to_cols,
                    )
                )

        return SchemaDefinition(
            tables=tables,
            foreign_keys=foreign_keys,
            source_file=source_file,
        )

    def _parse_columns_regex(self, columns_str: str) -> list[ColumnDefinition]:
        """Parse columns using regex."""
        columns = []

        # Split by comma, but respect parentheses
        parts = []
        current = []
        depth = 0
        for char in columns_str:
            if char == "(":
                depth += 1
            elif char == ")":
                depth -= 1
            elif char == "," and depth == 0:
                parts.append("".join(current).strip())
                current = []
                continue
            current.append(char)
        if current:
            parts.append("".join(current).strip())

        for part in parts:
            # Skip constraints
            part_upper = part.strip().upper()
            if part_upper.startswith(("PRIMARY KEY", "FOREIGN KEY", "UNIQUE", "CHECK", "CONSTRAINT")):
                continue

            # Parse column definition
            col_match = re.match(
                r"[`\"]?(\w+)[`\"]?\s+(\w+(?:\([^)]+\))?)\s*(.*)",
                part.strip(),
                re.IGNORECASE,
            )
            if col_match:
                name = col_match.group(1)
                data_type = col_match.group(2)
                constraints = col_match.group(3).upper()

                is_pk = "PRIMARY KEY" in constraints
                nullable = "NOT NULL" not in constraints and not is_pk

                columns.append(
                    ColumnDefinition(
                        name=name,
                        data_type=data_type,
                        nullable=nullable,
                        is_primary_key=is_pk,
                    )
                )

        return columns
