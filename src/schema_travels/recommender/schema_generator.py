"""Schema generator module for creating target schema definitions."""

import logging
from typing import Any

from schema_travels.collector.models import SchemaDefinition, TableDefinition
from schema_travels.analyzer.models import AnalysisResult, TableStatistics
from schema_travels.recommender.models import (
    CollectionDefinition,
    EmbeddedDocument,
    FieldDefinition,
    RelationshipDecision,
    SchemaRecommendation,
    TargetDatabase,
    TargetSchema,
)
# v2.0.0: DynamoDB imports
from schema_travels.recommender.dynamodb_models import (
    DesignMode,
    DynamoDBDesign,
)
from schema_travels.recommender.dynamodb_designer import DynamoDBDesigner
from schema_travels.recommender.dynamodb_output import DynamoDBOutputFormatter

logger = logging.getLogger(__name__)


# Type mapping from SQL to MongoDB
SQL_TO_MONGO_TYPES = {
    "integer": "int",
    "int": "int",
    "bigint": "long",
    "smallint": "int",
    "decimal": "decimal",
    "numeric": "decimal",
    "real": "double",
    "double": "double",
    "double precision": "double",
    "float": "double",
    "varchar": "string",
    "character varying": "string",
    "char": "string",
    "text": "string",
    "boolean": "bool",
    "bool": "bool",
    "date": "date",
    "timestamp": "date",
    "timestamp without time zone": "date",
    "timestamp with time zone": "date",
    "timestamptz": "date",
    "json": "object",
    "jsonb": "object",
    "uuid": "string",
    "bytea": "binData",
    "serial": "int",
    "bigserial": "long",
}


class SchemaGenerator:
    """
    Generates target schema from source schema and recommendations.

    Combines rule-based analysis with AI recommendations to produce
    optimal target schema definitions.
    
    v2.0.0: Added DynamoDB single-table design support via DynamoDBDesigner.
    """

    def __init__(
        self,
        source_schema: SchemaDefinition,
        analysis: AnalysisResult,
        recommendations: list[SchemaRecommendation] | None = None,
        # v2.0.0: DynamoDB options
        dynamodb_mode: DesignMode = DesignMode.AUTO,
        filtered_columns: dict[str, dict[str, int]] | None = None,
        selected_columns: dict[str, dict[str, int]] | None = None,
        select_star_tables: set[str] | None = None,
    ):
        """
        Initialize schema generator.

        Args:
            source_schema: Source database schema
            analysis: Analysis result from pattern analyzer
            recommendations: Optional pre-computed recommendations
            dynamodb_mode: DynamoDB design mode (AUTO, SINGLE_TABLE, MULTI_TABLE)
            filtered_columns: Column filter frequencies from MutationAnalyzer
            selected_columns: Column select frequencies from MutationAnalyzer
            select_star_tables: Tables with SELECT * usage
        """
        self.source_schema = source_schema
        self.analysis = analysis
        self.recommendations = recommendations or []
        
        # v2.0.0: DynamoDB options
        self.dynamodb_mode = dynamodb_mode
        self.filtered_columns = filtered_columns or {}
        self.selected_columns = selected_columns or {}
        self.select_star_tables = select_star_tables or set()

        # Build lookup maps
        self._table_lookup = {t.name.lower(): t for t in source_schema.tables}
        self._rec_lookup = self._build_recommendation_lookup()

    def _build_recommendation_lookup(self) -> dict[tuple[str, str], SchemaRecommendation]:
        """Build lookup map for recommendations."""
        lookup = {}
        for rec in self.recommendations:
            key = (rec.parent_table.lower(), rec.child_table.lower())
            lookup[key] = rec
            # Also add reverse for easy lookup
            lookup[(rec.child_table.lower(), rec.parent_table.lower())] = rec
        return lookup

    def generate(
        self,
        target: TargetDatabase = TargetDatabase.MONGODB,
    ) -> TargetSchema:
        """
        Generate target schema.

        Args:
            target: Target database type

        Returns:
            Complete target schema definition
        """
        if target == TargetDatabase.MONGODB:
            return self._generate_mongodb_schema()
        elif target == TargetDatabase.DYNAMODB:
            return self._generate_dynamodb_schema()
        else:
            raise ValueError(f"Unsupported target: {target}")

    def _generate_mongodb_schema(self) -> TargetSchema:
        """Generate MongoDB schema."""
        collections: list[CollectionDefinition] = []
        embedded_tables: set[str] = set()
        warnings: list[str] = []

        # First pass: identify embedded tables
        for rec in self.recommendations:
            if rec.decision == RelationshipDecision.EMBED:
                embedded_tables.add(rec.child_table.lower())

        # Second pass: create collections for non-embedded tables
        for table in self.source_schema.tables:
            table_name = table.name.lower()

            if table_name in embedded_tables:
                continue  # Will be embedded in parent

            collection = self._create_collection(table, embedded_tables)
            collections.append(collection)

        # Add warnings for potential issues
        for rec in self.recommendations:
            warnings.extend(rec.warnings)

        return TargetSchema(
            target_type=TargetDatabase.MONGODB,
            collections=collections,
            recommendations=self.recommendations,
            warnings=warnings,
            metadata={
                "source_tables": len(self.source_schema.tables),
                "target_collections": len(collections),
                "embedded_tables": list(embedded_tables),
            },
        )

    def _create_collection(
        self,
        table: TableDefinition,
        embedded_tables: set[str],
    ) -> CollectionDefinition:
        """Create a collection definition from a table."""
        table_name = table.name.lower()

        # Convert fields
        fields = [
            self._convert_column(col)
            for col in table.columns
        ]

        # Find embedded documents
        embedded_docs = []
        references = []

        for fk in self.source_schema.foreign_keys:
            # Check if this table is the parent in a relationship
            if fk.to_table.lower() == table_name:
                child_table = fk.from_table.lower()
                rec = self._rec_lookup.get((table_name, child_table))

                if rec and rec.decision == RelationshipDecision.EMBED:
                    # Create embedded document
                    child_def = self._table_lookup.get(child_table)
                    if child_def:
                        embedded_docs.append(
                            self._create_embedded_document(child_def)
                        )
                elif rec and rec.decision == RelationshipDecision.REFERENCE:
                    references.append(child_table)

        return CollectionDefinition(
            name=table_name,
            source_tables=[table_name],
            fields=fields,
            embedded_documents=embedded_docs,
            references=references,
        )

    def _create_embedded_document(
        self,
        table: TableDefinition,
    ) -> EmbeddedDocument:
        """Create an embedded document definition."""
        fields = [
            self._convert_column(col)
            for col in table.columns
            # Skip foreign key columns as they're implicit
            if not self._is_fk_column(table.name, col.name)
        ]

        return EmbeddedDocument(
            name=table.name.lower(),
            source_table=table.name,
            is_array=True,  # Assume array for one-to-many
            fields=fields,
        )

    def _convert_column(self, col) -> FieldDefinition:
        """Convert SQL column to field definition."""
        sql_type = col.data_type.lower().split("(")[0].strip()
        mongo_type = SQL_TO_MONGO_TYPES.get(sql_type, "string")

        return FieldDefinition(
            name=col.name,
            type=mongo_type,
            nullable=col.nullable,
            is_key=col.is_primary_key,
            source_column=col.name,
        )

    def _is_fk_column(self, table_name: str, column_name: str) -> bool:
        """Check if a column is a foreign key column."""
        for fk in self.source_schema.foreign_keys:
            if fk.from_table.lower() == table_name.lower():
                if column_name.lower() in [c.lower() for c in fk.from_columns]:
                    return True
        return False

    # =========================================================================
    # v2.0.0: Enhanced DynamoDB Schema Generation
    # =========================================================================

    def _generate_dynamodb_schema(self) -> TargetSchema:
        """
        Generate DynamoDB schema using DynamoDBDesigner.
        
        v2.0.0: Uses access cluster analysis for single-table design.
        """
        # Build table statistics if not already in analysis
        table_stats = self._build_table_stats()
        
        # Create designer with specified mode
        designer = DynamoDBDesigner(
            mode=self.dynamodb_mode,
            co_access_threshold=0.70,
        )
        
        # Generate DynamoDB design
        design = designer.design(
            table_stats=table_stats,
            access_patterns=self.analysis.access_patterns,
            join_patterns=self.analysis.join_patterns,
            mutation_patterns=self.analysis.mutation_patterns,
            filtered_columns=self.filtered_columns,
            selected_columns=self.selected_columns,
            select_star_tables=self.select_star_tables,
        )
        
        # Convert DynamoDB design to TargetSchema for compatibility
        return self._dynamodb_design_to_target_schema(design)

    def _build_table_stats(self) -> list[TableStatistics]:
        """Build TableStatistics from analysis if not already present."""
        # Use existing table_statistics from analysis
        if self.analysis.table_statistics:
            # Enrich with selected columns if available
            stats = []
            for ts in self.analysis.table_statistics:
                # Add frequently_selected_columns from our data
                table_selected = self.selected_columns.get(ts.table, {})
                sorted_selected = sorted(
                    table_selected.items(),
                    key=lambda x: x[1],
                    reverse=True
                )
                
                # Create new TableStatistics with selected columns
                stats.append(TableStatistics(
                    table=ts.table,
                    total_accesses=ts.total_accesses,
                    solo_accesses=ts.solo_accesses,
                    joined_accesses=ts.joined_accesses,
                    total_time_ms=ts.total_time_ms,
                    frequently_filtered_columns=ts.frequently_filtered_columns,
                    frequently_updated_columns=ts.frequently_updated_columns,
                    frequently_selected_columns=[col for col, _ in sorted_selected[:10]],
                    has_select_star=ts.table in self.select_star_tables,
                ))
            return stats
        
        # Build from source schema if no analysis stats
        stats = []
        for table in self.source_schema.tables:
            table_name = table.name.lower()
            
            # Get filtered columns
            table_filtered = self.filtered_columns.get(table_name, {})
            sorted_filtered = sorted(
                table_filtered.items(),
                key=lambda x: x[1],
                reverse=True
            )
            
            # Get selected columns
            table_selected = self.selected_columns.get(table_name, {})
            sorted_selected = sorted(
                table_selected.items(),
                key=lambda x: x[1],
                reverse=True
            )
            
            stats.append(TableStatistics(
                table=table_name,
                total_accesses=0,
                solo_accesses=0,
                joined_accesses=0,
                frequently_filtered_columns=[col for col, _ in sorted_filtered[:5]],
                frequently_selected_columns=[col for col, _ in sorted_selected[:10]],
                has_select_star=table_name in self.select_star_tables,
            ))
        
        return stats

    def _dynamodb_design_to_target_schema(
        self,
        design: DynamoDBDesign,
    ) -> TargetSchema:
        """Convert DynamoDBDesign to TargetSchema for API compatibility."""
        collections: list[CollectionDefinition] = []
        warnings: list[str] = list(design.warnings)
        
        if design.design_mode == DesignMode.SINGLE_TABLE:
            # Single table with entities
            collection = self._create_dynamodb_single_table_collection(design)
            collections.append(collection)
            
            # Add orphan tables as separate collections
            for orphan_table in design.orphan_tables:
                table_def = self._table_lookup.get(orphan_table)
                if table_def:
                    collections.append(self._create_dynamodb_table(table_def))
                    warnings.append(
                        f"Table '{orphan_table}' not included in single-table design - "
                        f"created as separate table"
                    )
        else:
            # Multi-table design
            for table_design in design.tables:
                collections.append(
                    self._table_design_to_collection(table_design)
                )
        
        return TargetSchema(
            target_type=TargetDatabase.DYNAMODB,
            collections=collections,
            recommendations=self.recommendations,
            warnings=warnings,
            metadata={
                "source_tables": len(self.source_schema.tables),
                "target_tables": len(collections),
                "design_mode": design.design_mode.value,
                "confidence": design.confidence,
                "rationale": design.rationale,
                # v2.0.0: Include full design for downstream tools
                "dynamodb_design": design.to_dict(),
            },
        )

    def _create_dynamodb_single_table_collection(
        self,
        design: DynamoDBDesign,
    ) -> CollectionDefinition:
        """Create collection definition for single-table DynamoDB design."""
        # Collect all source tables from entities
        source_tables = [e.source_table for e in design.entities]
        
        # Build fields from PK/SK
        fields = [
            FieldDefinition(
                name=design.partition_key,
                type="string",
                nullable=False,
                is_key=True,
            ),
        ]
        
        if design.sort_key:
            fields.append(FieldDefinition(
                name=design.sort_key,
                type="string",
                nullable=False,
                is_key=True,
            ))
        
        # Add entity type field
        fields.append(FieldDefinition(
            name="_entity_type",
            type="string",
            nullable=False,
        ))
        
        # Build GSI definitions
        gsis = []
        for gsi in design.gsis:
            gsis.append({
                "IndexName": gsi.name,
                "KeySchema": [
                    {"AttributeName": gsi.pk_attribute, "KeyType": "HASH"},
                ] + ([{"AttributeName": gsi.sk_attribute, "KeyType": "RANGE"}] if gsi.sk_attribute else []),
                "Projection": {
                    "ProjectionType": gsi.projection_type.value,
                    **({"NonKeyAttributes": gsi.projected_attributes} if gsi.projected_attributes else {}),
                },
            })
        
        return CollectionDefinition(
            name=design.table_name or "main_table",
            source_tables=source_tables,
            fields=fields,
            partition_key=design.partition_key,
            sort_key=design.sort_key,
            gsi=gsis,
        )

    def _table_design_to_collection(
        self,
        table_design,
    ) -> CollectionDefinition:
        """Convert TableDesign to CollectionDefinition."""
        # Get source table definition
        source_table = self._table_lookup.get(table_design.source_table.lower())
        
        fields = []
        if source_table:
            fields = [self._convert_column(col) for col in source_table.columns]
        else:
            fields = [
                FieldDefinition(
                    name=table_design.partition_key,
                    type="string",
                    nullable=False,
                    is_key=True,
                ),
            ]
            if table_design.sort_key:
                fields.append(FieldDefinition(
                    name=table_design.sort_key,
                    type="string",
                    nullable=False,
                    is_key=True,
                ))
        
        # Build GSI definitions
        gsis = []
        for gsi in table_design.gsis:
            gsis.append({
                "IndexName": gsi.name,
                "KeySchema": [
                    {"AttributeName": gsi.pk_attribute, "KeyType": "HASH"},
                ] + ([{"AttributeName": gsi.sk_attribute, "KeyType": "RANGE"}] if gsi.sk_attribute else []),
                "Projection": {
                    "ProjectionType": gsi.projection_type.value,
                },
            })
        
        return CollectionDefinition(
            name=table_design.table_name,
            source_tables=[table_design.source_table],
            fields=fields,
            partition_key=table_design.partition_key,
            sort_key=table_design.sort_key,
            gsi=gsis,
        )

    def _create_dynamodb_table(
        self,
        table: TableDefinition,
    ) -> CollectionDefinition:
        """Create a DynamoDB table definition (legacy method, still used for orphan tables)."""
        fields = [self._convert_column(col) for col in table.columns]

        # Determine partition and sort keys
        partition_key = None
        sort_key = None

        if table.primary_key:
            partition_key = table.primary_key[0]
            if len(table.primary_key) > 1:
                sort_key = table.primary_key[1]

        # If no primary key, use first column
        if not partition_key and table.columns:
            partition_key = table.columns[0].name

        # Analyze access patterns for GSIs
        gsi = self._suggest_gsis(table)

        return CollectionDefinition(
            name=table.name,
            source_tables=[table.name],
            fields=fields,
            partition_key=partition_key,
            sort_key=sort_key,
            gsi=gsi,
        )

    def _suggest_gsis(self, table: TableDefinition) -> list[dict]:
        """Suggest Global Secondary Indexes based on access patterns."""
        gsis = []
        table_name = table.name.lower()

        # First check filtered_columns from MutationAnalyzer (v2.0.0)
        if table_name in self.filtered_columns:
            filtered = self.filtered_columns[table_name]
            sorted_cols = sorted(filtered.items(), key=lambda x: x[1], reverse=True)
            
            for col, freq in sorted_cols[:3]:
                if col not in (table.primary_key or []) and freq >= 5:
                    gsis.append({
                        "IndexName": f"{table_name}-{col}-index",
                        "KeySchema": [
                            {"AttributeName": col, "KeyType": "HASH"},
                        ],
                        "Projection": {"ProjectionType": "ALL"},
                    })
            
            if gsis:
                return gsis

        # Fallback: Find frequently filtered columns from analysis
        for ts in self.analysis.table_statistics:
            if ts.table.lower() == table_name:
                for col in ts.frequently_filtered_columns[:3]:
                    # Don't create GSI for primary key
                    if col not in (table.primary_key or []):
                        gsis.append({
                            "IndexName": f"{table_name}-{col}-index",
                            "KeySchema": [
                                {"AttributeName": col, "KeyType": "HASH"},
                            ],
                            "Projection": {"ProjectionType": "ALL"},
                        })
                break

        return gsis

    # =========================================================================
    # v2.0.0: DynamoDB Output Helpers
    # =========================================================================

    def generate_dynamodb_output(
        self,
        output_format: str = "json",
        **kwargs,
    ) -> str:
        """
        Generate DynamoDB schema in specified format.
        
        Args:
            output_format: One of "json", "terraform", "nosql_workbench"
            **kwargs: Format-specific options
            
        Returns:
            Formatted schema string
        """
        # Generate design
        table_stats = self._build_table_stats()
        
        designer = DynamoDBDesigner(
            mode=self.dynamodb_mode,
            co_access_threshold=0.70,
        )
        
        design = designer.design(
            table_stats=table_stats,
            access_patterns=self.analysis.access_patterns,
            join_patterns=self.analysis.join_patterns,
            mutation_patterns=self.analysis.mutation_patterns,
            filtered_columns=self.filtered_columns,
            selected_columns=self.selected_columns,
            select_star_tables=self.select_star_tables,
        )
        
        # Format output
        return DynamoDBOutputFormatter.format(design, output_format, **kwargs)

    def generate_sample_documents(
        self,
        target_schema: TargetSchema,
        num_samples: int = 1,
    ) -> dict[str, list[dict]]:
        """
        Generate sample documents for each collection.

        Args:
            target_schema: Generated target schema
            num_samples: Number of samples per collection

        Returns:
            Dictionary mapping collection names to sample documents
        """
        samples = {}

        for collection in target_schema.collections:
            samples[collection.name] = [
                self._generate_sample_document(collection)
                for _ in range(num_samples)
            ]

        return samples

    def _generate_sample_document(
        self,
        collection: CollectionDefinition,
    ) -> dict[str, Any]:
        """Generate a sample document for a collection."""
        doc: dict[str, Any] = {}

        for field in collection.fields:
            doc[field.name] = self._sample_value(field.type, field.name)

        for embedded in collection.embedded_documents:
            if embedded.is_array:
                doc[embedded.name] = [
                    {
                        f.name: self._sample_value(f.type, f.name)
                        for f in embedded.fields
                    }
                ]
            else:
                doc[embedded.name] = {
                    f.name: self._sample_value(f.type, f.name)
                    for f in embedded.fields
                }

        return doc

    def _sample_value(self, field_type: str, field_name: str) -> Any:
        """Generate a sample value for a field type."""
        if "id" in field_name.lower():
            return "abc123"
        
        samples = {
            "string": "sample_text",
            "int": 42,
            "long": 1234567890,
            "double": 3.14,
            "decimal": "99.99",
            "bool": True,
            "date": "2024-01-15T10:30:00Z",
            "object": {},
            "binData": "<binary>",
        }
        return samples.get(field_type, None)
