"""Schema generator module for creating target schema definitions."""

import logging
from typing import Any

from schema_travels.collector.models import SchemaDefinition, TableDefinition
from schema_travels.analyzer.models import AnalysisResult
from schema_travels.recommender.models import (
    CollectionDefinition,
    EmbeddedDocument,
    FieldDefinition,
    RelationshipDecision,
    SchemaRecommendation,
    TargetDatabase,
    TargetSchema,
)

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
    """

    def __init__(
        self,
        source_schema: SchemaDefinition,
        analysis: AnalysisResult,
        recommendations: list[SchemaRecommendation] | None = None,
    ):
        """
        Initialize schema generator.

        Args:
            source_schema: Source database schema
            analysis: Analysis result from pattern analyzer
            recommendations: Optional pre-computed recommendations
        """
        self.source_schema = source_schema
        self.analysis = analysis
        self.recommendations = recommendations or []

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

    def _generate_dynamodb_schema(self) -> TargetSchema:
        """Generate DynamoDB schema."""
        collections: list[CollectionDefinition] = []
        warnings: list[str] = []

        # DynamoDB requires different approach - single table design
        # or multiple tables with GSIs

        # For now, create a table per entity with appropriate keys
        for table in self.source_schema.tables:
            collection = self._create_dynamodb_table(table)
            collections.append(collection)
            
        warnings.append(
            "DynamoDB schema generation is basic. "
            "Consider single-table design for complex access patterns."
        )

        return TargetSchema(
            target_type=TargetDatabase.DYNAMODB,
            collections=collections,
            recommendations=self.recommendations,
            warnings=warnings,
            metadata={
                "source_tables": len(self.source_schema.tables),
                "target_tables": len(collections),
            },
        )

    def _create_dynamodb_table(
        self,
        table: TableDefinition,
    ) -> CollectionDefinition:
        """Create a DynamoDB table definition."""
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

        # Find frequently filtered columns from analysis
        for ts in self.analysis.table_statistics:
            if ts.table.lower() == table_name:
                for col in ts.frequently_filtered_columns[:3]:
                    # Don't create GSI for primary key
                    if col not in table.primary_key:
                        gsis.append({
                            "IndexName": f"{table_name}-{col}-index",
                            "KeySchema": [
                                {"AttributeName": col, "KeyType": "HASH"},
                            ],
                            "Projection": {"ProjectionType": "ALL"},
                        })
                break

        return gsis

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
