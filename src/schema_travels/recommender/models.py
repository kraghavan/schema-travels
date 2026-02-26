"""Data models for the recommender module."""

from dataclasses import dataclass, field
from enum import Enum
from typing import Any


class RelationshipDecision(Enum):
    """Decision for how to handle a relationship in target schema."""

    EMBED = "embed"
    REFERENCE = "reference"
    SEPARATE = "separate"
    BUCKET = "bucket"  # For time-series patterns


class TargetDatabase(Enum):
    """Supported target database types."""

    MONGODB = "mongodb"
    DYNAMODB = "dynamodb"


@dataclass
class SchemaRecommendation:
    """Recommendation for a specific table relationship."""

    parent_table: str
    child_table: str
    decision: RelationshipDecision
    confidence: float
    reasoning: list[str]
    warnings: list[str] = field(default_factory=list)
    metrics: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary."""
        return {
            "parent_table": self.parent_table,
            "child_table": self.child_table,
            "decision": self.decision.value,
            "confidence": self.confidence,
            "reasoning": self.reasoning,
            "warnings": self.warnings,
            "metrics": self.metrics,
        }


@dataclass
class FieldDefinition:
    """Field in a target collection/table."""

    name: str
    type: str
    nullable: bool = True
    is_key: bool = False
    source_column: str | None = None


@dataclass
class EmbeddedDocument:
    """Definition of an embedded document."""

    name: str
    source_table: str
    is_array: bool = True
    fields: list[FieldDefinition] = field(default_factory=list)


@dataclass
class CollectionDefinition:
    """Definition of a collection (MongoDB) or table (DynamoDB)."""

    name: str
    source_tables: list[str]
    fields: list[FieldDefinition] = field(default_factory=list)
    embedded_documents: list[EmbeddedDocument] = field(default_factory=list)
    references: list[str] = field(default_factory=list)

    # DynamoDB specific
    partition_key: str | None = None
    sort_key: str | None = None
    gsi: list[dict] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary."""
        result = {
            "name": self.name,
            "source_tables": self.source_tables,
            "fields": [
                {
                    "name": f.name,
                    "type": f.type,
                    "nullable": f.nullable,
                    "is_key": f.is_key,
                }
                for f in self.fields
            ],
            "embedded_documents": [
                {
                    "name": ed.name,
                    "source_table": ed.source_table,
                    "is_array": ed.is_array,
                    "fields": [
                        {"name": f.name, "type": f.type}
                        for f in ed.fields
                    ],
                }
                for ed in self.embedded_documents
            ],
            "references": self.references,
        }

        if self.partition_key:
            result["partition_key"] = self.partition_key
        if self.sort_key:
            result["sort_key"] = self.sort_key
        if self.gsi:
            result["gsi"] = self.gsi

        return result


@dataclass
class TargetSchema:
    """Complete target schema definition."""

    target_type: TargetDatabase
    collections: list[CollectionDefinition] = field(default_factory=list)
    recommendations: list[SchemaRecommendation] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)
    metadata: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary."""
        return {
            "target_type": self.target_type.value,
            "collections": [c.to_dict() for c in self.collections],
            "recommendations": [r.to_dict() for r in self.recommendations],
            "warnings": self.warnings,
            "metadata": self.metadata,
        }

    def to_mongodb_schema(self) -> dict[str, Any]:
        """Generate MongoDB JSON schema format."""
        if self.target_type != TargetDatabase.MONGODB:
            raise ValueError("Schema is not for MongoDB")

        schema = {}
        for collection in self.collections:
            schema[collection.name] = self._collection_to_mongo_schema(collection)

        return schema

    def _collection_to_mongo_schema(self, collection: CollectionDefinition) -> dict:
        """Convert collection to MongoDB schema format."""
        properties = {}
        required = []

        for field in collection.fields:
            properties[field.name] = {"bsonType": field.type}
            if not field.nullable:
                required.append(field.name)

        for embedded in collection.embedded_documents:
            embedded_props = {}
            for field in embedded.fields:
                embedded_props[field.name] = {"bsonType": field.type}

            if embedded.is_array:
                properties[embedded.name] = {
                    "bsonType": "array",
                    "items": {
                        "bsonType": "object",
                        "properties": embedded_props,
                    },
                }
            else:
                properties[embedded.name] = {
                    "bsonType": "object",
                    "properties": embedded_props,
                }

        return {
            "bsonType": "object",
            "properties": properties,
            "required": required,
        }

    def to_dynamodb_schema(self) -> dict[str, Any]:
        """Generate DynamoDB table definitions."""
        if self.target_type != TargetDatabase.DYNAMODB:
            raise ValueError("Schema is not for DynamoDB")

        tables = {}
        for collection in self.collections:
            tables[collection.name] = {
                "TableName": collection.name,
                "KeySchema": self._build_key_schema(collection),
                "AttributeDefinitions": self._build_attribute_definitions(collection),
                "GlobalSecondaryIndexes": collection.gsi if collection.gsi else None,
            }

        return tables

    def _build_key_schema(self, collection: CollectionDefinition) -> list[dict]:
        """Build DynamoDB key schema."""
        schema = []
        if collection.partition_key:
            schema.append({
                "AttributeName": collection.partition_key,
                "KeyType": "HASH",
            })
        if collection.sort_key:
            schema.append({
                "AttributeName": collection.sort_key,
                "KeyType": "RANGE",
            })
        return schema

    def _build_attribute_definitions(
        self, collection: CollectionDefinition
    ) -> list[dict]:
        """Build DynamoDB attribute definitions."""
        definitions = []
        key_attrs = {collection.partition_key, collection.sort_key}

        for field in collection.fields:
            if field.name in key_attrs:
                definitions.append({
                    "AttributeName": field.name,
                    "AttributeType": self._to_dynamodb_type(field.type),
                })

        return definitions

    def _to_dynamodb_type(self, field_type: str) -> str:
        """Map field type to DynamoDB attribute type."""
        mapping = {
            "string": "S",
            "int": "N",
            "long": "N",
            "double": "N",
            "decimal": "N",
            "bool": "BOOL",
            "binary": "B",
        }
        return mapping.get(field_type.lower(), "S")
