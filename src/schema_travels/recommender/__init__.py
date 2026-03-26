"""Recommender module for schema recommendations."""

from schema_travels.recommender.claude_advisor import ClaudeAdvisor
from schema_travels.recommender.schema_generator import SchemaGenerator
from schema_travels.recommender.models import (
    TargetDatabase,
    RelationshipDecision,
    SchemaRecommendation,
    TargetSchema,
    CollectionDefinition,
)
from schema_travels.recommender.cache import (
    get_cache,
    compute_input_hash,
    RecommendationCache,
    CacheMode,
    RECOMMENDATION_VERSION,
)
from schema_travels.recommender.query_rewriter import (
    generate_rewrites,
    QueryRewriteExample,
    RewriteResult,
)
# v2.0.0: DynamoDB support
from schema_travels.recommender.dynamodb_models import (
    DesignMode,
    ProjectionType,
    AccessCluster,
    EntityDefinition,
    GSIDefinition,
    DynamoDBDesign,
    TableDesign,
)
from schema_travels.recommender.dynamodb_designer import DynamoDBDesigner
from schema_travels.recommender.dynamodb_output import DynamoDBOutputFormatter

__all__ = [
    # Core
    "ClaudeAdvisor",
    "SchemaGenerator",
    "TargetDatabase",
    "RelationshipDecision",
    "SchemaRecommendation",
    "TargetSchema",
    "CollectionDefinition",
    # Cache
    "get_cache",
    "compute_input_hash",
    "RecommendationCache",
    "CacheMode",
    "RECOMMENDATION_VERSION",
    # Query rewriter
    "generate_rewrites",
    "QueryRewriteExample",
    "RewriteResult",
    # v2.0.0: DynamoDB
    "DesignMode",
    "ProjectionType",
    "AccessCluster",
    "EntityDefinition",
    "GSIDefinition",
    "DynamoDBDesign",
    "TableDesign",
    "DynamoDBDesigner",
    "DynamoDBOutputFormatter",
]
