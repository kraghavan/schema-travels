"""Recommender module for schema recommendations.

v2.0.0: Added DynamoDB support with single-table design.
v2.0.1: Added AI review workflow for DynamoDB designs.
v2.3.0: Added multi-provider LLM support (Claude, OpenAI, Gemini, Grok, Ollama).
"""
from schema_travels.recommender.advisor import Advisor
from schema_travels.recommender.claude_advisor import ClaudeAdvisor  # Backwards compat
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
    # v2.0.1: Review models
    DynamoDBReview,
    EntityChange,
    GSIChange,
    GSIChangeAction,
    ReviewChangeType,
)
from schema_travels.recommender.dynamodb_designer import DynamoDBDesigner
from schema_travels.recommender.dynamodb_output import DynamoDBOutputFormatter
# v2.0.1: Review helpers
from schema_travels.recommender.dynamodb_review import (
    apply_review,
    summarize_review_changes,
)

__all__ = [
    # Core - v2.3.0: Advisor is primary, ClaudeAdvisor is alias
    "Advisor",
    "ClaudeAdvisor",  # Backwards compatibility
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
    # v2.0.1: DynamoDB Review
    "DynamoDBReview",
    "EntityChange",
    "GSIChange",
    "GSIChangeAction",
    "ReviewChangeType",
    "apply_review",
    "summarize_review_changes",
]
