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

__all__ = [
    "ClaudeAdvisor",
    "SchemaGenerator",
    "TargetDatabase",
    "RelationshipDecision",
    "SchemaRecommendation",
    "TargetSchema",
    "CollectionDefinition",
    "get_cache",
    "compute_input_hash",
    "RecommendationCache",
    "CacheMode",
    "RECOMMENDATION_VERSION",
]
