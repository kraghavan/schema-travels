"""Recommender module for AI-powered schema recommendations."""

from schema_travels.recommender.schema_generator import SchemaGenerator
from schema_travels.recommender.claude_advisor import ClaudeAdvisor
from schema_travels.recommender.models import (
    SchemaRecommendation,
    TargetSchema,
    CollectionDefinition,
)

__all__ = [
    "SchemaGenerator",
    "ClaudeAdvisor",
    "SchemaRecommendation",
    "TargetSchema",
    "CollectionDefinition",
]
