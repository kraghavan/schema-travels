"""Query rewriter module - generates SQL to MongoDB query rewrite examples."""

import logging
from dataclasses import dataclass, field
from typing import Any

from schema_travels.recommender.models import SchemaRecommendation, RelationshipDecision

logger = logging.getLogger(__name__)


@dataclass
class QueryRewriteExample:
    """A single SQL → MongoDB query rewrite example."""

    relationship: str           # e.g. "users → orders"
    decision: str               # embed / reference / separate
    scenario: str               # e.g. "Fetch user with their orders"
    sql: str                    # Original SQL
    mongodb: str                # Equivalent MongoDB query
    explanation: str            # Why this rewrite works


@dataclass
class RewriteResult:
    """Collection of rewrite examples for all recommendations."""

    examples: list[QueryRewriteExample] = field(default_factory=list)
    errors: list[str] = field(default_factory=list)


# ---------------------------------------------------------------------------
# Rule-based rewrites — no API call needed for common patterns
# ---------------------------------------------------------------------------

def _embed_rewrite(parent: str, child: str) -> QueryRewriteExample:
    return QueryRewriteExample(
        relationship=f"{parent} → {child}",
        decision="EMBED",
        scenario=f"Fetch {parent} with all its {child}",
        sql=(
            f"SELECT p.*, c.*\n"
            f"FROM {parent} p\n"
            f"JOIN {child} c ON c.{parent}_id = p.id\n"
            f"WHERE p.id = :id;"
        ),
        mongodb=(
            f"// {child} are embedded inside {parent} — single read, no join\n"
            f"db.{parent}.findOne({{ _id: id }})\n\n"
            f"// Result already contains embedded {child}:\n"
            f"// {{\n"
            f"//   _id: ...,\n"
            f"//   ...,\n"
            f"//   {child}: [ {{ ... }}, {{ ... }} ]\n"
            f"// }}"
        ),
        explanation=(
            f"Because {child} are always fetched with {parent} and have high co-access, "
            f"embedding eliminates the JOIN entirely. One read fetches the full document."
        ),
    )


def _reference_rewrite(parent: str, child: str) -> QueryRewriteExample:
    return QueryRewriteExample(
        relationship=f"{parent} → {child}",
        decision="REFERENCE",
        scenario=f"Fetch {parent} then optionally load {child}",
        sql=(
            f"SELECT p.*, c.*\n"
            f"FROM {parent} p\n"
            f"LEFT JOIN {child} c ON c.{parent}_id = p.id\n"
            f"WHERE p.id = :id;"
        ),
        mongodb=(
            f"// Step 1 — fetch the parent\n"
            f"const parent = db.{parent}.findOne({{ _id: id }})\n\n"
            f"// Step 2 — fetch related {child} only when needed\n"
            f"const children = db.{child}.find({{ {parent}_id: id }}).toArray()\n\n"
            f"// Or use $lookup for a single aggregation pipeline:\n"
            f"db.{parent}.aggregate([\n"
            f"  {{ $match: {{ _id: id }} }},\n"
            f"  {{ $lookup: {{\n"
            f"    from: '{child}',\n"
            f"    localField: '_id',\n"
            f"    foreignField: '{parent}_id',\n"
            f"    as: '{child}'\n"
            f"  }} }}\n"
            f"])"
        ),
        explanation=(
            f"{child} are kept as a separate collection because they are large, "
            f"frequently updated independently, or queried on their own. "
            f"Use $lookup only when you need both together."
        ),
    )


def _separate_rewrite(parent: str, child: str) -> QueryRewriteExample:
    return QueryRewriteExample(
        relationship=f"{parent} → {child}",
        decision="SEPARATE",
        scenario=f"Query {child} independently from {parent}",
        sql=(
            f"-- Query 1: get parent\n"
            f"SELECT * FROM {parent} WHERE id = :id;\n\n"
            f"-- Query 2: get child independently\n"
            f"SELECT * FROM {child} WHERE created_at > :since\n"
            f"ORDER BY created_at DESC LIMIT 100;"
        ),
        mongodb=(
            f"// {child} has its own collection — query independently\n"
            f"db.{parent}.findOne({{ _id: id }})\n\n"
            f"db.{child}.find(\n"
            f"  {{ created_at: {{ $gt: since }} }}\n"
            f").sort({{ created_at: -1 }}).limit(100)\n\n"
            f"// Add indexes to {child} for common access patterns:\n"
            f"db.{child}.createIndex({{ created_at: -1 }})\n"
            f"db.{child}.createIndex({{ {parent}_id: 1, created_at: -1 }})"
        ),
        explanation=(
            f"{child} is kept completely separate because it has independent "
            f"query patterns, high write volume, or is accessed more often alone "
            f"than together with {parent}. Index carefully to avoid collection scans."
        ),
    )


def _bucket_rewrite(parent: str, child: str) -> QueryRewriteExample:
    return QueryRewriteExample(
        relationship=f"{parent} → {child}",
        decision="BUCKET",
        scenario=f"Time-series bucketing for {child} under {parent}",
        sql=(
            f"SELECT * FROM {child}\n"
            f"WHERE {parent}_id = :id\n"
            f"  AND recorded_at BETWEEN :start AND :end\n"
            f"ORDER BY recorded_at ASC;"
        ),
        mongodb=(
            f"// Bucket pattern: group time-series events into hourly documents\n"
            f"// Each bucket holds up to N {child} entries for one {parent}\n\n"
            f"// Bucket document structure:\n"
            f"// {{\n"
            f"//   {parent}_id: ...,\n"
            f"//   bucket_hour: ISODate('2024-01-01T14:00:00Z'),\n"
            f"//   count: 42,\n"
            f"//   events: [ {{ recorded_at: ..., value: ... }}, ... ]\n"
            f"// }}\n\n"
            f"// Query a time range:\n"
            f"db.{child}_buckets.find({{\n"
            f"  {parent}_id: id,\n"
            f"  bucket_hour: {{ $gte: startHour, $lte: endHour }}\n"
            f"}}).sort({{ bucket_hour: 1 }})"
        ),
        explanation=(
            f"Time-series data for {child} is bucketed by time window to avoid "
            f"unbounded arrays and improve range query performance. "
            f"Each bucket document covers one time period (e.g. 1 hour) per {parent}."
        ),
    )


_REWRITE_DISPATCH = {
    RelationshipDecision.EMBED: _embed_rewrite,
    RelationshipDecision.REFERENCE: _reference_rewrite,
    RelationshipDecision.SEPARATE: _separate_rewrite,
    RelationshipDecision.BUCKET: _bucket_rewrite,
}


def generate_rewrites(
    recommendations: list[SchemaRecommendation],
    min_confidence: float | None = None,
) -> RewriteResult:
    """
    Generate SQL → MongoDB query rewrite examples for recommendations.

    Uses rule-based templates keyed on the relationship decision.
    No API call required.

    Args:
        recommendations: List of schema recommendations.
        min_confidence: If set, only generate rewrites for recommendations
                        at or above this confidence threshold.

    Returns:
        RewriteResult containing examples and any errors.
    """
    result = RewriteResult()

    for rec in recommendations:
        # Respect confidence filter if provided
        if min_confidence is not None and rec.confidence < min_confidence:
            continue

        try:
            fn = _REWRITE_DISPATCH.get(rec.decision)
            if fn:
                example = fn(rec.parent_table, rec.child_table)
                result.examples.append(example)
            else:
                result.errors.append(
                    f"No rewrite template for decision '{rec.decision}' "
                    f"({rec.parent_table} → {rec.child_table})"
                )
        except Exception as e:
            result.errors.append(
                f"Failed to generate rewrite for {rec.parent_table} → "
                f"{rec.child_table}: {e}"
            )
            logger.debug("Rewrite error", exc_info=True)

    return result