"""Tests for query_rewriter module."""

import pytest

from schema_travels.recommender.models import RelationshipDecision, SchemaRecommendation
from schema_travels.recommender.query_rewriter import (
    QueryRewriteExample,
    RewriteResult,
    generate_rewrites,
    _embed_rewrite,
    _reference_rewrite,
    _separate_rewrite,
    _bucket_rewrite,
    _REWRITE_DISPATCH,
)


# =============================================================================
# Fixtures
# =============================================================================

@pytest.fixture
def embed_recommendation():
    """Create a sample EMBED recommendation."""
    return SchemaRecommendation(
        parent_table="users",
        child_table="profiles",
        decision=RelationshipDecision.EMBED,
        confidence=0.95,
        reasoning=["1:1 relationship", "Always accessed together"],
        warnings=[],
    )


@pytest.fixture
def reference_recommendation():
    """Create a sample REFERENCE recommendation."""
    return SchemaRecommendation(
        parent_table="users",
        child_table="orders",
        decision=RelationshipDecision.REFERENCE,
        confidence=0.85,
        reasoning=["1:many relationship", "Orders queried independently"],
        warnings=["May require additional indexes"],
    )


@pytest.fixture
def separate_recommendation():
    """Create a sample SEPARATE recommendation."""
    return SchemaRecommendation(
        parent_table="users",
        child_table="activity_logs",
        decision=RelationshipDecision.SEPARATE,
        confidence=0.97,
        reasoning=["Very high write volume", "Independent query patterns"],
        warnings=[],
    )


@pytest.fixture
def bucket_recommendation():
    """Create a sample BUCKET recommendation."""
    return SchemaRecommendation(
        parent_table="devices",
        child_table="telemetry",
        decision=RelationshipDecision.BUCKET,
        confidence=0.92,
        reasoning=["Time-series data", "Range queries common"],
        warnings=["Consider bucket size based on write rate"],
    )


@pytest.fixture
def mixed_recommendations(embed_recommendation, reference_recommendation, 
                          separate_recommendation, bucket_recommendation):
    """Create a list of all recommendation types."""
    return [
        embed_recommendation,
        reference_recommendation,
        separate_recommendation,
        bucket_recommendation,
    ]


@pytest.fixture
def ecommerce_recommendations():
    """Create realistic e-commerce schema recommendations."""
    return [
        SchemaRecommendation(
            parent_table="users",
            child_table="user_profiles",
            decision=RelationshipDecision.EMBED,
            confidence=0.95,
            reasoning=["1:1 relationship", "Always accessed together"],
            warnings=[],
        ),
        SchemaRecommendation(
            parent_table="users",
            child_table="orders",
            decision=RelationshipDecision.REFERENCE,
            confidence=0.88,
            reasoning=["1:many relationship", "Orders queried independently"],
            warnings=[],
        ),
        SchemaRecommendation(
            parent_table="orders",
            child_table="order_items",
            decision=RelationshipDecision.EMBED,
            confidence=0.92,
            reasoning=["Bounded 1:few", "Always fetched with order"],
            warnings=[],
        ),
        SchemaRecommendation(
            parent_table="users",
            child_table="activity_logs",
            decision=RelationshipDecision.SEPARATE,
            confidence=0.97,
            reasoning=["Very high volume", "Independent query patterns"],
            warnings=[],
        ),
    ]


# =============================================================================
# QueryRewriteExample Tests
# =============================================================================

class TestQueryRewriteExample:
    """Tests for QueryRewriteExample dataclass."""

    def test_create_basic_example(self):
        """Test creating a basic QueryRewriteExample."""
        example = QueryRewriteExample(
            relationship="users → orders",
            decision="EMBED",
            scenario="Fetch user with all orders",
            sql="SELECT * FROM users JOIN orders ON ...",
            mongodb="db.users.findOne({ _id: id })",
            explanation="Orders are embedded in users."
        )
        
        assert example.relationship == "users → orders"
        assert example.decision == "EMBED"
        assert example.scenario == "Fetch user with all orders"
        assert "SELECT" in example.sql
        assert "findOne" in example.mongodb
        assert "embedded" in example.explanation

    def test_all_fields_required(self):
        """Test that all fields are required."""
        with pytest.raises(TypeError):
            QueryRewriteExample(
                relationship="users → orders",
                decision="EMBED",
            )

    def test_example_with_complex_content(self):
        """Test example with multi-line SQL and MongoDB."""
        example = QueryRewriteExample(
            relationship="parent → child",
            decision="REFERENCE",
            scenario="Complex query",
            sql="SELECT p.*, c.*\nFROM parent p\nJOIN child c ON c.parent_id = p.id",
            mongodb="db.parent.aggregate([\n  { $lookup: { ... } }\n])",
            explanation="Multi-line explanation\nwith details."
        )
        
        assert "\n" in example.sql
        assert "\n" in example.mongodb


# =============================================================================
# RewriteResult Tests
# =============================================================================

class TestRewriteResult:
    """Tests for RewriteResult dataclass."""

    def test_default_empty_lists(self):
        """Test that RewriteResult defaults to empty lists."""
        result = RewriteResult()
        assert result.examples == []
        assert result.errors == []

    def test_with_examples(self):
        """Test RewriteResult with examples."""
        example = QueryRewriteExample(
            relationship="users → orders",
            decision="EMBED",
            scenario="Test",
            sql="SELECT ...",
            mongodb="db.find()",
            explanation="Test explanation"
        )
        result = RewriteResult(examples=[example])
        
        assert len(result.examples) == 1
        assert result.examples[0].relationship == "users → orders"

    def test_with_errors(self):
        """Test RewriteResult with errors."""
        result = RewriteResult(errors=["Error 1", "Error 2"])
        
        assert len(result.errors) == 2
        assert "Error 1" in result.errors

    def test_with_both_examples_and_errors(self):
        """Test RewriteResult with both examples and errors."""
        example = QueryRewriteExample(
            relationship="a → b",
            decision="EMBED",
            scenario="Test",
            sql="SELECT",
            mongodb="db.find()",
            explanation="Test"
        )
        result = RewriteResult(
            examples=[example],
            errors=["Partial failure"]
        )
        
        assert len(result.examples) == 1
        assert len(result.errors) == 1


# =============================================================================
# Embed Rewrite Tests
# =============================================================================

class TestEmbedRewrite:
    """Tests for _embed_rewrite function."""

    def test_basic_embed_rewrite(self):
        """Test basic embed rewrite generation."""
        example = _embed_rewrite("users", "orders")
        
        assert example.relationship == "users → orders"
        assert example.decision == "EMBED"
        assert "Fetch users with all its orders" in example.scenario

    def test_embed_sql_contains_join(self):
        """Test that embed SQL contains JOIN."""
        example = _embed_rewrite("users", "orders")
        
        assert "JOIN" in example.sql
        assert "users" in example.sql
        assert "orders" in example.sql
        assert "users_id" in example.sql

    def test_embed_mongodb_uses_findone(self):
        """Test that embed MongoDB uses findOne."""
        example = _embed_rewrite("users", "orders")
        
        assert "findOne" in example.mongodb
        assert "single read" in example.mongodb.lower()
        assert "no join" in example.mongodb.lower()

    def test_embed_explanation_mentions_coaccess(self):
        """Test that embed explanation mentions co-access."""
        example = _embed_rewrite("products", "reviews")
        
        assert "co-access" in example.explanation
        assert "embedding eliminates the JOIN" in example.explanation

    def test_embed_with_underscore_names(self):
        """Test embed rewrite with underscore table names."""
        example = _embed_rewrite("blog_posts", "post_comments")
        
        assert "blog_posts → post_comments" == example.relationship
        assert "blog_posts" in example.sql
        assert "post_comments" in example.sql
        assert "blog_posts_id" in example.sql


# =============================================================================
# Reference Rewrite Tests
# =============================================================================

class TestReferenceRewrite:
    """Tests for _reference_rewrite function."""

    def test_basic_reference_rewrite(self):
        """Test basic reference rewrite generation."""
        example = _reference_rewrite("users", "orders")
        
        assert example.relationship == "users → orders"
        assert example.decision == "REFERENCE"
        assert "optionally load" in example.scenario

    def test_reference_sql_uses_left_join(self):
        """Test that reference SQL uses LEFT JOIN."""
        example = _reference_rewrite("users", "orders")
        
        assert "LEFT JOIN" in example.sql

    def test_reference_mongodb_shows_two_queries(self):
        """Test that reference MongoDB shows two-query pattern."""
        example = _reference_rewrite("users", "orders")
        
        assert "Step 1" in example.mongodb
        assert "Step 2" in example.mongodb
        assert "findOne" in example.mongodb
        assert ".find(" in example.mongodb

    def test_reference_mongodb_shows_lookup_alternative(self):
        """Test that reference MongoDB shows $lookup alternative."""
        example = _reference_rewrite("customers", "invoices")
        
        assert "$lookup" in example.mongodb
        assert "aggregate" in example.mongodb
        assert "from: 'invoices'" in example.mongodb
        assert "localField" in example.mongodb
        assert "foreignField" in example.mongodb

    def test_reference_explanation_mentions_separate(self):
        """Test that reference explanation mentions separate collection."""
        example = _reference_rewrite("users", "orders")
        
        assert "separate collection" in example.explanation
        assert "$lookup" in example.explanation


# =============================================================================
# Separate Rewrite Tests
# =============================================================================

class TestSeparateRewrite:
    """Tests for _separate_rewrite function."""

    def test_basic_separate_rewrite(self):
        """Test basic separate rewrite generation."""
        example = _separate_rewrite("users", "audit_logs")
        
        assert example.relationship == "users → audit_logs"
        assert example.decision == "SEPARATE"
        assert "independently" in example.scenario

    def test_separate_sql_shows_two_queries(self):
        """Test that separate SQL shows two independent queries."""
        example = _separate_rewrite("users", "logs")
        
        assert "Query 1" in example.sql
        assert "Query 2" in example.sql
        assert "ORDER BY" in example.sql
        assert "LIMIT" in example.sql

    def test_separate_mongodb_suggests_indexes(self):
        """Test that separate MongoDB suggests indexes."""
        example = _separate_rewrite("devices", "metrics")
        
        assert "createIndex" in example.mongodb
        assert "created_at" in example.mongodb
        assert "devices_id" in example.mongodb

    def test_separate_explanation_mentions_independence(self):
        """Test that separate explanation mentions independence."""
        example = _separate_rewrite("users", "events")
        
        assert "independent" in example.explanation
        assert "query patterns" in example.explanation
        assert "high write volume" in example.explanation.lower()


# =============================================================================
# Bucket Rewrite Tests
# =============================================================================

class TestBucketRewrite:
    """Tests for _bucket_rewrite function."""

    def test_basic_bucket_rewrite(self):
        """Test basic bucket rewrite generation."""
        example = _bucket_rewrite("sensors", "readings")
        
        assert example.relationship == "sensors → readings"
        assert example.decision == "BUCKET"
        assert "Time-series" in example.scenario

    def test_bucket_sql_uses_between(self):
        """Test that bucket SQL uses BETWEEN for time range."""
        example = _bucket_rewrite("devices", "events")
        
        assert "BETWEEN" in example.sql
        assert "recorded_at" in example.sql
        assert "ORDER BY recorded_at ASC" in example.sql

    def test_bucket_mongodb_shows_bucket_structure(self):
        """Test that bucket MongoDB shows bucket document structure."""
        example = _bucket_rewrite("sensors", "readings")
        
        assert "bucket_hour" in example.mongodb
        assert "ISODate" in example.mongodb
        assert "readings_buckets" in example.mongodb

    def test_bucket_mongodb_shows_range_query(self):
        """Test that bucket MongoDB shows range query pattern."""
        example = _bucket_rewrite("machines", "telemetry")
        
        assert "$gte" in example.mongodb
        assert "$lte" in example.mongodb
        assert "sort" in example.mongodb

    def test_bucket_explanation_mentions_time_window(self):
        """Test that bucket explanation mentions time window."""
        example = _bucket_rewrite("iot", "data")
        
        assert "time window" in example.explanation
        assert "unbounded arrays" in example.explanation
        assert "range query" in example.explanation


# =============================================================================
# Dispatch Table Tests
# =============================================================================

class TestRewriteDispatch:
    """Tests for _REWRITE_DISPATCH dictionary."""

    def test_dispatch_contains_all_decisions(self):
        """Test that dispatch has entries for all relationship decisions."""
        assert RelationshipDecision.EMBED in _REWRITE_DISPATCH
        assert RelationshipDecision.REFERENCE in _REWRITE_DISPATCH
        assert RelationshipDecision.SEPARATE in _REWRITE_DISPATCH
        assert RelationshipDecision.BUCKET in _REWRITE_DISPATCH

    def test_dispatch_has_four_entries(self):
        """Test dispatch has exactly four entries."""
        assert len(_REWRITE_DISPATCH) == 4

    def test_dispatch_functions_are_callable(self):
        """Test that all dispatch values are callable."""
        for decision, fn in _REWRITE_DISPATCH.items():
            assert callable(fn)

    def test_dispatch_functions_return_correct_type(self):
        """Test that all dispatch functions return QueryRewriteExample."""
        for decision, fn in _REWRITE_DISPATCH.items():
            result = fn("parent", "child")
            assert isinstance(result, QueryRewriteExample)

    def test_dispatch_decisions_match_function_output(self):
        """Test dispatch function outputs match expected decisions."""
        assert _REWRITE_DISPATCH[RelationshipDecision.EMBED]("a", "b").decision == "EMBED"
        assert _REWRITE_DISPATCH[RelationshipDecision.REFERENCE]("a", "b").decision == "REFERENCE"
        assert _REWRITE_DISPATCH[RelationshipDecision.SEPARATE]("a", "b").decision == "SEPARATE"
        assert _REWRITE_DISPATCH[RelationshipDecision.BUCKET]("a", "b").decision == "BUCKET"


# =============================================================================
# generate_rewrites Basic Tests
# =============================================================================

class TestGenerateRewritesBasic:
    """Basic tests for generate_rewrites function."""

    def test_empty_recommendations(self):
        """Test generate_rewrites with empty list."""
        result = generate_rewrites([])
        
        assert result.examples == []
        assert result.errors == []

    def test_single_embed_recommendation(self, embed_recommendation):
        """Test generate_rewrites with single EMBED recommendation."""
        result = generate_rewrites([embed_recommendation])
        
        assert len(result.examples) == 1
        assert result.examples[0].decision == "EMBED"
        assert result.examples[0].relationship == "users → profiles"
        assert len(result.errors) == 0

    def test_single_reference_recommendation(self, reference_recommendation):
        """Test generate_rewrites with single REFERENCE recommendation."""
        result = generate_rewrites([reference_recommendation])
        
        assert len(result.examples) == 1
        assert result.examples[0].decision == "REFERENCE"
        assert len(result.errors) == 0

    def test_single_separate_recommendation(self, separate_recommendation):
        """Test generate_rewrites with single SEPARATE recommendation."""
        result = generate_rewrites([separate_recommendation])
        
        assert len(result.examples) == 1
        assert result.examples[0].decision == "SEPARATE"

    def test_single_bucket_recommendation(self, bucket_recommendation):
        """Test generate_rewrites with single BUCKET recommendation."""
        result = generate_rewrites([bucket_recommendation])
        
        assert len(result.examples) == 1
        assert result.examples[0].decision == "BUCKET"

    def test_multiple_recommendations(self, mixed_recommendations):
        """Test generate_rewrites with multiple recommendations."""
        result = generate_rewrites(mixed_recommendations)
        
        assert len(result.examples) == 4
        decisions = {ex.decision for ex in result.examples}
        assert decisions == {"EMBED", "REFERENCE", "SEPARATE", "BUCKET"}


# =============================================================================
# generate_rewrites Confidence Filter Tests
# =============================================================================

class TestGenerateRewritesConfidence:
    """Tests for generate_rewrites min_confidence parameter."""

    def test_min_confidence_filters_low_confidence(self):
        """Test that min_confidence filters out low-confidence recommendations."""
        recommendations = [
            SchemaRecommendation(
                parent_table="users",
                child_table="profiles",
                decision=RelationshipDecision.EMBED,
                confidence=0.9,
                reasoning=[],
            ),
            SchemaRecommendation(
                parent_table="users",
                child_table="orders",
                decision=RelationshipDecision.REFERENCE,
                confidence=0.5,  # Below threshold
                reasoning=[],
            ),
        ]
        
        result = generate_rewrites(recommendations, min_confidence=0.7)
        
        assert len(result.examples) == 1
        assert result.examples[0].relationship == "users → profiles"

    def test_min_confidence_none_includes_all(self):
        """Test that min_confidence=None includes all recommendations."""
        recommendations = [
            SchemaRecommendation(
                parent_table="a",
                child_table="b",
                decision=RelationshipDecision.EMBED,
                confidence=0.1,
                reasoning=[],
            ),
            SchemaRecommendation(
                parent_table="c",
                child_table="d",
                decision=RelationshipDecision.REFERENCE,
                confidence=0.99,
                reasoning=[],
            ),
        ]
        
        result = generate_rewrites(recommendations, min_confidence=None)
        
        assert len(result.examples) == 2

    def test_min_confidence_exact_boundary_included(self):
        """Test that exact boundary value is included."""
        rec = SchemaRecommendation(
            parent_table="users",
            child_table="posts",
            decision=RelationshipDecision.EMBED,
            confidence=0.7,
            reasoning=[],
        )
        
        # At boundary - should be included (>= not >)
        result = generate_rewrites([rec], min_confidence=0.7)
        assert len(result.examples) == 1

    def test_min_confidence_above_boundary_excluded(self):
        """Test that values below boundary are excluded."""
        rec = SchemaRecommendation(
            parent_table="users",
            child_table="posts",
            decision=RelationshipDecision.EMBED,
            confidence=0.7,
            reasoning=[],
        )
        
        # Just above boundary - should be excluded
        result = generate_rewrites([rec], min_confidence=0.71)
        assert len(result.examples) == 0

    def test_min_confidence_zero(self):
        """Test min_confidence=0 includes all."""
        rec = SchemaRecommendation(
            parent_table="a",
            child_table="b",
            decision=RelationshipDecision.EMBED,
            confidence=0.0,
            reasoning=[],
        )
        
        result = generate_rewrites([rec], min_confidence=0.0)
        assert len(result.examples) == 1

    def test_min_confidence_one(self):
        """Test min_confidence=1.0 filters most."""
        recommendations = [
            SchemaRecommendation(
                parent_table="a",
                child_table="b",
                decision=RelationshipDecision.EMBED,
                confidence=0.99,
                reasoning=[],
            ),
            SchemaRecommendation(
                parent_table="c",
                child_table="d",
                decision=RelationshipDecision.EMBED,
                confidence=1.0,
                reasoning=[],
            ),
        ]
        
        result = generate_rewrites(recommendations, min_confidence=1.0)
        assert len(result.examples) == 1
        assert result.examples[0].relationship == "c → d"


# =============================================================================
# generate_rewrites Order Tests
# =============================================================================

class TestGenerateRewritesOrder:
    """Tests for generate_rewrites output ordering."""

    def test_preserves_order(self):
        """Test that output order matches input order."""
        recommendations = [
            SchemaRecommendation(
                parent_table="first",
                child_table="child1",
                decision=RelationshipDecision.EMBED,
                confidence=0.9,
                reasoning=[],
            ),
            SchemaRecommendation(
                parent_table="second",
                child_table="child2",
                decision=RelationshipDecision.REFERENCE,
                confidence=0.85,
                reasoning=[],
            ),
            SchemaRecommendation(
                parent_table="third",
                child_table="child3",
                decision=RelationshipDecision.SEPARATE,
                confidence=0.8,
                reasoning=[],
            ),
        ]
        
        result = generate_rewrites(recommendations)
        
        assert result.examples[0].relationship == "first → child1"
        assert result.examples[1].relationship == "second → child2"
        assert result.examples[2].relationship == "third → child3"

    def test_all_decision_types_in_mixed_batch(self, mixed_recommendations):
        """Test that all four decision types work in a single batch."""
        result = generate_rewrites(mixed_recommendations)
        
        assert len(result.examples) == 4
        decisions = {ex.decision for ex in result.examples}
        assert decisions == {"EMBED", "REFERENCE", "SEPARATE", "BUCKET"}


# =============================================================================
# Edge Case Tests
# =============================================================================

class TestGenerateRewritesEdgeCases:
    """Edge case tests for generate_rewrites."""

    def test_special_characters_in_table_names(self):
        """Test handling of table names with underscores."""
        rec = SchemaRecommendation(
            parent_table="user_accounts",
            child_table="order_line_items",
            decision=RelationshipDecision.EMBED,
            confidence=0.9,
            reasoning=[],
        )
        
        result = generate_rewrites([rec])
        
        assert len(result.examples) == 1
        assert "user_accounts" in result.examples[0].sql
        assert "order_line_items" in result.examples[0].sql

    def test_very_long_table_names(self):
        """Test handling of very long table names."""
        long_parent = "very_long_parent_table_name_that_exceeds_normal_length"
        long_child = "another_extremely_long_child_table_name_for_testing"
        
        rec = SchemaRecommendation(
            parent_table=long_parent,
            child_table=long_child,
            decision=RelationshipDecision.REFERENCE,
            confidence=0.8,
            reasoning=[],
        )
        
        result = generate_rewrites([rec])
        
        assert len(result.examples) == 1
        assert long_parent in result.examples[0].mongodb
        assert long_child in result.examples[0].mongodb

    def test_single_character_table_names(self):
        """Test handling of single character table names."""
        rec = SchemaRecommendation(
            parent_table="a",
            child_table="b",
            decision=RelationshipDecision.SEPARATE,
            confidence=0.7,
            reasoning=[],
        )
        
        result = generate_rewrites([rec])
        
        assert len(result.examples) == 1
        assert "a → b" == result.examples[0].relationship

    def test_numeric_table_names(self):
        """Test handling of numeric-like table names."""
        rec = SchemaRecommendation(
            parent_table="table123",
            child_table="table456",
            decision=RelationshipDecision.BUCKET,
            confidence=0.9,
            reasoning=[],
        )
        
        result = generate_rewrites([rec])
        
        assert len(result.examples) == 1
        assert "table123" in result.examples[0].sql

    def test_duplicate_recommendations(self):
        """Test handling of duplicate recommendations."""
        rec1 = SchemaRecommendation(
            parent_table="users",
            child_table="orders",
            decision=RelationshipDecision.EMBED,
            confidence=0.9,
            reasoning=[],
        )
        rec2 = SchemaRecommendation(
            parent_table="users",
            child_table="orders",
            decision=RelationshipDecision.EMBED,
            confidence=0.9,
            reasoning=[],
        )
        
        result = generate_rewrites([rec1, rec2])
        
        # Should generate both (no deduplication at this level)
        assert len(result.examples) == 2

    def test_large_batch_performance(self):
        """Test handling of a large batch of recommendations."""
        decisions = list(RelationshipDecision)
        recommendations = [
            SchemaRecommendation(
                parent_table=f"parent_{i}",
                child_table=f"child_{i}",
                decision=decisions[i % 4],
                confidence=0.5 + (i % 50) / 100,
                reasoning=[],
            )
            for i in range(100)
        ]
        
        result = generate_rewrites(recommendations)
        
        assert len(result.examples) == 100
        assert len(result.errors) == 0


# =============================================================================
# Integration Tests
# =============================================================================

class TestQueryRewriterIntegration:
    """Integration tests simulating real-world usage."""

    def test_ecommerce_schema_scenario(self, ecommerce_recommendations):
        """Test realistic e-commerce schema migration scenario."""
        result = generate_rewrites(ecommerce_recommendations)
        
        assert len(result.examples) == 4
        assert len(result.errors) == 0
        
        # Verify each example has proper structure
        for example in result.examples:
            assert example.relationship
            assert example.decision in ["EMBED", "REFERENCE", "SEPARATE", "BUCKET"]
            assert example.sql
            assert example.mongodb
            assert example.explanation

    def test_ecommerce_with_high_confidence_filter(self, ecommerce_recommendations):
        """Test e-commerce scenario with confidence filtering."""
        result = generate_rewrites(ecommerce_recommendations, min_confidence=0.9)
        
        # Only high-confidence recommendations
        assert len(result.examples) == 3  # 0.95, 0.92, 0.97 pass; 0.88 fails

    def test_iot_schema_scenario(self):
        """Test realistic IoT schema migration scenario."""
        recommendations = [
            SchemaRecommendation(
                parent_table="devices",
                child_table="device_configs",
                decision=RelationshipDecision.EMBED,
                confidence=0.90,
                reasoning=["Config always needed with device"],
            ),
            SchemaRecommendation(
                parent_table="devices",
                child_table="telemetry",
                decision=RelationshipDecision.BUCKET,
                confidence=0.95,
                reasoning=["Time-series data", "Range queries common"],
            ),
            SchemaRecommendation(
                parent_table="devices",
                child_table="alerts",
                decision=RelationshipDecision.REFERENCE,
                confidence=0.82,
                reasoning=["Alerts queried across devices"],
            ),
        ]
        
        result = generate_rewrites(recommendations, min_confidence=0.8)
        
        assert len(result.examples) == 3
        
        # Check bucket example specifically
        bucket_examples = [ex for ex in result.examples if ex.decision == "BUCKET"]
        assert len(bucket_examples) == 1
        assert "bucket_hour" in bucket_examples[0].mongodb
        assert "telemetry_buckets" in bucket_examples[0].mongodb

    def test_all_decisions_produce_valid_output(self, mixed_recommendations):
        """Test that all decision types produce valid SQL and MongoDB."""
        result = generate_rewrites(mixed_recommendations)
        
        for example in result.examples:
            # SQL should have SELECT or equivalent
            assert any(kw in example.sql.upper() for kw in ["SELECT", "QUERY"])
            
            # MongoDB should have db. reference
            assert "db." in example.mongodb
            
            # Explanation should be non-empty
            assert len(example.explanation) > 10


# =============================================================================
# Output Format Tests
# =============================================================================

class TestOutputFormat:
    """Tests verifying the format of generated output."""

    def test_sql_format_embed(self):
        """Test SQL format for EMBED rewrites."""
        example = _embed_rewrite("customers", "addresses")
        
        assert example.sql.startswith("SELECT")
        assert "FROM customers" in example.sql
        assert "JOIN addresses" in example.sql
        assert "WHERE" in example.sql

    def test_mongodb_format_embed(self):
        """Test MongoDB format for EMBED rewrites."""
        example = _embed_rewrite("customers", "addresses")
        
        assert "db.customers.findOne" in example.mongodb
        assert "//" in example.mongodb  # Has comments

    def test_mongodb_format_reference_has_lookup(self):
        """Test MongoDB format for REFERENCE includes $lookup."""
        example = _reference_rewrite("orders", "products")
        
        assert "$lookup" in example.mongodb
        assert "localField" in example.mongodb
        assert "foreignField" in example.mongodb
        assert "as:" in example.mongodb

    def test_mongodb_format_separate_has_indexes(self):
        """Test MongoDB format for SEPARATE includes index suggestions."""
        example = _separate_rewrite("users", "sessions")
        
        assert "createIndex" in example.mongodb

    def test_mongodb_format_bucket_has_structure(self):
        """Test MongoDB format for BUCKET shows document structure."""
        example = _bucket_rewrite("sensors", "readings")
        
        assert "Bucket document structure" in example.mongodb
        assert "bucket_hour" in example.mongodb
        assert "count:" in example.mongodb
        assert "events:" in example.mongodb

    def test_relationship_arrow_format(self):
        """Test that relationship uses arrow format."""
        for decision, fn in _REWRITE_DISPATCH.items():
            example = fn("parent", "child")
            assert "→" in example.relationship
            assert example.relationship == "parent → child"