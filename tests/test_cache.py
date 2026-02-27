"""Tests for recommendation caching."""

import json
import pytest
from pathlib import Path
from unittest.mock import MagicMock

from schema_travels.recommender.cache import (
    CacheMode,
    RecommendationCache,
    compute_input_hash,
    RECOMMENDATION_VERSION,
)
from schema_travels.recommender.models import (
    RelationshipDecision,
    SchemaRecommendation,
    TargetDatabase,
)
from schema_travels.collector.models import (
    SchemaDefinition,
    TableDefinition,
    ColumnDefinition,
    ForeignKeyDefinition,
)
from schema_travels.analyzer.models import (
    AnalysisResult,
    JoinPattern,
    MutationPattern,
)


# =============================================================================
# Fixtures
# =============================================================================

@pytest.fixture
def sample_schema():
    """Create a sample schema for testing."""
    return SchemaDefinition(
        tables=[
            TableDefinition(
                name="users",
                columns=[
                    ColumnDefinition(name="id", data_type="integer", nullable=False, is_primary_key=True),
                    ColumnDefinition(name="email", data_type="varchar", nullable=False),
                ],
                primary_key=["id"],
            ),
            TableDefinition(
                name="orders",
                columns=[
                    ColumnDefinition(name="id", data_type="integer", nullable=False, is_primary_key=True),
                    ColumnDefinition(name="user_id", data_type="integer", nullable=False),
                    ColumnDefinition(name="total", data_type="decimal", nullable=False),
                ],
                primary_key=["id"],
            ),
        ],
        foreign_keys=[
            ForeignKeyDefinition(
                constraint_name="fk_orders_user_id",
                from_table="orders",
                from_columns=["user_id"],
                to_table="users",
                to_columns=["id"],
            ),
        ],
    )


@pytest.fixture
def sample_analysis():
    """Create a sample analysis result."""
    return AnalysisResult(
        analysis_id="test123",
        source_db_type="postgres",
        total_queries_analyzed=100,
        tables_analyzed=["users", "orders"],
        join_patterns=[
            JoinPattern(
                left_table="orders",
                right_table="users",
                join_type="INNER",
                join_columns=["user_id", "id"],
                frequency=50,
                total_time_ms=100.0,
                avg_time_ms=2.0,
            ),
        ],
        mutation_patterns=[
            MutationPattern(
                table="users",
                select_count=80,
                insert_count=10,
                update_count=8,
                delete_count=2,
            ),
            MutationPattern(
                table="orders",
                select_count=40,
                insert_count=30,
                update_count=20,
                delete_count=10,
            ),
        ],
        access_patterns=[],
        table_statistics=[],
    )


@pytest.fixture
def sample_recommendations():
    """Create sample recommendations."""
    return [
        SchemaRecommendation(
            parent_table="users",
            child_table="orders",
            decision=RelationshipDecision.REFERENCE,
            confidence=0.85,
            reasoning=["High write ratio on orders", "Orders accessed independently"],
            warnings=[],
        ),
    ]


@pytest.fixture
def cache_dir(tmp_path):
    """Create a temporary cache directory."""
    cache_path = tmp_path / "cache"
    cache_path.mkdir()
    return cache_path


# =============================================================================
# CacheMode Tests
# =============================================================================

class TestCacheMode:
    """Tests for CacheMode enum."""

    def test_relaxed_mode_value(self):
        assert CacheMode.RELAXED.value == "relaxed"

    def test_strict_mode_value(self):
        assert CacheMode.STRICT.value == "strict"

    def test_mode_from_string(self):
        assert CacheMode("relaxed") == CacheMode.RELAXED
        assert CacheMode("strict") == CacheMode.STRICT


# =============================================================================
# Hash Computation Tests
# =============================================================================

class TestComputeInputHash:
    """Tests for compute_input_hash function."""

    def test_same_inputs_same_hash(self, sample_schema, sample_analysis):
        """Same inputs should produce same hash."""
        hash1 = compute_input_hash(sample_schema, sample_analysis, TargetDatabase.MONGODB)
        hash2 = compute_input_hash(sample_schema, sample_analysis, TargetDatabase.MONGODB)
        assert hash1 == hash2

    def test_different_target_different_hash(self, sample_schema, sample_analysis):
        """Different target should produce different hash."""
        hash_mongo = compute_input_hash(sample_schema, sample_analysis, TargetDatabase.MONGODB)
        hash_dynamo = compute_input_hash(sample_schema, sample_analysis, TargetDatabase.DYNAMODB)
        assert hash_mongo != hash_dynamo

    def test_different_modes_different_hash(self, sample_schema, sample_analysis):
        """Different cache modes should produce different hashes."""
        hash_relaxed = compute_input_hash(
            sample_schema, sample_analysis, TargetDatabase.MONGODB, CacheMode.RELAXED
        )
        hash_strict = compute_input_hash(
            sample_schema, sample_analysis, TargetDatabase.MONGODB, CacheMode.STRICT
        )
        assert hash_relaxed != hash_strict

    def test_hash_length(self, sample_schema, sample_analysis):
        """Hash should be 16 characters."""
        hash_val = compute_input_hash(sample_schema, sample_analysis, TargetDatabase.MONGODB)
        assert len(hash_val) == 16

    def test_hash_is_hex(self, sample_schema, sample_analysis):
        """Hash should be valid hexadecimal."""
        hash_val = compute_input_hash(sample_schema, sample_analysis, TargetDatabase.MONGODB)
        int(hash_val, 16)  # Should not raise


class TestRelaxedModeHashing:
    """Tests specific to relaxed mode hash behavior."""

    def test_relaxed_ignores_frequency_changes(self, sample_schema):
        """Relaxed mode should ignore small frequency changes."""
        # Analysis with 50 join calls
        analysis1 = AnalysisResult(
            analysis_id="test1",
            source_db_type="postgres",
            total_queries_analyzed=100,
            tables_analyzed=["users", "orders"],
            join_patterns=[
                JoinPattern(
                    left_table="orders",
                    right_table="users",
                    join_type="INNER",
                    join_columns=["user_id", "id"],
                    frequency=50,
                    total_time_ms=100.0,
                    avg_time_ms=2.0,
                ),
            ],
            mutation_patterns=[],  # No mutations - keep it simple
            access_patterns=[],
            table_statistics=[],
        )

        # Analysis with 52 join calls (small change)
        analysis2 = AnalysisResult(
            analysis_id="test2",
            source_db_type="postgres",
            total_queries_analyzed=102,
            tables_analyzed=["users", "orders"],
            join_patterns=[
                JoinPattern(
                    left_table="orders",
                    right_table="users",
                    join_type="INNER",
                    join_columns=["user_id", "id"],
                    frequency=52,  # Small frequency change
                    total_time_ms=104.0,
                    avg_time_ms=2.0,
                ),
            ],
            mutation_patterns=[],  # No mutations - keep it simple
            access_patterns=[],
            table_statistics=[],
        )

        hash1 = compute_input_hash(sample_schema, analysis1, TargetDatabase.MONGODB, CacheMode.RELAXED)
        hash2 = compute_input_hash(sample_schema, analysis2, TargetDatabase.MONGODB, CacheMode.RELAXED)

        assert hash1 == hash2, "Relaxed mode should produce same hash for small frequency changes"

    def test_relaxed_detects_new_join_pair(self, sample_schema):
        """Relaxed mode should detect new join pairs."""
        analysis1 = AnalysisResult(
            analysis_id="test1",
            source_db_type="postgres",
            total_queries_analyzed=100,
            tables_analyzed=["users", "orders"],
            join_patterns=[
                JoinPattern(
                    left_table="orders",
                    right_table="users",
                    join_type="INNER",
                    join_columns=["user_id", "id"],
                    frequency=50,
                    total_time_ms=100.0,
                    avg_time_ms=2.0,
                ),
            ],
            mutation_patterns=[],
            access_patterns=[],
            table_statistics=[],
        )

        analysis2 = AnalysisResult(
            analysis_id="test2",
            source_db_type="postgres",
            total_queries_analyzed=100,
            tables_analyzed=["users", "orders", "products"],
            join_patterns=[
                JoinPattern(
                    left_table="orders",
                    right_table="users",
                    join_type="INNER",
                    join_columns=["user_id", "id"],
                    frequency=50,
                    total_time_ms=100.0,
                    avg_time_ms=2.0,
                ),
                JoinPattern(
                    left_table="orders",
                    right_table="products",  # New join!
                    join_type="INNER",
                    join_columns=["product_id", "id"],
                    frequency=30,
                    total_time_ms=60.0,
                    avg_time_ms=2.0,
                ),
            ],
            mutation_patterns=[],
            access_patterns=[],
            table_statistics=[],
        )

        hash1 = compute_input_hash(sample_schema, analysis1, TargetDatabase.MONGODB, CacheMode.RELAXED)
        hash2 = compute_input_hash(sample_schema, analysis2, TargetDatabase.MONGODB, CacheMode.RELAXED)

        assert hash1 != hash2, "Relaxed mode should detect new join pairs"


class TestStrictModeHashing:
    """Tests specific to strict mode hash behavior."""

    def test_strict_detects_frequency_changes(self, sample_schema):
        """Strict mode should detect frequency changes."""
        analysis1 = AnalysisResult(
            analysis_id="test1",
            source_db_type="postgres",
            total_queries_analyzed=100,
            tables_analyzed=["users", "orders"],
            join_patterns=[
                JoinPattern(
                    left_table="orders",
                    right_table="users",
                    join_type="INNER",
                    join_columns=["user_id", "id"],
                    frequency=50,
                    total_time_ms=100.0,
                    avg_time_ms=2.0,
                ),
            ],
            mutation_patterns=[],
            access_patterns=[],
            table_statistics=[],
        )

        analysis2 = AnalysisResult(
            analysis_id="test2",
            source_db_type="postgres",
            total_queries_analyzed=102,
            tables_analyzed=["users", "orders"],
            join_patterns=[
                JoinPattern(
                    left_table="orders",
                    right_table="users",
                    join_type="INNER",
                    join_columns=["user_id", "id"],
                    frequency=52,  # Different frequency
                    total_time_ms=104.0,
                    avg_time_ms=2.0,
                ),
            ],
            mutation_patterns=[],
            access_patterns=[],
            table_statistics=[],
        )

        hash1 = compute_input_hash(sample_schema, analysis1, TargetDatabase.MONGODB, CacheMode.STRICT)
        hash2 = compute_input_hash(sample_schema, analysis2, TargetDatabase.MONGODB, CacheMode.STRICT)

        assert hash1 != hash2, "Strict mode should detect frequency changes"


# =============================================================================
# RecommendationCache Tests
# =============================================================================

class TestRecommendationCache:
    """Tests for RecommendationCache class."""

    def test_cache_miss_returns_none(self, cache_dir):
        """Cache miss should return None."""
        cache = RecommendationCache(cache_dir)
        result = cache.get("nonexistent_hash")
        assert result is None

    def test_put_and_get(self, cache_dir, sample_recommendations):
        """Should be able to store and retrieve recommendations."""
        cache = RecommendationCache(cache_dir)
        input_hash = "abc123def456"

        cache.put(input_hash, sample_recommendations)
        retrieved = cache.get(input_hash)

        assert retrieved is not None
        assert len(retrieved) == 1
        assert retrieved[0].parent_table == "users"
        assert retrieved[0].child_table == "orders"
        assert retrieved[0].decision == RelationshipDecision.REFERENCE

    def test_cache_creates_files(self, cache_dir, sample_recommendations):
        """Cache should create index and data files."""
        cache = RecommendationCache(cache_dir)
        input_hash = "test_hash_123"

        cache.put(input_hash, sample_recommendations)

        assert (cache_dir / "index.json").exists()
        assert (cache_dir / f"{input_hash}.json").exists()

    def test_invalidate_single(self, cache_dir, sample_recommendations):
        """Should be able to invalidate a single cache entry."""
        cache = RecommendationCache(cache_dir)
        input_hash = "to_invalidate"

        cache.put(input_hash, sample_recommendations)
        assert cache.get(input_hash) is not None

        result = cache.invalidate(input_hash)
        assert result is True
        assert cache.get(input_hash) is None

    def test_invalidate_nonexistent(self, cache_dir):
        """Invalidating nonexistent entry should return False."""
        cache = RecommendationCache(cache_dir)
        result = cache.invalidate("nonexistent")
        assert result is False

    def test_invalidate_all(self, cache_dir, sample_recommendations):
        """Should be able to invalidate all cache entries."""
        cache = RecommendationCache(cache_dir)

        cache.put("hash1", sample_recommendations)
        cache.put("hash2", sample_recommendations)
        cache.put("hash3", sample_recommendations)

        count = cache.invalidate_all()
        assert count == 3

        assert cache.get("hash1") is None
        assert cache.get("hash2") is None
        assert cache.get("hash3") is None

    def test_list_entries(self, cache_dir, sample_recommendations):
        """Should list all cache entries."""
        cache = RecommendationCache(cache_dir)

        cache.put("hash_a", sample_recommendations)
        cache.put("hash_b", sample_recommendations)

        entries = cache.list_entries()
        assert len(entries) == 2

        hashes = [e["hash"] for e in entries]
        assert "hash_a" in hashes
        assert "hash_b" in hashes

    def test_version_mismatch_invalidates(self, cache_dir, sample_recommendations):
        """Cache entry with different version should be invalidated."""
        cache = RecommendationCache(cache_dir)
        input_hash = "versioned_hash"

        cache.put(input_hash, sample_recommendations)

        # Manually modify the cached version in the file
        cache_file = cache_dir / f"{input_hash}.json"
        with open(cache_file) as f:
            data = json.load(f)
        data["version"] = "0.0.0"  # Old version
        with open(cache_file, "w") as f:
            json.dump(data, f)

        # Also update index
        index_file = cache_dir / "index.json"
        with open(index_file) as f:
            index = json.load(f)
        index["entries"][input_hash]["version"] = "0.0.0"
        with open(index_file, "w") as f:
            json.dump(index, f)

        # Create a NEW cache instance to reload from disk
        cache2 = RecommendationCache(cache_dir)
        
        # Should return None due to version mismatch
        result = cache2.get(input_hash)
        assert result is None

    def test_metadata_stored(self, cache_dir, sample_recommendations):
        """Metadata should be stored with cache entry."""
        cache = RecommendationCache(cache_dir)
        input_hash = "meta_test"
        metadata = {"analysis_id": "abc123", "cache_mode": "relaxed"}

        cache.put(input_hash, sample_recommendations, metadata=metadata)

        cache_file = cache_dir / f"{input_hash}.json"
        with open(cache_file) as f:
            data = json.load(f)

        assert data["metadata"]["analysis_id"] == "abc123"
        assert data["metadata"]["cache_mode"] == "relaxed"


class TestCacheComparison:
    """Tests for cache comparison functionality."""

    def test_compare_identical(self, cache_dir, sample_recommendations):
        """Comparing identical caches should show no changes."""
        cache = RecommendationCache(cache_dir)

        cache.put("hash1", sample_recommendations)
        cache.put("hash2", sample_recommendations)

        comparison = cache.compare("hash1", "hash2")

        assert comparison["is_identical"] is True
        assert len(comparison["changes"]) == 0

    def test_compare_decision_change(self, cache_dir):
        """Should detect decision changes between caches."""
        cache = RecommendationCache(cache_dir)

        recs1 = [
            SchemaRecommendation(
                parent_table="users",
                child_table="orders",
                decision=RelationshipDecision.EMBED,
                confidence=0.8,
                reasoning=[],
                warnings=[],
            )
        ]

        recs2 = [
            SchemaRecommendation(
                parent_table="users",
                child_table="orders",
                decision=RelationshipDecision.REFERENCE,  # Changed!
                confidence=0.8,
                reasoning=[],
                warnings=[],
            )
        ]

        cache.put("hash1", recs1)
        cache.put("hash2", recs2)

        comparison = cache.compare("hash1", "hash2")

        assert comparison["is_identical"] is False
        assert len(comparison["changes"]) == 1
        assert comparison["changes"][0]["type"] == "decision_changed"


# =============================================================================
# Integration Tests
# =============================================================================

class TestCacheIntegration:
    """Integration tests for cache with real-ish data flow."""

    def test_full_cache_workflow(self, cache_dir, sample_schema, sample_analysis, sample_recommendations):
        """Test complete cache workflow."""
        cache = RecommendationCache(cache_dir)

        # Compute hash
        input_hash = compute_input_hash(
            sample_schema, sample_analysis, TargetDatabase.MONGODB, CacheMode.RELAXED
        )

        # Cache miss
        assert cache.get(input_hash) is None

        # Store recommendations
        cache.put(input_hash, sample_recommendations)

        # Cache hit
        retrieved = cache.get(input_hash)
        assert retrieved is not None
        assert len(retrieved) == len(sample_recommendations)

        # Verify content
        assert retrieved[0].parent_table == sample_recommendations[0].parent_table
        assert retrieved[0].decision == sample_recommendations[0].decision

    def test_different_modes_separate_caches(self, cache_dir, sample_schema, sample_analysis, sample_recommendations):
        """Different cache modes should have separate cache entries."""
        cache = RecommendationCache(cache_dir)

        hash_relaxed = compute_input_hash(
            sample_schema, sample_analysis, TargetDatabase.MONGODB, CacheMode.RELAXED
        )
        hash_strict = compute_input_hash(
            sample_schema, sample_analysis, TargetDatabase.MONGODB, CacheMode.STRICT
        )

        # Store in relaxed
        cache.put(hash_relaxed, sample_recommendations)

        # Strict should miss
        assert cache.get(hash_strict) is None

        # Relaxed should hit
        assert cache.get(hash_relaxed) is not None