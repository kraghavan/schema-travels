"""Tests for DynamoDB Designer module (v2.0.0).

Tests cover:
- Access clustering with Union-Find
- Single-table vs multi-table mode decision
- PK/SK assignment logic
- GSI detection from filtered columns
- Complete design generation
"""

import pytest

from schema_travels.analyzer.models import (
    AccessPattern,
    TableStatistics,
    JoinPattern,
    MutationPattern,
)
from schema_travels.recommender.dynamodb_models import (
    DesignMode,
    ProjectionType,
)
from schema_travels.recommender.dynamodb_designer import (
    UnionFind,
    DynamoDBDesigner,
)


# =============================================================================
# Union-Find Tests
# =============================================================================

class TestUnionFind:
    """Tests for Union-Find data structure."""

    def test_basic_union(self):
        """Test basic union operation."""
        uf = UnionFind(["a", "b", "c"])
        uf.union("a", "b")
        
        assert uf.find("a") == uf.find("b")
        assert uf.find("c") != uf.find("a")

    def test_transitive_union(self):
        """Test transitive union (a-b, b-c → a,b,c in same cluster)."""
        uf = UnionFind(["a", "b", "c"])
        uf.union("a", "b")
        uf.union("b", "c")
        
        assert uf.find("a") == uf.find("b") == uf.find("c")

    def test_get_clusters(self):
        """Test cluster extraction."""
        uf = UnionFind(["a", "b", "c", "d"])
        uf.union("a", "b")
        uf.union("c", "d")
        
        clusters = uf.get_clusters()
        assert len(clusters) == 2
        
        # Find which cluster has 'a'
        a_cluster = None
        for root, members in clusters.items():
            if "a" in members:
                a_cluster = members
        
        assert a_cluster is not None
        assert "b" in a_cluster
        assert "c" not in a_cluster

    def test_no_unions(self):
        """Test when no unions performed - each item is own cluster."""
        uf = UnionFind(["a", "b", "c"])
        clusters = uf.get_clusters()
        
        assert len(clusters) == 3

    def test_path_compression(self):
        """Test that path compression works."""
        uf = UnionFind(["a", "b", "c", "d", "e"])
        uf.union("a", "b")
        uf.union("b", "c")
        uf.union("c", "d")
        uf.union("d", "e")
        
        # After finds, all should point to same root
        root = uf.find("e")
        assert uf.find("a") == root
        assert uf.find("c") == root


# =============================================================================
# Access Clustering Tests
# =============================================================================

class TestAccessClustering:
    """Tests for access cluster detection."""

    def test_high_co_access_creates_cluster(self):
        """Tables with high co-access should be clustered."""
        designer = DynamoDBDesigner(co_access_threshold=0.70)
        
        access_patterns = [
            AccessPattern(
                table_a="users",
                table_b="orders",
                co_access_count=80,
                table_a_solo_count=10,
                table_b_solo_count=10,
            ),
        ]
        
        table_stats = [
            TableStatistics(table="users", total_accesses=100, solo_accesses=20, joined_accesses=80),
            TableStatistics(table="orders", total_accesses=100, solo_accesses=20, joined_accesses=80),
        ]
        
        stats_map = {s.table: s for s in table_stats}
        
        clusters = designer._build_access_clusters(
            tables=["users", "orders"],
            access_patterns=access_patterns,
            stats_map=stats_map,
        )
        
        # Should be 1 cluster with both tables
        assert len(clusters) == 1
        assert "users" in clusters[0].tables
        assert "orders" in clusters[0].tables

    def test_low_co_access_separate_clusters(self):
        """Tables with low co-access should be in separate clusters."""
        designer = DynamoDBDesigner(co_access_threshold=0.70)
        
        access_patterns = [
            AccessPattern(
                table_a="users",
                table_b="logs",
                co_access_count=10,
                table_a_solo_count=90,
                table_b_solo_count=90,
            ),
        ]
        
        table_stats = [
            TableStatistics(table="users", total_accesses=100, solo_accesses=90, joined_accesses=10),
            TableStatistics(table="logs", total_accesses=100, solo_accesses=90, joined_accesses=10),
        ]
        
        stats_map = {s.table: s for s in table_stats}
        
        clusters = designer._build_access_clusters(
            tables=["users", "logs"],
            access_patterns=access_patterns,
            stats_map=stats_map,
        )
        
        # Should be 2 clusters (separate)
        assert len(clusters) == 2

    def test_pk_table_assignment(self):
        """PK table should be the one with highest solo_access_ratio."""
        designer = DynamoDBDesigner()
        
        access_patterns = [
            AccessPattern(
                table_a="users",
                table_b="orders",
                co_access_count=70,
                table_a_solo_count=30,  # users has 30% solo
                table_b_solo_count=5,   # orders has 5% solo
            ),
        ]
        
        table_stats = [
            TableStatistics(table="users", total_accesses=100, solo_accesses=30, joined_accesses=70),
            TableStatistics(table="orders", total_accesses=75, solo_accesses=5, joined_accesses=70),
        ]
        
        stats_map = {s.table: s for s in table_stats}
        
        clusters = designer._build_access_clusters(
            tables=["users", "orders"],
            access_patterns=access_patterns,
            stats_map=stats_map,
        )
        
        assert len(clusters) == 1
        # Users should be PK table (higher solo ratio)
        assert clusters[0].pk_table == "users"


# =============================================================================
# Mode Decision Tests
# =============================================================================

class TestModeDecision:
    """Tests for single-table vs multi-table decision."""

    def test_explicit_single_table_mode(self):
        """Explicit mode should override auto decision."""
        designer = DynamoDBDesigner(mode=DesignMode.SINGLE_TABLE)
        
        mode = designer._decide_design_mode(
            clusters=[],
            join_patterns=[],
            mutation_map={},
        )
        
        assert mode == DesignMode.SINGLE_TABLE

    def test_explicit_multi_table_mode(self):
        """Explicit mode should override auto decision."""
        designer = DynamoDBDesigner(mode=DesignMode.MULTI_TABLE)
        
        mode = designer._decide_design_mode(
            clusters=[],
            join_patterns=[],
            mutation_map={},
        )
        
        assert mode == DesignMode.MULTI_TABLE

    def test_auto_favors_single_with_hot_joins(self):
        """Auto mode should favor single-table with many hot joins."""
        designer = DynamoDBDesigner(mode=DesignMode.AUTO)
        
        from schema_travels.recommender.dynamodb_models import AccessCluster
        
        clusters = [
            AccessCluster(
                cluster_id="c1",
                tables={"users", "orders", "items"},
                pk_table="users",
                sk_tables=["orders", "items"],
                co_access_strength=0.85,
            )
        ]
        
        # 5 hot join patterns
        join_patterns = [
            JoinPattern(left_table="users", right_table="orders", frequency=100),
            JoinPattern(left_table="users", right_table="items", frequency=80),
            JoinPattern(left_table="orders", right_table="items", frequency=60),
            JoinPattern(left_table="users", right_table="profiles", frequency=50),
            JoinPattern(left_table="orders", right_table="payments", frequency=40),
        ]
        
        mode = designer._decide_design_mode(
            clusters=clusters,
            join_patterns=join_patterns,
            mutation_map={},
        )
        
        assert mode == DesignMode.SINGLE_TABLE

    def test_auto_favors_multi_with_write_heavy(self):
        """Auto mode should favor multi-table when many tables are write-heavy."""
        designer = DynamoDBDesigner(mode=DesignMode.AUTO)
        
        from schema_travels.recommender.dynamodb_models import AccessCluster
        
        clusters = [
            AccessCluster(
                cluster_id="c1",
                tables={"logs", "events", "metrics"},
                pk_table="logs",
                co_access_strength=0.4,  # Low co-access
            )
        ]
        
        mutation_map = {
            "logs": MutationPattern(table="logs", insert_count=1000, select_count=100),
            "events": MutationPattern(table="events", insert_count=800, select_count=50),
            "metrics": MutationPattern(table="metrics", insert_count=500, select_count=100),
        }
        
        mode = designer._decide_design_mode(
            clusters=clusters,
            join_patterns=[],
            mutation_map=mutation_map,
        )
        
        assert mode == DesignMode.MULTI_TABLE


# =============================================================================
# Entity Generation Tests
# =============================================================================

class TestEntityGeneration:
    """Tests for entity definition generation."""

    def test_entity_name_conversion(self):
        """Test table name to entity name conversion."""
        designer = DynamoDBDesigner()
        
        assert designer._to_entity_name("users") == "User"
        assert designer._to_entity_name("order_items") == "OrderItem"
        assert designer._to_entity_name("categories") == "Categorie"  # Simple singularization

    def test_pk_entity_pattern(self):
        """PK table should have simple PK/SK patterns."""
        designer = DynamoDBDesigner()
        
        from schema_travels.recommender.dynamodb_models import AccessCluster
        
        cluster = AccessCluster(
            cluster_id="c1",
            tables={"users", "orders"},
            pk_table="users",
            sk_tables=["orders"],
            co_access_strength=0.8,
        )
        
        stats_map = {
            "users": TableStatistics(table="users", frequently_selected_columns=["name", "email"]),
            "orders": TableStatistics(table="orders", frequently_selected_columns=["total", "status"]),
        }
        
        entities = designer._generate_entities(cluster, stats_map)
        
        # Find user entity
        user_entity = next(e for e in entities if e.name == "User")
        assert user_entity.pk_pattern == "USER#<id>"
        assert user_entity.sk_pattern == "USER"

    def test_sk_entity_pattern(self):
        """SK tables should reference parent PK."""
        designer = DynamoDBDesigner()
        
        from schema_travels.recommender.dynamodb_models import AccessCluster
        
        cluster = AccessCluster(
            cluster_id="c1",
            tables={"users", "orders"},
            pk_table="users",
            sk_tables=["orders"],
            co_access_strength=0.8,
        )
        
        stats_map = {
            "users": TableStatistics(table="users"),
            "orders": TableStatistics(table="orders"),
        }
        
        entities = designer._generate_entities(cluster, stats_map)
        
        # Find order entity
        order_entity = next(e for e in entities if e.name == "Order")
        assert "USER#" in order_entity.pk_pattern
        assert order_entity.sk_pattern == "ORDER#<id>"


# =============================================================================
# GSI Detection Tests
# =============================================================================

class TestGSIDetection:
    """Tests for GSI detection."""

    def test_gsi_from_filtered_columns(self):
        """Frequently filtered columns should become GSIs."""
        designer = DynamoDBDesigner()
        
        filtered_columns = {
            "users": {"email": 50, "status": 30, "id": 100},
        }
        
        gsis = designer._detect_gsis(
            tables=["users"],
            filtered_columns=filtered_columns,
            selected_columns={},
            select_star_tables=set(),
        )
        
        # Should have GSIs for email and status (not id - it's likely PK)
        gsi_sources = [g.source_columns[0] for g in gsis]
        assert "email" in gsi_sources
        assert "status" in gsi_sources
        assert "id" not in gsi_sources

    def test_gsi_respects_frequency_threshold(self):
        """Low-frequency columns should not become GSIs."""
        designer = DynamoDBDesigner()
        designer.GSI_FREQUENCY_THRESHOLD = 10
        
        filtered_columns = {
            "users": {"email": 50, "rare_column": 2},
        }
        
        gsis = designer._detect_gsis(
            tables=["users"],
            filtered_columns=filtered_columns,
            selected_columns={},
            select_star_tables=set(),
        )
        
        gsi_sources = [g.source_columns[0] for g in gsis]
        assert "email" in gsi_sources
        assert "rare_column" not in gsi_sources

    def test_gsi_max_limit(self):
        """Should not exceed MAX_GSIS."""
        designer = DynamoDBDesigner()
        designer.MAX_GSIS = 3
        
        filtered_columns = {
            "users": {
                "col1": 100, "col2": 90, "col3": 80,
                "col4": 70, "col5": 60,
            },
        }
        
        gsis = designer._detect_gsis(
            tables=["users"],
            filtered_columns=filtered_columns,
            selected_columns={},
            select_star_tables=set(),
        )
        
        assert len(gsis) <= 3

    def test_projection_type_all_for_select_star(self):
        """Tables with SELECT * should get ALL projection."""
        designer = DynamoDBDesigner()
        
        projection = designer._determine_projection(
            table="users",
            selected_columns={},
            select_star_tables={"users"},
        )
        
        assert projection == ProjectionType.ALL

    def test_projection_type_include_for_specific_columns(self):
        """Tables with specific columns should get INCLUDE projection."""
        designer = DynamoDBDesigner()
        
        projection = designer._determine_projection(
            table="users",
            selected_columns={"users": {"name": 10, "email": 8}},
            select_star_tables=set(),
        )
        
        assert projection == ProjectionType.INCLUDE

    def test_projection_type_keys_only_for_no_columns(self):
        """Tables with no selected columns should get KEYS_ONLY."""
        designer = DynamoDBDesigner()
        
        projection = designer._determine_projection(
            table="users",
            selected_columns={},
            select_star_tables=set(),
        )
        
        assert projection == ProjectionType.KEYS_ONLY


# =============================================================================
# Full Design Tests
# =============================================================================

class TestFullDesign:
    """Integration tests for complete design generation."""

    def test_single_table_design_structure(self):
        """Test complete single-table design output."""
        designer = DynamoDBDesigner(mode=DesignMode.SINGLE_TABLE)
        
        table_stats = [
            TableStatistics(
                table="users",
                total_accesses=100,
                solo_accesses=30,
                joined_accesses=70,
                frequently_selected_columns=["name", "email"],
            ),
            TableStatistics(
                table="orders",
                total_accesses=80,
                solo_accesses=10,
                joined_accesses=70,
                frequently_selected_columns=["total", "status"],
            ),
        ]
        
        access_patterns = [
            AccessPattern(
                table_a="users",
                table_b="orders",
                co_access_count=70,
                table_a_solo_count=30,
                table_b_solo_count=10,
            ),
        ]
        
        design = designer.design(
            table_stats=table_stats,
            access_patterns=access_patterns,
            join_patterns=[],
            mutation_patterns=[],
            filtered_columns={"users": {"email": 20}},
            selected_columns={"users": {"name": 10}},
            select_star_tables=set(),
        )
        
        assert design.design_mode == DesignMode.SINGLE_TABLE
        assert design.table_name is not None
        assert design.partition_key == "PK"
        assert design.sort_key == "SK"
        assert len(design.entities) >= 2

    def test_multi_table_design_structure(self):
        """Test complete multi-table design output."""
        designer = DynamoDBDesigner(mode=DesignMode.MULTI_TABLE)
        
        table_stats = [
            TableStatistics(table="users", total_accesses=100),
            TableStatistics(table="logs", total_accesses=50),
        ]
        
        design = designer.design(
            table_stats=table_stats,
            access_patterns=[],
            join_patterns=[],
            mutation_patterns=[],
            filtered_columns={},
            selected_columns={},
            select_star_tables=set(),
        )
        
        assert design.design_mode == DesignMode.MULTI_TABLE
        assert len(design.tables) == 2

    def test_design_to_dict(self):
        """Test design serialization."""
        designer = DynamoDBDesigner(mode=DesignMode.SINGLE_TABLE)
        
        design = designer.design(
            table_stats=[TableStatistics(table="users", total_accesses=100)],
            access_patterns=[],
            join_patterns=[],
            mutation_patterns=[],
            filtered_columns={},
            selected_columns={},
            select_star_tables=set(),
        )
        
        d = design.to_dict()
        
        assert "design_mode" in d
        assert "entities" in d
        assert d["design_mode"] == "single_table"

    def test_warnings_for_write_heavy_tables(self):
        """Test that write-heavy tables generate warnings."""
        designer = DynamoDBDesigner(mode=DesignMode.SINGLE_TABLE)
        
        table_stats = [
            TableStatistics(table="events", total_accesses=100, solo_accesses=50, joined_accesses=50),
        ]
        
        mutation_patterns = [
            MutationPattern(table="events", insert_count=900, select_count=100),
        ]
        
        design = designer.design(
            table_stats=table_stats,
            access_patterns=[],
            join_patterns=[],
            mutation_patterns=mutation_patterns,
            filtered_columns={},
            selected_columns={},
            select_star_tables=set(),
        )
        
        # Should have warning about write-heavy table
        assert any("write-heavy" in w.lower() for w in design.warnings)


# =============================================================================
# Edge Cases
# =============================================================================

class TestEdgeCases:
    """Edge case tests."""

    def test_empty_input(self):
        """Test handling of empty input."""
        designer = DynamoDBDesigner()
        
        design = designer.design(
            table_stats=[],
            access_patterns=[],
            join_patterns=[],
            mutation_patterns=[],
            filtered_columns={},
            selected_columns={},
            select_star_tables=set(),
        )
        
        # Should return a valid design
        assert design.design_mode in [DesignMode.SINGLE_TABLE, DesignMode.MULTI_TABLE]

    def test_single_table_input(self):
        """Test with only one table."""
        designer = DynamoDBDesigner()
        
        design = designer.design(
            table_stats=[TableStatistics(table="users", total_accesses=100)],
            access_patterns=[],
            join_patterns=[],
            mutation_patterns=[],
            filtered_columns={},
            selected_columns={},
            select_star_tables=set(),
        )
        
        assert len(design.entities) >= 1 or len(design.tables) >= 1

    def test_orphan_tables_identified(self):
        """Tables not in clusters should be marked as orphans."""
        designer = DynamoDBDesigner(mode=DesignMode.SINGLE_TABLE)
        
        table_stats = [
            TableStatistics(table="users", total_accesses=100, solo_accesses=20, joined_accesses=80),
            TableStatistics(table="orders", total_accesses=80, solo_accesses=10, joined_accesses=70),
            TableStatistics(table="audit_logs", total_accesses=50, solo_accesses=50, joined_accesses=0),
        ]
        
        access_patterns = [
            AccessPattern(
                table_a="users",
                table_b="orders",
                co_access_count=70,
                table_a_solo_count=20,
                table_b_solo_count=10,
            ),
            # audit_logs has no strong co-access
            AccessPattern(
                table_a="users",
                table_b="audit_logs",
                co_access_count=5,
                table_a_solo_count=95,
                table_b_solo_count=45,
            ),
        ]
        
        design = designer.design(
            table_stats=table_stats,
            access_patterns=access_patterns,
            join_patterns=[],
            mutation_patterns=[],
            filtered_columns={},
            selected_columns={},
            select_star_tables=set(),
        )
        
        # audit_logs should be in orphan_tables
        assert "audit_logs" in design.orphan_tables
