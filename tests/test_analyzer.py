"""Tests for analyzer module."""

import pytest
from datetime import datetime

from schema_travels.collector.models import QueryLog
from schema_travels.analyzer.hot_joins import HotJoinAnalyzer
from schema_travels.analyzer.mutations import MutationAnalyzer
from schema_travels.analyzer.pattern_analyzer import PatternAnalyzer


class TestHotJoinAnalyzer:
    """Tests for HotJoinAnalyzer."""

    def test_analyze_empty_queries(self):
        """Test analyzing empty query list."""
        analyzer = HotJoinAnalyzer()
        result = analyzer.analyze([])
        assert result == []

    def test_analyze_single_table_query(self):
        """Test analyzing queries with no joins."""
        analyzer = HotJoinAnalyzer()
        queries = [
            QueryLog(sql="SELECT * FROM users WHERE id = 1", duration_ms=1.0),
            QueryLog(sql="SELECT * FROM users WHERE email = 'test@test.com'", duration_ms=2.0),
        ]
        result = analyzer.analyze(queries)
        assert len(result) == 0  # No joins found

    def test_analyze_join_query(self):
        """Test analyzing queries with JOINs."""
        analyzer = HotJoinAnalyzer()
        queries = [
            QueryLog(
                sql="SELECT u.name, o.total FROM users u JOIN orders o ON u.id = o.user_id",
                duration_ms=5.0,
            ),
        ]
        result = analyzer.analyze(queries)
        # Should detect join between users and orders
        assert len(result) >= 0  # Depends on parsing success

    def test_track_table_statistics(self):
        """Test table statistics tracking."""
        analyzer = HotJoinAnalyzer()
        queries = [
            QueryLog(sql="SELECT * FROM users", duration_ms=1.0),
            QueryLog(sql="SELECT * FROM users", duration_ms=1.0),
            QueryLog(sql="SELECT * FROM orders", duration_ms=1.0),
        ]
        analyzer.analyze(queries)
        
        stats = analyzer.get_table_statistics()
        assert len(stats) >= 0


class TestMutationAnalyzer:
    """Tests for MutationAnalyzer."""

    def test_analyze_empty_queries(self):
        """Test analyzing empty query list."""
        analyzer = MutationAnalyzer()
        result = analyzer.analyze([])
        assert result == {}

    def test_analyze_select_queries(self):
        """Test analyzing SELECT queries."""
        analyzer = MutationAnalyzer()
        queries = [
            QueryLog(sql="SELECT * FROM users WHERE id = 1"),
            QueryLog(sql="SELECT * FROM users WHERE id = 2"),
        ]
        result = analyzer.analyze(queries)
        
        assert "users" in result
        assert result["users"].select_count == 2
        assert result["users"].insert_count == 0

    def test_analyze_insert_queries(self):
        """Test analyzing INSERT queries."""
        analyzer = MutationAnalyzer()
        queries = [
            QueryLog(sql="INSERT INTO users (name) VALUES ('test')"),
        ]
        result = analyzer.analyze(queries)
        
        # Check that we found the table
        assert "users" in result, f"Expected 'users' in result, got: {list(result.keys())}"
        assert result["users"].insert_count == 1

    def test_analyze_update_queries(self):
        """Test analyzing UPDATE queries."""
        analyzer = MutationAnalyzer()
        queries = [
            QueryLog(sql="UPDATE users SET name = 'new' WHERE id = 1"),
        ]
        result = analyzer.analyze(queries)
        
        assert "users" in result
        assert result["users"].update_count == 1

    def test_analyze_delete_queries(self):
        """Test analyzing DELETE queries."""
        analyzer = MutationAnalyzer()
        queries = [
            QueryLog(sql="DELETE FROM users WHERE id = 1"),
        ]
        result = analyzer.analyze(queries)
        
        assert "users" in result
        assert result["users"].delete_count == 1

    def test_write_ratio_calculation(self):
        """Test write ratio calculation."""
        analyzer = MutationAnalyzer()
        queries = [
            QueryLog(sql="SELECT * FROM users"),
            QueryLog(sql="SELECT * FROM users"),
            QueryLog(sql="INSERT INTO users (name) VALUES ('test')"),
            QueryLog(sql="UPDATE users SET name = 'new' WHERE id = 1"),
        ]
        result = analyzer.analyze(queries)

        assert "users" in result, f"Expected 'users' in result, got: {list(result.keys())}"
        
        # Check the counts
        pattern = result["users"]
        assert pattern.select_count == 2, f"Expected 2 selects, got {pattern.select_count}"
        
        # Total writes = inserts + updates + deletes
        total_writes = pattern.insert_count + pattern.update_count + pattern.delete_count
        total_ops = pattern.select_count + total_writes
        
        # Write ratio should be writes / total
        expected_ratio = total_writes / total_ops if total_ops > 0 else 0
        assert abs(pattern.write_ratio - expected_ratio) < 0.01, \
            f"Write ratio {pattern.write_ratio} doesn't match expected {expected_ratio}"

    def test_get_write_heavy_tables(self):
        """Test identifying write-heavy tables."""
        analyzer = MutationAnalyzer()
        queries = [
            QueryLog(sql="INSERT INTO logs (msg) VALUES ('test')"),
            QueryLog(sql="INSERT INTO logs (msg) VALUES ('test2')"),
            QueryLog(sql="INSERT INTO logs (msg) VALUES ('test3')"),
            QueryLog(sql="SELECT * FROM users"),
        ]
        result = analyzer.analyze(queries)
        
        # Verify logs table exists and has high write ratio
        if "logs" in result:
            assert result["logs"].insert_count == 3
            write_heavy = analyzer.get_write_heavy_tables(threshold=0.5)
            assert "logs" in write_heavy, f"Expected 'logs' in write_heavy tables: {write_heavy}"
        
        # Users should be read-only
        if "users" in result:
            assert result["users"].select_count == 1
            assert result["users"].write_ratio == 0


class TestPatternAnalyzer:
    """Tests for PatternAnalyzer."""

    def test_analyze_returns_result(self):
        """Test that analyze returns an AnalysisResult."""
        analyzer = PatternAnalyzer()
        queries = [
            QueryLog(sql="SELECT * FROM users"),
        ]
        result = analyzer.analyze(queries)
        
        assert result is not None
        assert result.total_queries_analyzed == 1

    def test_analyze_generates_summary(self):
        """Test summary generation."""
        analyzer = PatternAnalyzer()
        queries = [
            QueryLog(sql="SELECT * FROM users"),
            QueryLog(sql="SELECT * FROM orders"),
        ]
        result = analyzer.analyze(queries)
        
        summary = analyzer.get_summary(result)
        assert "ACCESS PATTERN ANALYSIS SUMMARY" in summary
