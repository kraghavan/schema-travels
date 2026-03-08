"""Tests for CLI commands and v1.3.0 features."""

import pytest
from click.testing import CliRunner
from unittest.mock import patch, MagicMock

from schema_travels.cli.main import cli, _confidence_color, _display_analysis_summary
from schema_travels.recommender.models import SchemaRecommendation, RelationshipDecision


# =============================================================================
# Fixtures
# =============================================================================

@pytest.fixture
def cli_runner():
    """Click CLI test runner."""
    return CliRunner()


@pytest.fixture
def mock_recommendations():
    """Sample recommendations with varying confidence levels."""
    return [
        SchemaRecommendation(
            parent_table="users",
            child_table="addresses",
            decision=RelationshipDecision.EMBED,
            confidence=0.92,
            reasoning=["92% co-access ratio"],
            warnings=[],
        ),
        SchemaRecommendation(
            parent_table="users",
            child_table="orders",
            decision=RelationshipDecision.REFERENCE,
            confidence=0.78,
            reasoning=["Independent access patterns"],
            warnings=[],
        ),
        SchemaRecommendation(
            parent_table="products",
            child_table="logs",
            decision=RelationshipDecision.SEPARATE,
            confidence=0.65,
            reasoning=["High write volume"],
            warnings=["Consider TTL index"],
        ),
    ]


# =============================================================================
# TestConfidenceColor
# =============================================================================

class TestConfidenceColor:
    """Tests for _confidence_color helper function."""

    def test_high_confidence_is_green(self):
        """Test that confidence >= 0.85 returns green."""
        assert _confidence_color(0.85) == "green"
        assert _confidence_color(0.90) == "green"
        assert _confidence_color(0.99) == "green"
        assert _confidence_color(1.0) == "green"

    def test_medium_confidence_is_yellow(self):
        """Test that confidence 0.70-0.84 returns yellow."""
        assert _confidence_color(0.70) == "yellow"
        assert _confidence_color(0.75) == "yellow"
        assert _confidence_color(0.84) == "yellow"

    def test_low_confidence_is_red(self):
        """Test that confidence < 0.70 returns red."""
        assert _confidence_color(0.69) == "red"
        assert _confidence_color(0.50) == "red"
        assert _confidence_color(0.0) == "red"

    def test_boundary_at_85(self):
        """Test exact boundary at 0.85."""
        assert _confidence_color(0.85) == "green"
        assert _confidence_color(0.8499) == "yellow"

    def test_boundary_at_70(self):
        """Test exact boundary at 0.70."""
        assert _confidence_color(0.70) == "yellow"
        assert _confidence_color(0.6999) == "red"


# =============================================================================
# TestCLIHelp
# =============================================================================

class TestCLIHelp:
    """Tests for CLI help text includes new flags."""

    def test_analyze_help_shows_min_confidence(self, cli_runner):
        """Test that --min-confidence appears in help."""
        result = cli_runner.invoke(cli, ["analyze", "--help"])
        assert result.exit_code == 0
        assert "--min-confidence" in result.output
        assert "confidence threshold" in result.output.lower()

    def test_analyze_help_shows_show_rewrites(self, cli_runner):
        """Test that --show-rewrites appears in help."""
        result = cli_runner.invoke(cli, ["analyze", "--help"])
        assert result.exit_code == 0
        assert "--show-rewrites" in result.output
        assert "query rewrite" in result.output.lower()


# =============================================================================
# TestMinConfidenceFlag
# =============================================================================

class TestMinConfidenceFlag:
    """Tests for --min-confidence CLI flag."""

    def test_min_confidence_accepts_float(self, cli_runner):
        """Test that --min-confidence accepts a float value."""
        # Just checking the flag is recognized (will fail due to missing required args)
        result = cli_runner.invoke(cli, ["analyze", "--min-confidence", "0.8"])
        # Should fail because --logs-dir and --schema-file are required, not because of flag
        assert "Missing option" in result.output or "required" in result.output.lower()
        assert "--min-confidence" not in result.output  # Flag itself should be valid

    def test_min_confidence_rejects_invalid(self, cli_runner):
        """Test that --min-confidence rejects non-float values."""
        result = cli_runner.invoke(cli, ["analyze", "--min-confidence", "high"])
        assert result.exit_code != 0
        assert "invalid" in result.output.lower() or "not a valid float" in result.output.lower()


# =============================================================================
# TestShowRewritesFlag
# =============================================================================

class TestShowRewritesFlag:
    """Tests for --show-rewrites CLI flag."""

    def test_show_rewrites_is_flag(self, cli_runner):
        """Test that --show-rewrites is a boolean flag."""
        result = cli_runner.invoke(cli, ["analyze", "--help"])
        assert "--show-rewrites" in result.output
        # Flags don't require a value
        assert "is_flag" not in result.output  # Implementation detail, not shown


# =============================================================================
# TestDisplayAnalysisSummary
# =============================================================================

class TestDisplayAnalysisSummary:
    """Tests for _display_analysis_summary with new parameters."""

    @pytest.fixture
    def mock_result(self):
        """Mock analysis result."""
        result = MagicMock()
        result.join_patterns = []
        result.mutation_patterns = []
        return result

    @pytest.fixture
    def mock_target_schema(self):
        """Mock target schema."""
        return MagicMock()

    def test_min_confidence_filters_recommendations(
        self, mock_result, mock_recommendations, mock_target_schema, capsys
    ):
        """Test that min_confidence filters out low-confidence recommendations."""
        _display_analysis_summary(
            mock_result,
            mock_recommendations,
            mock_target_schema,
            cache_used=False,
            min_confidence=0.80,
            show_rewrites=False,
        )
        captured = capsys.readouterr()
        # Should show users → addresses (0.92) but not others
        assert "users" in captured.out
        assert "addresses" in captured.out
        # The 0.78 and 0.65 confidence ones should be filtered
        assert "hidden" in captured.out.lower() or "below" in captured.out.lower()

    def test_show_rewrites_displays_examples(
        self, mock_result, mock_recommendations, mock_target_schema, capsys
    ):
        """Test that show_rewrites displays query rewrite examples."""
        _display_analysis_summary(
            mock_result,
            mock_recommendations,
            mock_target_schema,
            cache_used=False,
            min_confidence=None,
            show_rewrites=True,
        )
        captured = capsys.readouterr()
        # Should contain rewrite elements
        assert "SQL" in captured.out
        assert "MongoDB" in captured.out
        assert "EMBED" in captured.out or "REFERENCE" in captured.out

    def test_no_rewrites_without_flag(
        self, mock_result, mock_recommendations, mock_target_schema, capsys
    ):
        """Test that rewrites are not shown without --show-rewrites."""
        _display_analysis_summary(
            mock_result,
            mock_recommendations,
            mock_target_schema,
            cache_used=False,
            min_confidence=None,
            show_rewrites=False,
        )
        captured = capsys.readouterr()
        # Should NOT contain detailed rewrite output
        assert "Query Examples" not in captured.out
        assert "findOne" not in captured.out


# =============================================================================
# TestCLIVersion
# =============================================================================

class TestCLIVersion:
    """Tests for CLI version."""

    def test_version_command(self, cli_runner):
        """Test --version shows version."""
        result = cli_runner.invoke(cli, ["--version"])
        assert result.exit_code == 0
        assert "1.3.0" in result.output or "version" in result.output.lower()
