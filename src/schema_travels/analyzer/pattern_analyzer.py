"""Main pattern analyzer that orchestrates all analysis modules."""

import uuid
from datetime import datetime
from pathlib import Path

from schema_travels.collector.models import QueryLog, SchemaDefinition
from schema_travels.analyzer.models import (
    AccessPattern,
    AnalysisResult,
    JoinPattern,
    MutationPattern,
    TableStatistics,
)
from schema_travels.analyzer.hot_joins import HotJoinAnalyzer
from schema_travels.analyzer.mutations import MutationAnalyzer


class PatternAnalyzer:
    """
    Main analyzer that orchestrates query pattern analysis.

    Combines hot join analysis, mutation analysis, and access pattern
    detection to provide a complete picture of database usage.
    """

    def __init__(self, schema: SchemaDefinition | None = None):
        """
        Initialize the pattern analyzer.

        Args:
            schema: Optional schema definition for enhanced analysis
        """
        self.schema = schema
        self.hot_join_analyzer = HotJoinAnalyzer()
        self.mutation_analyzer = MutationAnalyzer()
        self._analysis_id = str(uuid.uuid4())[:8]

    def analyze(
        self,
        queries: list[QueryLog],
        source_db_type: str = "postgres",
    ) -> AnalysisResult:
        """
        Perform complete analysis on query logs.

        Args:
            queries: List of query logs to analyze
            source_db_type: Source database type (postgres, mysql)

        Returns:
            Complete analysis result
        """
        # Run individual analyzers
        join_patterns = self.hot_join_analyzer.analyze(queries)
        mutation_patterns = self.mutation_analyzer.analyze(queries)
        table_stats = self.hot_join_analyzer.get_table_statistics()

        # Compute access patterns
        access_patterns = self._compute_access_patterns(table_stats)

        # Get all tables
        all_tables = set()
        for jp in join_patterns:
            all_tables.add(jp.left_table)
            all_tables.add(jp.right_table)
        for mp in mutation_patterns.values():
            all_tables.add(mp.table)

        # Count embedding candidates
        embedding_candidates = self._count_embedding_candidates(
            join_patterns, mutation_patterns, access_patterns
        )

        return AnalysisResult(
            analysis_id=self._analysis_id,
            created_at=datetime.now(),
            source_db_type=source_db_type,
            total_queries_analyzed=len(queries),
            join_patterns=join_patterns,
            mutation_patterns=list(mutation_patterns.values()),
            access_patterns=access_patterns,
            table_statistics=table_stats,
            tables_analyzed=sorted(all_tables),
            hot_joins_count=len([jp for jp in join_patterns if jp.cost_score > 0]),
            embedding_candidates_count=embedding_candidates,
        )

    def _compute_access_patterns(
        self, table_stats: list[TableStatistics]
    ) -> list[AccessPattern]:
        """Compute co-access patterns from table statistics."""
        access_patterns = []
        co_access_matrix = self.hot_join_analyzer.get_co_access_matrix()

        # Create lookup for table stats
        stats_lookup = {ts.table: ts for ts in table_stats}

        # For each pair of tables that appear in joins
        processed_pairs = set()

        for (table_a, table_b), co_access_count in co_access_matrix.items():
            pair = tuple(sorted([table_a, table_b]))
            if pair in processed_pairs:
                continue
            processed_pairs.add(pair)

            stats_a = stats_lookup.get(table_a)
            stats_b = stats_lookup.get(table_b)

            if not stats_a or not stats_b:
                continue

            access_patterns.append(
                AccessPattern(
                    table_a=table_a,
                    table_b=table_b,
                    co_access_count=co_access_count,
                    table_a_solo_count=stats_a.solo_accesses,
                    table_b_solo_count=stats_b.solo_accesses,
                )
            )

        # Sort by co-access ratio
        access_patterns.sort(key=lambda ap: ap.co_access_ratio, reverse=True)

        return access_patterns

    def _count_embedding_candidates(
        self,
        join_patterns: list[JoinPattern],
        mutation_patterns: dict[str, MutationPattern],
        access_patterns: list[AccessPattern],
    ) -> int:
        """Count table pairs that are good embedding candidates."""
        candidates = 0

        for ap in access_patterns:
            # High co-access ratio
            if ap.co_access_ratio < 0.7:
                continue

            # Check write ratio of potential child table
            # (lower independence = likely child)
            if ap.table_a_independence < ap.table_b_independence:
                child_table = ap.table_a
            else:
                child_table = ap.table_b

            child_mutations = mutation_patterns.get(child_table)
            if child_mutations and child_mutations.write_ratio > 0.5:
                continue  # Too write-heavy to embed

            # Check if either table is independently accessed too often
            if ap.table_a_independence > 0.4 and ap.table_b_independence > 0.4:
                continue  # Both tables accessed independently often

            candidates += 1

        return candidates

    def get_embedding_recommendations(
        self,
        result: AnalysisResult,
        cardinality_info: dict[tuple[str, str], dict] | None = None,
    ) -> list[dict]:
        """
        Generate embedding recommendations based on analysis.

        Args:
            result: Analysis result
            cardinality_info: Optional cardinality information for relationships
                              Format: {(parent, child): {"avg": N, "max": M}}

        Returns:
            List of embedding recommendations
        """
        recommendations = []
        mutation_lookup = {mp.table: mp for mp in result.mutation_patterns}

        for ap in result.access_patterns:
            rec = self._evaluate_pair(ap, mutation_lookup, cardinality_info)
            if rec:
                recommendations.append(rec)

        # Sort by confidence
        recommendations.sort(key=lambda r: r["confidence"], reverse=True)

        return recommendations

    def _evaluate_pair(
        self,
        ap: AccessPattern,
        mutation_lookup: dict[str, MutationPattern],
        cardinality_info: dict | None,
    ) -> dict | None:
        """Evaluate a table pair for embedding recommendation."""
        # Determine parent/child relationship
        # Table with higher independence is likely parent
        if ap.table_a_independence > ap.table_b_independence:
            parent = ap.table_a
            child = ap.table_b
            child_independence = ap.table_b_independence
        else:
            parent = ap.table_b
            child = ap.table_a
            child_independence = ap.table_a_independence

        # Get mutation patterns
        child_mutations = mutation_lookup.get(child)
        child_write_ratio = child_mutations.write_ratio if child_mutations else 0

        # Get cardinality if available
        cardinality = None
        if cardinality_info:
            cardinality = cardinality_info.get((parent, child)) or cardinality_info.get(
                (child, parent)
            )

        max_children = cardinality.get("max", 0) if cardinality else 0

        # Apply decision rules
        reasoning = []
        warnings = []
        decision = "EVALUATE"
        confidence = 0.5

        # Rule 1: Unbounded children
        if max_children > 1000:
            decision = "REFERENCE"
            confidence = 0.9
            reasoning.append(f"Potentially unbounded children ({max_children} max)")
            warnings.append("Document size limits could be exceeded")

        # Rule 2: High co-access + low writes + bounded
        elif ap.co_access_ratio > 0.7 and child_write_ratio < 0.3:
            if max_children == 0 or max_children < 100:
                decision = "EMBED"
                confidence = 0.85
                reasoning.append(f"High co-access ({ap.co_access_ratio:.0%})")
                reasoning.append(f"Low child write ratio ({child_write_ratio:.0%})")
                if max_children > 0:
                    reasoning.append(f"Bounded children ({max_children} max)")

        # Rule 3: Child frequently accessed alone
        elif child_independence > 0.4:
            decision = "REFERENCE"
            confidence = 0.8
            reasoning.append(
                f"{child} accessed independently {child_independence:.0%} of the time"
            )

        # Rule 4: High child write ratio
        elif child_write_ratio > 0.5:
            decision = "REFERENCE"
            confidence = 0.85
            reasoning.append(f"High write ratio on {child} ({child_write_ratio:.0%})")
            reasoning.append("Embedding would require rewriting parent on every update")

        # Default
        else:
            decision = "EVALUATE"
            confidence = 0.5
            reasoning.append("Mixed signals - manual review recommended")
            warnings.append("Consider access patterns and cardinality carefully")

        return {
            "parent_table": parent,
            "child_table": child,
            "decision": decision,
            "confidence": confidence,
            "reasoning": reasoning,
            "warnings": warnings,
            "metrics": {
                "co_access_ratio": ap.co_access_ratio,
                "child_independence": child_independence,
                "child_write_ratio": child_write_ratio,
                "max_children": max_children,
            },
        }

    def get_summary(self, result: AnalysisResult) -> str:
        """Generate a human-readable summary of the analysis."""
        lines = [
            "=" * 60,
            "ACCESS PATTERN ANALYSIS SUMMARY",
            "=" * 60,
            "",
            f"Analysis ID: {result.analysis_id}",
            f"Source DB Type: {result.source_db_type}",
            f"Queries Analyzed: {result.total_queries_analyzed:,}",
            f"Tables Found: {len(result.tables_analyzed)}",
            "",
            "-" * 40,
            "HOT JOINS (Top 10 by cost score)",
            "-" * 40,
        ]

        for jp in result.join_patterns[:10]:
            lines.append(
                f"  {jp.left_table} ⟷ {jp.right_table}: "
                f"{jp.frequency:,} calls, {jp.avg_time_ms:.1f}ms avg"
            )

        lines.extend([
            "",
            "-" * 40,
            "MUTATION PATTERNS (Top 10 by operations)",
            "-" * 40,
        ])

        sorted_mutations = sorted(
            result.mutation_patterns,
            key=lambda m: m.total_operations,
            reverse=True,
        )[:10]

        for mp in sorted_mutations:
            lines.append(
                f"  {mp.table}: R={mp.select_count:,} W={mp.total_writes:,} "
                f"({mp.write_ratio:.0%} writes)"
            )

        lines.extend([
            "",
            "-" * 40,
            "EMBEDDING CANDIDATES",
            "-" * 40,
            f"  Potential candidates: {result.embedding_candidates_count}",
            "",
            "=" * 60,
        ])

        return "\n".join(lines)
