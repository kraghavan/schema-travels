"""Migration simulator for estimating impact of schema changes."""

import logging
from typing import Any

from schema_travels.collector.models import SchemaDefinition
from schema_travels.analyzer.models import AnalysisResult
from schema_travels.recommender.models import TargetSchema, RelationshipDecision
from schema_travels.simulator.cost_model import CostModel, SimulationConfig
from schema_travels.simulator.models import (
    CostEstimate,
    LatencyEstimate,
    SimulationResult,
    StorageEstimate,
)

logger = logging.getLogger(__name__)


class MigrationSimulator:
    """
    Simulates migration impact to estimate storage, latency, and cost changes.

    Provides before/after comparison to help make informed migration decisions.
    """

    def __init__(
        self,
        source_schema: SchemaDefinition,
        target_schema: TargetSchema,
        analysis: AnalysisResult,
        config: SimulationConfig | None = None,
        table_row_counts: dict[str, int] | None = None,
    ):
        """
        Initialize simulator.

        Args:
            source_schema: Source database schema
            target_schema: Generated target schema
            analysis: Analysis result from pattern analyzer
            config: Simulation configuration
            table_row_counts: Optional row counts per table
        """
        self.source_schema = source_schema
        self.target_schema = target_schema
        self.analysis = analysis
        self.config = config or SimulationConfig()
        self.cost_model = CostModel(self.config)

        # Use provided row counts or estimate
        self.row_counts = table_row_counts or self._estimate_row_counts()

    def simulate(self) -> SimulationResult:
        """
        Run migration simulation.

        Returns:
            Complete simulation result with comparisons
        """
        # Estimate storage
        source_storage = self._estimate_source_storage()
        target_storage = self._estimate_target_storage()
        storage_change = self._calculate_change_pct(
            source_storage.total_gb,
            target_storage.total_gb,
        )

        # Estimate latency
        source_latency = self._estimate_source_latency()
        target_latency = self._estimate_target_latency()
        latency_improvement = self._calculate_change_pct(
            source_latency.avg_ms,
            target_latency.avg_ms,
        )

        # Estimate cost
        source_cost = self._estimate_source_cost(source_storage)
        target_cost = self._estimate_target_cost(target_storage)
        cost_change = self._calculate_change_pct(
            source_cost.monthly_usd,
            target_cost.monthly_usd,
        )

        # Generate warnings
        warnings = self._generate_warnings(
            storage_change,
            latency_improvement,
            cost_change,
        )

        return SimulationResult(
            source_storage=source_storage,
            target_storage=target_storage,
            storage_change_pct=storage_change,
            source_latency=source_latency,
            target_latency=target_latency,
            latency_improvement_pct=-latency_improvement,  # Negative change = improvement
            source_cost=source_cost,
            target_cost=target_cost,
            cost_change_pct=cost_change,
            assumptions=self._get_assumptions(),
            warnings=warnings,
        )

    def _estimate_row_counts(self) -> dict[str, int]:
        """Estimate row counts based on query patterns."""
        # Use a default of 10,000 rows per table if not provided
        counts = {}
        for table in self.source_schema.tables:
            counts[table.name.lower()] = 10_000
        return counts

    def _estimate_source_storage(self) -> StorageEstimate:
        """Estimate source database storage."""
        total_bytes = 0
        breakdown = {}

        for table in self.source_schema.tables:
            table_name = table.name.lower()
            row_count = self.row_counts.get(table_name, 10_000)

            # Estimate row size
            columns = [{"type": c.data_type} for c in table.columns]
            row_size = self.cost_model.estimate_row_size(columns)

            table_bytes = row_count * row_size
            total_bytes += table_bytes
            breakdown[table_name] = table_bytes / (1024 ** 3)  # Convert to GB

        total_gb = total_bytes / (1024 ** 3)
        total_gb *= self.config.source_storage_overhead

        return StorageEstimate(
            total_gb=total_gb,
            breakdown=breakdown,
            overhead_factor=self.config.source_storage_overhead,
        )

    def _estimate_target_storage(self) -> StorageEstimate:
        """Estimate target database storage."""
        total_bytes = 0
        breakdown = {}

        for collection in self.target_schema.collections:
            # Get row count for source table
            source_table = collection.source_tables[0] if collection.source_tables else ""
            row_count = self.row_counts.get(source_table.lower(), 10_000)

            # Estimate document size
            base_fields = [{"type": f.type} for f in collection.fields]
            
            embedded_info = []
            for embedded in collection.embedded_documents:
                # Estimate average items per parent
                child_count = self.row_counts.get(embedded.source_table.lower(), 10_000)
                avg_items = child_count / max(row_count, 1)
                
                embedded_info.append({
                    "fields": [{"type": f.type} for f in embedded.fields],
                    "avg_items": min(avg_items, 100),  # Cap at 100 to avoid unrealistic estimates
                })

            doc_size = self.cost_model.estimate_document_size(base_fields, embedded_info)

            collection_bytes = row_count * doc_size
            total_bytes += collection_bytes
            breakdown[collection.name] = collection_bytes / (1024 ** 3)

        total_gb = total_bytes / (1024 ** 3)
        total_gb *= self.config.target_storage_overhead

        return StorageEstimate(
            total_gb=total_gb,
            breakdown=breakdown,
            overhead_factor=self.config.target_storage_overhead,
        )

    def _estimate_source_latency(self) -> LatencyEstimate:
        """Estimate source database query latency."""
        if not self.analysis.join_patterns:
            return LatencyEstimate(avg_ms=self.config.avg_simple_read_ms)

        total_time = 0
        total_calls = 0
        breakdown = {}

        for jp in self.analysis.join_patterns:
            total_time += jp.total_time_ms
            total_calls += jp.frequency
            
            key = f"{jp.left_table}-{jp.right_table}"
            breakdown[key] = jp.avg_time_ms

        # Add simple queries from mutation patterns
        for mp in self.analysis.mutation_patterns:
            simple_calls = mp.select_count
            total_time += simple_calls * self.config.avg_simple_read_ms
            total_calls += simple_calls

        avg_ms = total_time / max(total_calls, 1)

        return LatencyEstimate(
            avg_ms=avg_ms,
            breakdown=breakdown,
        )

    def _estimate_target_latency(self) -> LatencyEstimate:
        """Estimate target database query latency."""
        total_time = 0
        total_calls = 0
        breakdown = {}

        # Find embedded tables
        embedded_tables = set()
        for collection in self.target_schema.collections:
            for embedded in collection.embedded_documents:
                embedded_tables.add(embedded.source_table.lower())

        # Estimate latency for each join pattern
        for jp in self.analysis.join_patterns:
            left = jp.left_table.lower()
            right = jp.right_table.lower()

            # Check if this join becomes an embedded read
            if left in embedded_tables or right in embedded_tables:
                # Embedded read - much faster
                latency = self.cost_model.estimate_document_read_latency(
                    has_embedded=True,
                    num_references=0,
                )
            else:
                # Reference lookup - similar to join
                latency = self.cost_model.estimate_document_read_latency(
                    has_embedded=False,
                    num_references=1,
                )

            total_time += latency * jp.frequency
            total_calls += jp.frequency
            breakdown[f"{left}-{right}"] = latency

        # Add simple reads
        for mp in self.analysis.mutation_patterns:
            simple_calls = mp.select_count
            latency = self.config.avg_simple_read_ms
            total_time += simple_calls * latency
            total_calls += simple_calls

        avg_ms = total_time / max(total_calls, 1)

        return LatencyEstimate(
            avg_ms=avg_ms,
            breakdown=breakdown,
        )

    def _estimate_source_cost(self, storage: StorageEstimate) -> CostEstimate:
        """Estimate source database monthly cost."""
        storage_cost = self.cost_model.estimate_monthly_storage_cost(
            storage.total_gb,
            is_target=False,
        )

        # Estimate operations from analysis
        total_reads = sum(mp.select_count for mp in self.analysis.mutation_patterns)
        total_writes = sum(mp.total_writes for mp in self.analysis.mutation_patterns)

        # Scale to monthly (assume analysis covers ~1 hour of traffic)
        monthly_reads = total_reads * 24 * 30
        monthly_writes = total_writes * 24 * 30

        read_cost, write_cost = self.cost_model.estimate_monthly_operation_cost(
            monthly_reads,
            monthly_writes,
            is_target=False,
        )

        return CostEstimate(
            monthly_usd=storage_cost + read_cost + write_cost,
            storage_cost=storage_cost,
            read_cost=read_cost,
            write_cost=write_cost,
        )

    def _estimate_target_cost(self, storage: StorageEstimate) -> CostEstimate:
        """Estimate target database monthly cost."""
        storage_cost = self.cost_model.estimate_monthly_storage_cost(
            storage.total_gb,
            is_target=True,
        )

        # Operations similar to source but may have different patterns
        total_reads = sum(mp.select_count for mp in self.analysis.mutation_patterns)
        total_writes = sum(mp.total_writes for mp in self.analysis.mutation_patterns)

        # Embedded writes may require more operations (update parent document)
        embedded_count = sum(
            len(c.embedded_documents)
            for c in self.target_schema.collections
        )
        write_multiplier = 1.0 + (embedded_count * 0.1)  # 10% more writes per embedded doc

        monthly_reads = total_reads * 24 * 30
        monthly_writes = int(total_writes * 24 * 30 * write_multiplier)

        read_cost, write_cost = self.cost_model.estimate_monthly_operation_cost(
            monthly_reads,
            monthly_writes,
            is_target=True,
        )

        return CostEstimate(
            monthly_usd=storage_cost + read_cost + write_cost,
            storage_cost=storage_cost,
            read_cost=read_cost,
            write_cost=write_cost,
        )

    def _calculate_change_pct(self, source: float, target: float) -> float:
        """Calculate percentage change from source to target."""
        if source == 0:
            return 0.0
        return ((target - source) / source) * 100

    def _generate_warnings(
        self,
        storage_change: float,
        latency_change: float,
        cost_change: float,
    ) -> list[str]:
        """Generate warnings based on simulation results."""
        warnings = []

        if storage_change > 100:
            warnings.append(
                f"Storage increase of {storage_change:.0f}% is significant. "
                "Review embedding strategy."
            )

        if latency_change > 0:
            warnings.append(
                "Estimated latency may increase. "
                "Consider reviewing reference patterns."
            )

        if cost_change > 50:
            warnings.append(
                f"Estimated cost increase of {cost_change:.0f}%. "
                "Review pricing model for target database."
            )

        # Check for unbounded embedding
        for rec in self.target_schema.recommendations:
            if rec.decision == RelationshipDecision.EMBED:
                for warning in rec.warnings:
                    if "unbounded" in warning.lower():
                        warnings.append(warning)

        return warnings

    def _get_assumptions(self) -> dict[str, Any]:
        """Get assumptions used in simulation."""
        return {
            "avg_join_latency_ms": self.config.avg_join_latency_ms,
            "avg_embed_read_latency_ms": self.config.avg_embed_read_latency_ms,
            "avg_reference_lookup_ms": self.config.avg_reference_lookup_ms,
            "source_storage_overhead": self.config.source_storage_overhead,
            "target_storage_overhead": self.config.target_storage_overhead,
            "source_storage_cost_per_gb": self.config.source_storage_cost_per_gb,
            "target_storage_cost_per_gb": self.config.target_storage_cost_per_gb,
            "row_counts": self.row_counts,
        }
