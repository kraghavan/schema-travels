"""Data models for the simulator module."""

from dataclasses import dataclass, field
from typing import Any


@dataclass
class StorageEstimate:
    """Storage estimation for a database."""

    total_gb: float
    breakdown: dict[str, float] = field(default_factory=dict)
    overhead_factor: float = 1.0


@dataclass
class LatencyEstimate:
    """Latency estimation for queries."""

    avg_ms: float
    p50_ms: float = 0.0
    p95_ms: float = 0.0
    p99_ms: float = 0.0
    breakdown: dict[str, float] = field(default_factory=dict)


@dataclass
class CostEstimate:
    """Cost estimation for a database."""

    monthly_usd: float
    storage_cost: float = 0.0
    read_cost: float = 0.0
    write_cost: float = 0.0
    breakdown: dict[str, float] = field(default_factory=dict)


@dataclass
class SimulationResult:
    """Complete simulation result comparing source and target."""

    # Storage
    source_storage: StorageEstimate
    target_storage: StorageEstimate
    storage_change_pct: float

    # Latency
    source_latency: LatencyEstimate
    target_latency: LatencyEstimate
    latency_improvement_pct: float

    # Cost
    source_cost: CostEstimate
    target_cost: CostEstimate
    cost_change_pct: float

    # Metadata
    assumptions: dict[str, Any] = field(default_factory=dict)
    warnings: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary."""
        return {
            "storage": {
                "source_gb": self.source_storage.total_gb,
                "target_gb": self.target_storage.total_gb,
                "change_pct": self.storage_change_pct,
                "source_breakdown": self.source_storage.breakdown,
                "target_breakdown": self.target_storage.breakdown,
            },
            "latency": {
                "source_avg_ms": self.source_latency.avg_ms,
                "target_avg_ms": self.target_latency.avg_ms,
                "improvement_pct": self.latency_improvement_pct,
                "source_breakdown": self.source_latency.breakdown,
                "target_breakdown": self.target_latency.breakdown,
            },
            "cost": {
                "source_monthly_usd": self.source_cost.monthly_usd,
                "target_monthly_usd": self.target_cost.monthly_usd,
                "change_pct": self.cost_change_pct,
                "source_breakdown": {
                    "storage": self.source_cost.storage_cost,
                    "reads": self.source_cost.read_cost,
                    "writes": self.source_cost.write_cost,
                },
                "target_breakdown": {
                    "storage": self.target_cost.storage_cost,
                    "reads": self.target_cost.read_cost,
                    "writes": self.target_cost.write_cost,
                },
            },
            "assumptions": self.assumptions,
            "warnings": self.warnings,
        }

    def get_summary(self) -> str:
        """Generate human-readable summary."""
        lines = [
            "=" * 60,
            "MIGRATION SIMULATION RESULTS",
            "=" * 60,
            "",
            "STORAGE IMPACT",
            "-" * 40,
            f"  Source:  {self.source_storage.total_gb:.2f} GB",
            f"  Target:  {self.target_storage.total_gb:.2f} GB",
            f"  Change:  {'+' if self.storage_change_pct > 0 else ''}{self.storage_change_pct:.1f}%",
            "",
            "LATENCY IMPACT",
            "-" * 40,
            f"  Source avg:  {self.source_latency.avg_ms:.2f} ms",
            f"  Target avg:  {self.target_latency.avg_ms:.2f} ms",
        ]

        if self.latency_improvement_pct > 0:
            lines.append(f"  Improvement: {self.latency_improvement_pct:.1f}% faster")
        else:
            lines.append(f"  Change:      {abs(self.latency_improvement_pct):.1f}% slower")

        lines.extend([
            "",
            "MONTHLY COST IMPACT",
            "-" * 40,
            f"  Source:  ${self.source_cost.monthly_usd:.2f}",
            f"  Target:  ${self.target_cost.monthly_usd:.2f}",
            f"  Change:  {'+' if self.cost_change_pct > 0 else ''}{self.cost_change_pct:.1f}%",
            "",
        ])

        # Summary recommendation
        lines.append("SUMMARY")
        lines.append("-" * 40)

        if self.latency_improvement_pct > 20 and self.cost_change_pct < 50:
            lines.append("  ✓ RECOMMENDED: Significant latency improvement")
        elif self.latency_improvement_pct > 0:
            lines.append("  ○ MODERATE BENEFIT: Some improvement expected")
        else:
            lines.append("  ⚠ REVIEW CAREFULLY: May not improve performance")

        if self.warnings:
            lines.append("")
            lines.append("WARNINGS")
            lines.append("-" * 40)
            for warning in self.warnings:
                lines.append(f"  • {warning}")

        lines.append("")
        lines.append("=" * 60)

        return "\n".join(lines)
