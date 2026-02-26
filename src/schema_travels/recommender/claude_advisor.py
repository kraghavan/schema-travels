"""Claude advisor module for AI-powered schema recommendations."""

import json
import logging
from typing import Any

from anthropic import Anthropic

from schema_travels.config import get_settings
from schema_travels.collector.models import SchemaDefinition
from schema_travels.analyzer.models import AnalysisResult
from schema_travels.recommender.models import (
    RelationshipDecision,
    SchemaRecommendation,
    TargetDatabase,
)

logger = logging.getLogger(__name__)


class ClaudeAdvisor:
    """
    AI advisor using Claude for schema recommendations.

    Provides intelligent analysis and recommendations for NoSQL schema design
    based on access patterns and schema structure.
    """

    def __init__(self, api_key: str | None = None, model: str | None = None):
        """
        Initialize Claude advisor.

        Args:
            api_key: Anthropic API key (defaults to env var)
            model: Claude model to use (defaults to config)
        """
        settings = get_settings()
        self.api_key = api_key or settings.anthropic_api_key
        self.model = model or settings.anthropic_model

        if not self.api_key:
            raise ValueError(
                "Anthropic API key not configured. "
                "Set ANTHROPIC_API_KEY environment variable."
            )

        self.client = Anthropic(api_key=self.api_key)

    def get_recommendations(
        self,
        schema: SchemaDefinition,
        analysis: AnalysisResult,
        target: TargetDatabase = TargetDatabase.MONGODB,
        additional_context: str | None = None,
    ) -> list[SchemaRecommendation]:
        """
        Get AI-powered schema recommendations.

        Args:
            schema: Source schema definition
            analysis: Analysis result from pattern analyzer
            target: Target database type
            additional_context: Additional context about the application

        Returns:
            List of schema recommendations
        """
        prompt = self._build_prompt(schema, analysis, target, additional_context)

        try:
            response = self.client.messages.create(
                model=self.model,
                max_tokens=4096,
                messages=[{"role": "user", "content": prompt}],
                system=self._get_system_prompt(),
            )

            # Parse response
            content = response.content[0].text
            recommendations = self._parse_recommendations(content)

            return recommendations

        except Exception as e:
            logger.error(f"Claude API error: {e}")
            raise

    def analyze_specific_relationship(
        self,
        parent_table: str,
        child_table: str,
        schema: SchemaDefinition,
        analysis: AnalysisResult,
        target: TargetDatabase = TargetDatabase.MONGODB,
    ) -> SchemaRecommendation:
        """
        Get detailed recommendation for a specific relationship.

        Args:
            parent_table: Parent table name
            child_table: Child table name
            schema: Source schema definition
            analysis: Analysis result
            target: Target database type

        Returns:
            Detailed schema recommendation
        """
        prompt = self._build_specific_prompt(
            parent_table, child_table, schema, analysis, target
        )

        try:
            response = self.client.messages.create(
                model=self.model,
                max_tokens=2048,
                messages=[{"role": "user", "content": prompt}],
                system=self._get_system_prompt(),
            )

            content = response.content[0].text
            recommendations = self._parse_recommendations(content)

            if recommendations:
                return recommendations[0]

            # Fallback if parsing fails
            return SchemaRecommendation(
                parent_table=parent_table,
                child_table=child_table,
                decision=RelationshipDecision.REFERENCE,
                confidence=0.5,
                reasoning=["Unable to parse AI recommendation - defaulting to reference"],
                warnings=["Manual review recommended"],
            )

        except Exception as e:
            logger.error(f"Claude API error: {e}")
            return SchemaRecommendation(
                parent_table=parent_table,
                child_table=child_table,
                decision=RelationshipDecision.REFERENCE,
                confidence=0.3,
                reasoning=[f"AI analysis failed: {str(e)}"],
                warnings=["Manual review required"],
            )

    def _get_system_prompt(self) -> str:
        """Get system prompt for Claude."""
        return """You are an expert database architect specializing in SQL to NoSQL migrations.
Your role is to analyze access patterns and recommend optimal document/key-value schema designs.

Key principles you follow:
1. Embed data that is frequently accessed together
2. Reference data that is accessed independently or updated frequently
3. Consider cardinality - avoid embedding unbounded arrays
4. Balance read optimization against write complexity
5. Consider the document size limits (16MB for MongoDB)

When making recommendations, you:
- Provide clear reasoning based on the access patterns
- Assign confidence levels (0.0 to 1.0) based on data clarity
- Warn about potential issues (unbounded growth, update complexity)
- Consider both current patterns and likely future needs

Always respond with valid JSON in the specified format."""

    def _build_prompt(
        self,
        schema: SchemaDefinition,
        analysis: AnalysisResult,
        target: TargetDatabase,
        additional_context: str | None,
    ) -> str:
        """Build the main analysis prompt."""
        # Prepare schema summary
        schema_summary = self._summarize_schema(schema)

        # Prepare analysis summary
        analysis_summary = self._summarize_analysis(analysis)

        prompt = f"""Analyze this SQL database and recommend an optimal {target.value} schema design.

## Source Schema
{schema_summary}

## Access Pattern Analysis
{analysis_summary}

## Additional Context
{additional_context or "No additional context provided."}

## Task
Based on the access patterns, recommend how to structure the data in {target.value}.
For each relationship, decide whether to:
- EMBED: Include child documents within parent
- REFERENCE: Keep separate with ID reference
- SEPARATE: Keep completely separate collections
- BUCKET: Use bucketing pattern for time-series data

Respond with JSON in this exact format:
```json
{{
  "recommendations": [
    {{
      "parent_table": "table_name",
      "child_table": "related_table_name",
      "decision": "EMBED|REFERENCE|SEPARATE|BUCKET",
      "confidence": 0.0-1.0,
      "reasoning": ["reason 1", "reason 2"],
      "warnings": ["warning 1"]
    }}
  ],
  "general_advice": "Overall migration advice",
  "target_collections": [
    {{
      "name": "collection_name",
      "source_tables": ["table1", "table2"],
      "embedded": ["embedded_table"],
      "references": ["referenced_table"]
    }}
  ]
}}
```"""

        return prompt

    def _build_specific_prompt(
        self,
        parent_table: str,
        child_table: str,
        schema: SchemaDefinition,
        analysis: AnalysisResult,
        target: TargetDatabase,
    ) -> str:
        """Build prompt for specific relationship analysis."""
        # Get relevant metrics
        metrics = self._get_relationship_metrics(parent_table, child_table, analysis)

        # Get table definitions
        parent_def = schema.get_table(parent_table)
        child_def = schema.get_table(child_table)

        prompt = f"""Analyze this specific relationship for {target.value} migration.

## Parent Table: {parent_table}
Columns: {[c.name + ' ' + c.data_type for c in parent_def.columns] if parent_def else 'Unknown'}

## Child Table: {child_table}
Columns: {[c.name + ' ' + c.data_type for c in child_def.columns] if child_def else 'Unknown'}

## Access Metrics
{json.dumps(metrics, indent=2)}

## Task
Recommend whether to EMBED, REFERENCE, SEPARATE, or BUCKET this relationship.

Respond with JSON:
```json
{{
  "recommendations": [
    {{
      "parent_table": "{parent_table}",
      "child_table": "{child_table}",
      "decision": "EMBED|REFERENCE|SEPARATE|BUCKET",
      "confidence": 0.0-1.0,
      "reasoning": ["detailed reason 1", "detailed reason 2"],
      "warnings": ["any warnings"],
      "suggested_structure": {{
        "description": "How the document should look"
      }}
    }}
  ]
}}
```"""

        return prompt

    def _summarize_schema(self, schema: SchemaDefinition) -> str:
        """Create a summary of the schema for the prompt."""
        lines = []

        for table in schema.tables:
            cols = ", ".join([f"{c.name} ({c.data_type})" for c in table.columns[:10]])
            if len(table.columns) > 10:
                cols += f", ... ({len(table.columns) - 10} more)"
            lines.append(f"- {table.name}: {cols}")

        lines.append("\nForeign Keys:")
        for fk in schema.foreign_keys:
            lines.append(
                f"- {fk.from_table}.{fk.from_columns[0]} → "
                f"{fk.to_table}.{fk.to_columns[0]}"
            )

        return "\n".join(lines)

    def _summarize_analysis(self, analysis: AnalysisResult) -> str:
        """Create a summary of the analysis for the prompt."""
        lines = [
            f"Total Queries Analyzed: {analysis.total_queries_analyzed}",
            "",
            "Top Hot Joins (by cost):",
        ]

        for jp in analysis.join_patterns[:10]:
            lines.append(
                f"- {jp.left_table} ⟷ {jp.right_table}: "
                f"{jp.frequency} calls, {jp.avg_time_ms:.1f}ms avg"
            )

        lines.append("\nTable Mutation Patterns:")
        for mp in sorted(
            analysis.mutation_patterns,
            key=lambda m: m.total_operations,
            reverse=True,
        )[:10]:
            lines.append(
                f"- {mp.table}: reads={mp.select_count}, writes={mp.total_writes} "
                f"({mp.write_ratio:.0%} write ratio)"
            )

        lines.append("\nCo-Access Patterns:")
        for ap in analysis.access_patterns[:10]:
            lines.append(
                f"- {ap.table_a} + {ap.table_b}: "
                f"{ap.co_access_ratio:.0%} co-accessed"
            )

        return "\n".join(lines)

    def _get_relationship_metrics(
        self,
        parent_table: str,
        child_table: str,
        analysis: AnalysisResult,
    ) -> dict[str, Any]:
        """Get metrics for a specific relationship."""
        metrics: dict[str, Any] = {
            "parent_table": parent_table,
            "child_table": child_table,
        }

        # Find access pattern
        for ap in analysis.access_patterns:
            if set([ap.table_a, ap.table_b]) == set([parent_table, child_table]):
                metrics["co_access_ratio"] = ap.co_access_ratio
                metrics["parent_independence"] = (
                    ap.table_a_independence
                    if ap.table_a == parent_table
                    else ap.table_b_independence
                )
                metrics["child_independence"] = (
                    ap.table_a_independence
                    if ap.table_a == child_table
                    else ap.table_b_independence
                )
                break

        # Find mutation patterns
        for mp in analysis.mutation_patterns:
            if mp.table == child_table:
                metrics["child_write_ratio"] = mp.write_ratio
                metrics["child_reads"] = mp.select_count
                metrics["child_writes"] = mp.total_writes
                break

        # Find join pattern
        for jp in analysis.join_patterns:
            if set([jp.left_table, jp.right_table]) == set([parent_table, child_table]):
                metrics["join_frequency"] = jp.frequency
                metrics["join_avg_time_ms"] = jp.avg_time_ms
                break

        return metrics

    def _parse_recommendations(self, content: str) -> list[SchemaRecommendation]:
        """Parse Claude's response into SchemaRecommendation objects."""
        recommendations = []

        try:
            # Extract JSON from response
            json_start = content.find("{")
            json_end = content.rfind("}") + 1

            if json_start == -1 or json_end == 0:
                logger.warning("No JSON found in response")
                return recommendations

            json_str = content[json_start:json_end]
            data = json.loads(json_str)

            for rec in data.get("recommendations", []):
                decision_str = rec.get("decision", "REFERENCE").upper()
                decision = RelationshipDecision[decision_str]

                recommendations.append(
                    SchemaRecommendation(
                        parent_table=rec.get("parent_table", ""),
                        child_table=rec.get("child_table", ""),
                        decision=decision,
                        confidence=float(rec.get("confidence", 0.5)),
                        reasoning=rec.get("reasoning", []),
                        warnings=rec.get("warnings", []),
                        metrics=rec.get("metrics", {}),
                    )
                )

        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse JSON response: {e}")
        except KeyError as e:
            logger.error(f"Invalid decision value: {e}")
        except Exception as e:
            logger.error(f"Error parsing recommendations: {e}")

        return recommendations
