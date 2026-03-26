"""Claude advisor module for AI-powered schema recommendations."""

import json
import logging
from typing import Any

from anthropic import Anthropic

from schema_travels.config import get_settings, APIKeyNotConfiguredError
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
    
    v2.0.0: Added DynamoDB support with single-table design decisions.
    """

    def __init__(self, api_key: str | None = None, model: str | None = None):
        """
        Initialize Claude advisor.

        Args:
            api_key: Anthropic API key (defaults to env var)
            model: Claude model to use (defaults to config)
            
        Raises:
            APIKeyNotConfiguredError: If no API key is available
        """
        settings = get_settings()
        
        # Use provided key or get from settings
        self.api_key = api_key or settings.anthropic_api_key
        self.model = model or settings.anthropic_model

        # Validate API key - raise clear error if missing
        if not self.api_key or self.api_key.strip() == "":
            raise APIKeyNotConfiguredError()
        
        if self.api_key in ("your-api-key-here", "sk-ant-xxxxx"):
            raise APIKeyNotConfiguredError()

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
                system=self._get_system_prompt(target),
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
                system=self._get_system_prompt(target),
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

    # =========================================================================
    # v2.0.0: DynamoDB Gray-Zone Decisions
    # =========================================================================

    def get_dynamodb_design_decision(
        self,
        clusters: list[dict],
        join_patterns: list[dict],
        mutation_patterns: list[dict],
        table_stats: list[dict],
    ) -> dict[str, Any]:
        """
        Get AI decision for DynamoDB design gray zones.
        
        Called when deterministic heuristics are inconclusive:
        - High co-access + write-heavy child tables
        - Exactly 2-3 table pairs at threshold boundary
        - Mixed signals between single/multi-table indicators
        
        Args:
            clusters: Access cluster data from DynamoDBDesigner
            join_patterns: Hot join patterns
            mutation_patterns: Table write ratios
            table_stats: Per-table access statistics
            
        Returns:
            Decision dict with 'mode', 'confidence', 'rationale', 'warnings'
        """
        prompt = self._build_dynamodb_decision_prompt(
            clusters, join_patterns, mutation_patterns, table_stats
        )
        
        try:
            response = self.client.messages.create(
                model=self.model,
                max_tokens=2048,
                messages=[{"role": "user", "content": prompt}],
                system=self._get_dynamodb_decision_system_prompt(),
            )
            
            content = response.content[0].text
            return self._parse_dynamodb_decision(content)
            
        except Exception as e:
            logger.error(f"Claude API error in DynamoDB decision: {e}")
            # Fallback to multi-table (safer default)
            return {
                "mode": "multi_table",
                "confidence": 0.5,
                "rationale": f"AI decision failed ({e}), defaulting to multi-table for safety",
                "warnings": ["Manual review recommended"],
            }

    def get_gsi_recommendation(
        self,
        table: str,
        filtered_columns: dict[str, int],
        selected_columns: dict[str, int],
        access_patterns: list[str],
    ) -> list[dict]:
        """
        Get AI recommendation for GSI design.
        
        Args:
            table: Table name
            filtered_columns: {column: frequency} from WHERE clauses
            selected_columns: {column: frequency} from SELECT clauses
            access_patterns: Descriptions of access patterns
            
        Returns:
            List of GSI recommendations
        """
        prompt = f"""Analyze these access patterns for DynamoDB table '{table}' and recommend GSIs.

## Filtered Columns (WHERE clauses)
{json.dumps(filtered_columns, indent=2)}

## Selected Columns (SELECT clauses)
{json.dumps(selected_columns, indent=2)}

## Access Patterns
{chr(10).join(f'- {p}' for p in access_patterns)}

Recommend GSIs that would optimize these patterns. Consider:
1. Which columns are queried together
2. Projection type (KEYS_ONLY, INCLUDE, ALL) based on selected columns
3. Cost vs performance tradeoff

Respond with JSON:
```json
{{
  "gsis": [
    {{
      "name": "GSI name",
      "pk_attribute": "column for partition key",
      "sk_attribute": "column for sort key (optional)",
      "projection_type": "KEYS_ONLY|INCLUDE|ALL",
      "projected_attributes": ["col1", "col2"],
      "rationale": "Why this GSI helps"
    }}
  ],
  "warnings": ["Any concerns"]
}}
```"""

        try:
            response = self.client.messages.create(
                model=self.model,
                max_tokens=2048,
                messages=[{"role": "user", "content": prompt}],
                system=self._get_dynamodb_decision_system_prompt(),
            )
            
            content = response.content[0].text
            return self._parse_gsi_recommendations(content)
            
        except Exception as e:
            logger.error(f"Claude API error in GSI recommendation: {e}")
            return []

    def _build_dynamodb_decision_prompt(
        self,
        clusters: list[dict],
        join_patterns: list[dict],
        mutation_patterns: list[dict],
        table_stats: list[dict],
    ) -> str:
        """Build prompt for DynamoDB design decision."""
        return f"""Analyze these SQL access patterns and decide on DynamoDB design mode.

## Access Clusters
{json.dumps(clusters, indent=2)}

## Hot Join Patterns
{json.dumps(join_patterns, indent=2)}

## Mutation Patterns (Write Ratios)
{json.dumps(mutation_patterns, indent=2)}

## Table Statistics
{json.dumps(table_stats, indent=2)}

## Decision Required
Based on this data, should we use SINGLE_TABLE or MULTI_TABLE design?

Use the Decision Matrix:
| Signal | Single-Table | Multi-Table |
|--------|--------------|-------------|
| Co-access ratio | >70% | <50% |
| Hot join pairs | ≥3 | <2 |
| Write-heavy children | Few | Many |
| Independent queries | Rare | Common |

Respond with JSON:
```json
{{
  "mode": "single_table" or "multi_table",
  "confidence": 0.0-1.0,
  "rationale": "Detailed explanation",
  "key_factors": ["factor1", "factor2"],
  "warnings": ["potential issues"],
  "entity_groupings": [
    {{
      "pk_table": "main entity",
      "sk_tables": ["child entities to embed"]
    }}
  ]
}}
```"""

    def _get_dynamodb_decision_system_prompt(self) -> str:
        """System prompt for DynamoDB design decisions."""
        return """You are an expert DynamoDB schema architect specializing in single-table design patterns.

## Decision Matrix for Single-Table vs Multi-Table

FAVOR SINGLE-TABLE when:
- Tables have >70% co-access ratio (queried together)
- ≥3 hot join patterns between tables
- Clear parent-child hierarchy
- Read-heavy workload
- Need transactional consistency across entities

FAVOR MULTI-TABLE when:
- Tables accessed independently >50% of time
- Child tables are write-heavy (>50% writes)
- No clear entity hierarchy
- Different scaling requirements per entity
- Teams own different tables independently

GRAY ZONES (require careful analysis):
- 2-3 table pairs at 60-70% co-access threshold
- High co-access but write-heavy children
- Mixed read/write patterns
- Partial independence (some queries solo, some joined)

## Key DynamoDB Constraints
- Item size limit: 400KB
- Partition throughput: 3000 RCU / 1000 WCU per partition
- GSI limit: 20 per table (but 5 is practical)
- Hot partitions cause throttling

## Response Format
Always respond with valid JSON. Be specific about which tables should be PK entities vs SK entities in single-table design."""

    def _parse_dynamodb_decision(self, content: str) -> dict[str, Any]:
        """Parse DynamoDB decision response."""
        try:
            json_start = content.find("{")
            json_end = content.rfind("}") + 1
            
            if json_start == -1 or json_end == 0:
                logger.warning("No JSON found in DynamoDB decision response")
                return {
                    "mode": "multi_table",
                    "confidence": 0.5,
                    "rationale": "Failed to parse AI response",
                    "warnings": ["Manual review required"],
                }
            
            json_str = content[json_start:json_end]
            data = json.loads(json_str)
            
            return {
                "mode": data.get("mode", "multi_table"),
                "confidence": float(data.get("confidence", 0.5)),
                "rationale": data.get("rationale", ""),
                "key_factors": data.get("key_factors", []),
                "warnings": data.get("warnings", []),
                "entity_groupings": data.get("entity_groupings", []),
            }
            
        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse DynamoDB decision JSON: {e}")
            return {
                "mode": "multi_table",
                "confidence": 0.5,
                "rationale": f"JSON parse error: {e}",
                "warnings": ["Manual review required"],
            }

    def _parse_gsi_recommendations(self, content: str) -> list[dict]:
        """Parse GSI recommendations response."""
        try:
            json_start = content.find("{")
            json_end = content.rfind("}") + 1
            
            if json_start == -1 or json_end == 0:
                return []
            
            json_str = content[json_start:json_end]
            data = json.loads(json_str)
            
            return data.get("gsis", [])
            
        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse GSI recommendations: {e}")
            return []

    # =========================================================================
    # System Prompts
    # =========================================================================

    def _get_system_prompt(self, target: TargetDatabase = TargetDatabase.MONGODB) -> str:
        """Get system prompt for Claude based on target database."""
        base_prompt = """You are an expert database architect specializing in SQL to NoSQL migrations.
Your role is to analyze access patterns and recommend optimal document/key-value schema designs.

Key principles you follow:
1. Embed data that is frequently accessed together
2. Reference data that is accessed independently or updated frequently
3. Consider cardinality - avoid embedding unbounded arrays
4. Balance read optimization against write complexity
5. Consider the document size limits"""

        if target == TargetDatabase.MONGODB:
            return base_prompt + """

MongoDB-specific considerations:
- Document size limit: 16MB
- Use $lookup sparingly (it's expensive)
- Embed for read-heavy, reference for write-heavy
- Consider bucket pattern for time-series

When making recommendations, you:
- Provide clear reasoning based on the access patterns
- Assign confidence levels (0.0 to 1.0) based on data clarity
- Warn about potential issues (unbounded growth, update complexity)
- Consider both current patterns and likely future needs

Always respond with valid JSON in the specified format."""

        elif target == TargetDatabase.DYNAMODB:
            return base_prompt + """

DynamoDB-specific considerations:
- Item size limit: 400KB
- Single-table design for related entities accessed together
- Use PK/SK patterns: USER#<id> / ORDER#<order_id>
- GSIs for alternative access patterns (max 20, but 5 is practical)
- Projection types affect cost: KEYS_ONLY < INCLUDE < ALL
- Hot partitions cause throttling - distribute writes evenly
- Consider write sharding for high-volume tables

Single-Table Design Patterns:
- Parent entity: PK = ENTITY#<id>, SK = ENTITY
- Child entity: PK = PARENT#<parent_id>, SK = CHILD#<id>
- Use GSI for inverted lookups (child → parent)

When making recommendations, you:
- Recommend EMBED for high co-access (>70%)
- Recommend REFERENCE (separate items, same table) for moderate co-access
- Recommend SEPARATE (different tables) for independent access patterns
- Consider write amplification in single-table design
- Warn about hot partition risks

Always respond with valid JSON in the specified format."""

        return base_prompt + "\n\nAlways respond with valid JSON in the specified format."

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

        if target == TargetDatabase.DYNAMODB:
            return self._build_dynamodb_prompt(
                schema_summary, analysis_summary, additional_context
            )

        # MongoDB prompt (default)
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

    def _build_dynamodb_prompt(
        self,
        schema_summary: str,
        analysis_summary: str,
        additional_context: str | None,
    ) -> str:
        """Build DynamoDB-specific analysis prompt."""
        return f"""Analyze this SQL database and recommend an optimal DynamoDB schema design.

## Source Schema
{schema_summary}

## Access Pattern Analysis
{analysis_summary}

## Additional Context
{additional_context or "No additional context provided."}

## Task
Based on the access patterns, recommend how to structure the data in DynamoDB.
Consider single-table design vs multi-table based on:
- Co-access patterns (>70% = single table candidate)
- Write patterns (write-heavy children may need separation)
- Query independence (tables queried alone vs together)

For each relationship, decide whether to:
- EMBED: Same item (use composite SK)
- REFERENCE: Separate items in same table (PK/SK pattern)
- SEPARATE: Different DynamoDB tables
- BUCKET: Time-series bucketing pattern

Respond with JSON in this exact format:
```json
{{
  "design_mode": "single_table" or "multi_table",
  "recommendations": [
    {{
      "parent_table": "table_name",
      "child_table": "related_table_name",
      "decision": "EMBED|REFERENCE|SEPARATE|BUCKET",
      "confidence": 0.0-1.0,
      "reasoning": ["reason 1", "reason 2"],
      "warnings": ["warning 1"],
      "pk_pattern": "PARENT#<id>",
      "sk_pattern": "CHILD#<id>"
    }}
  ],
  "general_advice": "Overall migration advice",
  "suggested_gsis": [
    {{
      "name": "GSI1",
      "pk_attribute": "GSI1PK",
      "sk_attribute": "GSI1SK",
      "purpose": "Enable lookup by X"
    }}
  ]
}}
```"""

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
