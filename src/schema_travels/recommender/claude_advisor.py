"""Claude advisor module for AI-powered schema recommendations.

v2.0.0: Added DynamoDB support with single-table design decisions.
v2.0.1: Added review_dynamodb_design() for AI review of local designs.
"""

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
# v2.0.1: Import DynamoDB review models
from schema_travels.recommender.dynamodb_models import (
    DynamoDBDesign,
    DynamoDBReview,
    EntityChange,
    GSIChange,
    GSIChangeAction,
    ReviewChangeType,
    DesignMode,
    ProjectionType,
)

logger = logging.getLogger(__name__)


class ClaudeAdvisor:
    """
    AI advisor using Claude for schema recommendations.

    Provides intelligent analysis and recommendations for NoSQL schema design
    based on access patterns and schema structure.
    
    v2.0.0: Added DynamoDB support with single-table design decisions.
    v2.0.1: Added review workflow for DynamoDB - local design + AI review.
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
        Get AI-powered schema recommendations (MongoDB only).

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
    # v2.0.1: DynamoDB Design Review
    # =========================================================================

    def review_dynamodb_design(
        self,
        design: DynamoDBDesign,
        analysis: AnalysisResult,
        schema: SchemaDefinition,
    ) -> DynamoDBReview:
        """
        Review a locally-generated DynamoDB design and suggest improvements.
        
        This is the primary AI integration point for DynamoDB. The design
        is created by DynamoDBDesigner using deterministic algorithms, then
        reviewed by Claude for edge cases and optimizations.
        
        Args:
            design: DynamoDB design from DynamoDBDesigner
            analysis: Analysis result with access patterns
            schema: Source schema definition
            
        Returns:
            DynamoDBReview with approval status and suggested changes
        """
        prompt = self._build_dynamodb_review_prompt(design, analysis, schema)
        
        try:
            response = self.client.messages.create(
                model=self.model,
                max_tokens=4096,
                messages=[{"role": "user", "content": prompt}],
                system=self._get_dynamodb_review_system_prompt(),
            )
            
            content = response.content[0].text
            return self._parse_dynamodb_review(content)
            
        except Exception as e:
            logger.error(f"Claude API error in DynamoDB review: {e}")
            # Return approved review with warning on failure
            return DynamoDBReview(
                approved=True,
                confidence=0.5,
                summary=f"AI review failed ({e}). Design approved by default.",
                warnings=["AI review failed - manual review recommended"],
            )

    def _build_dynamodb_review_prompt(
        self,
        design: DynamoDBDesign,
        analysis: AnalysisResult,
        schema: SchemaDefinition,
    ) -> str:
        """Build the prompt for DynamoDB design review."""
        # Summarize the design
        design_summary = self._summarize_dynamodb_design(design)
        
        # Summarize access patterns
        access_summary = self._summarize_analysis(analysis)
        
        # Summarize source schema
        schema_summary = self._summarize_schema(schema)
        
        return f"""Review this DynamoDB schema design and suggest improvements.

## Proposed Design

{design_summary}

## Access Patterns from Query Logs

{access_summary}

## Source SQL Schema

{schema_summary}

## Review Task

Analyze this design and provide feedback:

1. **Design Mode**: Is {design.design_mode.value} the right choice? Consider:
   - Co-access patterns (are tables frequently queried together?)
   - Write patterns (are there write-heavy tables that should be separate?)
   - Query independence (are some tables queried alone often?)

2. **PK/SK Patterns**: Are the key patterns optimal?
   - Can queries be satisfied with key conditions (no filters)?
   - Is there risk of hot partitions?
   - Are hierarchical relationships modeled correctly?

3. **GSIs**: Are the Global Secondary Indexes appropriate?
   - Are there missing GSIs for common access patterns?
   - Are any GSIs unnecessary (could use main table)?
   - Is the projection type (KEYS_ONLY/INCLUDE/ALL) optimal?

4. **Warnings**: Identify potential issues:
   - Hot partition risks (low cardinality keys)
   - Unbounded growth (missing TTL considerations)
   - Missing access patterns

Respond with JSON in this exact format:
```json
{{
  "approved": true,
  "confidence": 0.85,
  "summary": "Overall assessment of the design",
  "design_mode_change": null,
  "design_mode_reason": null,
  "entity_changes": [
    {{
      "entity": "entity_name",
      "change_type": "modify_pk|modify_sk|add_attribute|remove_attribute",
      "current_value": "current pattern",
      "new_value": "suggested pattern",
      "reason": "why this change helps"
    }}
  ],
  "gsi_changes": [
    {{
      "action": "add|remove|modify",
      "gsi_name": "GSI name",
      "pk_attribute": "pk attr (for add/modify)",
      "sk_attribute": "sk attr (optional)",
      "projection_type": "KEYS_ONLY|INCLUDE|ALL",
      "projected_attributes": ["col1", "col2"],
      "access_pattern": "what pattern this supports",
      "reason": "why this change"
    }}
  ],
  "warnings": [
    "Warning about potential issues"
  ],
  "suggestions": [
    "Optional improvements (TTL, streams, etc.)"
  ],
  "uncovered_patterns": [
    "Access patterns not well-supported"
  ]
}}
```

Important:
- Set "approved": true if the design is good (even with minor suggestions)
- Set "approved": false only if there are critical issues
- "design_mode_change" should be null unless you strongly recommend changing
- Keep entity_changes and gsi_changes empty if no changes needed
- Be specific in reasons - reference actual access patterns"""

    def _summarize_dynamodb_design(self, design: DynamoDBDesign) -> str:
        """Create a summary of the DynamoDB design for the prompt."""
        lines = [
            f"Design Mode: {design.design_mode.value}",
            f"Confidence: {design.confidence:.0%}",
            f"Rationale: {design.rationale}",
            "",
        ]
        
        if design.design_mode == DesignMode.SINGLE_TABLE:
            lines.append(f"Table Name: {design.table_name}")
            lines.append(f"Partition Key: {design.partition_key} (String)")
            lines.append(f"Sort Key: {design.sort_key} (String)")
            lines.append("")
            lines.append("Entities:")
            for entity in design.entities:
                lines.append(f"  - {entity.name} (from {entity.source_table})")
                lines.append(f"    PK: {entity.pk_pattern}")
                lines.append(f"    SK: {entity.sk_pattern}")
                if entity.attributes:
                    lines.append(f"    Attributes: {', '.join(entity.attributes[:5])}")
            
            lines.append("")
            lines.append("GSIs:")
            if design.gsis:
                for gsi in design.gsis:
                    lines.append(f"  - {gsi.name}")
                    lines.append(f"    PK: {gsi.pk_attribute}")
                    if gsi.sk_attribute:
                        lines.append(f"    SK: {gsi.sk_attribute}")
                    lines.append(f"    Projection: {gsi.projection_type.value}")
                    if gsi.access_pattern:
                        lines.append(f"    Pattern: {gsi.access_pattern}")
            else:
                lines.append("  (none)")
        else:
            lines.append("Tables:")
            for table in design.tables:
                lines.append(f"  - {table.table_name} (from {table.source_table})")
                lines.append(f"    PK: {table.partition_key}")
                if table.sort_key:
                    lines.append(f"    SK: {table.sort_key}")
                if table.gsis:
                    lines.append(f"    GSIs: {', '.join(g.name for g in table.gsis)}")
        
        if design.clusters:
            lines.append("")
            lines.append("Access Clusters Identified:")
            for cluster in design.clusters:
                lines.append(f"  - {cluster.cluster_id}: {', '.join(cluster.tables)}")
                lines.append(f"    Co-access strength: {cluster.co_access_strength:.0%}")
        
        if design.orphan_tables:
            lines.append("")
            lines.append(f"Orphan Tables (low co-access): {', '.join(design.orphan_tables)}")
        
        if design.warnings:
            lines.append("")
            lines.append("Current Warnings:")
            for w in design.warnings:
                lines.append(f"  - {w}")
        
        return "\n".join(lines)

    def _get_dynamodb_review_system_prompt(self) -> str:
        """System prompt for DynamoDB design review."""
        return """You are an expert AWS DynamoDB architect reviewing schema designs.

Your role is to:
1. Validate design decisions against access patterns
2. Identify potential issues (hot partitions, inefficient queries)
3. Suggest optimizations (better key patterns, GSI improvements)
4. Ensure all access patterns are efficiently supported

Key DynamoDB principles to apply:
- Partition keys should have high cardinality
- Sort keys enable range queries and hierarchical data
- GSIs should be used sparingly (cost $$$) but are necessary for alternate access patterns
- KEYS_ONLY projection is cheapest, ALL is most flexible
- Single-table design works best when data is accessed together
- Multi-table design is better for independent data or different scaling needs

Be practical and specific. Reference actual entities and access patterns in your feedback.
If the design is good, approve it with high confidence. Only suggest changes that provide clear value."""

    def _parse_dynamodb_review(self, content: str) -> DynamoDBReview:
        """Parse Claude's response into a DynamoDBReview object."""
        try:
            # Extract JSON from response
            json_start = content.find("{")
            json_end = content.rfind("}") + 1
            
            if json_start == -1 or json_end == 0:
                logger.warning("No JSON found in review response")
                return DynamoDBReview(
                    approved=True,
                    confidence=0.5,
                    summary="Could not parse AI response - approving by default",
                    warnings=["AI response parsing failed"],
                )
            
            json_str = content[json_start:json_end]
            data = json.loads(json_str)
            
            # Parse entity changes
            entity_changes = []
            for ec in data.get("entity_changes", []):
                try:
                    change_type = ReviewChangeType(ec.get("change_type", "modify_sk"))
                except ValueError:
                    change_type = ReviewChangeType.MODIFY_SK
                
                entity_changes.append(EntityChange(
                    entity=ec.get("entity", ""),
                    change_type=change_type,
                    current_value=ec.get("current_value"),
                    new_value=ec.get("new_value", ""),
                    reason=ec.get("reason", ""),
                ))
            
            # Parse GSI changes
            gsi_changes = []
            for gc in data.get("gsi_changes", []):
                try:
                    action = GSIChangeAction(gc.get("action", "modify"))
                except ValueError:
                    action = GSIChangeAction.MODIFY
                
                projection_type = None
                if gc.get("projection_type"):
                    try:
                        projection_type = ProjectionType(gc.get("projection_type"))
                    except ValueError:
                        projection_type = ProjectionType.ALL
                
                gsi_changes.append(GSIChange(
                    action=action,
                    gsi_name=gc.get("gsi_name", ""),
                    pk_attribute=gc.get("pk_attribute"),
                    sk_attribute=gc.get("sk_attribute"),
                    projection_type=projection_type,
                    projected_attributes=gc.get("projected_attributes", []),
                    access_pattern=gc.get("access_pattern"),
                    reason=gc.get("reason", ""),
                ))
            
            # Parse design mode change
            design_mode_change = None
            if data.get("design_mode_change"):
                try:
                    design_mode_change = DesignMode(data["design_mode_change"])
                except ValueError:
                    pass
            
            return DynamoDBReview(
                approved=data.get("approved", True),
                confidence=float(data.get("confidence", 0.8)),
                summary=data.get("summary", ""),
                design_mode_change=design_mode_change,
                design_mode_reason=data.get("design_mode_reason"),
                entity_changes=entity_changes,
                gsi_changes=gsi_changes,
                warnings=data.get("warnings", []),
                suggestions=data.get("suggestions", []),
                uncovered_patterns=data.get("uncovered_patterns", []),
            )
            
        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse review JSON: {e}")
            return DynamoDBReview(
                approved=True,
                confidence=0.5,
                summary="JSON parsing failed - approving by default",
                warnings=["AI response JSON parsing failed"],
            )
        except Exception as e:
            logger.error(f"Error parsing DynamoDB review: {e}")
            return DynamoDBReview(
                approved=True,
                confidence=0.5,
                summary=f"Parsing error: {e}",
                warnings=["AI response parsing error"],
            )

    # =========================================================================
    # v2.0.0: DynamoDB Gray-Zone Decisions (kept for compatibility)
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
        return f"""Analyze this database access pattern data and decide between single-table and multi-table DynamoDB design.

## Access Clusters
{json.dumps(clusters, indent=2)}

## Hot Join Patterns
{json.dumps(join_patterns, indent=2)}

## Mutation Patterns
{json.dumps(mutation_patterns, indent=2)}

## Table Statistics
{json.dumps(table_stats, indent=2)}

Based on this data, recommend:
1. Single-table design (all data in one table with composite keys)
2. Multi-table design (separate tables for different entities)

Respond with JSON:
```json
{{
  "mode": "single_table" or "multi_table",
  "confidence": 0.0-1.0,
  "rationale": "Explanation of why this mode is recommended",
  "warnings": ["Any concerns or caveats"]
}}
```"""

    def _get_dynamodb_decision_system_prompt(self) -> str:
        """System prompt for DynamoDB design decisions."""
        return """You are an expert DynamoDB architect. Analyze access patterns and recommend the optimal design.

Single-table design is best when:
- Tables are frequently accessed together (>70% co-access)
- Relationships are hierarchical (parent-child)
- You need transactional consistency across entities

Multi-table design is best when:
- Tables are accessed independently
- Different scaling requirements per table
- Write-heavy tables that would cause contention
- Different TTL or backup requirements

Be decisive and provide clear rationale based on the data."""

    def _parse_dynamodb_decision(self, content: str) -> dict[str, Any]:
        """Parse DynamoDB decision response."""
        try:
            json_start = content.find("{")
            json_end = content.rfind("}") + 1
            
            if json_start == -1 or json_end == 0:
                return {
                    "mode": "multi_table",
                    "confidence": 0.5,
                    "rationale": "Could not parse response",
                    "warnings": ["Manual review recommended"],
                }
            
            json_str = content[json_start:json_end]
            data = json.loads(json_str)
            
            return {
                "mode": data.get("mode", "multi_table"),
                "confidence": float(data.get("confidence", 0.5)),
                "rationale": data.get("rationale", ""),
                "warnings": data.get("warnings", []),
            }
            
        except Exception as e:
            logger.error(f"Error parsing DynamoDB decision: {e}")
            return {
                "mode": "multi_table",
                "confidence": 0.5,
                "rationale": f"Parsing error: {e}",
                "warnings": ["Manual review recommended"],
            }

    def _parse_gsi_recommendations(self, content: str) -> list[dict]:
        """Parse GSI recommendation response."""
        try:
            json_start = content.find("{")
            json_end = content.rfind("}") + 1
            
            if json_start == -1 or json_end == 0:
                return []
            
            json_str = content[json_start:json_end]
            data = json.loads(json_str)
            
            return data.get("gsis", [])
            
        except Exception as e:
            logger.error(f"Error parsing GSI recommendations: {e}")
            return []

    # =========================================================================
    # Common Methods
    # =========================================================================

    def _get_system_prompt(self, target: TargetDatabase) -> str:
        """Get system prompt based on target database."""
        if target == TargetDatabase.DYNAMODB:
            return self._get_dynamodb_system_prompt()
        return self._get_mongodb_system_prompt()

    def _get_mongodb_system_prompt(self) -> str:
        """System prompt for MongoDB recommendations."""
        return """You are an expert MongoDB schema architect. Your role is to analyze SQL database schemas 
and access patterns to recommend optimal MongoDB document designs.

Key principles to apply:
- Embed data that is accessed together (read patterns)
- Reference data that is updated independently or has unbounded growth
- Consider document size limits (16MB)
- Optimize for the most common access patterns
- Balance between read and write performance

For each relationship, provide clear reasoning based on:
- Co-access frequency
- Update patterns
- Cardinality
- Document size implications

Always respond with valid JSON in the specified format."""

    def _get_dynamodb_system_prompt(self) -> str:
        """System prompt for DynamoDB recommendations."""
        base_prompt = """You are an expert AWS DynamoDB architect. Your role is to analyze SQL database schemas 
and access patterns to recommend optimal DynamoDB designs.

Key principles to apply:
- Design for access patterns first, not entities
- Use single-table design when data is accessed together (>70% co-access)
- Use composite sort keys for hierarchical relationships
- Create GSIs only when necessary (they cost money)
- Avoid hot partitions by using high-cardinality partition keys
- Consider write patterns - write-heavy tables may need separation
- Use sparse indexes to reduce GSI size

For DynamoDB designs, consider:
- PK/SK patterns that enable efficient queries
- When to use GSIs vs table scans
- Projection types for GSIs (KEYS_ONLY saves cost)
- Item collection limits (10GB per partition key value)
- Warn about hot partition risks

Always respond with valid JSON in the specified format."""

        return base_prompt

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
