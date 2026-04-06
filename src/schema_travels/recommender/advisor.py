"""
Schema Advisor - AI-powered schema recommendations.

This module provides AI recommendations for schema migrations using
any supported LLM provider (Claude, OpenAI, Gemini, Grok, Ollama).

v2.3.0: Refactored from ClaudeAdvisor to support multiple providers.

Usage:
    from schema_travels.recommender import Advisor
    from schema_travels.llm import get_provider
    
    # Use default provider (Claude)
    advisor = Advisor()
    
    # Use specific provider
    provider = get_provider("openai", model="gpt-4o")
    advisor = Advisor(provider=provider)
    
    # Or configure inline
    advisor = Advisor(provider_name="ollama", model="llama3.1:70b")
    
    # Get recommendations
    recommendations = advisor.get_recommendations(schema, analysis, target)
"""

import json
import logging
from typing import Any

from schema_travels.llm import (
    LLMProvider,
    LLMResponse,
    get_provider,
    LLMProviderError,
)
from schema_travels.collector.models import SchemaDefinition
from schema_travels.analyzer.models import AnalysisResult
from schema_travels.recommender.models import (
    SchemaRecommendation,
    RelationshipDecision,
    TargetDatabase,
)
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


class Advisor:
    """
    AI-powered schema advisor supporting multiple LLM providers.
    
    This is a provider-agnostic replacement for the original ClaudeAdvisor.
    It uses the LLMProvider protocol to work with any supported backend:
    - Claude (Anthropic)
    - GPT-4 (OpenAI)
    - Gemini (Google)
    - Grok (xAI)
    - Ollama (Local models)
    """
    
    def __init__(
        self,
        provider: LLMProvider | None = None,
        provider_name: str | None = None,
        model: str | None = None,
        **provider_kwargs: Any,
    ):
        """
        Initialize the advisor.
        
        Args:
            provider: Pre-configured LLMProvider instance
            provider_name: Provider name if not passing provider instance
                          ('claude', 'openai', 'gemini', 'grok', 'ollama')
            model: Model to use (if not passing provider instance)
            **provider_kwargs: Additional arguments for provider initialization
                              (e.g., api_key, host for ollama)
            
        Examples:
            # Use default (Claude)
            advisor = Advisor()
            
            # Use pre-configured provider
            provider = get_provider("openai", model="gpt-4o")
            advisor = Advisor(provider=provider)
            
            # Configure inline
            advisor = Advisor(provider_name="ollama", model="llama3.1:70b")
            
            # With custom Ollama host
            advisor = Advisor(
                provider_name="ollama", 
                model="mistral:7b",
                host="http://192.168.1.100:11434"
            )
        """
        if provider is not None:
            self._provider = provider
        else:
            self._provider = get_provider(
                provider=provider_name,
                model=model,
                **provider_kwargs,
            )
        
        logger.info(f"Advisor initialized with {self._provider.name} ({self._provider.model})")
    
    @property
    def provider(self) -> LLMProvider:
        """Get the current LLM provider."""
        return self._provider
    
    @property
    def provider_name(self) -> str:
        """Get the provider name."""
        return self._provider.name
    
    @property
    def model(self) -> str:
        """Get the model name."""
        return self._provider.model
    
    # =========================================================================
    # MongoDB Recommendations
    # =========================================================================
    
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
            schema: Source SQL schema
            analysis: Query pattern analysis results
            target: Target database type (MONGODB or DYNAMODB)
            additional_context: Additional context about the application
            
        Returns:
            List of schema recommendations (embed/reference decisions)
        """
        prompt = self._build_prompt(schema, analysis, target, additional_context)
        system = self._get_system_prompt(target)
        
        try:
            response = self._provider.complete(
                prompt=prompt,
                system=system,
                json_mode=True,
                temperature=0.0,
            )
            
            logger.debug(f"Got response from {self._provider.name}: {len(response.content)} chars")
            
            return self._parse_recommendations(response.content)
            
        except LLMProviderError as e:
            logger.error(f"LLM provider error: {e}")
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
            response = self._provider.complete(
                prompt=prompt,
                system=self._get_system_prompt(target),
                json_mode=True,
                temperature=0.0,
            )

            recommendations = self._parse_recommendations(response.content)

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

        except LLMProviderError as e:
            logger.error(f"LLM provider error: {e}")
            return SchemaRecommendation(
                parent_table=parent_table,
                child_table=child_table,
                decision=RelationshipDecision.REFERENCE,
                confidence=0.3,
                reasoning=[f"AI analysis failed: {str(e)}"],
                warnings=["Manual review required"],
            )
    
    # =========================================================================
    # DynamoDB Review
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
        reviewed by AI for edge cases and optimizations.
        
        Args:
            design: DynamoDB design from DynamoDBDesigner
            analysis: Analysis result with access patterns
            schema: Source schema definition
            
        Returns:
            DynamoDBReview with approval status and suggested changes
        """
        prompt = self._build_dynamodb_review_prompt(design, analysis, schema)
        system = self._get_dynamodb_review_system_prompt()
        
        try:
            response = self._provider.complete(
                prompt=prompt,
                system=system,
                json_mode=True,
                temperature=0.0,
            )
            
            logger.debug(f"Got DynamoDB review from {self._provider.name}")
            
            return self._parse_dynamodb_review(response.content)
            
        except LLMProviderError as e:
            logger.error(f"DynamoDB review failed: {e}")
            # Return approval with low confidence on error
            return DynamoDBReview(
                approved=True,
                confidence=0.5,
                summary=f"AI review failed ({e.message}). Design approved by default.",
                warnings=["AI review failed - manual review recommended"],
            )
    
    # =========================================================================
    # Prompt Building - MongoDB
    # =========================================================================
    
    def _build_prompt(
        self,
        schema: SchemaDefinition,
        analysis: AnalysisResult,
        target: TargetDatabase,
        additional_context: str | None = None,
    ) -> str:
        """Build the main recommendation prompt."""
        schema_summary = self._summarize_schema(schema)
        analysis_summary = self._summarize_analysis(analysis)
        
        if target == TargetDatabase.DYNAMODB:
            return self._build_dynamodb_prompt(
                schema_summary, analysis_summary, additional_context
            )
        
        return f"""Analyze this SQL schema and query patterns for MongoDB migration.

## Source Schema
{schema_summary}

## Access Pattern Analysis
{analysis_summary}

## Additional Context
{additional_context or "No additional context provided."}

## Task
For each parent-child relationship, recommend whether to EMBED or REFERENCE.

Consider:
- Co-access ratio (>70% suggests embedding)
- Write patterns (high child writes suggest referencing)
- Cardinality (1:few vs 1:many)
- Document size limits (16MB for MongoDB)

Respond with JSON:
```json
{{
  "recommendations": [
    {{
      "parent_table": "string",
      "child_table": "string",
      "decision": "EMBED" | "REFERENCE",
      "confidence": 0.0-1.0,
      "reasoning": ["reason 1", "reason 2"],
      "warnings": ["warning if any"]
    }}
  ]
}}
```"""
    
    def _build_dynamodb_prompt(
        self,
        schema_summary: str,
        analysis_summary: str,
        additional_context: str | None = None,
    ) -> str:
        """Build prompt for DynamoDB recommendations."""
        return f"""Analyze this SQL schema and query patterns for DynamoDB migration.

## Source Schema
{schema_summary}

## Access Pattern Analysis
{analysis_summary}

## Additional Context
{additional_context or "No additional context provided."}

## Task
Design a DynamoDB single-table schema based on access patterns.

Consider:
- Single-table vs multi-table design
- PK/SK patterns for each entity
- GSIs needed for access patterns
- Hot partition risks

Respond with JSON:
```json
{{
  "design_mode": "single_table" | "multi_table",
  "recommendations": [
    {{
      "parent_table": "string",
      "child_table": "string",
      "decision": "EMBED" | "REFERENCE" | "SEPARATE" | "BUCKET",
      "confidence": 0.0-1.0,
      "reasoning": ["reason 1", "reason 2"],
      "warnings": ["warning if any"],
      "pk_pattern": "ENTITY#<id>",
      "sk_pattern": "METADATA#<id>"
    }}
  ],
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
        metrics = self._get_relationship_metrics(parent_table, child_table, analysis)
        
        parent_def = schema.get_table(parent_table)
        child_def = schema.get_table(child_table)

        return f"""Analyze this specific relationship for {target.value} migration.

## Parent Table: {parent_table}
Columns: {[c.name + ' ' + c.data_type for c in parent_def.columns] if parent_def else 'Unknown'}

## Child Table: {child_table}
Columns: {[c.name + ' ' + c.data_type for c in child_def.columns] if child_def else 'Unknown'}

## Access Metrics
{json.dumps(metrics, indent=2)}

## Task
Recommend whether to EMBED or REFERENCE this relationship.

Respond with JSON:
```json
{{
  "recommendations": [
    {{
      "parent_table": "{parent_table}",
      "child_table": "{child_table}",
      "decision": "EMBED" | "REFERENCE",
      "confidence": 0.0-1.0,
      "reasoning": ["detailed reason 1", "detailed reason 2"],
      "warnings": ["any warnings"]
    }}
  ]
}}
```"""
    
    def _get_system_prompt(self, target: TargetDatabase) -> str:
        """Get the system prompt for recommendations."""
        return f"""You are a database migration expert specializing in SQL to {target.value} migrations.

Your task is to analyze query patterns and recommend optimal schema designs.

Key principles for MongoDB:
- EMBED when: high co-access (>70%), bounded cardinality (<100), low child writes
- REFERENCE when: unbounded growth, independent access, high child writes, shared data

Key principles for DynamoDB:
- Single-table design when: entities frequently accessed together
- Use composite keys (PK + SK) for hierarchical relationships
- GSIs for alternative access patterns (max 5 per table)
- Avoid hot partitions with high-cardinality partition keys

Always provide confidence scores and clear reasoning.
Respond with valid JSON only. No markdown code blocks."""
    
    # =========================================================================
    # Prompt Building - DynamoDB Review
    # =========================================================================
    
    def _build_dynamodb_review_prompt(
        self,
        design: DynamoDBDesign,
        analysis: AnalysisResult,
        schema: SchemaDefinition,
    ) -> str:
        """Build prompt for DynamoDB design review."""
        design_summary = self._format_dynamodb_design(design)
        patterns_summary = self._format_access_patterns(analysis)
        schema_summary = self._summarize_schema(schema)
        
        return f"""Review this DynamoDB single-table design and suggest improvements.

## Proposed Design
{design_summary}

## Access Patterns (from query logs)
{patterns_summary}

## Source SQL Schema
{schema_summary}

## Review Tasks
1. Validate PK/SK patterns for each entity
2. Check GSI coverage for access patterns
3. Identify hot partition risks
4. Suggest missing GSIs (max 5 total)
5. Flag any anti-patterns

Respond with JSON:
```json
{{
  "approved": true | false,
  "confidence": 0.0-1.0,
  "summary": "Brief assessment",
  "design_mode_change": null | "single_table" | "multi_table",
  "entity_changes": [
    {{
      "entity": "string",
      "change_type": "MODIFY_PK" | "MODIFY_SK" | "ADD_ATTRIBUTE" | "REMOVE_ATTRIBUTE",
      "current_value": "string or null",
      "new_value": "string or null",
      "reason": "string"
    }}
  ],
  "gsi_changes": [
    {{
      "action": "ADD" | "REMOVE" | "MODIFY",
      "gsi_name": "string",
      "pk_attribute": "string or null",
      "sk_attribute": "string or null",
      "projection_type": "KEYS_ONLY" | "INCLUDE" | "ALL" | null,
      "reason": "string"
    }}
  ],
  "warnings": ["string"],
  "suggestions": ["string"],
  "uncovered_patterns": ["string"]
}}
```"""
    
    def _get_dynamodb_review_system_prompt(self) -> str:
        """Get system prompt for DynamoDB review."""
        return """You are a DynamoDB single-table design expert.

Your task is to review and improve DynamoDB designs.

Key principles:
- Partition keys should distribute load evenly (high cardinality)
- Sort keys enable efficient range queries
- GSIs should cover frequent access patterns (max 5 per table)
- Avoid hot partitions
- Consider item collections and size limits

Review criteria:
- PK/SK patterns follow best practices
- GSIs cover the main access patterns
- No obvious hot partition risks
- Entity relationships are correctly modeled

Respond with valid JSON only. No markdown code blocks."""
    
    # =========================================================================
    # Formatting Helpers
    # =========================================================================
    
    def _summarize_schema(self, schema: SchemaDefinition) -> str:
        """Create a summary of the schema for the prompt."""
        lines = []

        for table in schema.tables:
            cols = ", ".join([f"{c.name} ({c.data_type})" for c in table.columns[:10]])
            if len(table.columns) > 10:
                cols += f", ... ({len(table.columns) - 10} more)"
            lines.append(f"- {table.name}: {cols}")

        if schema.foreign_keys:
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
            "Top Hot Joins (by frequency):",
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
    
    def _format_dynamodb_design(self, design: DynamoDBDesign) -> str:
        """Format DynamoDB design for prompt."""
        lines = [
            f"Design Mode: {design.design_mode.value}",
            f"Table: {design.table_name}",
            f"PK: {design.partition_key}, SK: {design.sort_key}",
            "",
            "Entities:",
        ]
        
        for entity in design.entities:
            lines.append(f"  - {entity.name}: PK={entity.pk_pattern}, SK={entity.sk_pattern}")
            if entity.attributes:
                attrs = ", ".join(entity.attributes[:5])
                if len(entity.attributes) > 5:
                    attrs += f", ... (+{len(entity.attributes) - 5} more)"
                lines.append(f"    Attributes: {attrs}")
        
        if design.gsis:
            lines.append("\nGSIs:")
            for gsi in design.gsis:
                sk_info = f", SK={gsi.sk_attribute}" if gsi.sk_attribute else ""
                lines.append(f"  - {gsi.name}: PK={gsi.pk_attribute}{sk_info}")
                if gsi.purpose:
                    lines.append(f"    Purpose: {gsi.purpose}")
        
        return "\n".join(lines)
    
    def _format_access_patterns(self, analysis: AnalysisResult) -> str:
        """Format access patterns for DynamoDB review."""
        lines = []
        
        # Table access frequencies
        for mp in sorted(
            analysis.mutation_patterns,
            key=lambda m: m.total_operations,
            reverse=True,
        )[:15]:
            lines.append(
                f"- {mp.table}: {mp.total_operations} ops, "
                f"read={100 - int(mp.write_ratio * 100)}%, write={int(mp.write_ratio * 100)}%"
            )
        
        return "\n".join(lines) if lines else "No access patterns available"
    
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
    
    # =========================================================================
    # Response Parsing
    # =========================================================================
    
    def _parse_recommendations(self, content: str) -> list[SchemaRecommendation]:
        """Parse AI response into SchemaRecommendation objects."""
        recommendations = []
        
        # Clean up response - remove markdown code blocks
        content = content.strip()
        if content.startswith("```"):
            lines = content.split("\n")
            # Remove first line (```json) and last line (```)
            content = "\n".join(lines[1:-1] if lines[-1] == "```" else lines[1:])
        content = content.strip()

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
                try:
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
                except (KeyError, ValueError) as e:
                    logger.warning(f"Skipping invalid recommendation: {e}")
                    continue

        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse JSON response: {e}")
            logger.debug(f"Response content: {content[:500]}")
        except Exception as e:
            logger.error(f"Error parsing recommendations: {e}")

        return recommendations
    
    def _parse_dynamodb_review(self, content: str) -> DynamoDBReview:
        """Parse AI response into DynamoDBReview."""
        
        # Clean up response - remove markdown code blocks
        content = content.strip()
        if content.startswith("```"):
            lines = content.split("\n")
            content = "\n".join(lines[1:-1] if lines[-1] == "```" else lines[1:])
        content = content.strip()
        
        # Extract JSON
        json_start = content.find("{")
        json_end = content.rfind("}") + 1
        
        if json_start != -1 and json_end > json_start:
            content = content[json_start:json_end]
        
        try:
            data = json.loads(content)
        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse DynamoDB review: {e}")
            logger.debug(f"Response content: {content[:500]}")
            return DynamoDBReview(
                approved=True,
                confidence=0.5,
                summary="Failed to parse AI response",
                warnings=["JSON parse error - manual review recommended"],
            )
        
        # Parse entity changes
        entity_changes = []
        for ec in data.get("entity_changes", []):
            try:
                entity_changes.append(EntityChange(
                    entity=ec.get("entity", ""),
                    change_type=ReviewChangeType[ec.get("change_type", "MODIFY_SK")],
                    current_value=ec.get("current_value"),
                    new_value=ec.get("new_value"),
                    reason=ec.get("reason", ""),
                ))
            except (KeyError, ValueError) as e:
                logger.warning(f"Skipping invalid entity change: {e}")
        
        # Parse GSI changes
        gsi_changes = []
        for gc in data.get("gsi_changes", []):
            try:
                projection = None
                if gc.get("projection_type"):
                    projection = ProjectionType[gc["projection_type"]]
                
                gsi_changes.append(GSIChange(
                    action=GSIChangeAction[gc.get("action", "ADD")],
                    gsi_name=gc.get("gsi_name", ""),
                    pk_attribute=gc.get("pk_attribute"),
                    sk_attribute=gc.get("sk_attribute"),
                    projection_type=projection,
                    reason=gc.get("reason", ""),
                ))
            except (KeyError, ValueError) as e:
                logger.warning(f"Skipping invalid GSI change: {e}")
        
        # Parse design mode change
        design_mode_change = None
        if data.get("design_mode_change"):
            try:
                design_mode_change = DesignMode(data["design_mode_change"])
            except ValueError:
                pass
        
        return DynamoDBReview(
            approved=data.get("approved", True),
            confidence=float(data.get("confidence", 0.5)),
            summary=data.get("summary", ""),
            design_mode_change=design_mode_change,
            entity_changes=entity_changes,
            gsi_changes=gsi_changes,
            warnings=data.get("warnings", []),
            suggestions=data.get("suggestions", []),
            uncovered_patterns=data.get("uncovered_patterns", []),
        )
