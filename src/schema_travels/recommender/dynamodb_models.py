"""Pydantic models for DynamoDB schema design (v2.0.0).

These models represent the output of DynamoDB schema analysis,
supporting both single-table and multi-table designs.
"""

from enum import Enum
from typing import Any, Optional

from pydantic import BaseModel, Field


class DesignMode(str, Enum):
    """DynamoDB design mode."""
    SINGLE_TABLE = "single_table"
    MULTI_TABLE = "multi_table"
    AUTO = "auto"


class ProjectionType(str, Enum):
    """GSI projection type - determines what attributes are copied to the index."""
    KEYS_ONLY = "KEYS_ONLY"      # Only key attributes (cheapest, smallest)
    INCLUDE = "INCLUDE"          # Keys + specified non-key attributes
    ALL = "ALL"                  # All attributes (most expensive, largest)


class AccessCluster(BaseModel):
    """
    A group of tables that are frequently accessed together.
    
    Access clusters are behavioral groupings based on query patterns,
    not foreign key relationships. Tables in the same cluster are
    candidates for single-table design.
    """
    cluster_id: str = Field(description="Unique identifier for this cluster")
    tables: set[str] = Field(description="Tables in this cluster")
    pk_table: str = Field(description="Table with highest solo_access_ratio - becomes PK owner")
    sk_tables: list[str] = Field(
        default_factory=list,
        description="Tables with high joined_accesses - become SK entities"
    )
    co_access_strength: float = Field(
        default=0.0,
        description="Average co-access ratio within cluster (0.0-1.0)"
    )
    total_accesses: int = Field(
        default=0,
        description="Total query accesses across all tables in cluster"
    )
    
    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary."""
        return {
            "cluster_id": self.cluster_id,
            "tables": list(self.tables),
            "pk_table": self.pk_table,
            "sk_tables": self.sk_tables,
            "co_access_strength": self.co_access_strength,
            "total_accesses": self.total_accesses,
        }


class EntityDefinition(BaseModel):
    """
    Defines how a source SQL table maps to a DynamoDB entity.
    
    In single-table design, multiple entities share the same table
    but use different PK/SK patterns to differentiate item types.
    """
    name: str = Field(description="Entity name (e.g., 'User', 'Order')")
    source_table: str = Field(description="Original SQL table name")
    pk_pattern: str = Field(
        description="Partition key pattern (e.g., 'USER#<id>', 'ORDER#<order_id>')"
    )
    sk_pattern: str = Field(
        description="Sort key pattern (e.g., 'PROFILE', 'ORDER#<id>', 'ITEM#<item_id>')"
    )
    pk_source_column: str = Field(
        default="id",
        description="Source column for PK value"
    )
    sk_source_column: Optional[str] = Field(
        default=None,
        description="Source column for SK value (if dynamic)"
    )
    attributes: list[str] = Field(
        default_factory=list,
        description="Non-key attributes to include"
    )
    
    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary."""
        return {
            "name": self.name,
            "source_table": self.source_table,
            "pk_pattern": self.pk_pattern,
            "sk_pattern": self.sk_pattern,
            "pk_source_column": self.pk_source_column,
            "sk_source_column": self.sk_source_column,
            "attributes": self.attributes,
        }


class GSIDefinition(BaseModel):
    """
    Global Secondary Index definition.
    
    GSIs enable additional access patterns beyond the base table's PK/SK.
    Projection type affects cost and query flexibility.
    """
    name: str = Field(description="Index name (e.g., 'GSI1', 'ByEmail')")
    pk_attribute: str = Field(description="Partition key attribute name")
    sk_attribute: Optional[str] = Field(
        default=None,
        description="Sort key attribute name (optional)"
    )
    pk_pattern: Optional[str] = Field(
        default=None,
        description="PK pattern if using overloaded keys"
    )
    sk_pattern: Optional[str] = Field(
        default=None,
        description="SK pattern if using overloaded keys"
    )
    projection_type: ProjectionType = Field(
        default=ProjectionType.ALL,
        description="What attributes to project into the index"
    )
    projected_attributes: list[str] = Field(
        default_factory=list,
        description="Attributes to project (only for INCLUDE projection)"
    )
    source_columns: list[str] = Field(
        default_factory=list,
        description="SQL columns that drive this GSI (from frequently_filtered_columns)"
    )
    access_pattern: str = Field(
        default="",
        description="Description of access pattern this GSI supports"
    )
    estimated_rcu_savings: Optional[float] = Field(
        default=None,
        description="Estimated RCU savings vs table scan"
    )
    
    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary."""
        return {
            "name": self.name,
            "pk_attribute": self.pk_attribute,
            "sk_attribute": self.sk_attribute,
            "pk_pattern": self.pk_pattern,
            "sk_pattern": self.sk_pattern,
            "projection_type": self.projection_type.value,
            "projected_attributes": self.projected_attributes,
            "source_columns": self.source_columns,
            "access_pattern": self.access_pattern,
            "estimated_rcu_savings": self.estimated_rcu_savings,
        }


class AccessPatternDefinition(BaseModel):
    """
    Documents an access pattern and how the design supports it.
    """
    name: str = Field(description="Pattern name (e.g., 'Get user by ID')")
    description: str = Field(description="What this pattern does")
    operation: str = Field(
        default="Query",
        description="DynamoDB operation: Query, GetItem, Scan"
    )
    table_or_index: str = Field(
        description="Which table or GSI to use"
    )
    pk_condition: str = Field(
        description="PK condition (e.g., 'PK = USER#<user_id>')"
    )
    sk_condition: Optional[str] = Field(
        default=None,
        description="SK condition if applicable"
    )
    filter_expression: Optional[str] = Field(
        default=None,
        description="Additional filter expression"
    )
    frequency: int = Field(
        default=0,
        description="How often this pattern occurs in query logs"
    )
    
    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary."""
        return {
            "name": self.name,
            "description": self.description,
            "operation": self.operation,
            "table_or_index": self.table_or_index,
            "pk_condition": self.pk_condition,
            "sk_condition": self.sk_condition,
            "filter_expression": self.filter_expression,
            "frequency": self.frequency,
        }


class TableDesign(BaseModel):
    """
    Design for a single DynamoDB table (used in multi-table mode).
    """
    table_name: str = Field(description="DynamoDB table name")
    source_table: str = Field(description="Original SQL table")
    partition_key: str = Field(description="PK attribute name")
    partition_key_type: str = Field(default="S", description="PK type: S, N, B")
    sort_key: Optional[str] = Field(default=None, description="SK attribute name")
    sort_key_type: Optional[str] = Field(default=None, description="SK type: S, N, B")
    gsis: list[GSIDefinition] = Field(default_factory=list)
    billing_mode: str = Field(
        default="PAY_PER_REQUEST",
        description="PAY_PER_REQUEST or PROVISIONED"
    )
    
    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary."""
        return {
            "table_name": self.table_name,
            "source_table": self.source_table,
            "partition_key": self.partition_key,
            "partition_key_type": self.partition_key_type,
            "sort_key": self.sort_key,
            "sort_key_type": self.sort_key_type,
            "gsis": [g.to_dict() for g in self.gsis],
            "billing_mode": self.billing_mode,
        }


class DynamoDBDesign(BaseModel):
    """
    Complete DynamoDB schema design output.
    
    Supports both single-table and multi-table designs.
    """
    # Metadata
    design_mode: DesignMode = Field(description="single_table or multi_table")
    confidence: float = Field(
        default=0.0,
        ge=0.0,
        le=1.0,
        description="Confidence in design (0.0-1.0)"
    )
    rationale: str = Field(
        default="",
        description="Explanation of why this design was chosen"
    )
    
    # Single-table design fields
    table_name: Optional[str] = Field(
        default=None,
        description="Table name (single-table mode)"
    )
    partition_key: str = Field(
        default="PK",
        description="Partition key attribute name"
    )
    partition_key_type: str = Field(
        default="S",
        description="PK type: S (string), N (number), B (binary)"
    )
    sort_key: Optional[str] = Field(
        default="SK",
        description="Sort key attribute name"
    )
    sort_key_type: Optional[str] = Field(
        default="S",
        description="SK type: S, N, B"
    )
    entities: list[EntityDefinition] = Field(
        default_factory=list,
        description="Entity definitions (single-table mode)"
    )
    gsis: list[GSIDefinition] = Field(
        default_factory=list,
        description="Global Secondary Indexes"
    )
    
    # Multi-table design fields
    tables: list[TableDesign] = Field(
        default_factory=list,
        description="Individual table designs (multi-table mode)"
    )
    
    # Access patterns
    access_patterns: list[AccessPatternDefinition] = Field(
        default_factory=list,
        description="Documented access patterns"
    )
    
    # Analysis metadata
    clusters: list[AccessCluster] = Field(
        default_factory=list,
        description="Access clusters identified during analysis"
    )
    orphan_tables: list[str] = Field(
        default_factory=list,
        description="Tables without strong co-access (separate tables)"
    )
    
    # Warnings and recommendations
    warnings: list[str] = Field(
        default_factory=list,
        description="Potential issues with the design"
    )
    recommendations: list[str] = Field(
        default_factory=list,
        description="Additional optimization suggestions"
    )
    
    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        result = {
            "design_mode": self.design_mode.value,
            "confidence": self.confidence,
            "rationale": self.rationale,
            "access_patterns": [ap.to_dict() for ap in self.access_patterns],
            "clusters": [c.to_dict() for c in self.clusters],
            "orphan_tables": self.orphan_tables,
            "warnings": self.warnings,
            "recommendations": self.recommendations,
        }
        
        if self.design_mode == DesignMode.SINGLE_TABLE:
            result.update({
                "table_name": self.table_name,
                "partition_key": self.partition_key,
                "partition_key_type": self.partition_key_type,
                "sort_key": self.sort_key,
                "sort_key_type": self.sort_key_type,
                "entities": [e.to_dict() for e in self.entities],
                "gsis": [g.to_dict() for g in self.gsis],
            })
        else:
            result["tables"] = [t.to_dict() for t in self.tables]
        
        return result


class DesignDecision(BaseModel):
    """
    Represents a design decision made during analysis.
    
    Used for transparency and debugging - shows why specific
    choices were made.
    """
    decision_type: str = Field(description="Type: mode_selection, pk_assignment, gsi_creation, etc.")
    choice: str = Field(description="What was decided")
    rationale: str = Field(description="Why this choice was made")
    confidence: float = Field(default=1.0, ge=0.0, le=1.0)
    alternatives_considered: list[str] = Field(default_factory=list)
    data_points: dict[str, Any] = Field(
        default_factory=dict,
        description="Supporting data for the decision"
    )
    
    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary."""
        return {
            "decision_type": self.decision_type,
            "choice": self.choice,
            "rationale": self.rationale,
            "confidence": self.confidence,
            "alternatives_considered": self.alternatives_considered,
            "data_points": self.data_points,
        }
