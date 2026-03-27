"""DynamoDB schema designer (v2.0.0).

Core logic for transforming SQL query patterns into DynamoDB schema designs.
Supports both single-table and multi-table designs based on access patterns.

Key concepts:
- Access Clusters: Tables frequently accessed together (behavioral grouping)
- PK/SK Assignment: Based on solo vs joined access ratios
- GSI Detection: From frequently_filtered_columns not in PK
"""

from collections import defaultdict
from typing import Optional

from schema_travels.analyzer.models import (
    AccessPattern,
    TableStatistics,
    JoinPattern,
    MutationPattern,
)
from schema_travels.recommender.dynamodb_models import (
    AccessCluster,
    DesignMode,
    DynamoDBDesign,
    EntityDefinition,
    GSIDefinition,
    ProjectionType,
    TableDesign,
    AccessPatternDefinition,
    DesignDecision,
)


# =============================================================================
# Union-Find for Access Clustering
# =============================================================================

class UnionFind:
    """Union-Find data structure for clustering tables by co-access."""
    
    def __init__(self, items: list[str]):
        """Initialize with list of items."""
        self.parent = {item: item for item in items}
        self.rank = {item: 0 for item in items}
    
    def find(self, x: str) -> str:
        """Find root with path compression."""
        if self.parent[x] != x:
            self.parent[x] = self.find(self.parent[x])
        return self.parent[x]
    
    def union(self, x: str, y: str) -> bool:
        """Union by rank. Returns True if union performed."""
        px, py = self.find(x), self.find(y)
        if px == py:
            return False
        
        if self.rank[px] < self.rank[py]:
            px, py = py, px
        self.parent[py] = px
        if self.rank[px] == self.rank[py]:
            self.rank[px] += 1
        return True
    
    def get_clusters(self) -> dict[str, set[str]]:
        """Get all clusters as {root: {members}}."""
        clusters: dict[str, set[str]] = defaultdict(set)
        for item in self.parent:
            root = self.find(item)
            clusters[root].add(item)
        return dict(clusters)


# =============================================================================
# DynamoDB Designer
# =============================================================================

class DynamoDBDesigner:
    """
    Designs DynamoDB schemas from SQL query pattern analysis.
    
    Workflow:
    1. Build access clusters from co-access patterns
    2. Decide single-table vs multi-table based on cluster strength
    3. Assign PK/SK patterns based on access ratios
    4. Detect GSIs from filter patterns
    5. Generate complete design
    """
    
    # Thresholds (tunable)
    CO_ACCESS_THRESHOLD = 0.70      # Min co-access ratio to cluster tables
    HOT_JOIN_THRESHOLD = 3          # Min hot join pairs for single-table
    SINGLE_TABLE_CONFIDENCE = 0.70  # Min avg co-access for single-table
    GSI_FREQUENCY_THRESHOLD = 5     # Min filter frequency to create GSI
    MAX_GSIS = 5                    # DynamoDB limit is 20, but 5 is practical
    
    def __init__(
        self,
        mode: DesignMode = DesignMode.AUTO,
        co_access_threshold: float = 0.70,
    ):
        """
        Initialize designer.
        
        Args:
            mode: Design mode - SINGLE_TABLE, MULTI_TABLE, or AUTO
            co_access_threshold: Minimum co-access ratio to cluster tables
        """
        self.mode = mode
        self.co_access_threshold = co_access_threshold
        self.decisions: list[DesignDecision] = []
    
    def design(
        self,
        table_stats: list[TableStatistics],
        access_patterns: list[AccessPattern],
        join_patterns: list[JoinPattern],
        mutation_patterns: list[MutationPattern],
        filtered_columns: dict[str, dict[str, int]],
        selected_columns: dict[str, dict[str, int]],
        select_star_tables: set[str],
    ) -> DynamoDBDesign:
        """
        Generate DynamoDB design from analysis results.
        
        Args:
            table_stats: Per-table statistics
            access_patterns: Co-access patterns between tables
            join_patterns: JOIN patterns from queries
            mutation_patterns: Read/write patterns per table
            filtered_columns: {table: {column: count}} from WHERE clauses
            selected_columns: {table: {column: count}} from SELECT clauses
            select_star_tables: Tables with SELECT * usage
            
        Returns:
            Complete DynamoDB design
        """
        self.decisions = []
        
        # Build lookup maps
        stats_map = {s.table: s for s in table_stats}
        mutation_map = {m.table: m for m in mutation_patterns}
        
        # Step 1: Build access clusters
        clusters = self._build_access_clusters(
            tables=list(stats_map.keys()),
            access_patterns=access_patterns,
            stats_map=stats_map,
        )
        
        # Step 2: Decide design mode
        design_mode = self._decide_design_mode(
            clusters=clusters,
            join_patterns=join_patterns,
            mutation_map=mutation_map,
        )
        
        # Step 3: Generate design based on mode
        if design_mode == DesignMode.SINGLE_TABLE:
            return self._generate_single_table_design(
                clusters=clusters,
                stats_map=stats_map,
                mutation_map=mutation_map,
                filtered_columns=filtered_columns,
                selected_columns=selected_columns,
                select_star_tables=select_star_tables,
            )
        else:
            return self._generate_multi_table_design(
                clusters=clusters,
                stats_map=stats_map,
                filtered_columns=filtered_columns,
                selected_columns=selected_columns,
                select_star_tables=select_star_tables,
            )
    
    # =========================================================================
    # Step 1: Access Clustering
    # =========================================================================
    
    def _build_access_clusters(
        self,
        tables: list[str],
        access_patterns: list[AccessPattern],
        stats_map: dict[str, TableStatistics],
    ) -> list[AccessCluster]:
        """
        Build access clusters using Union-Find on co-access patterns.
        
        Tables are clustered if their co_access_ratio exceeds threshold.
        """
        if not tables:
            return []
        
        # Initialize Union-Find
        uf = UnionFind(tables)
        
        # Union tables with high co-access
        for ap in access_patterns:
            if ap.co_access_ratio >= self.co_access_threshold:
                if ap.table_a in tables and ap.table_b in tables:
                    uf.union(ap.table_a, ap.table_b)
        
        # Build cluster objects
        raw_clusters = uf.get_clusters()
        clusters = []
        
        # Sort by root name for deterministic cluster_id assignment
        sorted_roots = sorted(raw_clusters.keys())
        
        for cluster_id, root in enumerate(sorted_roots):
            members = raw_clusters[root]
            # Convert to sorted list for deterministic iteration
            members_list = sorted(members)
            
            # Find PK table (highest solo_access_ratio)
            pk_table = max(
                members_list,
                key=lambda t: stats_map.get(t, TableStatistics(table=t)).solo_ratio
            )
            
            # SK tables are others with significant joined accesses (sorted)
            sk_tables = sorted([
                t for t in members_list
                if t != pk_table and stats_map.get(t, TableStatistics(table=t)).joined_accesses > 0
            ])
            
            # Calculate average co-access strength within cluster
            cluster_co_access = []
            for ap in access_patterns:
                if ap.table_a in members and ap.table_b in members:
                    cluster_co_access.append(ap.co_access_ratio)
            
            avg_strength = sum(cluster_co_access) / len(cluster_co_access) if cluster_co_access else 0.0
            
            # Total accesses in cluster
            total_accesses = sum(
                stats_map.get(t, TableStatistics(table=t)).total_accesses
                for t in members_list
            )
            
            clusters.append(AccessCluster(
                cluster_id=f"cluster_{cluster_id}",
                tables=members_list,  # Use sorted list for deterministic order
                pk_table=pk_table,
                sk_tables=sk_tables,
                co_access_strength=avg_strength,
                total_accesses=total_accesses,
            ))
        
        # Sort by total accesses (most active first)
        clusters.sort(key=lambda c: c.total_accesses, reverse=True)
        
        self.decisions.append(DesignDecision(
            decision_type="clustering",
            choice=f"Created {len(clusters)} access clusters",
            rationale=f"Used co_access_threshold={self.co_access_threshold}",
            data_points={
                "cluster_sizes": [len(c.tables) for c in clusters],
                "co_access_strengths": [c.co_access_strength for c in clusters],
            }
        ))
        
        return clusters
    
    # =========================================================================
    # Step 2: Mode Decision
    # =========================================================================
    
    def _decide_design_mode(
        self,
        clusters: list[AccessCluster],
        join_patterns: list[JoinPattern],
        mutation_map: dict[str, MutationPattern],
    ) -> DesignMode:
        """
        Decide between single-table and multi-table design.
        
        Heuristics:
        - Single-table if ≥3 hot join pairs with >70% co-access
        - Multi-table if write-heavy children or low co-access
        - AI tie-breaker for gray zones (deferred to ClaudeAdvisor)
        """
        if self.mode != DesignMode.AUTO:
            self.decisions.append(DesignDecision(
                decision_type="mode_selection",
                choice=self.mode.value,
                rationale="Explicitly specified by user",
            ))
            return self.mode
        
        # Count hot join pairs
        hot_joins = [jp for jp in join_patterns if jp.frequency >= 10]
        
        # Check for strong clusters
        strong_clusters = [c for c in clusters if c.co_access_strength >= self.SINGLE_TABLE_CONFIDENCE]
        
        # Check for write-heavy tables that would complicate embedding
        write_heavy_tables = [
            table for table, mp in mutation_map.items()
            if mp.write_ratio > 0.5
        ]
        
        # Decision logic
        reasons = []
        
        # Favor single-table
        if len(hot_joins) >= self.HOT_JOIN_THRESHOLD:
            reasons.append(f"{len(hot_joins)} hot join pairs detected")
        
        if len(strong_clusters) > 0 and any(len(c.tables) >= 3 for c in strong_clusters):
            reasons.append(f"Strong cluster with {max(len(c.tables) for c in strong_clusters)} tables")
        
        # Favor multi-table
        contra_reasons = []
        if len(write_heavy_tables) > len(mutation_map) * 0.5:
            contra_reasons.append(f"{len(write_heavy_tables)} write-heavy tables would complicate single-table")
        
        if all(c.co_access_strength < 0.5 for c in clusters):
            contra_reasons.append("No strong co-access patterns found")
        
        # Make decision
        if len(reasons) >= 2 and len(contra_reasons) == 0:
            mode = DesignMode.SINGLE_TABLE
            rationale = "Strong co-access patterns: " + "; ".join(reasons)
        elif len(contra_reasons) >= 1:
            mode = DesignMode.MULTI_TABLE
            rationale = "Multi-table preferred: " + "; ".join(contra_reasons)
        elif len(reasons) >= 1:
            # Weak signal for single-table
            mode = DesignMode.SINGLE_TABLE
            rationale = "Moderate co-access: " + "; ".join(reasons)
        else:
            # Default to multi-table (safer)
            mode = DesignMode.MULTI_TABLE
            rationale = "No strong patterns; defaulting to multi-table for simplicity"
        
        self.decisions.append(DesignDecision(
            decision_type="mode_selection",
            choice=mode.value,
            rationale=rationale,
            data_points={
                "hot_joins": len(hot_joins),
                "strong_clusters": len(strong_clusters),
                "write_heavy_tables": write_heavy_tables,
            }
        ))
        
        return mode
    
    # =========================================================================
    # Step 3a: Single-Table Design
    # =========================================================================
    
    def _generate_single_table_design(
        self,
        clusters: list[AccessCluster],
        stats_map: dict[str, TableStatistics],
        mutation_map: dict[str, MutationPattern],
        filtered_columns: dict[str, dict[str, int]],
        selected_columns: dict[str, dict[str, int]],
        select_star_tables: set[str],
    ) -> DynamoDBDesign:
        """Generate single-table design from clusters."""
        
        # Use the largest/most active cluster
        primary_cluster = clusters[0] if clusters else None
        
        if not primary_cluster:
            # Fallback: no clusters, create minimal design
            return DynamoDBDesign(
                design_mode=DesignMode.SINGLE_TABLE,
                table_name="main_table",
                confidence=0.3,
                rationale="No access patterns detected; minimal single-table design",
                warnings=["No query patterns analyzed - design may not be optimal"],
            )
        
        # Determine table name from PK table
        table_name = self._generate_table_name(primary_cluster)
        
        # Generate entities
        entities = self._generate_entities(primary_cluster, stats_map)
        
        # Detect GSIs
        gsis = self._detect_gsis(
            tables=list(primary_cluster.tables),
            filtered_columns=filtered_columns,
            selected_columns=selected_columns,
            select_star_tables=select_star_tables,
        )
        
        # Generate access patterns documentation
        access_patterns = self._document_access_patterns(
            entities=entities,
            gsis=gsis,
            stats_map=stats_map,
        )
        
        # Identify orphan tables (not in primary cluster) - sorted for deterministic order
        all_tables = set(stats_map.keys())
        clustered_tables = set(primary_cluster.tables)
        orphan_tables = sorted(all_tables - clustered_tables)
        
        # Generate warnings
        warnings = self._generate_warnings(
            primary_cluster, mutation_map, orphan_tables
        )
        
        # Calculate confidence
        confidence = min(primary_cluster.co_access_strength + 0.2, 1.0)
        
        return DynamoDBDesign(
            design_mode=DesignMode.SINGLE_TABLE,
            table_name=table_name,
            partition_key="PK",
            partition_key_type="S",
            sort_key="SK",
            sort_key_type="S",
            entities=entities,
            gsis=gsis,
            access_patterns=access_patterns,
            clusters=[primary_cluster],
            orphan_tables=orphan_tables,
            warnings=warnings,
            confidence=confidence,
            rationale=f"Single-table design based on access cluster with {len(primary_cluster.tables)} tables, "
                      f"co-access strength {primary_cluster.co_access_strength:.2f}",
        )
    
    def _generate_table_name(self, cluster: AccessCluster) -> str:
        """Generate a meaningful table name from cluster."""
        # Use PK table as base, or combine if multiple
        if len(cluster.tables) == 1:
            return f"{cluster.pk_table}_table"
        
        # For multi-entity tables, use a domain name
        tables = sorted(cluster.tables)
        if len(tables) <= 3:
            return "_".join(tables)
        
        return f"{cluster.pk_table}_and_related"
    
    def _generate_entities(
        self,
        cluster: AccessCluster,
        stats_map: dict[str, TableStatistics],
    ) -> list[EntityDefinition]:
        """Generate entity definitions for single-table design."""
        entities = []
        
        # PK table becomes the root entity
        pk_table = cluster.pk_table
        pk_entity_name = self._to_entity_name(pk_table)
        
        entities.append(EntityDefinition(
            name=pk_entity_name,
            source_table=pk_table,
            pk_pattern=f"{pk_entity_name.upper()}#<id>",
            sk_pattern=f"{pk_entity_name.upper()}",
            pk_source_column="id",
            attributes=stats_map.get(pk_table, TableStatistics(table=pk_table)).frequently_selected_columns[:10],
        ))
        
        # SK tables become child entities
        for sk_table in cluster.sk_tables:
            sk_entity_name = self._to_entity_name(sk_table)
            
            # Child entities use parent's PK and their own ID in SK
            entities.append(EntityDefinition(
                name=sk_entity_name,
                source_table=sk_table,
                pk_pattern=f"{pk_entity_name.upper()}#<{pk_table}_id>",
                sk_pattern=f"{sk_entity_name.upper()}#<id>",
                pk_source_column=f"{pk_table}_id",
                sk_source_column="id",
                attributes=stats_map.get(sk_table, TableStatistics(table=sk_table)).frequently_selected_columns[:10],
            ))
        
        # Tables in cluster but not PK or SK (sorted for deterministic order)
        remaining = set(cluster.tables) - {pk_table} - set(cluster.sk_tables)
        for table in sorted(remaining):
            entity_name = self._to_entity_name(table)
            entities.append(EntityDefinition(
                name=entity_name,
                source_table=table,
                pk_pattern=f"{pk_entity_name.upper()}#<{pk_table}_id>",
                sk_pattern=f"{entity_name.upper()}#<id>",
                pk_source_column=f"{pk_table}_id",
                sk_source_column="id",
                attributes=stats_map.get(table, TableStatistics(table=table)).frequently_selected_columns[:10],
            ))
        
        self.decisions.append(DesignDecision(
            decision_type="entity_assignment",
            choice=f"PK entity: {pk_entity_name}, SK entities: {len(entities) - 1}",
            rationale=f"Based on solo_ratio: {pk_table} has highest independent access",
            data_points={
                "pk_table": pk_table,
                "sk_tables": cluster.sk_tables,
            }
        ))
        
        return entities
    
    def _to_entity_name(self, table_name: str) -> str:
        """Convert table name to entity name (PascalCase, singular)."""
        # Simple singularization
        name = table_name.rstrip("s") if table_name.endswith("s") else table_name
        # PascalCase
        return "".join(word.capitalize() for word in name.split("_"))
    
    # =========================================================================
    # Step 3b: Multi-Table Design
    # =========================================================================
    
    def _generate_multi_table_design(
        self,
        clusters: list[AccessCluster],
        stats_map: dict[str, TableStatistics],
        filtered_columns: dict[str, dict[str, int]],
        selected_columns: dict[str, dict[str, int]],
        select_star_tables: set[str],
    ) -> DynamoDBDesign:
        """Generate multi-table design."""
        tables = []
        
        # Each table gets its own DynamoDB table (sorted for deterministic output)
        for table_name in sorted(stats_map.keys()):
            stats = stats_map[table_name]
            # Determine PK (usually 'id')
            pk = "id"
            
            # Check for composite keys from filtered columns
            top_filtered = list(filtered_columns.get(table_name, {}).keys())[:3]
            
            # Detect GSIs for this table
            table_gsis = self._detect_gsis(
                tables=[table_name],
                filtered_columns=filtered_columns,
                selected_columns=selected_columns,
                select_star_tables=select_star_tables,
            )
            
            tables.append(TableDesign(
                table_name=table_name,
                source_table=table_name,
                partition_key=pk,
                partition_key_type="S",
                sort_key=None,
                gsis=table_gsis,
            ))
        
        return DynamoDBDesign(
            design_mode=DesignMode.MULTI_TABLE,
            tables=tables,
            clusters=clusters,
            confidence=0.8,
            rationale="Multi-table design: each SQL table maps to a DynamoDB table",
        )
    
    # =========================================================================
    # GSI Detection
    # =========================================================================
    
    def _detect_gsis(
        self,
        tables: list[str],
        filtered_columns: dict[str, dict[str, int]],
        selected_columns: dict[str, dict[str, int]],
        select_star_tables: set[str],
    ) -> list[GSIDefinition]:
        """
        Detect GSIs from frequently filtered columns.
        
        GSI candidates:
        - Columns frequently in WHERE clauses
        - Not already the PK
        - High enough frequency to justify index cost
        """
        gsis = []
        gsi_count = 0
        
        for table in tables:
            if gsi_count >= self.MAX_GSIS:
                break
            
            table_filters = filtered_columns.get(table, {})
            
            # Sort by frequency
            sorted_filters = sorted(
                table_filters.items(),
                key=lambda x: x[1],
                reverse=True
            )
            
            for col, freq in sorted_filters:
                if gsi_count >= self.MAX_GSIS:
                    break
                
                if freq < self.GSI_FREQUENCY_THRESHOLD:
                    continue
                
                # Skip if it's likely the PK
                if col in ("id", "pk", "partition_key"):
                    continue
                
                # Determine projection type
                projection_type = self._determine_projection(
                    table, selected_columns, select_star_tables
                )
                
                # Get projected attributes for INCLUDE
                projected_attrs = []
                if projection_type == ProjectionType.INCLUDE:
                    table_selected = selected_columns.get(table, {})
                    projected_attrs = list(table_selected.keys())[:10]
                
                gsis.append(GSIDefinition(
                    name=f"GSI{gsi_count + 1}",
                    pk_attribute=f"GSI{gsi_count + 1}PK",
                    sk_attribute=f"GSI{gsi_count + 1}SK",
                    source_columns=[col],
                    projection_type=projection_type,
                    projected_attributes=projected_attrs,
                    access_pattern=f"Query {table} by {col}",
                ))
                gsi_count += 1
        
        if gsis:
            self.decisions.append(DesignDecision(
                decision_type="gsi_detection",
                choice=f"Created {len(gsis)} GSIs",
                rationale="Based on frequently_filtered_columns with frequency >= threshold",
                data_points={
                    "gsi_columns": [g.source_columns for g in gsis],
                }
            ))
        
        return gsis
    
    def _determine_projection(
        self,
        table: str,
        selected_columns: dict[str, dict[str, int]],
        select_star_tables: set[str],
    ) -> ProjectionType:
        """Determine GSI projection type based on SELECT patterns."""
        # SELECT * → ALL projection
        if table in select_star_tables:
            return ProjectionType.ALL
        
        # Check number of selected columns
        table_selected = selected_columns.get(table, {})
        
        if len(table_selected) == 0:
            return ProjectionType.KEYS_ONLY
        elif len(table_selected) > 10:
            return ProjectionType.ALL
        else:
            return ProjectionType.INCLUDE
    
    # =========================================================================
    # Access Pattern Documentation
    # =========================================================================
    
    def _document_access_patterns(
        self,
        entities: list[EntityDefinition],
        gsis: list[GSIDefinition],
        stats_map: dict[str, TableStatistics],
    ) -> list[AccessPatternDefinition]:
        """Document how the design supports various access patterns."""
        patterns = []
        
        # Primary access patterns (via PK/SK)
        for entity in entities:
            patterns.append(AccessPatternDefinition(
                name=f"Get {entity.name} by ID",
                description=f"Retrieve a single {entity.name} item",
                operation="GetItem",
                table_or_index="main_table",
                pk_condition=f"PK = {entity.pk_pattern}",
                sk_condition=f"SK = {entity.sk_pattern}" if "#<" not in entity.sk_pattern else None,
                frequency=stats_map.get(entity.source_table, TableStatistics(table=entity.source_table)).solo_accesses,
            ))
        
        # GSI access patterns
        for gsi in gsis:
            patterns.append(AccessPatternDefinition(
                name=gsi.access_pattern,
                description=f"Query using {gsi.name}",
                operation="Query",
                table_or_index=gsi.name,
                pk_condition=f"{gsi.pk_attribute} = <value>",
            ))
        
        return patterns
    
    # =========================================================================
    # Warnings
    # =========================================================================
    
    def _generate_warnings(
        self,
        cluster: AccessCluster,
        mutation_map: dict[str, MutationPattern],
        orphan_tables: list[str],
    ) -> list[str]:
        """Generate warnings about potential design issues."""
        warnings = []
        
        # Write-heavy tables in single-table design
        for table in cluster.tables:
            mp = mutation_map.get(table)
            if mp and mp.write_ratio > 0.7:
                warnings.append(
                    f"Table '{table}' is write-heavy ({mp.write_ratio:.0%} writes). "
                    f"Consider keeping it separate to avoid hot partitions."
                )
        
        # Orphan tables
        if orphan_tables:
            warnings.append(
                f"Tables not included in single-table design: {orphan_tables}. "
                f"These will need separate tables."
            )
        
        # Large cluster
        if len(cluster.tables) > 10:
            warnings.append(
                f"Large cluster ({len(cluster.tables)} tables) may lead to complex item collections. "
                f"Consider splitting into sub-domains."
            )
        
        return warnings
