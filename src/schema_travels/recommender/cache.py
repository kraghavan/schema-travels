"""Recommendation caching for consistent, reproducible results.

This module provides:
1. Input hashing - same inputs produce same cache key
2. Recommendation caching - reuse previous AI recommendations
3. Version tracking - know which model/prompt produced results
4. Comparison tools - diff between runs
"""

import hashlib
import json
import logging
from datetime import datetime
from pathlib import Path
from typing import Any

from schema_travels.config import get_settings
from schema_travels.collector.models import SchemaDefinition
from schema_travels.analyzer.models import AnalysisResult
from schema_travels.recommender.models import SchemaRecommendation, TargetDatabase

logger = logging.getLogger(__name__)

# Bump this when recommendation logic changes significantly
RECOMMENDATION_VERSION = "1.0.0"


def compute_input_hash(
    schema: SchemaDefinition,
    analysis: AnalysisResult,
    target: TargetDatabase,
) -> str:
    """
    Compute a deterministic hash of inputs.
    
    Same schema + analysis + target = same hash = can use cached recommendation.
    
    Args:
        schema: Source schema definition
        analysis: Analysis result
        target: Target database type
        
    Returns:
        SHA256 hash string (first 16 chars)
    """
    # Build deterministic representation
    data = {
        "version": RECOMMENDATION_VERSION,
        "target": target.value,
        "tables": sorted([
            {
                "name": t.name,
                "columns": sorted([c.name for c in t.columns]),
                "pk": sorted(t.primary_key),
            }
            for t in schema.tables
        ], key=lambda x: x["name"]),
        "foreign_keys": sorted([
            f"{fk.from_table}.{fk.from_columns[0]}->{fk.to_table}.{fk.to_columns[0]}"
            for fk in schema.foreign_keys
        ]),
        "join_patterns": sorted([
            {
                "tables": tuple(sorted([jp.left_table, jp.right_table])),
                "frequency": jp.frequency,
            }
            for jp in analysis.join_patterns[:20]  # Top 20 for stability
        ], key=lambda x: x["tables"]),
        "mutation_patterns": sorted([
            {
                "table": mp.table,
                "write_ratio": round(mp.write_ratio, 2),  # Round for stability
            }
            for mp in analysis.mutation_patterns
        ], key=lambda x: x["table"]),
    }
    
    # Compute hash
    json_str = json.dumps(data, sort_keys=True)
    hash_obj = hashlib.sha256(json_str.encode())
    return hash_obj.hexdigest()[:16]


class RecommendationCache:
    """
    Cache for AI recommendations with version tracking.
    
    Ensures:
    - Same inputs = same recommendations (if cached)
    - Can compare runs over time
    - Can invalidate cache when logic changes
    """
    
    def __init__(self, cache_dir: Path | None = None):
        """
        Initialize cache.
        
        Args:
            cache_dir: Directory for cache files (default: ~/.schema-travels/cache)
        """
        if cache_dir is None:
            settings = get_settings()
            cache_dir = settings.db_path.parent / "cache"
        
        self.cache_dir = Path(cache_dir)
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        
        # Index file tracks all cached recommendations
        self.index_file = self.cache_dir / "index.json"
        self._index = self._load_index()
    
    def _load_index(self) -> dict:
        """Load cache index."""
        if self.index_file.exists():
            try:
                with open(self.index_file) as f:
                    return json.load(f)
            except Exception as e:
                logger.warning(f"Failed to load cache index: {e}")
        return {"entries": {}, "version": RECOMMENDATION_VERSION}
    
    def _save_index(self) -> None:
        """Save cache index."""
        with open(self.index_file, "w") as f:
            json.dump(self._index, f, indent=2)
    
    def get(
        self,
        input_hash: str,
    ) -> list[SchemaRecommendation] | None:
        """
        Get cached recommendations if available.
        
        Args:
            input_hash: Hash from compute_input_hash()
            
        Returns:
            Cached recommendations or None if not found/expired
        """
        entry = self._index.get("entries", {}).get(input_hash)
        
        if not entry:
            logger.debug(f"Cache miss: {input_hash}")
            return None
        
        # Check version compatibility
        if entry.get("version") != RECOMMENDATION_VERSION:
            logger.info(f"Cache invalidated (version mismatch): {input_hash}")
            return None
        
        # Load cached recommendations
        cache_file = self.cache_dir / f"{input_hash}.json"
        if not cache_file.exists():
            logger.warning(f"Cache file missing: {cache_file}")
            return None
        
        try:
            with open(cache_file) as f:
                data = json.load(f)
            
            # Import here to avoid circular imports
            from schema_travels.recommender.models import RelationshipDecision
            
            recommendations = []
            for r in data.get("recommendations", []):
                # Convert decision string to enum
                decision = r["decision"]
                if isinstance(decision, str):
                    try:
                        decision = RelationshipDecision(decision.lower())
                    except ValueError:
                        # Try uppercase
                        decision = RelationshipDecision[decision.upper()]
                
                recommendations.append(
                    SchemaRecommendation(
                        parent_table=r["parent_table"],
                        child_table=r["child_table"],
                        decision=decision,
                        confidence=r["confidence"],
                        reasoning=r["reasoning"],
                        warnings=r["warnings"],
                        metrics=r.get("metrics", {}),
                    )
                )
            
            logger.info(f"Cache hit: {input_hash} ({len(recommendations)} recommendations)")
            return recommendations
            
        except Exception as e:
            logger.error(f"Failed to load cache: {e}")
            return None
    
    def put(
        self,
        input_hash: str,
        recommendations: list[SchemaRecommendation],
        metadata: dict[str, Any] | None = None,
    ) -> None:
        """
        Cache recommendations.
        
        Args:
            input_hash: Hash from compute_input_hash()
            recommendations: Recommendations to cache
            metadata: Optional metadata (model used, timestamp, etc.)
        """
        settings = get_settings()
        
        # Prepare cache entry
        cache_data = {
            "input_hash": input_hash,
            "version": RECOMMENDATION_VERSION,
            "model": settings.anthropic_model,
            "timestamp": datetime.now().isoformat(),
            "recommendations": [
                {
                    "parent_table": r.parent_table,
                    "child_table": r.child_table,
                    "decision": r.decision.value if hasattr(r.decision, 'value') else str(r.decision),
                    "confidence": r.confidence,
                    "reasoning": r.reasoning,
                    "warnings": r.warnings,
                    "metrics": r.metrics,
                }
                for r in recommendations
            ],
            "metadata": metadata or {},
        }
        
        # Save cache file
        cache_file = self.cache_dir / f"{input_hash}.json"
        with open(cache_file, "w") as f:
            json.dump(cache_data, f, indent=2)
        
        # Update index
        self._index.setdefault("entries", {})[input_hash] = {
            "version": RECOMMENDATION_VERSION,
            "model": settings.anthropic_model,
            "timestamp": cache_data["timestamp"],
            "num_recommendations": len(recommendations),
        }
        self._save_index()
        
        logger.info(f"Cached {len(recommendations)} recommendations: {input_hash}")
    
    def invalidate(self, input_hash: str) -> bool:
        """
        Invalidate a cache entry.
        
        Args:
            input_hash: Hash to invalidate
            
        Returns:
            True if entry was found and removed
        """
        if input_hash in self._index.get("entries", {}):
            del self._index["entries"][input_hash]
            self._save_index()
            
            cache_file = self.cache_dir / f"{input_hash}.json"
            if cache_file.exists():
                cache_file.unlink()
            
            logger.info(f"Invalidated cache: {input_hash}")
            return True
        
        return False
    
    def invalidate_all(self) -> int:
        """
        Invalidate all cache entries.
        
        Returns:
            Number of entries invalidated
        """
        count = len(self._index.get("entries", {}))
        
        # Remove all cache files
        for f in self.cache_dir.glob("*.json"):
            if f.name != "index.json":
                f.unlink()
        
        # Reset index
        self._index = {"entries": {}, "version": RECOMMENDATION_VERSION}
        self._save_index()
        
        logger.info(f"Invalidated {count} cache entries")
        return count
    
    def list_entries(self) -> list[dict]:
        """List all cache entries."""
        return [
            {"hash": k, **v}
            for k, v in self._index.get("entries", {}).items()
        ]
    
    def compare(
        self,
        hash1: str,
        hash2: str,
    ) -> dict[str, Any]:
        """
        Compare two cached recommendation sets.
        
        Args:
            hash1: First input hash
            hash2: Second input hash
            
        Returns:
            Comparison report
        """
        recs1 = self.get(hash1) or []
        recs2 = self.get(hash2) or []
        
        # Build lookup maps
        map1 = {(r.parent_table, r.child_table): r for r in recs1}
        map2 = {(r.parent_table, r.child_table): r for r in recs2}
        
        all_keys = set(map1.keys()) | set(map2.keys())
        
        changes = []
        for key in all_keys:
            r1 = map1.get(key)
            r2 = map2.get(key)
            
            if r1 and r2:
                if r1.decision != r2.decision:
                    changes.append({
                        "relationship": f"{key[0]} -> {key[1]}",
                        "type": "decision_changed",
                        "from": str(r1.decision),
                        "to": str(r2.decision),
                    })
                elif abs(r1.confidence - r2.confidence) > 0.1:
                    changes.append({
                        "relationship": f"{key[0]} -> {key[1]}",
                        "type": "confidence_changed",
                        "from": r1.confidence,
                        "to": r2.confidence,
                    })
            elif r1:
                changes.append({
                    "relationship": f"{key[0]} -> {key[1]}",
                    "type": "removed",
                })
            else:
                changes.append({
                    "relationship": f"{key[0]} -> {key[1]}",
                    "type": "added",
                    "decision": str(r2.decision),
                })
        
        return {
            "hash1": hash1,
            "hash2": hash2,
            "total_in_1": len(recs1),
            "total_in_2": len(recs2),
            "changes": changes,
            "is_identical": len(changes) == 0,
        }


def get_cache() -> RecommendationCache:
    """Get the global recommendation cache."""
    return RecommendationCache()
