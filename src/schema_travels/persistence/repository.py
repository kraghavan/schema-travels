"""Repository for analysis data persistence."""

import json
from datetime import datetime
from typing import Any

from schema_travels.analyzer.models import AnalysisResult
from schema_travels.recommender.models import SchemaRecommendation, TargetSchema
from schema_travels.simulator.models import SimulationResult
from schema_travels.persistence.database import Database


class AnalysisRepository:
    """
    Repository for storing and retrieving analysis data.

    Provides a clean interface for persisting analysis results,
    recommendations, and simulation results.
    """

    def __init__(self, database: Database | None = None):
        """
        Initialize repository.

        Args:
            database: Database instance (creates new one if not provided)
        """
        self.db = database or Database()

    def create_analysis(
        self,
        analysis_id: str,
        source_db_type: str,
        target_db_type: str,
        logs_dir: str | None = None,
        schema_file: str | None = None,
    ) -> str:
        """
        Create a new analysis record.

        Args:
            analysis_id: Unique analysis identifier
            source_db_type: Source database type
            target_db_type: Target database type
            logs_dir: Path to logs directory
            schema_file: Path to schema file

        Returns:
            Analysis ID
        """
        self.db.execute(
            """
            INSERT INTO analyses (id, source_db_type, target_db_type, logs_dir, schema_file)
            VALUES (?, ?, ?, ?, ?)
            """,
            (analysis_id, source_db_type, target_db_type, logs_dir, schema_file),
        )
        return analysis_id

    def update_analysis_status(
        self,
        analysis_id: str,
        status: str,
        total_queries: int | None = None,
        tables_analyzed: int | None = None,
    ) -> None:
        """
        Update analysis status.

        Args:
            analysis_id: Analysis identifier
            status: New status (pending, running, completed, failed)
            total_queries: Number of queries analyzed
            tables_analyzed: Number of tables found
        """
        updates = ["status = ?"]
        params: list[Any] = [status]

        if total_queries is not None:
            updates.append("total_queries = ?")
            params.append(total_queries)

        if tables_analyzed is not None:
            updates.append("tables_analyzed = ?")
            params.append(tables_analyzed)

        params.append(analysis_id)

        self.db.execute(
            f"UPDATE analyses SET {', '.join(updates)} WHERE id = ?",
            tuple(params),
        )

    def save_analysis_result(self, result: AnalysisResult) -> None:
        """
        Save analysis result.

        Args:
            result: Analysis result to save
        """
        # Update analysis record
        self.update_analysis_status(
            result.analysis_id,
            "completed",
            total_queries=result.total_queries_analyzed,
            tables_analyzed=len(result.tables_analyzed),
        )

        # Save detailed results as JSON
        self.db.execute(
            """
            INSERT OR REPLACE INTO analysis_results
            (analysis_id, join_patterns_json, mutation_patterns_json,
             access_patterns_json, table_statistics_json)
            VALUES (?, ?, ?, ?, ?)
            """,
            (
                result.analysis_id,
                json.dumps([jp.to_dict() for jp in result.join_patterns]),
                json.dumps([mp.to_dict() for mp in result.mutation_patterns]),
                json.dumps([ap.to_dict() for ap in result.access_patterns]),
                json.dumps([ts.to_dict() for ts in result.table_statistics]),
            ),
        )

    def save_recommendations(
        self,
        analysis_id: str,
        recommendations: list[SchemaRecommendation],
    ) -> None:
        """
        Save schema recommendations.

        Args:
            analysis_id: Analysis identifier
            recommendations: List of recommendations to save
        """
        with self.db.transaction() as conn:
            cursor = conn.cursor()

            # Clear existing recommendations
            cursor.execute(
                "DELETE FROM recommendations WHERE analysis_id = ?",
                (analysis_id,),
            )

            # Insert new recommendations
            for rec in recommendations:
                cursor.execute(
                    """
                    INSERT INTO recommendations
                    (analysis_id, parent_table, child_table, decision, confidence,
                     reasoning_json, warnings_json)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        analysis_id,
                        rec.parent_table,
                        rec.child_table,
                        rec.decision.value,
                        rec.confidence,
                        json.dumps(rec.reasoning),
                        json.dumps(rec.warnings),
                    ),
                )

    def save_target_schema(
        self,
        analysis_id: str,
        schema: TargetSchema,
    ) -> None:
        """
        Save generated target schema.

        Args:
            analysis_id: Analysis identifier
            schema: Target schema to save
        """
        self.db.execute(
            """
            INSERT OR REPLACE INTO target_schemas
            (analysis_id, target_type, schema_json)
            VALUES (?, ?, ?)
            """,
            (
                analysis_id,
                schema.target_type.value,
                json.dumps(schema.to_dict()),
            ),
        )

    def save_simulation(
        self,
        analysis_id: str,
        result: SimulationResult,
    ) -> int:
        """
        Save simulation result.

        Args:
            analysis_id: Analysis identifier
            result: Simulation result to save

        Returns:
            Simulation ID
        """
        cursor = self.db.execute(
            """
            INSERT INTO simulations (analysis_id, result_json)
            VALUES (?, ?)
            """,
            (analysis_id, json.dumps(result.to_dict())),
        )
        return cursor.lastrowid or 0

    def get_analysis(self, analysis_id: str) -> dict[str, Any] | None:
        """
        Get analysis by ID.

        Args:
            analysis_id: Analysis identifier

        Returns:
            Analysis data or None if not found
        """
        row = self.db.fetch_one(
            "SELECT * FROM analyses WHERE id = ?",
            (analysis_id,),
        )

        if not row:
            return None

        return dict(row)

    def get_analysis_result(self, analysis_id: str) -> dict[str, Any] | None:
        """
        Get detailed analysis result.

        Args:
            analysis_id: Analysis identifier

        Returns:
            Analysis result data or None
        """
        row = self.db.fetch_one(
            "SELECT * FROM analysis_results WHERE analysis_id = ?",
            (analysis_id,),
        )

        if not row:
            return None

        return {
            "analysis_id": row["analysis_id"],
            "join_patterns": json.loads(row["join_patterns_json"] or "[]"),
            "mutation_patterns": json.loads(row["mutation_patterns_json"] or "[]"),
            "access_patterns": json.loads(row["access_patterns_json"] or "[]"),
            "table_statistics": json.loads(row["table_statistics_json"] or "[]"),
        }

    def get_recommendations(self, analysis_id: str) -> list[dict[str, Any]]:
        """
        Get recommendations for an analysis.

        Args:
            analysis_id: Analysis identifier

        Returns:
            List of recommendation data
        """
        rows = self.db.fetch_all(
            "SELECT * FROM recommendations WHERE analysis_id = ?",
            (analysis_id,),
        )

        return [
            {
                "id": row["id"],
                "parent_table": row["parent_table"],
                "child_table": row["child_table"],
                "decision": row["decision"],
                "confidence": row["confidence"],
                "reasoning": json.loads(row["reasoning_json"] or "[]"),
                "warnings": json.loads(row["warnings_json"] or "[]"),
            }
            for row in rows
        ]

    def get_target_schema(self, analysis_id: str) -> dict[str, Any] | None:
        """
        Get target schema for an analysis.

        Args:
            analysis_id: Analysis identifier

        Returns:
            Target schema data or None
        """
        row = self.db.fetch_one(
            "SELECT * FROM target_schemas WHERE analysis_id = ?",
            (analysis_id,),
        )

        if not row:
            return None

        return json.loads(row["schema_json"])

    def get_simulations(self, analysis_id: str) -> list[dict[str, Any]]:
        """
        Get simulation results for an analysis.

        Args:
            analysis_id: Analysis identifier

        Returns:
            List of simulation results
        """
        rows = self.db.fetch_all(
            "SELECT * FROM simulations WHERE analysis_id = ? ORDER BY created_at DESC",
            (analysis_id,),
        )

        return [
            {
                "id": row["id"],
                "created_at": row["created_at"],
                "result": json.loads(row["result_json"]),
            }
            for row in rows
        ]

    def list_analyses(
        self,
        limit: int = 20,
        offset: int = 0,
    ) -> list[dict[str, Any]]:
        """
        List all analyses.

        Args:
            limit: Maximum number of results
            offset: Number of results to skip

        Returns:
            List of analysis summaries
        """
        rows = self.db.fetch_all(
            """
            SELECT * FROM analyses
            ORDER BY created_at DESC
            LIMIT ? OFFSET ?
            """,
            (limit, offset),
        )

        return [dict(row) for row in rows]

    def delete_analysis(self, analysis_id: str) -> bool:
        """
        Delete an analysis and all related data.

        Args:
            analysis_id: Analysis identifier

        Returns:
            True if deleted, False if not found
        """
        with self.db.transaction() as conn:
            cursor = conn.cursor()

            # Check if exists
            cursor.execute(
                "SELECT id FROM analyses WHERE id = ?",
                (analysis_id,),
            )
            if not cursor.fetchone():
                return False

            # Delete related data
            cursor.execute(
                "DELETE FROM simulations WHERE analysis_id = ?",
                (analysis_id,),
            )
            cursor.execute(
                "DELETE FROM target_schemas WHERE analysis_id = ?",
                (analysis_id,),
            )
            cursor.execute(
                "DELETE FROM recommendations WHERE analysis_id = ?",
                (analysis_id,),
            )
            cursor.execute(
                "DELETE FROM analysis_results WHERE analysis_id = ?",
                (analysis_id,),
            )
            cursor.execute(
                "DELETE FROM analyses WHERE id = ?",
                (analysis_id,),
            )

            return True
