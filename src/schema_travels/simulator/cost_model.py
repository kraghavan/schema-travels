"""Cost model for database operations."""

from dataclasses import dataclass


@dataclass
class SimulationConfig:
    """Configuration for migration simulation."""

    # Storage costs (per GB/month)
    source_storage_cost_per_gb: float = 0.115  # e.g., RDS PostgreSQL
    target_storage_cost_per_gb: float = 0.25   # e.g., MongoDB Atlas

    # Read costs (per million operations)
    source_read_cost_per_million: float = 0.20
    target_read_cost_per_million: float = 0.30

    # Write costs (per million operations)
    source_write_cost_per_million: float = 1.00
    target_write_cost_per_million: float = 1.25

    # Performance assumptions (milliseconds)
    avg_join_latency_ms: float = 5.0
    avg_embed_read_latency_ms: float = 1.0
    avg_reference_lookup_ms: float = 3.0
    avg_simple_read_ms: float = 1.0
    avg_write_ms: float = 2.0

    # Storage overhead
    source_storage_overhead: float = 1.0   # No overhead
    target_storage_overhead: float = 1.2   # 20% BSON/index overhead

    # Data type sizes (bytes)
    int_size: int = 4
    bigint_size: int = 8
    float_size: int = 8
    boolean_size: int = 1
    date_size: int = 8
    uuid_size: int = 16
    default_string_size: int = 50  # Average string length

    @classmethod
    def for_mongodb_atlas(cls) -> "SimulationConfig":
        """Config for MongoDB Atlas M10 cluster."""
        return cls(
            target_storage_cost_per_gb=0.25,
            target_read_cost_per_million=0.30,
            target_write_cost_per_million=1.00,
            target_storage_overhead=1.2,
        )

    @classmethod
    def for_dynamodb(cls) -> "SimulationConfig":
        """Config for DynamoDB on-demand."""
        return cls(
            target_storage_cost_per_gb=0.25,
            target_read_cost_per_million=0.25,  # $0.25 per million RRU
            target_write_cost_per_million=1.25,  # $1.25 per million WRU
            target_storage_overhead=1.0,  # DynamoDB is efficient
        )

    @classmethod
    def for_rds_postgres(cls) -> "SimulationConfig":
        """Config for AWS RDS PostgreSQL."""
        return cls(
            source_storage_cost_per_gb=0.115,
            source_read_cost_per_million=0.20,
            source_write_cost_per_million=1.00,
            source_storage_overhead=1.0,
        )


class CostModel:
    """
    Model for estimating database operation costs.

    Provides methods to estimate storage, latency, and monetary costs
    for both source and target databases.
    """

    def __init__(self, config: SimulationConfig | None = None):
        """
        Initialize cost model.

        Args:
            config: Simulation configuration (defaults to standard config)
        """
        self.config = config or SimulationConfig()

    def estimate_column_size(self, data_type: str) -> int:
        """Estimate size of a column value in bytes."""
        data_type = data_type.lower()

        if "int" in data_type and "big" in data_type:
            return self.config.bigint_size
        elif "int" in data_type or "serial" in data_type:
            return self.config.int_size
        elif "float" in data_type or "double" in data_type or "decimal" in data_type:
            return self.config.float_size
        elif "bool" in data_type:
            return self.config.boolean_size
        elif "date" in data_type or "time" in data_type:
            return self.config.date_size
        elif "uuid" in data_type:
            return self.config.uuid_size
        else:
            # Default to string/text
            return self.config.default_string_size

    def estimate_row_size(self, columns: list[dict]) -> int:
        """
        Estimate size of a row in bytes.

        Args:
            columns: List of column definitions with 'type' key

        Returns:
            Estimated row size in bytes
        """
        total = 0
        for col in columns:
            col_type = col.get("type", col.get("data_type", "text"))
            total += self.estimate_column_size(col_type)
        return total

    def estimate_document_size(
        self,
        base_fields: list[dict],
        embedded_docs: list[dict] | None = None,
    ) -> int:
        """
        Estimate size of a MongoDB document in bytes.

        Args:
            base_fields: List of base field definitions
            embedded_docs: List of embedded document definitions

        Returns:
            Estimated document size in bytes
        """
        # Base document size
        size = self.estimate_row_size(base_fields)

        # Add overhead for field names (average 10 bytes per field)
        size += len(base_fields) * 10

        # Add embedded documents
        if embedded_docs:
            for embedded in embedded_docs:
                embedded_size = self.estimate_row_size(embedded.get("fields", []))
                embedded_size += len(embedded.get("fields", [])) * 10
                
                # Multiply by average number of embedded items
                avg_items = embedded.get("avg_items", 5)
                size += embedded_size * avg_items

        # BSON overhead (~20 bytes per document)
        size += 20

        return size

    def estimate_join_latency(self, num_tables: int) -> float:
        """
        Estimate latency for a JOIN operation.

        Args:
            num_tables: Number of tables in the JOIN

        Returns:
            Estimated latency in milliseconds
        """
        if num_tables <= 1:
            return self.config.avg_simple_read_ms

        # Each additional join adds latency
        joins = num_tables - 1
        return self.config.avg_simple_read_ms + (joins * self.config.avg_join_latency_ms)

    def estimate_document_read_latency(
        self,
        has_embedded: bool = False,
        num_references: int = 0,
    ) -> float:
        """
        Estimate latency for reading a document.

        Args:
            has_embedded: Whether document has embedded data
            num_references: Number of references to follow

        Returns:
            Estimated latency in milliseconds
        """
        base = self.config.avg_simple_read_ms

        if has_embedded:
            # Embedded reads are faster than joins but add some overhead
            base += self.config.avg_embed_read_latency_ms

        # Each reference lookup adds latency
        base += num_references * self.config.avg_reference_lookup_ms

        return base

    def estimate_monthly_storage_cost(
        self,
        storage_gb: float,
        is_target: bool = False,
    ) -> float:
        """
        Estimate monthly storage cost.

        Args:
            storage_gb: Storage in gigabytes
            is_target: Whether this is for target database

        Returns:
            Monthly cost in USD
        """
        cost_per_gb = (
            self.config.target_storage_cost_per_gb
            if is_target
            else self.config.source_storage_cost_per_gb
        )
        return storage_gb * cost_per_gb

    def estimate_monthly_operation_cost(
        self,
        read_count: int,
        write_count: int,
        is_target: bool = False,
    ) -> tuple[float, float]:
        """
        Estimate monthly operation costs.

        Args:
            read_count: Number of read operations per month
            write_count: Number of write operations per month
            is_target: Whether this is for target database

        Returns:
            Tuple of (read_cost, write_cost) in USD
        """
        if is_target:
            read_cost = (read_count / 1_000_000) * self.config.target_read_cost_per_million
            write_cost = (write_count / 1_000_000) * self.config.target_write_cost_per_million
        else:
            read_cost = (read_count / 1_000_000) * self.config.source_read_cost_per_million
            write_cost = (write_count / 1_000_000) * self.config.source_write_cost_per_million

        return read_cost, write_cost
