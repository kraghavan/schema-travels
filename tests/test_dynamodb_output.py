"""Tests for DynamoDB output formatters (v2.0.0).

Tests cover:
- JSON output format
- Terraform HCL output format
- NoSQL Workbench JSON output format
- Format auto-detection
- Edge cases
"""

import json
import pytest

from schema_travels.recommender.dynamodb_models import (
    DynamoDBDesign,
    DesignMode,
    EntityDefinition,
    GSIDefinition,
    ProjectionType,
    TableDesign,
    AccessCluster,
)
from schema_travels.recommender.dynamodb_output import DynamoDBOutputFormatter


# =============================================================================
# Test Fixtures
# =============================================================================

@pytest.fixture
def single_table_design():
    """Create a sample single-table design."""
    return DynamoDBDesign(
        design_mode=DesignMode.SINGLE_TABLE,
        table_name="ecommerce",
        partition_key="PK",
        partition_key_type="S",
        sort_key="SK",
        sort_key_type="S",
        entities=[
            EntityDefinition(
                name="User",
                source_table="users",
                pk_pattern="USER#<id>",
                sk_pattern="PROFILE",
                attributes=["name", "email", "created_at"],
            ),
            EntityDefinition(
                name="Order",
                source_table="orders",
                pk_pattern="USER#<user_id>",
                sk_pattern="ORDER#<id>",
                attributes=["total", "status", "created_at"],
            ),
        ],
        gsis=[
            GSIDefinition(
                name="GSI1",
                pk_attribute="GSI1PK",
                sk_attribute="GSI1SK",
                projection_type=ProjectionType.ALL,
                source_columns=["email"],
                access_pattern="Query users by email",
            ),
        ],
        confidence=0.85,
        rationale="Strong co-access between users and orders",
    )


@pytest.fixture
def multi_table_design():
    """Create a sample multi-table design."""
    return DynamoDBDesign(
        design_mode=DesignMode.MULTI_TABLE,
        tables=[
            TableDesign(
                table_name="users",
                source_table="users",
                partition_key="id",
                partition_key_type="S",
                gsis=[
                    GSIDefinition(
                        name="EmailIndex",
                        pk_attribute="email",
                        projection_type=ProjectionType.INCLUDE,
                        projected_attributes=["name", "status"],
                    ),
                ],
            ),
            TableDesign(
                table_name="orders",
                source_table="orders",
                partition_key="id",
                partition_key_type="S",
                sort_key="created_at",
                sort_key_type="S",
            ),
        ],
        confidence=0.8,
        rationale="Multi-table design for separate concerns",
    )


@pytest.fixture
def minimal_design():
    """Create a minimal design for edge case testing."""
    return DynamoDBDesign(
        design_mode=DesignMode.SINGLE_TABLE,
        table_name="minimal",
        partition_key="PK",
        confidence=0.5,
    )


# =============================================================================
# JSON Output Tests
# =============================================================================

class TestJSONOutput:
    """Tests for JSON output format."""

    def test_json_output_is_valid_json(self, single_table_design):
        """JSON output should be valid JSON."""
        output = DynamoDBOutputFormatter.to_json(single_table_design)
        
        # Should not raise
        parsed = json.loads(output)
        
        assert isinstance(parsed, dict)

    def test_json_contains_design_mode(self, single_table_design):
        """JSON should contain design_mode."""
        output = DynamoDBOutputFormatter.to_json(single_table_design)
        parsed = json.loads(output)
        
        assert parsed["design_mode"] == "single_table"

    def test_json_contains_entities(self, single_table_design):
        """JSON should contain entities for single-table design."""
        output = DynamoDBOutputFormatter.to_json(single_table_design)
        parsed = json.loads(output)
        
        assert "entities" in parsed
        assert len(parsed["entities"]) == 2
        assert parsed["entities"][0]["name"] == "User"

    def test_json_contains_gsis(self, single_table_design):
        """JSON should contain GSIs."""
        output = DynamoDBOutputFormatter.to_json(single_table_design)
        parsed = json.loads(output)
        
        assert "gsis" in parsed
        assert len(parsed["gsis"]) == 1
        assert parsed["gsis"][0]["name"] == "GSI1"

    def test_json_multi_table_contains_tables(self, multi_table_design):
        """Multi-table JSON should contain tables array."""
        output = DynamoDBOutputFormatter.to_json(multi_table_design)
        parsed = json.loads(output)
        
        assert "tables" in parsed
        assert len(parsed["tables"]) == 2

    def test_json_indent_option(self, minimal_design):
        """JSON indent option should work."""
        output_2 = DynamoDBOutputFormatter.to_json(minimal_design, indent=2)
        output_4 = DynamoDBOutputFormatter.to_json(minimal_design, indent=4)
        
        # More indentation = more characters
        assert len(output_4) > len(output_2)

    def test_to_dict_returns_dict(self, single_table_design):
        """to_dict should return a dictionary."""
        result = DynamoDBOutputFormatter.to_dict(single_table_design)
        
        assert isinstance(result, dict)
        assert result["design_mode"] == "single_table"


# =============================================================================
# Terraform Output Tests
# =============================================================================

class TestTerraformOutput:
    """Tests for Terraform HCL output format."""

    def test_terraform_contains_resource_block(self, single_table_design):
        """Terraform output should contain aws_dynamodb_table resource."""
        output = DynamoDBOutputFormatter.to_terraform(single_table_design)
        
        assert 'resource "aws_dynamodb_table"' in output

    def test_terraform_contains_table_name(self, single_table_design):
        """Terraform should include table name."""
        output = DynamoDBOutputFormatter.to_terraform(single_table_design)
        
        assert 'name         = "ecommerce"' in output

    def test_terraform_contains_hash_key(self, single_table_design):
        """Terraform should include hash_key."""
        output = DynamoDBOutputFormatter.to_terraform(single_table_design)
        
        assert 'hash_key     = "PK"' in output

    def test_terraform_contains_range_key(self, single_table_design):
        """Terraform should include range_key when present."""
        output = DynamoDBOutputFormatter.to_terraform(single_table_design)
        
        assert 'range_key    = "SK"' in output

    def test_terraform_contains_attribute_blocks(self, single_table_design):
        """Terraform should include attribute definitions."""
        output = DynamoDBOutputFormatter.to_terraform(single_table_design)
        
        assert "attribute {" in output
        assert 'name = "PK"' in output
        assert 'type = "S"' in output

    def test_terraform_contains_gsi_block(self, single_table_design):
        """Terraform should include GSI definitions."""
        output = DynamoDBOutputFormatter.to_terraform(single_table_design)
        
        assert "global_secondary_index {" in output
        assert 'name            = "GSI1"' in output
        assert 'hash_key        = "GSI1PK"' in output

    def test_terraform_gsi_projection_type(self, single_table_design):
        """Terraform GSI should include projection type."""
        output = DynamoDBOutputFormatter.to_terraform(single_table_design)
        
        assert 'projection_type = "ALL"' in output

    def test_terraform_gsi_include_projection(self, multi_table_design):
        """Terraform GSI with INCLUDE should have non_key_attributes."""
        output = DynamoDBOutputFormatter.to_terraform(multi_table_design)
        
        assert "non_key_attributes" in output
        assert '"name"' in output

    def test_terraform_billing_mode(self, single_table_design):
        """Terraform should include billing mode."""
        output = DynamoDBOutputFormatter.to_terraform(single_table_design)
        
        assert 'billing_mode = "PAY_PER_REQUEST"' in output

    def test_terraform_with_provider_alias(self, single_table_design):
        """Terraform should include provider alias when specified."""
        output = DynamoDBOutputFormatter.to_terraform(
            single_table_design,
            provider_alias="aws.us-east-1"
        )
        
        assert "provider = aws.us-east-1" in output

    def test_terraform_with_tags(self, single_table_design):
        """Terraform should include tags when specified."""
        output = DynamoDBOutputFormatter.to_terraform(
            single_table_design,
            tags={"Environment": "production", "Team": "backend"}
        )
        
        assert "tags = {" in output
        assert 'Environment = "production"' in output
        assert 'Team = "backend"' in output

    def test_terraform_multi_table(self, multi_table_design):
        """Terraform multi-table should have multiple resource blocks."""
        output = DynamoDBOutputFormatter.to_terraform(multi_table_design)
        
        # Count resource blocks
        resource_count = output.count('resource "aws_dynamodb_table"')
        assert resource_count == 2

    def test_terraform_entity_comments(self, single_table_design):
        """Terraform should include entity pattern comments."""
        output = DynamoDBOutputFormatter.to_terraform(single_table_design)
        
        assert "# Entity Patterns:" in output
        assert "USER#<id>" in output

    def test_terraform_header_comment(self, single_table_design):
        """Terraform should include header comment."""
        output = DynamoDBOutputFormatter.to_terraform(single_table_design)
        
        assert "schema-travels" in output
        assert "Design Mode:" in output

    def test_terraform_resource_name_sanitization(self):
        """Resource names should be sanitized for Terraform."""
        design = DynamoDBDesign(
            design_mode=DesignMode.SINGLE_TABLE,
            table_name="my-table.v2",
            partition_key="PK",
        )
        
        output = DynamoDBOutputFormatter.to_terraform(design)
        
        # Should replace - and . with _
        assert 'resource "aws_dynamodb_table" "my_table_v2"' in output


# =============================================================================
# NoSQL Workbench Output Tests
# =============================================================================

class TestNoSQLWorkbenchOutput:
    """Tests for NoSQL Workbench JSON output format."""

    def test_workbench_is_valid_json(self, single_table_design):
        """NoSQL Workbench output should be valid JSON."""
        output = DynamoDBOutputFormatter.to_nosql_workbench(single_table_design)
        
        # Should not raise
        parsed = json.loads(output)
        
        assert isinstance(parsed, dict)

    def test_workbench_contains_model_name(self, single_table_design):
        """Workbench JSON should contain ModelName."""
        output = DynamoDBOutputFormatter.to_nosql_workbench(
            single_table_design,
            model_name="Test Model"
        )
        parsed = json.loads(output)
        
        assert parsed["ModelName"] == "Test Model"

    def test_workbench_contains_model_metadata(self, single_table_design):
        """Workbench JSON should contain ModelMetadata."""
        output = DynamoDBOutputFormatter.to_nosql_workbench(single_table_design)
        parsed = json.loads(output)
        
        assert "ModelMetadata" in parsed
        assert parsed["ModelMetadata"]["Author"] == "schema-travels"
        assert parsed["ModelMetadata"]["Version"] == "2.0.0"

    def test_workbench_contains_data_model(self, single_table_design):
        """Workbench JSON should contain DataModel array."""
        output = DynamoDBOutputFormatter.to_nosql_workbench(single_table_design)
        parsed = json.loads(output)
        
        assert "DataModel" in parsed
        assert isinstance(parsed["DataModel"], list)
        assert len(parsed["DataModel"]) >= 1

    def test_workbench_table_has_key_attributes(self, single_table_design):
        """Workbench table should have KeyAttributes."""
        output = DynamoDBOutputFormatter.to_nosql_workbench(single_table_design)
        parsed = json.loads(output)
        
        table = parsed["DataModel"][0]
        
        assert "KeyAttributes" in table
        assert "PartitionKey" in table["KeyAttributes"]
        assert table["KeyAttributes"]["PartitionKey"]["AttributeName"] == "PK"

    def test_workbench_table_has_sort_key(self, single_table_design):
        """Workbench table should have SortKey when present."""
        output = DynamoDBOutputFormatter.to_nosql_workbench(single_table_design)
        parsed = json.loads(output)
        
        table = parsed["DataModel"][0]
        
        assert "SortKey" in table["KeyAttributes"]
        assert table["KeyAttributes"]["SortKey"]["AttributeName"] == "SK"

    def test_workbench_contains_gsis(self, single_table_design):
        """Workbench JSON should contain GSIs."""
        output = DynamoDBOutputFormatter.to_nosql_workbench(single_table_design)
        parsed = json.loads(output)
        
        table = parsed["DataModel"][0]
        
        assert "GlobalSecondaryIndexes" in table
        assert len(table["GlobalSecondaryIndexes"]) == 1
        assert table["GlobalSecondaryIndexes"][0]["IndexName"] == "GSI1"

    def test_workbench_contains_facets(self, single_table_design):
        """Workbench JSON should contain TableFacets (entities)."""
        output = DynamoDBOutputFormatter.to_nosql_workbench(single_table_design)
        parsed = json.loads(output)
        
        table = parsed["DataModel"][0]
        
        assert "TableFacets" in table
        assert len(table["TableFacets"]) == 2
        
        facet_names = [f["FacetName"] for f in table["TableFacets"]]
        assert "User" in facet_names
        assert "Order" in facet_names

    def test_workbench_facet_has_key_aliases(self, single_table_design):
        """Workbench facets should have KeyAttributeAlias."""
        output = DynamoDBOutputFormatter.to_nosql_workbench(single_table_design)
        parsed = json.loads(output)
        
        table = parsed["DataModel"][0]
        user_facet = next(f for f in table["TableFacets"] if f["FacetName"] == "User")
        
        assert "KeyAttributeAlias" in user_facet
        assert user_facet["KeyAttributeAlias"]["PartitionKeyAlias"] == "USER#<id>"
        assert user_facet["KeyAttributeAlias"]["SortKeyAlias"] == "PROFILE"

    def test_workbench_multi_table(self, multi_table_design):
        """Workbench multi-table should have multiple tables in DataModel."""
        output = DynamoDBOutputFormatter.to_nosql_workbench(multi_table_design)
        parsed = json.loads(output)
        
        assert len(parsed["DataModel"]) == 2


# =============================================================================
# Format Helper Tests
# =============================================================================

class TestFormatHelper:
    """Tests for format() convenience method."""

    def test_format_json(self, single_table_design):
        """format('json') should return JSON."""
        output = DynamoDBOutputFormatter.format(single_table_design, "json")
        
        # Should be valid JSON
        json.loads(output)

    def test_format_terraform(self, single_table_design):
        """format('terraform') should return Terraform HCL."""
        output = DynamoDBOutputFormatter.format(single_table_design, "terraform")
        
        assert 'resource "aws_dynamodb_table"' in output

    def test_format_tf_alias(self, single_table_design):
        """format('tf') should work as alias for terraform."""
        output = DynamoDBOutputFormatter.format(single_table_design, "tf")
        
        assert 'resource "aws_dynamodb_table"' in output

    def test_format_hcl_alias(self, single_table_design):
        """format('hcl') should work as alias for terraform."""
        output = DynamoDBOutputFormatter.format(single_table_design, "hcl")
        
        assert 'resource "aws_dynamodb_table"' in output

    def test_format_nosql_workbench(self, single_table_design):
        """format('nosql_workbench') should return Workbench JSON."""
        output = DynamoDBOutputFormatter.format(single_table_design, "nosql_workbench")
        
        parsed = json.loads(output)
        assert "ModelName" in parsed

    def test_format_workbench_alias(self, single_table_design):
        """format('workbench') should work as alias."""
        output = DynamoDBOutputFormatter.format(single_table_design, "workbench")
        
        parsed = json.loads(output)
        assert "ModelName" in parsed

    def test_format_case_insensitive(self, single_table_design):
        """format() should be case-insensitive."""
        output1 = DynamoDBOutputFormatter.format(single_table_design, "JSON")
        output2 = DynamoDBOutputFormatter.format(single_table_design, "json")
        
        assert output1 == output2

    def test_format_invalid_raises(self, single_table_design):
        """format() with invalid format should raise ValueError."""
        with pytest.raises(ValueError) as exc_info:
            DynamoDBOutputFormatter.format(single_table_design, "invalid")
        
        assert "Unknown format" in str(exc_info.value)
        assert "invalid" in str(exc_info.value)


# =============================================================================
# Edge Cases
# =============================================================================

class TestEdgeCases:
    """Edge case tests."""

    def test_empty_entities(self):
        """Design with no entities should still work."""
        design = DynamoDBDesign(
            design_mode=DesignMode.SINGLE_TABLE,
            table_name="empty",
            partition_key="PK",
        )
        
        json_output = DynamoDBOutputFormatter.to_json(design)
        tf_output = DynamoDBOutputFormatter.to_terraform(design)
        wb_output = DynamoDBOutputFormatter.to_nosql_workbench(design)
        
        # All should produce valid output
        assert "empty" in json_output
        assert "empty" in tf_output
        assert "empty" in wb_output

    def test_no_gsis(self):
        """Design with no GSIs should work."""
        design = DynamoDBDesign(
            design_mode=DesignMode.SINGLE_TABLE,
            table_name="no_gsis",
            partition_key="PK",
            entities=[
                EntityDefinition(
                    name="Item",
                    source_table="items",
                    pk_pattern="ITEM#<id>",
                    sk_pattern="ITEM",
                ),
            ],
        )
        
        tf_output = DynamoDBOutputFormatter.to_terraform(design)
        
        # Should not have GSI blocks
        assert "global_secondary_index" not in tf_output

    def test_no_sort_key(self):
        """Design without sort key should work."""
        design = DynamoDBDesign(
            design_mode=DesignMode.SINGLE_TABLE,
            table_name="no_sk",
            partition_key="PK",
            sort_key=None,
        )
        
        tf_output = DynamoDBOutputFormatter.to_terraform(design)
        
        # Should not have range_key
        assert "range_key" not in tf_output

    def test_gsi_without_sort_key(self):
        """GSI without sort key should work."""
        design = DynamoDBDesign(
            design_mode=DesignMode.SINGLE_TABLE,
            table_name="gsi_no_sk",
            partition_key="PK",
            gsis=[
                GSIDefinition(
                    name="SimpleGSI",
                    pk_attribute="status",
                    sk_attribute=None,
                    projection_type=ProjectionType.KEYS_ONLY,
                ),
            ],
        )
        
        tf_output = DynamoDBOutputFormatter.to_terraform(design)
        
        # Should have GSI without range_key
        assert "global_secondary_index {" in tf_output
        assert 'hash_key        = "status"' in tf_output

    def test_special_characters_in_table_name(self):
        """Table names with special characters should be handled."""
        design = DynamoDBDesign(
            design_mode=DesignMode.SINGLE_TABLE,
            table_name="my-app.prod-v2",
            partition_key="PK",
        )
        
        tf_output = DynamoDBOutputFormatter.to_terraform(design)
        
        # Resource name should be sanitized
        assert 'resource "aws_dynamodb_table" "my_app_prod_v2"' in tf_output
        # But table name in DynamoDB should be preserved
        assert 'name         = "my-app.prod-v2"' in tf_output

    def test_numeric_table_name_prefix(self):
        """Table names starting with number should be handled."""
        design = DynamoDBDesign(
            design_mode=DesignMode.SINGLE_TABLE,
            table_name="123_table",
            partition_key="PK",
        )
        
        tf_output = DynamoDBOutputFormatter.to_terraform(design)
        
        # Resource name should be prefixed
        assert 'resource "aws_dynamodb_table" "table_123_table"' in tf_output

    def test_keys_only_projection(self):
        """KEYS_ONLY projection should not have non_key_attributes."""
        design = DynamoDBDesign(
            design_mode=DesignMode.SINGLE_TABLE,
            table_name="keys_only",
            partition_key="PK",
            gsis=[
                GSIDefinition(
                    name="KeysOnlyGSI",
                    pk_attribute="status",
                    projection_type=ProjectionType.KEYS_ONLY,
                ),
            ],
        )
        
        tf_output = DynamoDBOutputFormatter.to_terraform(design)
        
        assert 'projection_type = "KEYS_ONLY"' in tf_output
        assert "non_key_attributes" not in tf_output

    def test_many_gsis(self):
        """Design with many GSIs should work."""
        gsis = [
            GSIDefinition(
                name=f"GSI{i}",
                pk_attribute=f"GSI{i}PK",
                projection_type=ProjectionType.ALL,
            )
            for i in range(5)
        ]
        
        design = DynamoDBDesign(
            design_mode=DesignMode.SINGLE_TABLE,
            table_name="many_gsis",
            partition_key="PK",
            gsis=gsis,
        )
        
        tf_output = DynamoDBOutputFormatter.to_terraform(design)
        
        # Should have all 5 GSIs
        assert tf_output.count("global_secondary_index {") == 5

    def test_entity_with_many_attributes(self):
        """Entity with many attributes should work."""
        design = DynamoDBDesign(
            design_mode=DesignMode.SINGLE_TABLE,
            table_name="many_attrs",
            partition_key="PK",
            entities=[
                EntityDefinition(
                    name="BigEntity",
                    source_table="big_table",
                    pk_pattern="BIG#<id>",
                    sk_pattern="BIG",
                    attributes=[f"attr{i}" for i in range(20)],
                ),
            ],
        )
        
        wb_output = DynamoDBOutputFormatter.to_nosql_workbench(design)
        parsed = json.loads(wb_output)
        
        # Should have all attributes
        non_key_attrs = parsed["DataModel"][0]["NonKeyAttributes"]
        assert len(non_key_attrs) == 20
