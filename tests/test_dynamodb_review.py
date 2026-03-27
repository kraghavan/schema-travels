#!/usr/bin/env python3
"""Test script for schema-travels v2.0.1 DynamoDB AI Review.

Run this after installing schema-travels to verify the implementation:

    python test_v2.0.1.py

Expected output: All tests pass with green checkmarks.
"""

import json
import sys


def test_imports():
    """Test all new imports work."""
    print("Testing imports...")
    
    from schema_travels.recommender.dynamodb_models import (
        DynamoDBReview,
        EntityChange,
        GSIChange,
        GSIChangeAction,
        ReviewChangeType,
        DynamoDBDesign,
        EntityDefinition,
        GSIDefinition,
        DesignMode,
        ProjectionType,
    )
    print("  ✓ dynamodb_models imports")
    
    from schema_travels.recommender.dynamodb_review import (
        apply_review,
        summarize_review_changes,
    )
    print("  ✓ dynamodb_review imports")
    
    from schema_travels.recommender.claude_advisor import ClaudeAdvisor
    print("  ✓ claude_advisor imports")
    
    from schema_travels.recommender.schema_generator import SchemaGenerator
    print("  ✓ schema_generator imports")
    
    # Check __init__.py exports
    from schema_travels.recommender import (
        DynamoDBReview,
        EntityChange,
        GSIChange,
        apply_review,
        summarize_review_changes,
    )
    print("  ✓ recommender __init__ exports")
    
    return True


def test_dynamodb_review_model():
    """Test DynamoDBReview model."""
    print("\nTesting DynamoDBReview model...")
    
    from schema_travels.recommender.dynamodb_models import (
        DynamoDBReview,
        EntityChange,
        GSIChange,
        GSIChangeAction,
        ReviewChangeType,
    )
    
    # Test empty review (approved, no changes)
    review = DynamoDBReview(
        approved=True,
        confidence=0.9,
        summary="Design looks good",
        warnings=["Watch for hot partitions"],
    )
    assert review.approved == True
    assert review.has_changes == False
    assert review.change_count == 0
    print("  ✓ Empty review (no changes)")
    
    # Test review with entity changes
    review_with_entity = DynamoDBReview(
        approved=True,
        confidence=0.85,
        summary="Good with tweaks",
        entity_changes=[
            EntityChange(
                entity="users",
                change_type=ReviewChangeType.MODIFY_SK,
                current_value="PROFILE",
                new_value="PROFILE#v2",
                reason="Better versioning",
            )
        ],
    )
    assert review_with_entity.has_changes == True
    assert review_with_entity.change_count == 1
    print("  ✓ Review with entity changes")
    
    # Test review with GSI changes
    review_with_gsi = DynamoDBReview(
        approved=True,
        confidence=0.8,
        summary="Add GSI for email lookup",
        gsi_changes=[
            GSIChange(
                action=GSIChangeAction.ADD,
                gsi_name="GSI-Email",
                pk_attribute="email",
                reason="Enable email lookup",
            ),
            GSIChange(
                action=GSIChangeAction.REMOVE,
                gsi_name="GSI-Old",
                reason="Not needed",
            ),
        ],
    )
    assert review_with_gsi.has_changes == True
    assert review_with_gsi.change_count == 2
    print("  ✓ Review with GSI changes")
    
    # Test to_dict()
    d = review_with_gsi.to_dict()
    assert "approved" in d
    assert "gsi_changes" in d
    assert len(d["gsi_changes"]) == 2
    print("  ✓ to_dict() works")
    
    return True


def test_apply_review():
    """Test apply_review function."""
    print("\nTesting apply_review...")
    
    from schema_travels.recommender.dynamodb_models import (
        DynamoDBDesign,
        DynamoDBReview,
        EntityDefinition,
        GSIDefinition,
        GSIChange,
        GSIChangeAction,
        DesignMode,
        ProjectionType,
    )
    from schema_travels.recommender.dynamodb_review import apply_review
    
    # Create a simple design
    design = DynamoDBDesign(
        design_mode=DesignMode.SINGLE_TABLE,
        confidence=0.75,
        rationale="High co-access",
        table_name="main_table",
        partition_key="PK",
        sort_key="SK",
        entities=[
            EntityDefinition(
                name="User",
                source_table="users",
                pk_pattern="USER#<id>",
                sk_pattern="PROFILE",
            ),
            EntityDefinition(
                name="Order",
                source_table="orders",
                pk_pattern="USER#<user_id>",
                sk_pattern="ORDER#<id>",
            ),
        ],
        gsis=[
            GSIDefinition(
                name="GSI1",
                pk_attribute="GSI1PK",
                sk_attribute="GSI1SK",
                projection_type=ProjectionType.ALL,
            ),
        ],
    )
    
    # Test with no-change review
    review_no_change = DynamoDBReview(
        approved=True,
        confidence=0.9,
        summary="Looks good",
    )
    
    result = apply_review(design, review_no_change)
    assert result.ai_reviewed == True
    assert result.ai_review_applied == False  # No changes to apply
    assert len(result.entities) == 2
    print("  ✓ apply_review with no changes")
    
    # Test with GSI add
    review_add_gsi = DynamoDBReview(
        approved=True,
        confidence=0.85,
        summary="Add email GSI",
        gsi_changes=[
            GSIChange(
                action=GSIChangeAction.ADD,
                gsi_name="GSI2",
                pk_attribute="email",
                projection_type=ProjectionType.KEYS_ONLY,
                reason="Email lookup",
            ),
        ],
        warnings=["Monitor GSI2 for hot partitions"],
    )
    
    result = apply_review(design, review_add_gsi)
    assert result.ai_reviewed == True
    assert result.ai_review_applied == True
    assert len(result.gsis) == 2  # Original GSI1 + new GSI2
    assert result.gsis[1].name == "GSI2"
    assert "Monitor GSI2 for hot partitions" in result.warnings
    print("  ✓ apply_review adds GSI")
    
    # Test with GSI remove
    review_remove_gsi = DynamoDBReview(
        approved=True,
        confidence=0.8,
        summary="Remove unnecessary GSI",
        gsi_changes=[
            GSIChange(
                action=GSIChangeAction.REMOVE,
                gsi_name="GSI1",
                reason="Not needed",
            ),
        ],
    )
    
    result = apply_review(design, review_remove_gsi)
    assert len(result.gsis) == 0  # GSI1 removed
    print("  ✓ apply_review removes GSI")
    
    return True


def test_summarize_review():
    """Test summarize_review_changes function."""
    print("\nTesting summarize_review_changes...")
    
    from schema_travels.recommender.dynamodb_models import (
        DynamoDBReview,
        EntityChange,
        GSIChange,
        GSIChangeAction,
        ReviewChangeType,
    )
    from schema_travels.recommender.dynamodb_review import summarize_review_changes
    
    # Test approved with no changes
    review = DynamoDBReview(approved=True, confidence=0.9, summary="Good")
    summary = summarize_review_changes(review)
    assert "approved" in summary.lower()
    print("  ✓ Summarize approved review")
    
    # Test with changes
    review_changes = DynamoDBReview(
        approved=True,
        confidence=0.8,
        summary="Good with tweaks",
        entity_changes=[
            EntityChange(
                entity="users",
                change_type=ReviewChangeType.MODIFY_SK,
                new_value="PROFILE#v2",
                reason="Better",
            )
        ],
        gsi_changes=[
            GSIChange(
                action=GSIChangeAction.ADD,
                gsi_name="GSI2",
                pk_attribute="email",
                reason="Email lookup",
            )
        ],
        warnings=["Watch out"],
    )
    summary = summarize_review_changes(review_changes)
    assert "entity" in summary.lower()
    assert "gsi" in summary.lower()
    print("  ✓ Summarize review with changes")
    
    return True


def test_claude_advisor_has_review_method():
    """Test ClaudeAdvisor has review_dynamodb_design method."""
    print("\nTesting ClaudeAdvisor...")
    
    from schema_travels.recommender.claude_advisor import ClaudeAdvisor
    
    # Check method exists
    assert hasattr(ClaudeAdvisor, 'review_dynamodb_design')
    print("  ✓ review_dynamodb_design method exists")
    
    # Check method signature
    import inspect
    sig = inspect.signature(ClaudeAdvisor.review_dynamodb_design)
    params = list(sig.parameters.keys())
    assert 'design' in params
    assert 'analysis' in params
    assert 'schema' in params
    print("  ✓ Method signature correct")
    
    return True


def test_schema_generator_accepts_review():
    """Test SchemaGenerator accepts dynamodb_review parameter."""
    print("\nTesting SchemaGenerator...")
    
    from schema_travels.recommender.schema_generator import SchemaGenerator
    import inspect
    
    # Check __init__ signature
    sig = inspect.signature(SchemaGenerator.__init__)
    params = list(sig.parameters.keys())
    assert 'dynamodb_review' in params
    print("  ✓ SchemaGenerator accepts dynamodb_review parameter")
    
    return True


def main():
    """Run all tests."""
    print("=" * 60)
    print("Schema Travels v2.0.1 - DynamoDB AI Review Tests")
    print("=" * 60)
    
    tests = [
        test_imports,
        test_dynamodb_review_model,
        test_apply_review,
        test_summarize_review,
        test_claude_advisor_has_review_method,
        test_schema_generator_accepts_review,
    ]
    
    passed = 0
    failed = 0
    
    for test in tests:
        try:
            if test():
                passed += 1
        except Exception as e:
            print(f"  ✗ FAILED: {e}")
            failed += 1
    
    print("\n" + "=" * 60)
    print(f"Results: {passed} passed, {failed} failed")
    print("=" * 60)
    
    return 0 if failed == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
