"""Claude advisor module for AI-powered schema recommendations.

v2.0.0: Added DynamoDB support with single-table design decisions.
v2.0.1: Added review_dynamodb_design() for AI review of local designs.
v2.3.0: Refactored to use provider-agnostic Advisor with multi-LLM support.

DEPRECATED: Use `from schema_travels.recommender.advisor import Advisor` instead.

Migration Guide:
    # Old (still works):
    from schema_travels.recommender import ClaudeAdvisor
    advisor = ClaudeAdvisor()
    
    # New (recommended):
    from schema_travels.recommender import Advisor
    from schema_travels.llm import get_provider
    
    # Use Claude (default)
    advisor = Advisor()
    
    # Use other providers
    advisor = Advisor(provider_name="openai", model="gpt-4o")
    advisor = Advisor(provider_name="ollama", model="llama3.1:70b")
"""

from schema_travels.recommender.advisor import Advisor

# Backwards compatibility alias
ClaudeAdvisor = Advisor

__all__ = ["ClaudeAdvisor", "Advisor"]
