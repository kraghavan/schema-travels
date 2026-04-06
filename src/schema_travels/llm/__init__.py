"""
LLM Provider abstraction layer for schema-travels.

This module provides a unified interface for multiple LLM backends:
- Claude (Anthropic)
- GPT-4 (OpenAI)
- Gemini (Google)
- Grok (xAI)
- Ollama (Local models)

Quick Start:
    from schema_travels.llm import get_provider
    
    # Use default provider (Claude)
    provider = get_provider()
    response = provider.complete("Analyze this schema...")
    
    # Use specific provider
    provider = get_provider("openai", model="gpt-4o")
    provider = get_provider("ollama", model="llama3.1:70b")
    
Environment Variables:
    SCHEMA_TRAVELS_PROVIDER  - Default provider (claude, openai, gemini, grok, ollama)
    SCHEMA_TRAVELS_MODEL     - Default model for the provider
    
    ANTHROPIC_API_KEY        - API key for Claude
    OPENAI_API_KEY           - API key for OpenAI
    GOOGLE_API_KEY           - API key for Gemini (or GEMINI_API_KEY)
    XAI_API_KEY              - API key for Grok (or GROK_API_KEY)
    OLLAMA_HOST              - Ollama server URL (default: http://localhost:11434)
"""

from schema_travels.llm.provider import (
    LLMProvider,
    LLMResponse,
    ProviderName,
    ModelInfo,
    KNOWN_MODELS,
    get_model_info,
    LLMProviderError,
    APIKeyMissingError,
    RateLimitError,
    ModelNotFoundError,
    ProviderUnavailableError,
)

from schema_travels.llm.factory import (
    get_provider,
    get_default_provider_name,
    list_providers,
    get_provider_info,
    DEFAULT_MODELS,
)

__all__ = [
    # Main factory function
    "get_provider",
    
    # Protocol and response
    "LLMProvider",
    "LLMResponse",
    
    # Enums and info
    "ProviderName",
    "ModelInfo",
    "KNOWN_MODELS",
    "DEFAULT_MODELS",
    
    # Helper functions
    "get_model_info",
    "get_default_provider_name",
    "list_providers",
    "get_provider_info",
    
    # Exceptions
    "LLMProviderError",
    "APIKeyMissingError",
    "RateLimitError",
    "ModelNotFoundError",
    "ProviderUnavailableError",
]
