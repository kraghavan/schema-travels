"""
LLM Provider Factory.

Creates the appropriate LLM provider based on configuration.

Usage:
    from schema_travels.llm import get_provider
    
    # Use default (Claude)
    provider = get_provider()
    
    # Specify provider
    provider = get_provider("openai")
    provider = get_provider("gemini", model="gemini-2.5-pro")
    provider = get_provider("ollama", model="llama3.1:70b")
    provider = get_provider("grok", model="grok-3")
    
    # Use provider
    response = provider.complete("Analyze this schema...")
"""

import os
import logging
from typing import Any

from schema_travels.llm.provider import (
    ProviderName,
    LLMProvider,
    LLMProviderError,
)

logger = logging.getLogger(__name__)


# Default models per provider
DEFAULT_MODELS = {
    ProviderName.CLAUDE: "claude-sonnet-4-20250514",
    ProviderName.OPENAI: "gpt-4o",
    ProviderName.GEMINI: "gemini-2.0-flash",
    ProviderName.GROK: "grok-3",
    ProviderName.OLLAMA: "llama3.1:8b",
}


def get_provider(
    provider: str | ProviderName | None = None,
    model: str | None = None,
    **kwargs: Any,
) -> LLMProvider:
    """
    Factory function to create an LLM provider.
    
    Args:
        provider: Provider name ('claude', 'openai', 'gemini', 'grok', 'ollama')
                  If None, uses SCHEMA_TRAVELS_PROVIDER env var or defaults to 'claude'
        model: Model identifier. If None, uses provider's default model.
        **kwargs: Additional provider-specific arguments (e.g., api_key, host)
        
    Returns:
        LLMProvider instance
        
    Raises:
        LLMProviderError: If provider is unknown or cannot be initialized
        
    Examples:
        >>> provider = get_provider()  # Default: Claude
        >>> provider = get_provider("openai", model="gpt-4o-mini")
        >>> provider = get_provider("ollama", model="mistral:7b", host="http://192.168.1.100:11434")
    """
    
    # Determine provider
    if provider is None:
        provider_str = os.environ.get("SCHEMA_TRAVELS_PROVIDER", "claude").lower()
    elif isinstance(provider, ProviderName):
        provider_str = provider.value
    else:
        provider_str = provider.lower()
    
    # Parse provider name
    try:
        provider_enum = ProviderName(provider_str)
    except ValueError:
        valid_providers = [p.value for p in ProviderName]
        raise LLMProviderError(
            provider=provider_str,
            message=f"Unknown provider '{provider_str}'. Valid options: {', '.join(valid_providers)}",
        )
    
    # Get default model if not specified
    if model is None:
        model = os.environ.get("SCHEMA_TRAVELS_MODEL") or DEFAULT_MODELS.get(provider_enum)
    
    # Create provider instance
    logger.debug(f"Creating {provider_enum.value} provider with model {model}")
    
    match provider_enum:
        case ProviderName.CLAUDE:
            from schema_travels.llm.providers.claude import ClaudeProvider
            return ClaudeProvider(model=model, **kwargs)
        
        case ProviderName.OPENAI:
            from schema_travels.llm.providers.openai import OpenAIProvider
            return OpenAIProvider(model=model, **kwargs)
        
        case ProviderName.GEMINI:
            from schema_travels.llm.providers.gemini import GeminiProvider
            return GeminiProvider(model=model, **kwargs)
        
        case ProviderName.GROK:
            from schema_travels.llm.providers.grok import GrokProvider
            return GrokProvider(model=model, **kwargs)
        
        case ProviderName.OLLAMA:
            from schema_travels.llm.providers.ollama import OllamaProvider
            # Rename 'host' to match Ollama's parameter if passed as 'ollama_host'
            if "ollama_host" in kwargs:
                kwargs["host"] = kwargs.pop("ollama_host")
            return OllamaProvider(model=model, **kwargs)
        
        case _:
            raise LLMProviderError(
                provider=provider_str,
                message=f"Provider '{provider_str}' is not implemented",
            )


def get_default_provider_name() -> str:
    """Get the default provider name from environment or config."""
    return os.environ.get("SCHEMA_TRAVELS_PROVIDER", "claude").lower()


def list_providers() -> list[str]:
    """List all available provider names."""
    return [p.value for p in ProviderName]


def get_provider_info(provider: str) -> dict[str, Any]:
    """
    Get information about a provider.
    
    Returns dict with:
        - name: Provider name
        - default_model: Default model for this provider
        - env_vars: Required environment variables
        - install: pip install command if needed
    """
    try:
        provider_enum = ProviderName(provider.lower())
    except ValueError:
        return {"error": f"Unknown provider: {provider}"}
    
    info = {
        "name": provider_enum.value,
        "default_model": DEFAULT_MODELS.get(provider_enum),
    }
    
    match provider_enum:
        case ProviderName.CLAUDE:
            info["env_vars"] = ["ANTHROPIC_API_KEY"]
            info["install"] = "pip install anthropic"
            info["docs"] = "https://console.anthropic.com/"
            
        case ProviderName.OPENAI:
            info["env_vars"] = ["OPENAI_API_KEY"]
            info["install"] = "pip install openai"
            info["docs"] = "https://platform.openai.com/"
            
        case ProviderName.GEMINI:
            info["env_vars"] = ["GOOGLE_API_KEY", "GEMINI_API_KEY"]
            info["install"] = "pip install google-generativeai"
            info["docs"] = "https://aistudio.google.com/"
            
        case ProviderName.GROK:
            info["env_vars"] = ["XAI_API_KEY", "GROK_API_KEY"]
            info["install"] = "pip install openai"  # Uses OpenAI-compatible API
            info["docs"] = "https://console.x.ai/"
            
        case ProviderName.OLLAMA:
            info["env_vars"] = ["OLLAMA_HOST (optional)"]
            info["install"] = "https://ollama.ai (no pip package needed)"
            info["docs"] = "https://ollama.ai/"
    
    return info
