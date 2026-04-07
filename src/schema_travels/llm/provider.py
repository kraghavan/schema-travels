"""
LLM Provider abstraction layer.

This module defines the interface that all LLM providers must implement,
enabling schema-travels to work with multiple AI backends:
- Claude (Anthropic)
- GPT-4 (OpenAI)
- Gemini (Google)
- Grok (xAI)
- Ollama (Local models)

Usage:
    from schema_travels.llm import get_provider
    
    provider = get_provider("claude")  # or "openai", "gemini", "grok", "ollama"
    response = provider.complete(prompt="Analyze this schema...", system="You are a database expert.")
"""

from typing import Protocol, runtime_checkable
from pydantic import BaseModel, Field
from enum import Enum


class ProviderName(Enum):
    """Supported LLM providers."""
    CLAUDE = "claude"
    OPENAI = "openai"
    GEMINI = "gemini"
    GROK = "grok"
    OLLAMA = "ollama"


class LLMResponse(BaseModel):
    """Standardized response from any LLM provider."""
    
    content: str = Field(..., description="The generated text content")
    model: str = Field(..., description="Model identifier used")
    provider: str = Field(..., description="Provider name (claude, openai, etc.)")
    
    # Optional metadata
    input_tokens: int | None = Field(None, description="Input tokens used")
    output_tokens: int | None = Field(None, description="Output tokens generated")
    latency_ms: float | None = Field(None, description="Response latency in milliseconds")
    finish_reason: str | None = Field(None, description="Why generation stopped")
    
    @property
    def total_tokens(self) -> int | None:
        """Total tokens used (input + output)."""
        if self.input_tokens is not None and self.output_tokens is not None:
            return self.input_tokens + self.output_tokens
        return None


class ModelInfo(BaseModel):
    """Information about a specific model."""
    
    id: str = Field(..., description="Model identifier")
    provider: ProviderName = Field(..., description="Provider this model belongs to")
    display_name: str = Field(..., description="Human-readable name")
    context_window: int = Field(..., description="Maximum context size in tokens")
    supports_json_mode: bool = Field(True, description="Whether model supports JSON output mode")
    supports_vision: bool = Field(False, description="Whether model supports image input")
    cost_per_1k_input: float | None = Field(None, description="Cost per 1K input tokens (USD)")
    cost_per_1k_output: float | None = Field(None, description="Cost per 1K output tokens (USD)")


# Model registry with known models
KNOWN_MODELS: dict[str, ModelInfo] = {
    # Claude models
    "claude-sonnet-4-20250514": ModelInfo(
        id="claude-sonnet-4-20250514",
        provider=ProviderName.CLAUDE,
        display_name="Claude Sonnet 4",
        context_window=200000,
        supports_json_mode=True,
        supports_vision=True,
        cost_per_1k_input=0.003,
        cost_per_1k_output=0.015,
    ),
    "claude-opus-4-20250514": ModelInfo(
        id="claude-opus-4-20250514",
        provider=ProviderName.CLAUDE,
        display_name="Claude Opus 4",
        context_window=200000,
        supports_json_mode=True,
        supports_vision=True,
        cost_per_1k_input=0.015,
        cost_per_1k_output=0.075,
    ),
    "claude-haiku-3-5-20241022": ModelInfo(
        id="claude-haiku-3-5-20241022",
        provider=ProviderName.CLAUDE,
        display_name="Claude Haiku 3.5",
        context_window=200000,
        supports_json_mode=True,
        supports_vision=True,
        cost_per_1k_input=0.0008,
        cost_per_1k_output=0.004,
    ),
    
    # OpenAI models
    "gpt-4o": ModelInfo(
        id="gpt-4o",
        provider=ProviderName.OPENAI,
        display_name="GPT-4o",
        context_window=128000,
        supports_json_mode=True,
        supports_vision=True,
        cost_per_1k_input=0.005,
        cost_per_1k_output=0.015,
    ),
    "gpt-4o-mini": ModelInfo(
        id="gpt-4o-mini",
        provider=ProviderName.OPENAI,
        display_name="GPT-4o Mini",
        context_window=128000,
        supports_json_mode=True,
        supports_vision=True,
        cost_per_1k_input=0.00015,
        cost_per_1k_output=0.0006,
    ),
    "o1": ModelInfo(
        id="o1",
        provider=ProviderName.OPENAI,
        display_name="o1",
        context_window=200000,
        supports_json_mode=True,
        supports_vision=True,
        cost_per_1k_input=0.015,
        cost_per_1k_output=0.06,
    ),
    
    # Gemini models
    "gemini-2.0-flash": ModelInfo(
        id="gemini-2.0-flash",
        provider=ProviderName.GEMINI,
        display_name="Gemini 2.0 Flash",
        context_window=1000000,
        supports_json_mode=True,
        supports_vision=True,
        cost_per_1k_input=0.0001,
        cost_per_1k_output=0.0004,
    ),
    "gemini-2.5-pro": ModelInfo(
        id="gemini-2.5-pro",
        provider=ProviderName.GEMINI,
        display_name="Gemini 2.5 Pro",
        context_window=1000000,
        supports_json_mode=True,
        supports_vision=True,
        cost_per_1k_input=0.00125,
        cost_per_1k_output=0.01,
    ),
    
    # Grok models
    "grok-3": ModelInfo(
        id="grok-3",
        provider=ProviderName.GROK,
        display_name="Grok 3",
        context_window=131072,
        supports_json_mode=True,
        supports_vision=True,
        cost_per_1k_input=0.003,
        cost_per_1k_output=0.015,
    ),
    "grok-3-mini": ModelInfo(
        id="grok-3-mini",
        provider=ProviderName.GROK,
        display_name="Grok 3 Mini",
        context_window=131072,
        supports_json_mode=True,
        supports_vision=False,
        cost_per_1k_input=0.0003,
        cost_per_1k_output=0.0005,
    ),
    
    # Ollama models (costs are $0 - local)
    "llama3.1:8b": ModelInfo(
        id="llama3.1:8b",
        provider=ProviderName.OLLAMA,
        display_name="Llama 3.1 8B",
        context_window=128000,
        supports_json_mode=False,
        supports_vision=False,
        cost_per_1k_input=0.0,
        cost_per_1k_output=0.0,
    ),
    "llama3.1:70b": ModelInfo(
        id="llama3.1:70b",
        provider=ProviderName.OLLAMA,
        display_name="Llama 3.1 70B",
        context_window=128000,
        supports_json_mode=False,
        supports_vision=False,
        cost_per_1k_input=0.0,
        cost_per_1k_output=0.0,
    ),
    "mistral:7b": ModelInfo(
        id="mistral:7b",
        provider=ProviderName.OLLAMA,
        display_name="Mistral 7B",
        context_window=32000,
        supports_json_mode=False,
        supports_vision=False,
        cost_per_1k_input=0.0,
        cost_per_1k_output=0.0,
    ),
    "qwen2.5:72b": ModelInfo(
        id="qwen2.5:72b",
        provider=ProviderName.OLLAMA,
        display_name="Qwen 2.5 72B",
        context_window=128000,
        supports_json_mode=True,
        supports_vision=False,
        cost_per_1k_input=0.0,
        cost_per_1k_output=0.0,
    ),
}


def get_model_info(model_id: str) -> ModelInfo | None:
    """Get information about a model by ID."""
    return KNOWN_MODELS.get(model_id)


@runtime_checkable
class LLMProvider(Protocol):
    """
    Protocol that all LLM providers must implement.
    
    This enables duck-typing - any class with these methods/properties
    can be used as a provider without explicit inheritance.
    """
    
    @property
    def name(self) -> str:
        """Provider name (e.g., 'claude', 'openai')."""
        ...
    
    @property
    def model(self) -> str:
        """Current model identifier."""
        ...
    
    @property
    def supports_json_mode(self) -> bool:
        """Whether provider supports native JSON output mode."""
        ...
    
    def complete(
        self,
        prompt: str,
        system: str | None = None,
        json_mode: bool = False,
        temperature: float = 0.0,
        max_tokens: int = 4096,
    ) -> LLMResponse:
        """
        Generate a completion from the LLM.
        
        Args:
            prompt: The user prompt/message
            system: Optional system prompt
            json_mode: Request JSON output (if supported)
            temperature: Sampling temperature (0.0 = deterministic)
            max_tokens: Maximum tokens to generate
            
        Returns:
            LLMResponse with generated content and metadata
        """
        ...


class LLMProviderError(Exception):
    """Base exception for LLM provider errors."""
    
    def __init__(self, provider: str, message: str, original_error: Exception | None = None):
        self.provider = provider
        self.message = message
        self.original_error = original_error
        super().__init__(f"[{provider}] {message}")


class APIKeyMissingError(LLMProviderError):
    """Raised when API key is not configured."""
    pass


class RateLimitError(LLMProviderError):
    """Raised when rate limit is exceeded."""
    pass


class ModelNotFoundError(LLMProviderError):
    """Raised when requested model doesn't exist."""
    pass


class ProviderUnavailableError(LLMProviderError):
    """Raised when provider is temporarily unavailable (e.g., overloaded)."""
    pass
