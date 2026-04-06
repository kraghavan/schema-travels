"""
Claude (Anthropic) LLM provider.

Requires: pip install anthropic
Env var: ANTHROPIC_API_KEY
"""

import os
import time
import logging
from typing import Any

from schema_travels.llm.provider import (
    LLMProvider,
    LLMResponse,
    APIKeyMissingError,
    RateLimitError,
    ProviderUnavailableError,
    LLMProviderError,
)

logger = logging.getLogger(__name__)


class ClaudeProvider:
    """Anthropic Claude provider."""
    
    DEFAULT_MODEL = "claude-sonnet-4-20250514"
    
    def __init__(
        self,
        model: str | None = None,
        api_key: str | None = None,
        max_retries: int = 2,
    ):
        """
        Initialize Claude provider.
        
        Args:
            model: Model to use (default: claude-sonnet-4-20250514)
            api_key: API key (default: from ANTHROPIC_API_KEY env var)
            max_retries: Number of retries on transient errors
        """
        self._model = model or self.DEFAULT_MODEL
        self._api_key = api_key or os.environ.get("ANTHROPIC_API_KEY")
        self._max_retries = max_retries
        self._client = None
        
        if not self._api_key:
            raise APIKeyMissingError(
                provider="claude",
                message="ANTHROPIC_API_KEY environment variable not set. "
                        "Get your key at https://console.anthropic.com/",
            )
    
    @property
    def name(self) -> str:
        return "claude"
    
    @property
    def model(self) -> str:
        return self._model
    
    @property
    def supports_json_mode(self) -> bool:
        return True
    
    @property
    def client(self) -> Any:
        """Lazy-load the Anthropic client."""
        if self._client is None:
            try:
                from anthropic import Anthropic
                self._client = Anthropic(api_key=self._api_key)
            except ImportError:
                raise LLMProviderError(
                    provider="claude",
                    message="anthropic package not installed. Run: pip install anthropic",
                )
        return self._client
    
    def complete(
        self,
        prompt: str,
        system: str | None = None,
        json_mode: bool = False,
        temperature: float = 0.0,
        max_tokens: int = 4096,
    ) -> LLMResponse:
        """Generate completion using Claude."""
        
        # Add JSON instruction if requested
        if json_mode:
            if system:
                system = system + "\n\nRespond with valid JSON only. No markdown code blocks."
            else:
                system = "Respond with valid JSON only. No markdown code blocks."
        
        messages = [{"role": "user", "content": prompt}]
        
        start_time = time.time()
        last_error = None
        
        for attempt in range(self._max_retries + 1):
            try:
                response = self.client.messages.create(
                    model=self._model,
                    system=system or "",
                    messages=messages,
                    temperature=temperature,
                    max_tokens=max_tokens,
                )
                
                latency_ms = (time.time() - start_time) * 1000
                
                return LLMResponse(
                    content=response.content[0].text,
                    model=self._model,
                    provider="claude",
                    input_tokens=response.usage.input_tokens,
                    output_tokens=response.usage.output_tokens,
                    latency_ms=latency_ms,
                    finish_reason=response.stop_reason,
                )
                
            except Exception as e:
                last_error = e
                error_str = str(e).lower()
                
                # Check for rate limit
                if "rate" in error_str and "limit" in error_str:
                    if attempt < self._max_retries:
                        wait_time = 2 ** attempt  # Exponential backoff
                        logger.warning(f"Rate limited, retrying in {wait_time}s...")
                        time.sleep(wait_time)
                        continue
                    raise RateLimitError(
                        provider="claude",
                        message="Rate limit exceeded",
                        original_error=e,
                    )
                
                # Check for overloaded
                if "overloaded" in error_str or "529" in error_str:
                    if attempt < self._max_retries:
                        wait_time = 2 ** attempt
                        logger.warning(f"API overloaded, retrying in {wait_time}s...")
                        time.sleep(wait_time)
                        continue
                    raise ProviderUnavailableError(
                        provider="claude",
                        message="API is overloaded. Try again later or use --no-ai flag.",
                        original_error=e,
                    )
                
                # Other errors - don't retry
                raise LLMProviderError(
                    provider="claude",
                    message=str(e),
                    original_error=e,
                )
        
        # Should not reach here, but just in case
        raise LLMProviderError(
            provider="claude",
            message=f"Failed after {self._max_retries + 1} attempts",
            original_error=last_error,
        )
