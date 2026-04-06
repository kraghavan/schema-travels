"""
OpenAI GPT provider.

Requires: pip install openai
Env var: OPENAI_API_KEY
"""

import os
import time
import logging
from typing import Any

from schema_travels.llm.provider import (
    LLMResponse,
    APIKeyMissingError,
    RateLimitError,
    ProviderUnavailableError,
    LLMProviderError,
)

logger = logging.getLogger(__name__)


class OpenAIProvider:
    """OpenAI GPT provider."""
    
    DEFAULT_MODEL = "gpt-4o"
    
    def __init__(
        self,
        model: str | None = None,
        api_key: str | None = None,
        max_retries: int = 2,
    ):
        """
        Initialize OpenAI provider.
        
        Args:
            model: Model to use (default: gpt-4o)
            api_key: API key (default: from OPENAI_API_KEY env var)
            max_retries: Number of retries on transient errors
        """
        self._model = model or self.DEFAULT_MODEL
        self._api_key = api_key or os.environ.get("OPENAI_API_KEY")
        self._max_retries = max_retries
        self._client = None
        
        if not self._api_key:
            raise APIKeyMissingError(
                provider="openai",
                message="OPENAI_API_KEY environment variable not set. "
                        "Get your key at https://platform.openai.com/api-keys",
            )
    
    @property
    def name(self) -> str:
        return "openai"
    
    @property
    def model(self) -> str:
        return self._model
    
    @property
    def supports_json_mode(self) -> bool:
        return True
    
    @property
    def client(self) -> Any:
        """Lazy-load the OpenAI client."""
        if self._client is None:
            try:
                from openai import OpenAI
                self._client = OpenAI(api_key=self._api_key)
            except ImportError:
                raise LLMProviderError(
                    provider="openai",
                    message="openai package not installed. Run: pip install openai",
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
        """Generate completion using OpenAI."""
        
        messages = []
        
        if system:
            # Add JSON instruction to system if json_mode
            if json_mode:
                system = system + "\n\nRespond with valid JSON only."
            messages.append({"role": "system", "content": system})
        elif json_mode:
            messages.append({"role": "system", "content": "Respond with valid JSON only."})
        
        messages.append({"role": "user", "content": prompt})
        
        # Build request kwargs
        request_kwargs = {
            "model": self._model,
            "messages": messages,
            "temperature": temperature,
            "max_tokens": max_tokens,
        }
        
        # Add response_format for JSON mode (not supported by all models)
        if json_mode and self._model in ("gpt-4o", "gpt-4o-mini", "gpt-4-turbo"):
            request_kwargs["response_format"] = {"type": "json_object"}
        
        start_time = time.time()
        last_error = None
        
        for attempt in range(self._max_retries + 1):
            try:
                response = self.client.chat.completions.create(**request_kwargs)
                
                latency_ms = (time.time() - start_time) * 1000
                
                return LLMResponse(
                    content=response.choices[0].message.content,
                    model=self._model,
                    provider="openai",
                    input_tokens=response.usage.prompt_tokens if response.usage else None,
                    output_tokens=response.usage.completion_tokens if response.usage else None,
                    latency_ms=latency_ms,
                    finish_reason=response.choices[0].finish_reason,
                )
                
            except Exception as e:
                last_error = e
                error_str = str(e).lower()
                
                # Check for rate limit
                if "rate" in error_str and "limit" in error_str:
                    if attempt < self._max_retries:
                        wait_time = 2 ** attempt
                        logger.warning(f"Rate limited, retrying in {wait_time}s...")
                        time.sleep(wait_time)
                        continue
                    raise RateLimitError(
                        provider="openai",
                        message="Rate limit exceeded",
                        original_error=e,
                    )
                
                # Check for server errors
                if "server" in error_str or "503" in error_str or "502" in error_str:
                    if attempt < self._max_retries:
                        wait_time = 2 ** attempt
                        logger.warning(f"Server error, retrying in {wait_time}s...")
                        time.sleep(wait_time)
                        continue
                    raise ProviderUnavailableError(
                        provider="openai",
                        message="API is unavailable. Try again later.",
                        original_error=e,
                    )
                
                # Other errors - don't retry
                raise LLMProviderError(
                    provider="openai",
                    message=str(e),
                    original_error=e,
                )
        
        raise LLMProviderError(
            provider="openai",
            message=f"Failed after {self._max_retries + 1} attempts",
            original_error=last_error,
        )
