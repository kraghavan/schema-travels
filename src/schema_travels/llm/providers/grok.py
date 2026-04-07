"""
xAI Grok provider.

Grok uses an OpenAI-compatible API, so we use the openai package.

Requires: pip install openai
Env var: XAI_API_KEY or GROK_API_KEY
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


class GrokProvider:
    """xAI Grok provider (OpenAI-compatible API)."""
    
    DEFAULT_MODEL = "grok-3"
    BASE_URL = "https://api.x.ai/v1"
    
    def __init__(
        self,
        model: str | None = None,
        api_key: str | None = None,
        base_url: str | None = None,
        max_retries: int = 2,
    ):
        """
        Initialize Grok provider.
        
        Args:
            model: Model to use (default: grok-3)
            api_key: API key (default: from XAI_API_KEY or GROK_API_KEY env var)
            base_url: API base URL (default: https://api.x.ai/v1)
            max_retries: Number of retries on transient errors
        """
        self._model = model or self.DEFAULT_MODEL
        self._api_key = api_key or os.environ.get("XAI_API_KEY") or os.environ.get("GROK_API_KEY")
        self._base_url = base_url or self.BASE_URL
        self._max_retries = max_retries
        self._client = None
        
        if not self._api_key:
            raise APIKeyMissingError(
                provider="grok",
                message="XAI_API_KEY or GROK_API_KEY environment variable not set. "
                        "Get your key at https://console.x.ai/",
            )
    
    @property
    def name(self) -> str:
        return "grok"
    
    @property
    def model(self) -> str:
        return self._model
    
    @property
    def supports_json_mode(self) -> bool:
        return True
    
    @property
    def client(self) -> Any:
        """Lazy-load the OpenAI-compatible client for xAI."""
        if self._client is None:
            try:
                from openai import OpenAI
                self._client = OpenAI(
                    api_key=self._api_key,
                    base_url=self._base_url,
                )
            except ImportError:
                raise LLMProviderError(
                    provider="grok",
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
        """Generate completion using Grok."""
        
        messages = []
        
        if system:
            if json_mode:
                system = system + "\n\nRespond with valid JSON only. No markdown code blocks."
            messages.append({"role": "system", "content": system})
        elif json_mode:
            messages.append({"role": "system", "content": "Respond with valid JSON only. No markdown code blocks."})
        
        messages.append({"role": "user", "content": prompt})
        
        # Build request kwargs
        request_kwargs = {
            "model": self._model,
            "messages": messages,
            "temperature": temperature,
            "max_tokens": max_tokens,
        }
        
        # Grok supports JSON mode via response_format
        if json_mode:
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
                    provider="grok",
                    input_tokens=response.usage.prompt_tokens if response.usage else None,
                    output_tokens=response.usage.completion_tokens if response.usage else None,
                    latency_ms=latency_ms,
                    finish_reason=response.choices[0].finish_reason,
                )
                
            except Exception as e:
                last_error = e
                error_str = str(e).lower()
                
                # Check for rate limit
                if "rate" in error_str and "limit" in error_str or "429" in error_str:
                    if attempt < self._max_retries:
                        wait_time = 2 ** attempt
                        logger.warning(f"Rate limited, retrying in {wait_time}s...")
                        time.sleep(wait_time)
                        continue
                    raise RateLimitError(
                        provider="grok",
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
                        provider="grok",
                        message="API is unavailable. Try again later.",
                        original_error=e,
                    )
                
                # Other errors - don't retry
                raise LLMProviderError(
                    provider="grok",
                    message=str(e),
                    original_error=e,
                )
        
        raise LLMProviderError(
            provider="grok",
            message=f"Failed after {self._max_retries + 1} attempts",
            original_error=last_error,
        )
