"""
Google Gemini provider.

Requires: pip install google-generativeai
Env var: GOOGLE_API_KEY or GEMINI_API_KEY
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


class GeminiProvider:
    """Google Gemini provider."""
    
    DEFAULT_MODEL = "gemini-2.0-flash"
    
    def __init__(
        self,
        model: str | None = None,
        api_key: str | None = None,
        max_retries: int = 2,
    ):
        """
        Initialize Gemini provider.
        
        Args:
            model: Model to use (default: gemini-2.0-flash)
            api_key: API key (default: from GOOGLE_API_KEY or GEMINI_API_KEY env var)
            max_retries: Number of retries on transient errors
        """
        self._model_name = model or self.DEFAULT_MODEL
        self._api_key = api_key or os.environ.get("GOOGLE_API_KEY") or os.environ.get("GEMINI_API_KEY")
        self._max_retries = max_retries
        self._model = None
        
        if not self._api_key:
            raise APIKeyMissingError(
                provider="gemini",
                message="GOOGLE_API_KEY or GEMINI_API_KEY environment variable not set. "
                        "Get your key at https://aistudio.google.com/apikey",
            )
    
    @property
    def name(self) -> str:
        return "gemini"
    
    @property
    def model(self) -> str:
        return self._model_name
    
    @property
    def supports_json_mode(self) -> bool:
        return True
    
    def _get_model(self) -> Any:
        """Lazy-load the Gemini model."""
        if self._model is None:
            try:
                import google.generativeai as genai
                genai.configure(api_key=self._api_key)
                self._model = genai.GenerativeModel(self._model_name)
            except ImportError:
                raise LLMProviderError(
                    provider="gemini",
                    message="google-generativeai package not installed. Run: pip install google-generativeai",
                )
        return self._model
    
    def complete(
        self,
        prompt: str,
        system: str | None = None,
        json_mode: bool = False,
        temperature: float = 0.0,
        max_tokens: int = 4096,
    ) -> LLMResponse:
        """Generate completion using Gemini."""
        
        model = self._get_model()
        
        # Build the full prompt (Gemini uses a single prompt, not separate system)
        full_prompt_parts = []
        
        if system:
            full_prompt_parts.append(f"System: {system}")
        
        if json_mode:
            full_prompt_parts.append("Important: Respond with valid JSON only. No markdown code blocks, no explanation outside JSON.")
        
        full_prompt_parts.append(prompt)
        full_prompt = "\n\n".join(full_prompt_parts)
        
        # Generation config
        try:
            import google.generativeai as genai
            generation_config = genai.GenerationConfig(
                temperature=temperature,
                max_output_tokens=max_tokens,
            )
            
            # Add JSON response type if supported and requested
            if json_mode:
                generation_config.response_mime_type = "application/json"
        except Exception:
            generation_config = None
        
        start_time = time.time()
        last_error = None
        
        for attempt in range(self._max_retries + 1):
            try:
                if generation_config:
                    response = model.generate_content(
                        full_prompt,
                        generation_config=generation_config,
                    )
                else:
                    response = model.generate_content(full_prompt)
                
                latency_ms = (time.time() - start_time) * 1000
                
                # Extract token counts if available
                input_tokens = None
                output_tokens = None
                if hasattr(response, 'usage_metadata') and response.usage_metadata:
                    input_tokens = getattr(response.usage_metadata, 'prompt_token_count', None)
                    output_tokens = getattr(response.usage_metadata, 'candidates_token_count', None)
                
                # Get finish reason
                finish_reason = None
                if response.candidates:
                    finish_reason = str(response.candidates[0].finish_reason)
                
                return LLMResponse(
                    content=response.text,
                    model=self._model_name,
                    provider="gemini",
                    input_tokens=input_tokens,
                    output_tokens=output_tokens,
                    latency_ms=latency_ms,
                    finish_reason=finish_reason,
                )
                
            except Exception as e:
                last_error = e
                error_str = str(e).lower()
                
                # Check for rate limit / quota
                if "quota" in error_str or "rate" in error_str or "429" in error_str:
                    if attempt < self._max_retries:
                        wait_time = 2 ** attempt
                        logger.warning(f"Rate limited, retrying in {wait_time}s...")
                        time.sleep(wait_time)
                        continue
                    raise RateLimitError(
                        provider="gemini",
                        message="Rate limit or quota exceeded",
                        original_error=e,
                    )
                
                # Check for server errors
                if "server" in error_str or "503" in error_str or "500" in error_str:
                    if attempt < self._max_retries:
                        wait_time = 2 ** attempt
                        logger.warning(f"Server error, retrying in {wait_time}s...")
                        time.sleep(wait_time)
                        continue
                    raise ProviderUnavailableError(
                        provider="gemini",
                        message="API is unavailable. Try again later.",
                        original_error=e,
                    )
                
                # Safety filter blocks
                if "safety" in error_str or "blocked" in error_str:
                    raise LLMProviderError(
                        provider="gemini",
                        message="Response blocked by safety filters. Try rephrasing the prompt.",
                        original_error=e,
                    )
                
                # Other errors - don't retry
                raise LLMProviderError(
                    provider="gemini",
                    message=str(e),
                    original_error=e,
                )
        
        raise LLMProviderError(
            provider="gemini",
            message=f"Failed after {self._max_retries + 1} attempts",
            original_error=last_error,
        )
