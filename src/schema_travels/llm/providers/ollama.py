"""
Ollama provider for local models.

Requires: Ollama installed and running (https://ollama.ai)
Optional env vars: OLLAMA_HOST (default: http://localhost:11434)
"""

import os
import time
import logging
from typing import Any

from schema_travels.llm.provider import (
    LLMResponse,
    ProviderUnavailableError,
    LLMProviderError,
)

logger = logging.getLogger(__name__)


class OllamaProvider:
    """Ollama provider for local LLM inference."""
    
    DEFAULT_MODEL = "llama3.1:8b"
    DEFAULT_HOST = "http://localhost:11434"
    
    def __init__(
        self,
        model: str | None = None,
        host: str | None = None,
        max_retries: int = 2,
        timeout: float = 120.0,
    ):
        """
        Initialize Ollama provider.
        
        Args:
            model: Model to use (default: llama3.1:8b)
            host: Ollama server URL (default: http://localhost:11434 or OLLAMA_HOST)
            max_retries: Number of retries on transient errors
            timeout: Request timeout in seconds
        """
        self._model = model or self.DEFAULT_MODEL
        self._host = host or os.environ.get("OLLAMA_HOST", self.DEFAULT_HOST)
        self._max_retries = max_retries
        self._timeout = timeout
        
        # Remove trailing slash if present
        self._host = self._host.rstrip("/")
    
    @property
    def name(self) -> str:
        return "ollama"
    
    @property
    def model(self) -> str:
        return self._model
    
    @property
    def supports_json_mode(self) -> bool:
        # Some Ollama models support JSON mode, but not all
        # We'll add JSON instructions to the prompt instead
        return False
    
    def _check_server(self) -> bool:
        """Check if Ollama server is running."""
        try:
            import httpx
            response = httpx.get(f"{self._host}/api/version", timeout=5.0)
            return response.status_code == 200
        except Exception:
            return False
    
    def _ensure_model_available(self) -> None:
        """Check if the model is available, provide helpful error if not."""
        try:
            import httpx
            response = httpx.get(f"{self._host}/api/tags", timeout=10.0)
            if response.status_code == 200:
                data = response.json()
                available_models = [m["name"] for m in data.get("models", [])]
                
                # Check for exact match or prefix match (e.g., "llama3.1:8b" matches "llama3.1:8b-instruct-q4_0")
                model_base = self._model.split(":")[0]
                if not any(self._model in m or model_base in m for m in available_models):
                    if available_models:
                        raise LLMProviderError(
                            provider="ollama",
                            message=f"Model '{self._model}' not found. Available models: {', '.join(available_models[:5])}. "
                                    f"Pull it with: ollama pull {self._model}",
                        )
                    else:
                        raise LLMProviderError(
                            provider="ollama",
                            message=f"No models found. Pull one with: ollama pull {self._model}",
                        )
        except LLMProviderError:
            raise
        except Exception as e:
            logger.debug(f"Could not check available models: {e}")
    
    def complete(
        self,
        prompt: str,
        system: str | None = None,
        json_mode: bool = False,
        temperature: float = 0.0,
        max_tokens: int = 4096,
    ) -> LLMResponse:
        """Generate completion using Ollama."""
        
        try:
            import httpx
        except ImportError:
            raise LLMProviderError(
                provider="ollama",
                message="httpx package not installed. Run: pip install httpx",
            )
        
        # Check server is running
        if not self._check_server():
            raise ProviderUnavailableError(
                provider="ollama",
                message=f"Ollama server not running at {self._host}. "
                        "Start it with: ollama serve",
            )
        
        # Build the prompt
        full_prompt_parts = []
        
        if system:
            full_prompt_parts.append(f"System: {system}")
        
        # Local models need explicit JSON instructions
        if json_mode:
            full_prompt_parts.append(
                "IMPORTANT: You must respond with valid JSON only. "
                "No markdown code blocks (no ```), no explanation, no text before or after. "
                "Start your response with { and end with }."
            )
        
        full_prompt_parts.append(prompt)
        full_prompt = "\n\n".join(full_prompt_parts)
        
        request_data = {
            "model": self._model,
            "prompt": full_prompt,
            "stream": False,
            "options": {
                "temperature": temperature,
                "num_predict": max_tokens,
            },
        }
        
        # Some models support format: json
        if json_mode:
            request_data["format"] = "json"
        
        start_time = time.time()
        last_error = None
        
        for attempt in range(self._max_retries + 1):
            try:
                response = httpx.post(
                    f"{self._host}/api/generate",
                    json=request_data,
                    timeout=self._timeout,
                )
                
                if response.status_code == 404:
                    self._ensure_model_available()
                    raise LLMProviderError(
                        provider="ollama",
                        message=f"Model '{self._model}' not found",
                    )
                
                response.raise_for_status()
                data = response.json()
                
                latency_ms = (time.time() - start_time) * 1000
                
                # Extract token counts if available
                input_tokens = data.get("prompt_eval_count")
                output_tokens = data.get("eval_count")
                
                # Clean up response if JSON mode
                content = data.get("response", "")
                if json_mode:
                    content = self._extract_json(content)
                
                return LLMResponse(
                    content=content,
                    model=self._model,
                    provider="ollama",
                    input_tokens=input_tokens,
                    output_tokens=output_tokens,
                    latency_ms=latency_ms,
                    finish_reason=data.get("done_reason", "stop"),
                )
                
            except httpx.TimeoutException:
                last_error = TimeoutError(f"Request timed out after {self._timeout}s")
                if attempt < self._max_retries:
                    logger.warning(f"Timeout, retrying ({attempt + 1}/{self._max_retries})...")
                    continue
                raise LLMProviderError(
                    provider="ollama",
                    message=f"Request timed out after {self._timeout}s. "
                            "Try a smaller model or increase timeout.",
                    original_error=last_error,
                )
                
            except httpx.ConnectError as e:
                raise ProviderUnavailableError(
                    provider="ollama",
                    message=f"Cannot connect to Ollama at {self._host}. "
                            "Start it with: ollama serve",
                    original_error=e,
                )
                
            except LLMProviderError:
                raise
                
            except Exception as e:
                last_error = e
                
                # Check for connection errors
                error_str = str(e).lower()
                if "connection" in error_str or "refused" in error_str:
                    raise ProviderUnavailableError(
                        provider="ollama",
                        message=f"Cannot connect to Ollama at {self._host}",
                        original_error=e,
                    )
                
                # Other errors - don't retry
                raise LLMProviderError(
                    provider="ollama",
                    message=str(e),
                    original_error=e,
                )
        
        raise LLMProviderError(
            provider="ollama",
            message=f"Failed after {self._max_retries + 1} attempts",
            original_error=last_error,
        )
    
    def _extract_json(self, content: str) -> str:
        """Extract JSON from response, handling markdown code blocks."""
        content = content.strip()
        
        # Remove markdown code blocks
        if content.startswith("```json"):
            content = content[7:]
        elif content.startswith("```"):
            content = content[3:]
        
        if content.endswith("```"):
            content = content[:-3]
        
        content = content.strip()
        
        # Find JSON object boundaries
        start = content.find("{")
        end = content.rfind("}") + 1
        
        if start != -1 and end > start:
            content = content[start:end]
        
        return content
    
    def list_models(self) -> list[str]:
        """List available models on the Ollama server."""
        try:
            import httpx
            response = httpx.get(f"{self._host}/api/tags", timeout=10.0)
            if response.status_code == 200:
                data = response.json()
                return [m["name"] for m in data.get("models", [])]
        except Exception as e:
            logger.warning(f"Failed to list models: {e}")
        return []
