#!/usr/bin/env python3
"""
Test script for LLM provider abstraction layer.

Run: python test_llm_providers.py
"""

import os
import sys


def test_imports():
    """Test all imports work."""
    print("Testing imports...")
    
    from schema_travels.llm.provider import (
        LLMProvider,
        LLMResponse,
        ProviderName,
        ModelInfo,
        KNOWN_MODELS,
        LLMProviderError,
        APIKeyMissingError,
    )
    print("  ✓ provider.py imports")
    
    from schema_travels.llm.factory import (
        get_provider,
        list_providers,
        get_provider_info,
        DEFAULT_MODELS,
    )
    print("  ✓ factory.py imports")
    
    from schema_travels.llm import (
        get_provider,
        LLMProvider,
        LLMResponse,
        ProviderName,
    )
    print("  ✓ __init__.py exports")
    
    return True


def test_provider_enum():
    """Test ProviderName enum."""
    print("\nTesting ProviderName enum...")
    
    from schema_travels.llm import ProviderName
    
    assert ProviderName.CLAUDE.value == "claude"
    assert ProviderName.OPENAI.value == "openai"
    assert ProviderName.GEMINI.value == "gemini"
    assert ProviderName.GROK.value == "grok"
    assert ProviderName.OLLAMA.value == "ollama"
    print("  ✓ All providers defined")
    
    return True


def test_known_models():
    """Test KNOWN_MODELS registry."""
    print("\nTesting KNOWN_MODELS...")
    
    from schema_travels.llm import KNOWN_MODELS, get_model_info
    
    # Check some models exist
    assert "claude-sonnet-4-20250514" in KNOWN_MODELS
    assert "gpt-4o" in KNOWN_MODELS
    assert "gemini-2.0-flash" in KNOWN_MODELS
    assert "grok-3" in KNOWN_MODELS
    assert "llama3.1:8b" in KNOWN_MODELS
    print("  ✓ Known models registered")
    
    # Test get_model_info
    info = get_model_info("gpt-4o")
    assert info is not None
    assert info.provider.value == "openai"
    assert info.context_window == 128000
    print("  ✓ get_model_info works")
    
    return True


def test_llm_response():
    """Test LLMResponse model."""
    print("\nTesting LLMResponse...")
    
    from schema_travels.llm import LLMResponse
    
    response = LLMResponse(
        content="Hello, world!",
        model="test-model",
        provider="test",
        input_tokens=10,
        output_tokens=5,
        latency_ms=100.5,
    )
    
    assert response.content == "Hello, world!"
    assert response.total_tokens == 15
    print("  ✓ LLMResponse works")
    
    return True


def test_list_providers():
    """Test list_providers function."""
    print("\nTesting list_providers...")
    
    from schema_travels.llm import list_providers
    
    providers = list_providers()
    assert "claude" in providers
    assert "openai" in providers
    assert "gemini" in providers
    assert "grok" in providers
    assert "ollama" in providers
    assert len(providers) == 5
    print(f"  ✓ Found {len(providers)} providers: {providers}")
    
    return True


def test_get_provider_info():
    """Test get_provider_info function."""
    print("\nTesting get_provider_info...")
    
    from schema_travels.llm import get_provider_info
    
    info = get_provider_info("claude")
    assert info["name"] == "claude"
    assert "ANTHROPIC_API_KEY" in info["env_vars"]
    print(f"  ✓ Claude info: {info}")
    
    info = get_provider_info("ollama")
    assert info["name"] == "ollama"
    assert "ollama.ai" in info["install"]
    print(f"  ✓ Ollama info: {info}")
    
    return True


def test_provider_creation_without_keys():
    """Test provider creation fails gracefully without API keys."""
    print("\nTesting provider creation without API keys...")
    
    from schema_travels.llm import get_provider, APIKeyMissingError
    
    # Save original env vars
    saved_vars = {}
    for key in ["ANTHROPIC_API_KEY", "OPENAI_API_KEY", "GOOGLE_API_KEY", "XAI_API_KEY"]:
        saved_vars[key] = os.environ.pop(key, None)
    
    try:
        # Claude should fail without key
        try:
            get_provider("claude")
            assert False, "Should have raised APIKeyMissingError"
        except APIKeyMissingError as e:
            assert "ANTHROPIC_API_KEY" in str(e)
            print("  ✓ Claude raises APIKeyMissingError")
        
        # OpenAI should fail without key
        try:
            get_provider("openai")
            assert False, "Should have raised APIKeyMissingError"
        except APIKeyMissingError as e:
            assert "OPENAI_API_KEY" in str(e)
            print("  ✓ OpenAI raises APIKeyMissingError")
        
        # Ollama should work without API key (local)
        # But might fail if server not running - that's expected
        from schema_travels.llm.providers.ollama import OllamaProvider
        provider = OllamaProvider(model="test")
        assert provider.name == "ollama"
        print("  ✓ Ollama works without API key")
        
    finally:
        # Restore env vars
        for key, value in saved_vars.items():
            if value is not None:
                os.environ[key] = value
    
    return True


def test_provider_protocol():
    """Test that providers implement LLMProvider protocol."""
    print("\nTesting LLMProvider protocol...")
    
    from schema_travels.llm import LLMProvider
    from schema_travels.llm.providers.ollama import OllamaProvider
    
    provider = OllamaProvider(model="test")
    
    # Check protocol compliance
    assert isinstance(provider, LLMProvider)
    assert hasattr(provider, "name")
    assert hasattr(provider, "model")
    assert hasattr(provider, "supports_json_mode")
    assert hasattr(provider, "complete")
    print("  ✓ OllamaProvider implements LLMProvider")
    
    return True


def test_default_models():
    """Test DEFAULT_MODELS mapping."""
    print("\nTesting DEFAULT_MODELS...")
    
    from schema_travels.llm import DEFAULT_MODELS, ProviderName
    
    assert DEFAULT_MODELS[ProviderName.CLAUDE] == "claude-sonnet-4-20250514"
    assert DEFAULT_MODELS[ProviderName.OPENAI] == "gpt-4o"
    assert DEFAULT_MODELS[ProviderName.GEMINI] == "gemini-2.0-flash"
    assert DEFAULT_MODELS[ProviderName.GROK] == "grok-3"
    assert DEFAULT_MODELS[ProviderName.OLLAMA] == "llama3.1:8b"
    print("  ✓ All default models defined")
    
    return True


def main():
    """Run all tests."""
    print("=" * 60)
    print("LLM Provider Abstraction Layer Tests")
    print("=" * 60)
    
    tests = [
        test_imports,
        test_provider_enum,
        test_known_models,
        test_llm_response,
        test_list_providers,
        test_get_provider_info,
        test_default_models,
        test_provider_protocol,
        test_provider_creation_without_keys,
    ]
    
    passed = 0
    failed = 0
    
    for test in tests:
        try:
            if test():
                passed += 1
        except Exception as e:
            print(f"  ✗ FAILED: {e}")
            import traceback
            traceback.print_exc()
            failed += 1
    
    print("\n" + "=" * 60)
    print(f"Results: {passed} passed, {failed} failed")
    print("=" * 60)
    
    return 0 if failed == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
