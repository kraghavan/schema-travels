"""Configuration management for Schema Travels."""

import os
import sys
from pathlib import Path
from functools import lru_cache

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class APIKeyNotConfiguredError(Exception):
    """Raised when the Anthropic API key is not configured."""
    
    def __init__(self):
        self.message = """
╭─────────────────────────────────────────────────────────────────────╮
│                    ⚠️  API KEY NOT CONFIGURED                       │
├─────────────────────────────────────────────────────────────────────┤
│                                                                     │
│  Schema Travels requires an Anthropic API key for AI-powered        │
│  schema recommendations.                                            │
│                                                                     │
│  To configure:                                                      │
│                                                                     │
│  Option 1: Set environment variable                                 │
│    export ANTHROPIC_API_KEY=sk-ant-xxxxx                            │
│                                                                     │
│  Option 2: Create .env file in project root                         │
│    echo "ANTHROPIC_API_KEY=sk-ant-xxxxx" > .env                     │
│                                                                     │
│  Get your API key at: https://console.anthropic.com/settings/keys   │
│                                                                     │
│  To skip AI recommendations, use: --no-ai                           │
│                                                                     │
╰─────────────────────────────────────────────────────────────────────╯
"""
        super().__init__(self.message)


class Settings(BaseSettings):
    """Application settings loaded from environment variables."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
    )

    # Anthropic API
    anthropic_api_key: str = Field(default="", description="Anthropic API key")
    anthropic_model: str = Field(
        default="claude-sonnet-4-20250514",
        description="Claude model to use for recommendations",
    )

    # Logging
    log_level: str = Field(default="INFO", description="Logging level")

    # Database
    database_path: str = Field(
        default="~/.schema-travels/schema_travels.db",
        description="Path to SQLite database file",
    )

    # Analysis defaults
    default_target: str = Field(
        default="mongodb",
        description="Default target database type (mongodb, dynamodb)",
    )
    default_db_type: str = Field(
        default="postgres",
        description="Default source database type (postgres, mysql)",
    )

    @property
    def db_path(self) -> Path:
        """Get resolved database path."""
        path = Path(self.database_path).expanduser()
        path.parent.mkdir(parents=True, exist_ok=True)
        return path

    def has_api_key(self) -> bool:
        """Check if API key is configured (does not raise)."""
        return bool(
            self.anthropic_api_key 
            and self.anthropic_api_key.strip() 
            and self.anthropic_api_key != "your-api-key-here"
            and self.anthropic_api_key != "sk-ant-xxxxx"
        )

    def require_api_key(self) -> str:
        """
        Get API key or raise error if not configured.
        
        Returns:
            The API key string
            
        Raises:
            APIKeyNotConfiguredError: If API key is not set
        """
        if not self.has_api_key():
            raise APIKeyNotConfiguredError()
        return self.anthropic_api_key

    def validate_api_key(self) -> bool:
        """Check if API key is configured (legacy method)."""
        return self.has_api_key()


@lru_cache
def get_settings() -> Settings:
    """Get cached settings instance."""
    return Settings()


# Convenience function for CLI
def get_config() -> Settings:
    """Get configuration (alias for get_settings)."""
    return get_settings()
