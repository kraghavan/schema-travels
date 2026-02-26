"""Configuration management for Schema Travels."""

import os
from pathlib import Path
from functools import lru_cache

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


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

    def validate_api_key(self) -> bool:
        """Check if API key is configured."""
        return bool(self.anthropic_api_key and self.anthropic_api_key != "your-api-key-here")


@lru_cache
def get_settings() -> Settings:
    """Get cached settings instance."""
    return Settings()


# Convenience function for CLI
def get_config() -> Settings:
    """Get configuration (alias for get_settings)."""
    return get_settings()
