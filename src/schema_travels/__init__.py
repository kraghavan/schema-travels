"""Schema Travels - Intelligent SQL to NoSQL Schema Migration.

Analyzes database query patterns to recommend optimal MongoDB schema design.
"""

__version__ = "1.0.0"
__author__ = "Karthik Raghavan"

from schema_travels.config import get_settings

__all__ = ["__version__", "get_settings"]
