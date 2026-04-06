"""Schema Travels - Intelligent SQL to NoSQL Schema Migration.

Analyzes database query patterns to recommend optimal MongoDB schema design.
"""

from importlib.metadata import version, metadata, PackageNotFoundError

try:
    __version__ = version("schema-travels")
    _meta = metadata("schema-travels")
    __author__ = _meta.get("Author", "")
    
except PackageNotFoundError:
    __version__ = "0.0.0.dev0"
    __author__ = "Karthika Raghavan"