# Changelog

All notable changes to Schema Travels will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).


## [1.0.0] - 2025-02-25

### Added
- **Core Analysis**
  - PostgreSQL query log parsing
  - MySQL slow query log parsing
  - SQL DDL schema parsing with sqlglot
  - Hot join detection and ranking
  - Co-access pattern analysis
  - Read/write ratio tracking per table
  - Table statistics collection

- **AI Recommendations**
  - Claude API integration for intelligent recommendations
  - EMBED vs REFERENCE decision making
  - Confidence scoring (0-100%)
  - Detailed reasoning for each decision
  - Warning detection for edge cases

- **Schema Generation**
  - MongoDB collection schema generation
  - Embedded document definitions
  - Reference relationship mapping
  - Sample document generation
  - JSON Schema output format

- **Migration Simulation**
  - Storage impact estimation
  - Query latency projection
  - Cost comparison (source vs target)
  - Configurable cost models

- **CLI Interface**
  - `analyze` command for full analysis
  - `report` command for viewing results
  - `history` command for listing analyses
  - `simulate` command for impact estimation
  - `config` command for checking setup
  - Rich terminal output with tables

- **Persistence**
  - SQLite storage for analysis history
  - Query result caching
  - Recommendation persistence

- **Visualization**
  - HTML interactive reports
  - Mermaid ER diagram generation
  - Console tree view

- **Testing Tools**
  - Synthetic workload generator
  - E-commerce workload patterns
  - OLTP workload patterns
  - Analytics workload patterns

### Technical
- Python 3.10+ support
- Pydantic for configuration
- Click for CLI
- Rich for terminal formatting
- 31% test coverage (initial)

---

## [0.1.0] - 2025-02-25

### Added
- Initial project structure
- Basic proof of concept
- Core module organization

