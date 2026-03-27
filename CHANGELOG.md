# Changelog

All notable changes to Schema Travels will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Planned
- Web UI dashboard
- Real-time log streaming
- GraphRAG-based semantic schema migration reasoning (v3.0.0)

---

## [2.0.1] - 2025-03-27

### Added
- **DynamoDB AI Review Workflow**
  - `ClaudeAdvisor.review_dynamodb_design()` — AI reviews local design
  - `DynamoDBReview` model — structured review with entity/GSI changes
  - `apply_review()` helper — merges AI suggestions into design
  - `summarize_review_changes()` — human-readable review summary

- **Consistent AI Flow**
  - MongoDB: Claude as architect (designs schema)
  - DynamoDB: Claude as reviewer (validates local design)
  - Both targets now use AI when `--use-ai` (default)

### Fixed
- **Table Alias Resolution** (`mutations.py`)
  - `SELECT u.* FROM users u` now correctly tracks `users` table
  - New `_extract_tables_with_aliases()` method
  - Alias mapping passed to column extraction methods

- **DynamoDB Output Cleanup**
  - No MongoDB-style EMBED/REFERENCE in DynamoDB output
  - `recommendations=[]` for DynamoDB (design in metadata)
  - Console skips recommendations table for DynamoDB

### Technical
- New module: `recommender/dynamodb_review.py`
- New models: `DynamoDBReview`, `EntityChange`, `GSIChange`, `GSIChangeAction`, `ReviewChangeType`
- Updated: `claude_advisor.py`, `schema_generator.py`, `mutations.py`, `cli/main.py`
- AI reviews cached separately with `_review` suffix

---

## [2.0.0] - 2025-03-26

### Added
- **DynamoDB Single-Table Design Support**
  - `--target dynamodb` option for DynamoDB schema generation
  - `--dynamodb-mode [auto|single|multi]` for design mode control
  - `--dynamodb-output [json|terraform|nosql_workbench]` for output formats

- **Access Cluster Analysis**
  - Union-Find algorithm groups co-accessed tables
  - `CO_ACCESS_THRESHOLD = 0.70` for single-table candidates
  - Automatic PK/SK pattern generation per entity

- **GSI Optimization**
  - SELECT clause extraction for projection type decisions
  - `KEYS_ONLY` / `INCLUDE` / `ALL` based on actual column usage
  - Frequently filtered columns → GSI candidates
  - Max 5 GSIs per table (DynamoDB limit)

- **Output Formats**
  - JSON: Full design with entities, GSIs, access patterns
  - Terraform HCL: Ready-to-deploy infrastructure code
  - NoSQL Workbench: Import-ready JSON format

- **New Modules**
  - `recommender/dynamodb_models.py` — Pydantic models for DynamoDB
  - `recommender/dynamodb_designer.py` — Algorithmic design engine
  - `recommender/dynamodb_output.py` — Output formatters

- **Visualization**
  - `tools/visualize_schema.py` updated for DynamoDB
  - Orange/black AWS-themed HTML output
  - Entity grid with PK/SK patterns
  - GSI table with projection types

### Changed
- `MutationAnalyzer` now tracks `selected_columns` and `select_star_tables`
- `SchemaGenerator` accepts DynamoDB-specific options
- Cache keys include target database type

### Technical
- New enum: `DesignMode` (SINGLE_TABLE, MULTI_TABLE, AUTO)
- New enum: `ProjectionType` (KEYS_ONLY, INCLUDE, ALL)
- New classes: `DynamoDBDesign`, `EntityDefinition`, `GSIDefinition`, `AccessCluster`
- `DynamoDBDesigner` with Union-Find clustering

---

## [1.3.0] - 2025-03-07

### Added
- **Query Rewrite Examples** (`--show-rewrites`)
  - Automatic SQL → MongoDB query rewrite generation
  - Rule-based templates for each relationship decision
  - Shows concrete before/after code for developers
  - No API call required — instant, deterministic output

- **Four Rewrite Patterns**
  - `EMBED`: JOIN → embedded document lookup (`findOne`)
  - `REFERENCE`: JOIN → two-query pattern or `$lookup` aggregation
  - `SEPARATE`: Independent queries with index recommendations
  - `BUCKET`: Time-series bucketing pattern for high-volume data

- **Confidence Threshold** (`--min-confidence`)
  - Filter recommendations below a confidence threshold
  - Example: `--min-confidence 0.8` shows only ≥80% confidence
  - Applies to both recommendations table and query rewrites

- **Confidence Color Coding**
  - Recommendations table now color-codes confidence scores
  - 🟢 Green (≥85%): High confidence
  - 🟡 Yellow (70-84%): Medium confidence  
  - 🔴 Red (<70%): Low confidence — review carefully

### Technical
- New module: `recommender/query_rewriter.py`
- New dataclasses: `QueryRewriteExample`, `RewriteResult`
- New function: `generate_rewrites()` — main entry point
- Dispatch dict `_REWRITE_DISPATCH` keyed on `RelationshipDecision` enum
- CLI flags: `--min-confidence FLOAT`, `--show-rewrites`

---

## [1.2.0] - 2025-02-27

### Added
- **Cache Modes** (`--cache-mode`)
  - `relaxed` (default): Ignores small log changes
  - `strict`: Any change in query counts invalidates cache

---

## [1.1.0] - 2025-02-26

### Added
- **Recommendation Caching**
  - Hash-based caching for deterministic, reproducible results
  - Same schema + logs = same recommendations (when cached)
  - Cache stored in `~/.schema-travels/cache/`
  - Version tracking to auto-invalidate on logic changes
  - `--no-cache` flag to bypass cache and get fresh AI recommendations
  - `--clear-cache` flag to invalidate all cached recommendations

- **Cache Modes** (`--cache-mode`)
  - `relaxed` (default): Ignores small log changes
  - `strict`: Any change in query counts invalidates cache

- **Cache Comparison**
  - Compare recommendations between different runs
  - Detect decision changes, confidence shifts, added/removed relationships
  - `cache.compare(hash1, hash2)` API for programmatic comparison

- **Improved API Key Validation**
  - Clear, actionable error message when API key is missing
  - Validates key format before making API calls
  - Suggests `--no-ai` flag for rule-based only analysis

### Changed
- `APIKeyNotConfiguredError` now displays a formatted box with setup instructions
- Cache index tracks model version and timestamp for each entry

### Technical
- New module: `recommender/cache.py`
- New enum: `CacheMode` (relaxed, strict)
- `RECOMMENDATION_VERSION` constant for cache invalidation control
- `compute_input_hash()` for deterministic input hashing

---

## [1.0.0] - 2025-02-25

### Added
- **Core Analysis**
  - PostgreSQL query log parsing
  - MySQL slow query log parsing
  - SQL DDL schema parsing with sqlglot
  - Hot join detection and ranking
  - Co-access pattern analysis
  - Read/write ratio tracking per table

- **AI Recommendations**
  - Claude API integration for intelligent recommendations
  - EMBED vs REFERENCE decision making
  - Confidence scoring (0-100%)
  - Detailed reasoning for each decision

- **Schema Generation**
  - MongoDB collection schema generation
  - Embedded document definitions
  - Reference relationship mapping

- **Migration Simulation**
  - Storage impact estimation
  - Query latency projection
  - Cost comparison (source vs target)

- **CLI Interface**
  - `analyze` command for full analysis
  - `report` command for viewing results
  - `history` command for listing analyses
  - Rich terminal output with tables

- **Persistence**
  - SQLite storage for analysis history
  - Query result caching

- **Visualization**
  - HTML interactive reports
  - Mermaid ER diagram generation

- **Testing Tools**
  - Synthetic workload generator
  - E-commerce/OLTP/Analytics patterns

---

## [0.1.0] - 2025-02-25

### Added
- Initial project structure
- Basic proof of concept

---

[2.0.1]: https://github.com/kraghavan/schema-travels/compare/v2.0.0...v2.0.1
[2.0.0]: https://github.com/kraghavan/schema-travels/compare/v1.3.0...v2.0.0
[1.3.0]: https://github.com/kraghavan/schema-travels/compare/v1.2.0...v1.3.0
[1.2.0]: https://github.com/kraghavan/schema-travels/compare/v1.1.0...v1.2.0
[1.1.0]: https://github.com/kraghavan/schema-travels/compare/v1.0.0...v1.1.0
[1.0.0]: https://github.com/kraghavan/schema-travels/releases/tag/v1.0.0
[0.1.0]: https://github.com/kraghavan/schema-travels/releases/tag/v0.1.0
