# CLAUDE.md

> This file provides context for Claude (AI) when working on this codebase.

## Project Overview

**Schema Travels** is a CLI tool that analyzes SQL database query patterns and recommends optimal NoSQL (MongoDB/DynamoDB) schema designs. It uses Claude AI for intelligent recommendations.

**Current Version:** 1.1.0

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                         CLI (Click)                              │
│                     schema-travels analyze                       │
└─────────────────────┬───────────────────────────────────────────┘
                      │
        ┌─────────────┼─────────────┐
        ▼             ▼             ▼
┌───────────┐  ┌───────────┐  ┌───────────┐
│ Collector │  │ Analyzer  │  │Recommender│
│           │  │           │  │           │
│ • Logs    │  │ • HotJoins│  │ • Claude  │
│ • Schema  │  │ • Mutation│  │ • Cache   │
└─────┬─────┘  └─────┬─────┘  └─────┬─────┘
      │              │              │
      └──────────────┼──────────────┘
                     ▼
              ┌───────────┐     ┌───────────┐
              │ Simulator │────▶│Persistence│
              │           │     │ (SQLite)  │
              └───────────┘     └───────────┘
```

## Key Modules

| Module | Purpose | Key Files |
|--------|---------|-----------|
| `collector/` | Parse DB logs & SQL schemas | `log_parser.py`, `schema_parser.py` |
| `analyzer/` | Detect patterns (joins, mutations) | `hot_joins.py`, `mutations.py`, `pattern_analyzer.py` |
| `recommender/` | AI recommendations + schema generation | `claude_advisor.py`, `schema_generator.py`, `cache.py` |
| `simulator/` | Estimate migration impact | `cost_model.py`, `simulator.py` |
| `persistence/` | SQLite storage | `database.py`, `repository.py` |
| `cli/` | Command-line interface | `main.py` |

## v1.1.0 Changes

### Recommendation Caching (`recommender/cache.py`)

Ensures reproducible results by caching AI recommendations:

```python
from schema_travels.recommender.cache import compute_input_hash, get_cache

# Compute deterministic hash of inputs
input_hash = compute_input_hash(schema, analysis, target)

# Check cache before calling Claude
cache = get_cache()
cached = cache.get(input_hash)

if cached:
    recommendations = cached  # Use cached (consistent)
else:
    recommendations = claude_advisor.get_recommendations(...)
    cache.put(input_hash, recommendations)  # Cache for next time
```

Key constants:
- `RECOMMENDATION_VERSION = "1.0.0"` — Bump to invalidate all caches

### API Key Validation (`config.py`)

```python
from schema_travels.config import APIKeyNotConfiguredError

# Raises clear error if API key missing
settings.require_api_key()
```

## Development Commands

```bash
# Install in dev mode
pip install -e ".[dev]"

# Run tests
pytest

# Run specific test
pytest tests/test_analyzer.py -v

# Lint
ruff check src/

# Format
ruff format src/

# Type check
mypy src/
```

## Code Conventions

1. **Type hints**: All functions should have type annotations
2. **Docstrings**: Google-style docstrings for public methods
3. **Models**: Use `dataclasses` for data structures, `pydantic` for config
4. **SQL Parsing**: Use `sqlglot` library (supports multiple dialects)
5. **CLI**: Use `click` for commands, `rich` for output formatting

## Data Flow

1. **Input**: User provides `--logs-dir` and `--schema-file`
2. **Collect**: Parse logs → `QueryLog` objects; Parse schema → `SchemaDefinition`
3. **Analyze**: Process queries → `JoinPattern`, `MutationPattern`, `AccessPattern`
4. **Cache Check**: Compute input hash → Check cache for existing recommendations
5. **Recommend**: If not cached, send to Claude API → `SchemaRecommendation` (EMBED/REFERENCE)
6. **Cache Store**: Save recommendations to cache for future runs
7. **Generate**: Build target schema → `TargetSchema` (MongoDB/DynamoDB format)
8. **Simulate**: Estimate impact → `SimulationResult` (storage/latency/cost)
9. **Persist**: Store in SQLite for history/reporting

## Key Decision Rules (Embed vs Reference)

```python
# Rule 1: Unbounded children → REFERENCE
if max_children > 1000:
    decision = "REFERENCE"

# Rule 2: High co-access + low writes + bounded → EMBED  
elif co_access_ratio > 0.7 and write_ratio < 0.3 and max_children < 100:
    decision = "EMBED"

# Rule 3: Child accessed alone frequently → REFERENCE
elif child_solo_ratio > 0.4:
    decision = "REFERENCE"

# Rule 4: High child writes → REFERENCE
elif child_write_ratio > 0.5:
    decision = "REFERENCE"

# Default: REFERENCE (safer)
else:
    decision = "REFERENCE"
```

## Environment Variables

| Variable | Description | Default |
|----------|-------------|---------|
| `ANTHROPIC_API_KEY` | Claude API key | Required for AI |
| `ANTHROPIC_MODEL` | Model to use | `claude-sonnet-4-20250514` |
| `DATABASE_PATH` | SQLite DB location | `~/.schema-travels/schema_travels.db` |
| `LOG_LEVEL` | Logging verbosity | `INFO` |

## File Locations

```
~/.schema-travels/
├── schema_travels.db     # Analysis history (SQLite)
└── cache/
    ├── index.json        # Cache index with metadata
    └── <hash>.json       # Cached recommendations per input hash
```

## Testing Strategy

- **Unit tests**: `tests/test_*.py` - Test individual modules
- **Fixtures**: `tests/conftest.py` - Shared test data
- **Example data**: `examples/` - Sample schema and logs for manual testing

## Common Tasks

### Adding a new database type (e.g., Oracle)

1. Create `collector/oracle_log_parser.py` extending `LogParser`
2. Add Oracle patterns to parse log format
3. Register in `collector/log_parser.py:get_parser()`
4. Add CLI option in `cli/main.py`

### Adding a new target (e.g., Cassandra)

1. Add to `recommender/models.py:TargetDatabase` enum
2. Create generator method in `schema_generator.py`
3. Add cost model config in `simulator/cost_model.py`
4. Update CLI choices in `cli/main.py`

### Modifying recommendation rules

1. Edit rules in `analyzer/pattern_analyzer.py:_evaluate_pair()`
2. For AI-based: Modify prompts in `recommender/claude_advisor.py`
3. **Important**: Bump `RECOMMENDATION_VERSION` in `recommender/cache.py` to invalidate caches

### Invalidating all caches

```python
from schema_travels.recommender.cache import get_cache
cache = get_cache()
cache.invalidate_all()
```

Or bump `RECOMMENDATION_VERSION` in `cache.py`.

## Dependencies

Core:
- `sqlglot` - SQL parsing
- `click` - CLI framework
- `rich` - Terminal formatting
- `anthropic` - Claude API
- `pydantic` - Configuration

Dev:
- `pytest` - Testing
- `ruff` - Linting/formatting
- `mypy` - Type checking