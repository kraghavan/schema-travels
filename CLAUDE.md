# CLAUDE.md

> This file provides context for Claude (AI) when working on this codebase.

## Project Overview

**Schema Travels** is a CLI tool that analyzes SQL database query patterns and recommends optimal NoSQL (MongoDB/DynamoDB) schema designs. It supports multiple LLM providers for AI recommendations.

**Current Version:** 2.3.0

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                         CLI (Click)                             │
│                     schema-travels analyze                      │
│         --target [mongodb|dynamodb] --provider [...]            │
└─────────────────────┬───────────────────────────────────────────┘
                      │
        ┌─────────────┼─────────────┐
        ▼             ▼             ▼
┌───────────┐  ┌───────────┐  ┌─────────────────┐
│ Collector │  │ Analyzer  │  │   Recommender   │
│           │  │           │  │                 │
│ • Logs    │  │ • HotJoins│  │ • Advisor       │
│ • Schema  │  │ • Mutation│  │   (provider-    │
│           │  │ • Pattern │  │    agnostic)    │
└─────┬─────┘  └─────┬─────┘  └────────┬────────┘
      │              │                 │
      └──────────────┼─────────────────┘
                     ▼
┌─────────────────────────────────────────────────────────────────┐
│                    LLM Provider Layer (v2.3.0)                  │
│  ┌──────────┐ ┌──────────┐ ┌──────────┐ ┌──────────┐ ┌────────┐ │
│  │  Claude  │ │  OpenAI  │ │  Gemini  │ │   Grok   │ │ Ollama │ │
│  └──────────┘ └──────────┘ └──────────┘ └──────────┘ └────────┘ │
└─────────────────────────────────────────────────────────────────┘
                     │
              ┌──────┴──────┐
              ▼             ▼
        ┌───────────┐  ┌───────────┐
        │ Simulator │  │Persistence│
        │           │  │ (SQLite)  │
        └───────────┘  └───────────┘
```

## Key Modules

| Module | Purpose | Key Files |
|--------|---------|-----------|
| `llm/` | LLM provider abstraction (v2.3.0) | `provider.py`, `factory.py`, `providers/*.py` |
| `collector/` | Parse DB logs & SQL schemas | `log_parser.py`, `schema_parser.py` |
| `analyzer/` | Detect patterns (joins, mutations, columns) | `hot_joins.py`, `mutations.py`, `pattern_analyzer.py` |
| `recommender/` | AI recommendations + schema generation | See detailed table below |
| `simulator/` | Estimate migration impact | `cost_model.py`, `simulator.py` |
| `persistence/` | SQLite storage | `database.py`, `repository.py` |
| `cli/` | Command-line interface | `main.py` |

### LLM Module (v2.3.0)

| File | Purpose |
|------|---------|
| `provider.py` | `LLMProvider` protocol, `LLMResponse`, exceptions |
| `factory.py` | `get_provider()`, `list_providers()`, `get_provider_info()` |
| `providers/claude.py` | Anthropic Claude implementation |
| `providers/openai.py` | OpenAI GPT implementation |
| `providers/gemini.py` | Google Gemini implementation |
| `providers/grok.py` | xAI Grok implementation (OpenAI-compatible) |
| `providers/ollama.py` | Ollama local models implementation |

### Recommender Module

| File | Purpose |
|------|---------|
| `advisor.py` | Provider-agnostic AI advisor (v2.3.0) |
| `claude_advisor.py` | Backwards-compatible alias → Advisor |
| `schema_generator.py` | Generate target schemas |
| `cache.py` | Hash-based caching |
| `query_rewriter.py` | SQL → MongoDB rewrites |
| `models.py` | Core data models |
| `dynamodb_models.py` | DynamoDB design models |
| `dynamodb_designer.py` | Algorithmic DynamoDB design |
| `dynamodb_output.py` | JSON/Terraform/NoSQL Workbench |
| `dynamodb_review.py` | Apply AI review to design |

## LLM Provider Usage

### Provider Selection

```python
from schema_travels.llm import get_provider, list_providers

# List available providers
providers = list_providers()  # ['claude', 'openai', 'gemini', 'grok', 'ollama']

# Get a provider
provider = get_provider("openai", model="gpt-4o")
response = provider.complete("Analyze this schema...")
```

### Using Advisor

```python
from schema_travels.recommender import Advisor

# Provider-agnostic advisor
advisor = Advisor(provider_name="openai", model="gpt-4o")

# MongoDB recommendations
recs = advisor.get_recommendations(schema, analysis, TargetDatabase.MONGODB)

# DynamoDB review
review = advisor.review_dynamodb_design(local_design, analysis, schema)
```

### Backwards Compatibility

```python
# Old code still works
from schema_travels.recommender import ClaudeAdvisor

advisor = ClaudeAdvisor()  # Uses Claude by default
# ClaudeAdvisor is now an alias for Advisor
```

## Provider Configuration

| Provider | Default Model | API Key Env Var | Extra Config |
|----------|--------------|-----------------|--------------|
| Claude | `claude-sonnet-4-20250514` | `ANTHROPIC_API_KEY` | — |
| OpenAI | `gpt-4o` | `OPENAI_API_KEY` | — |
| Gemini | `gemini-2.0-flash` | `GOOGLE_API_KEY` or `GEMINI_API_KEY` | — |
| Grok | `grok-3` | `XAI_API_KEY` or `GROK_API_KEY` | — |
| Ollama | `llama3.1:8b` | None | `OLLAMA_HOST` |

## Target Database Flows

### MongoDB Flow

```python
# LLM acts as ARCHITECT — designs the schema
advisor = Advisor(provider_name="openai")
recommendations = advisor.get_recommendations(schema, analysis, TargetDatabase.MONGODB)
# Returns: [SchemaRecommendation(decision=EMBED/REFERENCE, ...)]

generator = SchemaGenerator(schema, analysis, recommendations)
target_schema = generator.generate(TargetDatabase.MONGODB)
```

### DynamoDB Flow (v2.0.0+)

```python
# 1. Local algorithmic design
designer = DynamoDBDesigner(mode=DesignMode.AUTO)
local_design = designer.design(table_stats, access_patterns, ...)

# 2. LLM acts as REVIEWER — validates the design
advisor = Advisor(provider_name="gemini")
review = advisor.review_dynamodb_design(local_design, analysis, schema)
# Returns: DynamoDBReview(approved=True, entity_changes=[], gsi_changes=[], ...)

# 3. Apply review suggestions
from schema_travels.recommender.dynamodb_review import apply_review
final_design = apply_review(local_design, review)

# 4. Generate output
generator = SchemaGenerator(schema, analysis, dynamodb_review=review)
target_schema = generator.generate(TargetDatabase.DYNAMODB)
```

## CLI Options

### Basic Usage

```bash
# MongoDB (default provider: Claude)
schema-travels analyze \
    --logs-dir ./logs \
    --schema-file ./schema.sql \
    --output results.json

# With different provider
schema-travels analyze \
    --provider openai \
    --model gpt-4o \
    --logs-dir ./logs \
    --schema-file ./schema.sql

# Local Ollama
schema-travels analyze \
    --provider ollama \
    --model llama3.1:70b \
    --logs-dir ./logs \
    --schema-file ./schema.sql
```

### DynamoDB-Specific Options

```bash
schema-travels analyze \
    --target dynamodb \
    --provider gemini \
    --dynamodb-mode auto \
    --dynamodb-output terraform \
    --logs-dir ./logs \
    --schema-file ./schema.sql
```

### List Providers

```bash
schema-travels providers
```

## Environment Variables

| Variable | Description | Default |
|----------|-------------|---------|
| `ANTHROPIC_API_KEY` | Claude API key | Required for Claude |
| `OPENAI_API_KEY` | OpenAI API key | Required for OpenAI/Grok |
| `GOOGLE_API_KEY` | Google AI API key | Required for Gemini |
| `XAI_API_KEY` | xAI API key | Required for Grok |
| `SCHEMA_TRAVELS_PROVIDER` | Default LLM provider | `claude` |
| `SCHEMA_TRAVELS_MODEL` | Default model | Provider-specific |
| `OLLAMA_HOST` | Ollama server URL | `http://localhost:11434` |
| `DATABASE_PATH` | SQLite DB location | `~/.schema-travels/schema_travels.db` |
| `LOG_LEVEL` | Logging verbosity | `INFO` |

## File Locations

```
~/.schema-travels/
├── schema_travels.db     # Analysis history (SQLite)
└── cache/
    ├── index.json        # Cache index with metadata
    ├── <hash>.json       # Cached recommendations (includes provider/model)
    └── <hash>_review.json # Cached DynamoDB AI reviews
```

## Development Commands

```bash
# Install in dev mode
pip install -e ".[dev]"

# Install with all providers
pip install -e ".[dev,all-providers]"

# Run tests
pytest

# Run specific test
pytest tests/test_llm_providers.py -v

# Lint
ruff check src/

# Format
ruff format src/

# Type check
mypy src/schema_travels
```

## Testing Strategy

| Test File | Purpose |
|-----------|---------|
| `test_llm_providers.py` | LLM provider unit tests (v2.3.0) |
| `test_dynamodb_designer.py` | Unit tests for clustering algorithm |
| `test_dynamodb_output.py` | Output formatter tests |
| `test_dynamodb_review.py` | AI review + apply_review tests |
| `test_selected_columns.py` | SELECT clause extraction tests |
| `test_analyzer.py` | Pattern analysis tests |

## Common Tasks

### Adding a new LLM provider

1. Create provider in `src/schema_travels/llm/providers/{name}.py`:
   ```python
   class NewProvider:
       def __init__(self, model: str | None = None, **kwargs):
           self._model = model or "default-model"
       
       @property
       def provider_name(self) -> str:
           return "newprovider"
       
       @property
       def model(self) -> str:
           return self._model
       
       def complete(self, prompt: str, **kwargs) -> str:
           # Implementation
           pass
       
       def chat(self, messages: list[dict], **kwargs) -> str:
           # Implementation
           pass
   ```

2. Register in `llm/factory.py`:
   ```python
   _PROVIDER_REGISTRY = {
       "newprovider": {
           "class": "schema_travels.llm.providers.newprovider:NewProvider",
           "default_model": "default-model",
           "env_vars": ["NEW_API_KEY"],
           "install": "pip install new-sdk",
       },
       # ...
   }
   ```

3. Add tests in `tests/test_llm_providers.py`

4. Update documentation

### Adding a new target database (e.g., Cassandra)

1. Add to `recommender/models.py:TargetDatabase` enum
2. Create design models in `recommender/{target}_models.py`
3. Create designer in `recommender/{target}_designer.py`
4. Add generator method in `schema_generator.py`
5. Update CLI in `cli/main.py`

### Modifying DynamoDB thresholds

Edit constants in `recommender/dynamodb_designer.py`:
```python
CO_ACCESS_THRESHOLD = 0.70
HOT_JOIN_THRESHOLD = 3
GSI_FREQUENCY_THRESHOLD = 5
MAX_GSIS = 5
```

### Invalidating caches

```bash
# Clear all caches via CLI
schema-travels analyze --clear-cache ...

# Or manually
rm -rf ~/.schema-travels/cache/
```

### Bumping cache version

Edit `recommender/cache.py`:
```python
RECOMMENDATION_VERSION = "2.3.0"  # Bump this
```

## Dependencies

Core:
- `sqlglot` — SQL parsing
- `click` — CLI framework
- `rich` — Terminal formatting
- `anthropic` — Claude API
- `httpx` — HTTP client (Ollama)
- `pydantic` — Configuration

Optional Providers:
- `openai` — OpenAI/Grok API
- `google-generativeai` — Gemini API

Dev:
- `pytest` — Testing
- `ruff` — Linting/formatting
- `mypy` — Type checking
