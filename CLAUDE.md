# CLAUDE.md

> This file provides context for Claude (AI) when working on this codebase.

## Project Overview

**Schema Travels** is a CLI tool that analyzes SQL database query patterns and recommends optimal NoSQL (MongoDB/DynamoDB) schema designs. It uses Claude AI for intelligent recommendations.

**Current Version:** 2.0.1

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                         CLI (Click)                             │
│                     schema-travels analyze                      │
│                  --target [mongodb|dynamodb]                    │
└─────────────────────┬───────────────────────────────────────────┘
                      │
        ┌─────────────┼─────────────┐
        ▼             ▼             ▼
┌───────────┐  ┌───────────┐  ┌─────────────────┐
│ Collector │  │ Analyzer  │  │   Recommender   │
│           │  │           │  │                 │
│ • Logs    │  │ • HotJoins│  │ MongoDB:        │
│ • Schema  │  │ • Mutation│  │  • Claude recs  │
│           │  │ • Pattern │  │                 │
│           │  │           │  │ DynamoDB:       │
│           │  │           │  │  • Designer     │
│           │  │           │  │  • Claude review│
└─────┬─────┘  └─────┬─────┘  └────────┬────────┘
      │              │                 │
      └──────────────┼─────────────────┘
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
| `analyzer/` | Detect patterns (joins, mutations, columns) | `hot_joins.py`, `mutations.py`, `pattern_analyzer.py` |
| `recommender/` | AI recommendations + schema generation | See detailed table below |
| `simulator/` | Estimate migration impact | `cost_model.py`, `simulator.py` |
| `persistence/` | SQLite storage | `database.py`, `repository.py` |
| `cli/` | Command-line interface | `main.py` |

### Recommender Module (v2.0.1)

| File | Purpose |
|------|---------|
| `claude_advisor.py` | AI: MongoDB recs + DynamoDB review |
| `schema_generator.py` | Generate target schemas |
| `cache.py` | Hash-based caching |
| `query_rewriter.py` | SQL → MongoDB rewrites |
| `models.py` | Core data models |
| `dynamodb_models.py` | DynamoDB design models |
| `dynamodb_designer.py` | Algorithmic DynamoDB design |
| `dynamodb_output.py` | JSON/Terraform/NoSQL Workbench |
| `dynamodb_review.py` | Apply AI review to design |

## Target Database Flows

### MongoDB Flow

```python
# Claude acts as ARCHITECT — designs the schema
advisor = ClaudeAdvisor()
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

# 2. Claude acts as REVIEWER — validates the design
advisor = ClaudeAdvisor()
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
# MongoDB (default)
schema-travels analyze \
    --logs-dir ./logs \
    --schema-file ./schema.sql \
    --output results.json

# DynamoDB
schema-travels analyze \
    --logs-dir ./logs \
    --schema-file ./schema.sql \
    --target dynamodb \
    --output results.json
```

### DynamoDB-Specific Options

```bash
schema-travels analyze \
    --target dynamodb \
    --dynamodb-mode auto \           # auto | single | multi
    --dynamodb-output terraform \    # json | terraform | nosql_workbench
    --logs-dir ./logs \
    --schema-file ./schema.sql
```

### AI Control

```bash
# Skip AI (use local algorithmic design only for DynamoDB)
schema-travels analyze --target dynamodb --no-ai ...

# Force fresh AI analysis (bypass cache)
schema-travels analyze --no-cache ...

# Clear all cached results
schema-travels analyze --clear-cache ...
```

### Cache Modes

```bash
# Relaxed (default): Ignores small log changes
schema-travels analyze --cache-mode relaxed ...

# Strict: Any change invalidates cache
schema-travels analyze --cache-mode strict ...
```

## Key Data Models

### DynamoDB Models (v2.0.0)

```python
class DesignMode(Enum):
    SINGLE_TABLE = "single_table"
    MULTI_TABLE = "multi_table"
    AUTO = "auto"

class ProjectionType(Enum):
    KEYS_ONLY = "KEYS_ONLY"
    INCLUDE = "INCLUDE"
    ALL = "ALL"

class DynamoDBDesign(BaseModel):
    design_mode: DesignMode
    confidence: float
    entities: list[EntityDefinition]
    gsis: list[GSIDefinition]
    clusters: list[AccessCluster]
    orphan_tables: list[str]
    warnings: list[str]
    ai_reviewed: bool = False
    ai_review_applied: bool = False
```

### DynamoDB Review Models (v2.0.1)

```python
class DynamoDBReview(BaseModel):
    approved: bool
    confidence: float
    summary: str
    entity_changes: list[EntityChange]
    gsi_changes: list[GSIChange]
    warnings: list[str]
    suggestions: list[str]
    
    @property
    def has_changes(self) -> bool
    
    @property
    def change_count(self) -> int
```

## Algorithm Details

### DynamoDB Access Clustering

```python
# Union-Find to group co-accessed tables
CO_ACCESS_THRESHOLD = 0.70

for join in join_patterns:
    if join.co_access_ratio > CO_ACCESS_THRESHOLD:
        union(join.table_a, join.table_b)

# Tables in same cluster → single-table candidates
# Orphan tables → separate tables
```

### GSI Selection

```python
GSI_FREQUENCY_THRESHOLD = 5
MAX_GSIS = 5

for column, frequency in filtered_columns.items():
    if frequency >= GSI_FREQUENCY_THRESHOLD:
        if column not in primary_key:
            create_gsi(column)
            
# Projection type based on SELECT patterns:
# - SELECT * used → ALL
# - Specific columns → INCLUDE
# - Only keys → KEYS_ONLY
```

## Key Decision Rules (MongoDB Embed vs Reference)

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
    ├── <hash>.json       # Cached MongoDB recommendations
    └── <hash>_review.json # Cached DynamoDB AI reviews
```

## Development Commands

```bash
# Install in dev mode
pip install -e ".[dev]"

# Run tests
pytest

# Run specific test
pytest tests/test_dynamodb_review.py -v

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
| `test_dynamodb_designer.py` | Unit tests for clustering algorithm |
| `test_dynamodb_output.py` | Output formatter tests |
| `test_dynamodb_review.py` | AI review + apply_review tests |
| `test_selected_columns.py` | SELECT clause extraction tests |
| `test_analyzer.py` | Pattern analysis tests |

## Common Tasks

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
RECOMMENDATION_VERSION = "2.0.1"  # Bump this
```

## Dependencies

Core:
- `sqlglot` — SQL parsing
- `click` — CLI framework
- `rich` — Terminal formatting
- `anthropic` — Claude API
- `pydantic` — Configuration

Dev:
- `pytest` — Testing
- `ruff` — Linting/formatting
- `mypy` — Type checking
