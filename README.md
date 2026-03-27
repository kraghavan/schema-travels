# Schema Travels

[![PyPI version](https://badge.fury.io/py/schema-travels.svg)](https://badge.fury.io/py/schema-travels)
[![Python 3.10+](https://img.shields.io/badge/python-3.10+-blue.svg)](https://www.python.org/downloads/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

**Schema Travels** analyzes SQL database query patterns and recommends optimal NoSQL schema designs for MongoDB and DynamoDB migrations.

## Features

- 📊 **Query Pattern Analysis** — Parse PostgreSQL/MySQL logs to detect hot joins, access patterns, and read/write ratios
- 🤖 **AI-Powered Recommendations** — Claude AI designs MongoDB schemas and reviews DynamoDB designs
- 🗃️ **DynamoDB Single-Table Design** — Algorithmic clustering with Union-Find, automatic PK/SK patterns, GSI optimization
- 📄 **Multiple Output Formats** — JSON, Terraform HCL, NoSQL Workbench
- 🔄 **SQL → MongoDB Rewrites** — Automatic query rewrite examples
- 💾 **Caching** — Reproducible results with hash-based recommendation caching
- 📈 **Migration Simulation** — Storage and latency impact estimation

## Installation

```bash
pip install schema-travels
```

### 🔑 Review of Schema by AI

So Please remember that Frontier Model Claude and its API key is required to use this tool. 
```
╭─────────────────────────────────────────────────────────────────────╮
│                    ⚠️  API KEY NOT CONFIGURED                       │
├─────────────────────────────────────────────────────────────────────┤
│  Schema Travels requires an Anthropic API key for AI-powered        │
│  schema recommendations.                                            │
│                                                                     │
│  Option 1: export ANTHROPIC_API_KEY=sk-ant-xxxxx                    │
│  Option 2: echo "ANTHROPIC_API_KEY=sk-ant-xxxxx" > .env             │
│                                                                     │
│  Get your API key at: https://console.anthropic.com/settings/keys   │
╰─────────────────────────────────────────────────────────────────────╯

## Quick Start

### MongoDB Migration

```bash
export ANTHROPIC_API_KEY=sk-ant-...

schema-travels analyze \
    --logs-dir ./postgresql_logs \
    --schema-file ./schema.sql \
    --target mongodb \
    --output results.json
```

### DynamoDB Migration

```bash
schema-travels analyze \
    --logs-dir ./postgresql_logs \
    --schema-file ./schema.sql \
    --target dynamodb \
    --dynamodb-output terraform \
    --output results.json
```

## How It Works

### MongoDB Flow

Claude AI acts as **architect** — analyzes your access patterns and designs the schema:

```
SQL Schema + Query Logs → Pattern Analysis → Claude AI → EMBED/REFERENCE Decisions → MongoDB Schema
```

### DynamoDB Flow

Local algorithm designs, Claude AI **reviews**:

```
SQL Schema + Query Logs → Pattern Analysis → DynamoDB Designer → Claude Review → Final Design
                                                    │
                                            Union-Find clustering
                                            PK/SK pattern generation
                                            GSI candidate detection
```

## CLI Options

```bash
schema-travels analyze [OPTIONS]

Options:
  --logs-dir PATH              Directory with query logs [required]
  --schema-file PATH           SQL schema file [required]
  --target [mongodb|dynamodb]  Target database [default: mongodb]
  --output PATH                Output file
  
  # DynamoDB-specific
  --dynamodb-mode [auto|single|multi]     Design mode [default: auto]
  --dynamodb-output [json|terraform|nosql_workbench]  Output format
  
  # AI control
  --no-ai                      Skip AI (DynamoDB: algorithmic only)
  --no-cache                   Bypass recommendation cache
  --clear-cache                Clear all cached results
  --cache-mode [relaxed|strict]  Cache sensitivity
  
  # Filtering
  --min-confidence FLOAT       Filter by confidence threshold
  --show-rewrites              Show SQL → MongoDB query rewrites
```

## Example Output

### MongoDB

```json
{
  "recommendations": [
    {
      "parent_table": "users",
      "child_table": "addresses",
      "decision": "embed",
      "confidence": 0.92,
      "reasoning": "High co-access (87%), bounded cardinality (<10 per user)"
    }
  ],
  "target_schema": {
    "collections": [
      {
        "name": "users",
        "embedded_documents": ["addresses"]
      }
    ]
  }
}
```

### DynamoDB

```json
{
  "target_schema": {
    "metadata": {
      "design_mode": "single_table",
      "confidence": 0.85,
      "dynamodb_design": {
        "table_name": "main_table",
        "partition_key": "PK",
        "sort_key": "SK",
        "entities": [
          {"name": "User", "pk_pattern": "USER#<id>", "sk_pattern": "PROFILE"},
          {"name": "Order", "pk_pattern": "USER#<user_id>", "sk_pattern": "ORDER#<id>"}
        ],
        "gsis": [
          {"name": "GSI1", "pk_attribute": "email", "projection_type": "KEYS_ONLY"}
        ],
        "ai_reviewed": true,
        "ai_review_applied": true
      }
    }
  }
}
```

## Visualization

```bash
# Generate HTML visualization
python tools/visualize_schema.py \
    --input results.json \
    --output schema.html

open schema.html
```

## Documentation

- [ARCHITECTURE.md](./ARCHITECTURE.md) — System design and data flows
- [CHANGELOG.md](./CHANGELOG.md) — Version history
- [CONTRIBUTING.md](./CONTRIBUTING.md) — Development guide
- [TESTING_GUIDE.md](./TESTING_GUIDE.md) — Testing with real and synthetic data
- [CLAUDE.md](./CLAUDE.md) — AI assistant context

## Requirements

- Python 3.10+
- Anthropic API key (for AI recommendations)

## License

MIT License — see [LICENSE](./LICENSE) for details.
