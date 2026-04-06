# Schema Travels

[![PyPI version](https://badge.fury.io/py/schema-travels.svg)](https://badge.fury.io/py/schema-travels)
[![Python 3.10+](https://img.shields.io/badge/python-3.10+-blue.svg)](https://www.python.org/downloads/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

**Schema Travels** analyzes SQL database query patterns and recommends optimal NoSQL schema designs for MongoDB and DynamoDB migrations.

## Features

- 📊 **Query Pattern Analysis** — Parse PostgreSQL/MySQL logs to detect hot joins, access patterns, and read/write ratios
- 🤖 **Multi-Provider AI** — Claude, OpenAI, Gemini, Grok, or local Ollama models
- 🗃️ **DynamoDB Single-Table Design** — Algorithmic clustering with Union-Find, automatic PK/SK patterns, GSI optimization
- 📄 **Multiple Output Formats** — JSON, Terraform HCL, NoSQL Workbench
- 🔄 **SQL → MongoDB Rewrites** — Automatic query rewrite examples
- 💾 **Caching** — Reproducible results with hash-based recommendation caching
- 📈 **Migration Simulation** — Storage and latency impact estimation

## Installation

```bash
# Core (includes Claude support)
pip install schema-travels

# With OpenAI support
pip install schema-travels[openai]

# With Google Gemini support
pip install schema-travels[gemini]

# All cloud providers
pip install schema-travels[all-providers]
```

## LLM Providers (v2.3.0)

| Provider | Default Model | API Key | Install |
|----------|--------------|---------|---------|
| **Claude** | `claude-sonnet-4-20250514` | `ANTHROPIC_API_KEY` | Built-in |
| **OpenAI** | `gpt-4o` | `OPENAI_API_KEY` | `[openai]` |
| **Gemini** | `gemini-2.0-flash` | `GOOGLE_API_KEY` | `[gemini]` |
| **Grok** | `grok-3` | `XAI_API_KEY` | `[openai]` |
| **Ollama** | `llama3.1:8b` | None (local) | Built-in |

```bash
# List available providers
schema-travels providers
```

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

### Using Different LLM Providers

```bash
# OpenAI GPT-4o
export OPENAI_API_KEY=sk-...
schema-travels analyze --provider openai --logs-dir ./logs --schema-file ./schema.sql

# Google Gemini
export GOOGLE_API_KEY=...
schema-travels analyze --provider gemini --model gemini-2.5-pro ...

# xAI Grok
export XAI_API_KEY=...
schema-travels analyze --provider grok ...

# Local Ollama (free, private)
ollama serve  # Start Ollama server
schema-travels analyze --provider ollama --model llama3.1:70b ...

# Remote Ollama server
schema-travels analyze --provider ollama --model mistral:7b \
    --ollama-host http://192.168.1.100:11434 ...
```

### Environment Variables

```bash
# Set default provider (instead of --provider flag)
export SCHEMA_TRAVELS_PROVIDER=openai

# Set default model (instead of --model flag)
export SCHEMA_TRAVELS_MODEL=gpt-4o-mini

# Ollama server URL
export OLLAMA_HOST=http://localhost:11434
```

## How It Works

### MongoDB Flow

LLM acts as **architect** — analyzes your access patterns and designs the schema:

```
SQL Schema + Query Logs → Pattern Analysis → LLM → EMBED/REFERENCE Decisions → MongoDB Schema
```

### DynamoDB Flow

Local algorithm designs, LLM **reviews**:

```
SQL Schema + Query Logs → Pattern Analysis → DynamoDB Designer → LLM Review → Final Design
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
  
  # LLM Provider (v2.3.0)
  --provider [claude|openai|gemini|grok|ollama]  LLM provider
  --model TEXT                 Model to use (overrides provider default)
  --ollama-host TEXT           Ollama server URL
  
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
- LLM API key (Claude, OpenAI, Gemini, or Grok) OR local Ollama installation

## License

MIT License — see [LICENSE](./LICENSE) for details.
