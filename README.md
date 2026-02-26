# 🧭 Schema Travels

**Intelligent SQL → MongoDB Schema Migration**

> *"Stop guessing embed vs. reference. Let your query patterns decide."*

[![CI](https://github.com/kraghavan/schema-travels/actions/workflows/ci.yml/badge.svg)](https://github.com/kraghavan/schema-travels/actions/workflows/ci.yml)
[![Python 3.10+](https://img.shields.io/badge/python-3.10+-blue.svg)](https://www.python.org/downloads/)
[![License: MIT](https://img.shields.io/badge/License-MIT-green.svg)](https://opensource.org/licenses/MIT)
[![PyPI version](https://badge.fury.io/py/schema-travels.svg)](https://pypi.org/project/schema-travels/)

---

## The Problem

Migrating from PostgreSQL/MySQL to MongoDB? You'll face this question hundreds of times:

> *"Should I embed this data or reference it?"*

Most migration tools do **1:1 table-to-collection mapping** — which completely ignores why you're moving to NoSQL in the first place.

The right answer depends on **how you actually access your data**. But manually analyzing thousands of queries? Nobody has time for that.

## The Solution

**Schema Travels** analyzes your real query patterns and recommends an optimal MongoDB schema:

```
┌─────────────────┐     ┌───────────────────┐     ┌─────────────────┐
│  Query Logs     │────▶│  Pattern Analysis │────▶│  AI-Powered     │
│  + SQL Schema   │     │  • Hot joins      │     │  Recommendations│
│                 │     │  • Co-access %    │     │  • EMBED        │
│                 │     │  • Write ratios   │     │  • REFERENCE    │
└─────────────────┘     └───────────────────┘     └─────────────────┘
```

**Result:** A MongoDB schema optimized for *your* access patterns — not generic "best practices."

---

## What's New in v1.1.0

### 🔄 Reproducible Results with Caching

Same inputs now produce the **same recommendations** every time:

```bash
# First run - calls Claude API
schema-travels analyze --logs-dir ./logs --schema-file ./schema.sql
# → Cached to ~/.schema-travels/cache/

# Second run - uses cache (instant, consistent)
schema-travels analyze --logs-dir ./logs --schema-file ./schema.sql
# → Same recommendations, no API call

# Force fresh analysis
schema-travels analyze --logs-dir ./logs --schema-file ./schema.sql --no-cache
```

### 🔑 Better API Key Errors

Clear, actionable error when API key is missing:

```
╭───────────────────────────────────────────────────────────────────────╮
│                    ⚠️  API KEY NOT CONFIGURED                         │
├───────────────────────────────────────────────────────────────────────┤
│  Schema Travels requires an Anthropic API key for AI-powered          │
│  schema recommendations.                                              │
│                                                                       │
│  Option 1: export ANTHROPIC_API_KEY=sk-ant-xxxxx                      │
│  Option 2: echo "ANTHROPIC_API_KEY=sk-ant-xxxxx" > .env               │
│                                                                       │
│  Get your API key at: https://console.anthropic.com/settings/keys     │
╰───────────────────────────────────────────────────────────────────────╯
```

---

## Quick Start

### Installation

```bash
pip install schema-travels
```

### Basic Usage

```bash
# Set your API key
export ANTHROPIC_API_KEY=sk-ant-xxxxx

# Analyze your database
schema-travels analyze \
    --logs-dir ./postgresql-logs \
    --schema-file ./schema.sql \
    --target mongodb \
    --output results.json
```

### What You Get

```
📊 Analysis Summary
═══════════════════════════════════════════════════

Hot Joins (Top 5):
  users ⟷ orders      : 12,847 calls, 8.3ms avg
  orders ⟷ order_items: 11,203 calls, 5.1ms avg  
  products ⟷ reviews  : 8,456 calls, 12.7ms avg

Recommendations:
┌─────────────┬─────────────┬───────────┬────────────┐
│ Parent      │ Child       │ Decision  │ Confidence │
├─────────────┼─────────────┼───────────┼────────────┤
│ users       │ addresses   │ EMBED     │ 92%        │
│ orders      │ order_items │ EMBED     │ 87%        │
│ users       │ orders      │ REFERENCE │ 85%        │
│ products    │ reviews     │ REFERENCE │ 78%        │
└─────────────┴─────────────┴───────────┴────────────┘
```

---

## Why Schema Travels?

### ❌ What Other Tools Do

```
SQL Table          →    MongoDB Collection
─────────────────────────────────────────
users              →    users
addresses          →    addresses  
orders             →    orders
order_items        →    order_items
```

*This is just PostgreSQL with different syntax.*

### ✅ What Schema Travels Does

```javascript
// users collection — addresses embedded (92% co-accessed)
{
  _id: ObjectId("..."),
  email: "user@example.com",
  name: "John Doe",
  addresses: [                    // ← EMBEDDED (bounded, rarely updated)
    { street: "123 Main", city: "NYC", is_default: true }
  ]
}

// orders collection — items embedded, user referenced
{
  _id: ObjectId("..."),
  user_id: ObjectId("..."),       // ← REFERENCED (accessed independently)
  status: "shipped",
  items: [                        // ← EMBEDDED (always fetched together)
    { product_id: "...", quantity: 2, price: 29.99 }
  ]
}
```

---

## Features

### 📊 Access Pattern Analysis

- **Hot Join Detection** — Find frequently co-accessed tables
- **Co-access Ratios** — Measure how often tables are queried together
- **Write Ratio Tracking** — Identify update-heavy tables (bad embed candidates)
- **Solo Access Detection** — Find independently accessed entities

### 🤖 AI-Powered Recommendations

- **Claude Integration** — Intelligent embed/reference decisions
- **Confidence Scores** — Know how certain each recommendation is
- **Detailed Reasoning** — Understand *why* each decision was made
- **Warning Detection** — Get alerts for potential issues

### 🔄 Reproducible Results (v1.1.0)

- **Recommendation Caching** — Same inputs = same outputs
- **Version Tracking** — Cache auto-invalidates when logic changes
- **Comparison Tools** — Diff recommendations between runs
- **Cache Control** — `--no-cache` for fresh analysis

### ⚡ Migration Simulation

- **Storage Impact** — Estimate size changes from embedding
- **Latency Projection** — Predict query performance improvements
- **Cost Estimation** — Calculate infrastructure cost differences

### 📈 Visualization

- **HTML Reports** — Interactive schema visualization
- **Mermaid Diagrams** — ER diagrams for documentation
- **Console Output** — Rich terminal formatting

---

## How It Works

### 1. Collect Access Patterns

Schema Travels parses your PostgreSQL/MySQL query logs:

```
2024-01-15 10:30:45 LOG: statement: SELECT u.*, a.* FROM users u 
    JOIN addresses a ON u.id = a.user_id WHERE u.id = 123
2024-01-15 10:30:45 LOG: duration: 3.45 ms
```

### 2. Analyze Patterns

For each table relationship, it calculates:

| Metric | Description | Embed Signal |
|--------|-------------|--------------|
| **Co-access ratio** | % of queries accessing both tables | High = Embed |
| **Child independence** | % of queries accessing child alone | High = Reference |
| **Write ratio** | % of operations that are writes | High = Reference |
| **Cardinality** | Avg/max children per parent | High = Reference |

### 3. Apply Decision Rules

```python
# Simplified decision logic
if max_children > 1000:
    return REFERENCE  # Unbounded = always reference

if co_access > 70% and write_ratio < 30% and max_children < 100:
    return EMBED      # High co-access, low writes, bounded

if child_independence > 40%:
    return REFERENCE  # Accessed alone too often

if write_ratio > 50%:
    return REFERENCE  # Too many updates
```

### 4. Generate Schema

Output includes:
- MongoDB collection definitions
- Embedded document structures
- Reference relationships
- Sample documents

---

## Configuration

### Environment Variables

```bash
# Required for AI recommendations
export ANTHROPIC_API_KEY=sk-ant-xxxxx

# Optional
export ANTHROPIC_MODEL=claude-sonnet-4-20250514  # or claude-opus-4-5-20250514
export LOG_LEVEL=INFO
```

Or create a `.env` file:

```bash
cp .env.example .env
# Edit .env with your API key
```

### CLI Options

```bash
schema-travels analyze \
    --logs-dir ./logs           # Directory with query logs
    --schema-file ./schema.sql  # SQL DDL file
    --db-type postgres          # postgres or mysql
    --target mongodb            # Target database
    --output results.json       # Output file
    --use-ai                    # Enable AI recommendations (default)
    --no-ai                     # Use rule-based only
    --no-cache                  # Bypass recommendation cache
    --clear-cache               # Clear all cached recommendations
```

---

## Commands

| Command | Description |
|---------|-------------|
| `schema-travels analyze` | Run full analysis |
| `schema-travels report --analysis-id <id>` | View previous analysis |
| `schema-travels history` | List all analyses |
| `schema-travels simulate --analysis-id <id>` | Run migration simulation |
| `schema-travels config` | Show current configuration |

---

## Input Requirements

### Query Logs

**PostgreSQL** — Enable in `postgresql.conf`:
```ini
log_statement = 'all'
log_duration = on
log_line_prefix = '%t [%p] %u@%d '
```

**MySQL** — Enable slow query log:
```ini
slow_query_log = 1
long_query_time = 0
log_queries_not_using_indexes = 1
```

### Schema File

Standard SQL DDL:
```sql
CREATE TABLE users (
    id SERIAL PRIMARY KEY,
    email VARCHAR(255) NOT NULL
);

CREATE TABLE orders (
    id SERIAL PRIMARY KEY,
    user_id INTEGER REFERENCES users(id),
    total DECIMAL(10,2)
);
```

---

## Storage Locations

```
~/.schema-travels/
├── schema_travels.db     # Analysis history (SQLite)
└── cache/
    ├── index.json        # Cache index with metadata
    └── <hash>.json       # Cached recommendations
```

---

## Example Output

### Recommendations JSON

```json
{
  "recommendations": [
    {
      "parent_table": "users",
      "child_table": "addresses",
      "decision": "EMBED",
      "confidence": 0.92,
      "reasoning": [
        "92% co-access ratio",
        "Low write frequency (8%)",
        "Bounded cardinality (avg 2.1, max 5)"
      ],
      "warnings": []
    }
  ]
}
```

### Generated MongoDB Schema

```javascript
// users collection
{
  $jsonSchema: {
    bsonType: "object",
    required: ["email"],
    properties: {
      email: { bsonType: "string" },
      name: { bsonType: "string" },
      addresses: {
        bsonType: "array",
        items: {
          bsonType: "object",
          properties: {
            street: { bsonType: "string" },
            city: { bsonType: "string" },
            zip: { bsonType: "string" }
          }
        }
      }
    }
  }
}
```

---

## Architecture

```
schema-travels/
├── src/schema_travels/
│   ├── collector/      # Log parsing, schema extraction
│   ├── analyzer/       # Pattern detection (hot joins, mutations)
│   ├── recommender/    # AI recommendations, schema generation, caching
│   ├── simulator/      # Migration impact estimation
│   ├── persistence/    # SQLite storage for history
│   └── cli/            # Command-line interface
├── tools/              # Workload generator, visualizer
├── examples/           # Sample schemas and logs
└── tests/              # Test suite
```

---

## Development

```bash
# Clone
git clone https://github.com/kraghavan/schema-travels.git
cd schema-travels

# Setup
python -m venv venv
source venv/bin/activate
pip install -e ".[dev]"

# Test
pytest --cov=schema_travels

# Lint
ruff check src/
ruff format src/
```

---

## Roadmap

- [x] PostgreSQL log parsing
- [x] MySQL log parsing  
- [x] MongoDB schema generation
- [x] Claude AI integration
- [x] Migration simulation
- [x] Recommendation caching (v1.1.0)
- [ ] DynamoDB support
- [ ] Web UI dashboard
- [ ] Real-time log streaming
- [ ] Multi-database analysis

---

## Contributing

Contributions welcome! Please read [CONTRIBUTING.md](CONTRIBUTING.md) first.

---

## License

MIT License — see [LICENSE](LICENSE) for details.

---

## Acknowledgments

- Built with [Claude](https://anthropic.com) by Anthropic
- SQL parsing by [sqlglot](https://github.com/tobymao/sqlglot)
- CLI powered by [Click](https://click.palletsprojects.com/) and [Rich](https://rich.readthedocs.io/)

---

<p align="center">
  <b>Stop guessing. Start measuring.</b><br>
  <a href="https://github.com/kraghavan/schema-travels">⭐ Star on GitHub</a>
</p>