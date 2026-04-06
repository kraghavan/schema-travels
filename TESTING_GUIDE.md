# Testing Schema Travels with Real & Synthetic Data

This guide shows you how to test Schema Travels with various datasets and LLM providers.

## Quick Start: Synthetic Data

### 1. Run Complete Test

```bash
# Generate 10K queries and analyze
./run_test.sh 10000 ecommerce

# Or generate 50K queries with mixed workload
./run_test.sh 50000 mixed
```

### 2. View Results

The script will:
- Generate synthetic PostgreSQL logs
- Run analysis
- Create HTML visualization
- Open it in your browser automatically (macOS)

---

## Testing LLM Providers (v2.3.0)

### List Available Providers

```bash
schema-travels providers
```

### Test Individual Providers

```bash
# Set up API keys
export ANTHROPIC_API_KEY=sk-ant-...
export OPENAI_API_KEY=sk-...
export GOOGLE_API_KEY=...
export XAI_API_KEY=...

# Test Claude (default)
schema-travels analyze \
    --logs-dir ./test_data/small \
    --schema-file ./examples/ecommerce_schema.sql \
    --output ./results/claude.json

# Test OpenAI
schema-travels analyze \
    --provider openai \
    --model gpt-4o \
    --logs-dir ./test_data/small \
    --schema-file ./examples/ecommerce_schema.sql \
    --output ./results/openai.json

# Test Gemini
schema-travels analyze \
    --provider gemini \
    --model gemini-2.0-flash \
    --logs-dir ./test_data/small \
    --schema-file ./examples/ecommerce_schema.sql \
    --output ./results/gemini.json

# Test Grok
schema-travels analyze \
    --provider grok \
    --logs-dir ./test_data/small \
    --schema-file ./examples/ecommerce_schema.sql \
    --output ./results/grok.json

# Test Ollama (local)
ollama serve  # Start server first
schema-travels analyze \
    --provider ollama \
    --model llama3.1:8b \
    --logs-dir ./test_data/small \
    --schema-file ./examples/ecommerce_schema.sql \
    --output ./results/ollama.json
```

### Compare Provider Results

```bash
# Compare recommendations across providers
for f in ./results/*.json; do
    echo "=== $(basename $f) ==="
    jq '.recommendations | length' "$f"
    jq '.recommendations[0]' "$f"
done
```

---

## Comprehensive E2E Test Suite (v2.3.0)

The comprehensive E2E test suite validates all features across all providers and target databases.

### Test Suite Overview

| Step | Description | What It Tests |
|------|-------------|---------------|
| **Step 1** | Setup | Generate 3 workloads (100, 1K, 5K queries), clear cache |
| **Step 2** | Provider × Target Matrix | Every provider with MongoDB AND DynamoDB |
| **Step 3** | MongoDB Analysis | All dataset sizes, cache modes (relaxed/strict) |
| **Step 4** | DynamoDB Analysis | Auto/single/multi modes, Terraform, NoSQL Workbench |
| **Step 5** | Visualizations | HTML, Mermaid, Tree views for both targets |
| **Step 6** | Cache Consistency | Verify cached results match fresh results |
| **Step 7** | Output Verification | Check Terraform and Workbench files are valid |
| **Step 8** | Results Summary | Pass/fail matrix, output file listing |

### Running the E2E Test Suite

```bash
# Run with default providers (Claude, Gemini, Ollama)
./01-e2e-test-suite-v2.3.0.sh

# Enable all providers
TEST_CLAUDE=1 \
TEST_OPENAI=1 \
TEST_GEMINI=1 \
TEST_GROK=1 \
TEST_OLLAMA=1 \
OLLAMA_MODEL="gemma3:4b" \
./01-e2e-test-suite-v2.3.0.sh

# Test only Claude and Gemini
TEST_CLAUDE=1 \
TEST_OPENAI=0 \
TEST_GEMINI=1 \
TEST_GROK=0 \
TEST_OLLAMA=0 \
./01-e2e-test-suite-v2.3.0.sh
```

### Provider × Target Matrix

The matrix test (Step 2) ensures **every enabled provider works with every target database**. This catches integration issues that single-target tests miss.

**What it validates per combination:**

| Provider × Target | MongoDB Validation | DynamoDB Validation |
|-------------------|-------------------|---------------------|
| Claude → MongoDB | ✓ Recommendations generated | - |
| Claude → DynamoDB | - | ✓ Design mode, AI review |
| Gemini → MongoDB | ✓ Recommendations generated | - |
| Gemini → DynamoDB | - | ✓ Design mode, AI review |
| Ollama → MongoDB | ✓ Recommendations generated | - |
| Ollama → DynamoDB | - | ✓ Design mode, AI review |

**Sample output:**
```
┌─────────────┬──────────────┬──────────────┐
│  Provider   │   MongoDB    │   DynamoDB   │
├─────────────┼──────────────┼──────────────┤
│ claude      │ ✓ PASS       │ ✓ PASS       │
│ openai      │ ○ SKIP       │ ○ SKIP       │
│ gemini      │ ✓ PASS       │ ✓ PASS       │
│ grok        │ ○ SKIP       │ ○ SKIP       │
│ ollama      │ ✓ PASS       │ ✓ PASS       │
└─────────────┴──────────────┴──────────────┘
```

### Output Directories

```
test_results_matrix/     # Provider × Target results
├── claude_mongodb.json
├── claude_dynamodb.json
├── gemini_mongodb.json
├── gemini_dynamodb.json
├── ollama_mongodb.json
└── ollama_dynamodb.json

test_results_mongodb/    # MongoDB detailed tests
├── small.json
├── medium.json
├── large.json
├── large_cached.json
├── large_strict.json
├── schema_visualization.html
├── schema_diagram.mmd
└── schema_tree.txt

test_results_dynamodb/   # DynamoDB detailed tests
├── auto_mode.json
├── auto_mode_cached.json
├── single_table.json
├── multi_table.json
├── small.json
├── medium.json
├── schema.tf
├── workbench.json
├── schema_visualization.html
├── schema_diagram.mmd
└── schema_tree.txt
```

### Why Matrix Testing Matters

Without matrix testing, you might have:
- Provider A working with MongoDB but broken with DynamoDB
- DynamoDB AI review working with Claude but failing with Gemini
- Ollama working locally but timing out with large DynamoDB designs

The matrix test catches these issues by testing every combination systematically.

---

## Workload Patterns

| Pattern | Description | Read/Write Ratio |
|---------|-------------|------------------|
| `ecommerce` | Typical e-commerce (browsing, orders, reviews) | 85% / 15% |
| `oltp` | High-write transactional (logging, updates) | 40% / 60% |
| `analytics` | Complex aggregations, reports | 99% / 1% |
| `mixed` | Combination of all patterns | 70% / 30% |

```bash
# Examples
./run_test.sh 20000 oltp        # High-write workload
./run_test.sh 30000 analytics   # Analytics-heavy
./run_test.sh 50000 mixed       # Realistic mixed
```

---

## Real-World Datasets

### Option 1: Spider Dataset (SQL Queries)

The Spider dataset contains 10K+ queries across 200+ database schemas.

```bash
# Download
git clone https://github.com/taoyds/spider.git datasets/spider

# The queries are in JSON format, need conversion
# Use this script to convert Spider to PostgreSQL log format:
```

**spider_converter.py:**
```python
import json
from pathlib import Path
from datetime import datetime, timedelta
import random

def convert_spider_to_logs(spider_dir: Path, output_file: Path):
    """Convert Spider dataset to PostgreSQL log format."""
    
    # Load queries
    train_file = spider_dir / "train_spider.json"
    with open(train_file) as f:
        data = json.load(f)
    
    lines = []
    current_time = datetime.now() - timedelta(hours=2)
    
    for item in data:
        sql = item.get("query", "")
        if not sql:
            continue
        
        # Simulate timing
        current_time += timedelta(milliseconds=random.randint(50, 500))
        duration = random.uniform(1, 50)
        pid = random.randint(10000, 99999)
        db = item.get("db_id", "spider")
        
        timestamp = current_time.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
        
        lines.append(f"{timestamp} UTC [{pid}] app@{db} LOG:  statement: {sql}")
        lines.append(f"{timestamp} UTC [{pid}] app@{db} LOG:  duration: {duration:.3f} ms")
    
    output_file.write_text("\n".join(lines))
    print(f"Converted {len(data)} queries to {output_file}")

# Usage
convert_spider_to_logs(
    Path("datasets/spider"),
    Path("datasets/spider/postgresql.log")
)
```

### Option 2: TPC-H Benchmark

Industry-standard benchmark for analytical queries.

```bash
# Install dbgen
git clone https://github.com/electrum/tpch-dbgen.git
cd tpch-dbgen
make

# Generate data (scale factor 1 = ~1GB)
./dbgen -s 1

# Generate queries
./qgen -s 1 > queries.sql
```

### Option 3: Your Own PostgreSQL Logs

If you have a PostgreSQL database:

```sql
-- Enable query logging (postgresql.conf)
log_statement = 'all'
log_duration = on
log_min_duration_statement = 0  -- Log all queries
log_line_prefix = '%t [%p] %u@%d '

-- Restart PostgreSQL
-- Logs will be in pg_log directory
```

### Option 4: PgBadger Sample Logs

```bash
# Download sample PostgreSQL logs
curl -O https://raw.githubusercontent.com/darold/pgbadger/master/t/fixtures/light.log.bz2
bunzip2 light.log.bz2
mv light.log examples/logs/postgresql.log
```

---

## Testing Ollama Models

### Setup

```bash
# Install Ollama
curl -fsSL https://ollama.com/install.sh | sh

# Start server
ollama serve

# Pull models
ollama pull llama3.1:8b
ollama pull gemma3:4b
ollama pull mistral:7b
ollama pull qwen2.5-coder:7b
```

### Test Different Models

```bash
# Small, fast model
schema-travels analyze \
    --provider ollama \
    --model gemma3:4b \
    --logs-dir ./test_data/small \
    --schema-file ./examples/ecommerce_schema.sql

# Larger, more capable model
schema-travels analyze \
    --provider ollama \
    --model llama3.1:70b \
    --logs-dir ./test_data/small \
    --schema-file ./examples/ecommerce_schema.sql

# Code-focused model
schema-travels analyze \
    --provider ollama \
    --model qwen2.5-coder:7b \
    --logs-dir ./test_data/small \
    --schema-file ./examples/ecommerce_schema.sql
```

### Remote Ollama Server

```bash
# On remote server
OLLAMA_HOST=0.0.0.0:11434 ollama serve

# From your machine
schema-travels analyze \
    --provider ollama \
    --model llama3.1:70b \
    --ollama-host http://192.168.1.100:11434 \
    --logs-dir ./logs \
    --schema-file ./schema.sql
```

---

## Visualizing Results

### 1. HTML Visualization (Recommended)

```bash
python tools/visualize_schema.py \
    --input analysis.json \
    --output schema.html \
    --format html

open schema.html  # macOS
xdg-open schema.html  # Linux
```

### 2. Mermaid Diagram

```bash
python tools/visualize_schema.py \
    --input analysis.json \
    --format mermaid > diagram.mmd
```

Then paste to [mermaid.live](https://mermaid.live) or use in Markdown:

```markdown
​```mermaid
erDiagram
    users ||--o{ orders : has
    orders ||--o{ order_items : contains
    products ||--o{ order_items : "ordered in"
​```
```

### 3. MongoDB Compass

Once you have the schema:

1. Export the MongoDB schema JSON from `analysis.json`
2. Create a new database in MongoDB Compass
3. Use the schema to create collections
4. Import sample data

### 4. Moon Modeler (Commercial)

For professional schema design:
1. Export schema as JSON
2. Import into Moon Modeler
3. Visualize and refine

---

## Sample Commands

```bash
# Quick test with 5K queries
python tools/generate_workload.py -n 5000 -o ./quick_test/logs
schema-travels analyze \
    --logs-dir ./quick_test/logs \
    --schema-file examples/ecommerce_schema.sql \
    --output ./quick_test/results.json

# Large test with 100K queries
python tools/generate_workload.py -n 100000 -p mixed -o ./large_test/logs
schema-travels analyze \
    --logs-dir ./large_test/logs \
    --schema-file examples/ecommerce_extended_schema.sql \
    --output ./large_test/results.json

# Test with different provider
schema-travels analyze \
    --provider openai \
    --model gpt-4o-mini \
    --logs-dir ./quick_test/logs \
    --schema-file examples/ecommerce_schema.sql \
    --output ./quick_test/openai_results.json

# Test DynamoDB with Gemini
schema-travels analyze \
    --provider gemini \
    --model gemini-2.0-flash \
    --target dynamodb \
    --dynamodb-mode auto \
    --logs-dir ./quick_test/logs \
    --schema-file examples/ecommerce_schema.sql \
    --output ./quick_test/gemini_dynamodb.json

# View history
schema-travels history

# Detailed report
schema-travels report --analysis-id <id> --format markdown
```

---

## Troubleshooting

### "No queries parsed"

Check log format matches PostgreSQL/MySQL expected format:
```
2024-01-15 10:30:45.123 UTC [12345] user@db LOG:  statement: SELECT ...
```

### "Schema parsing failed"

Ensure your SQL file uses standard DDL:
```sql
CREATE TABLE users (
    id SERIAL PRIMARY KEY,
    ...
);
```

### "API key not configured"

```bash
export ANTHROPIC_API_KEY=sk-ant-...
# Or add to .env file

# For other providers:
export OPENAI_API_KEY=sk-...
export GOOGLE_API_KEY=...
export XAI_API_KEY=...
```

### "Provider not found"

Install the required dependency:
```bash
pip install schema-travels[openai]   # For OpenAI
pip install schema-travels[gemini]   # For Gemini
pip install schema-travels[all-providers]  # All providers
```

### "Ollama connection failed"

```bash
# Check if Ollama is running
curl http://localhost:11434/api/tags

# Start Ollama
ollama serve

# Check model is available
ollama list
```

### "DynamoDB AI review failed"

```bash
# Check provider is working
schema-travels providers

# Try with --no-ai flag to skip AI review
schema-travels analyze \
    --target dynamodb \
    --no-ai \
    --logs-dir ./test_data/small \
    --schema-file ./examples/ecommerce_schema.sql
```

### Matrix test shows FAIL

Check the specific output file for error details:
```bash
# View failed test output
cat test_results_matrix/gemini_dynamodb.json

# Or check the log
grep -i error test_results_matrix/*.json
```

### Provider-specific errors

```bash
# List available providers and their status
schema-travels providers
```

---

## Test Scripts Reference

| Script | Purpose | When to Use |
|--------|---------|-------------|
| `01-e2e-test-suite-v2.3.0.sh` | Full E2E test suite | Before releases, after major changes |
| `run_test.sh` | Quick single analysis | Rapid iteration |
| `tools/generate_workload.py` | Generate synthetic data | Creating test datasets |
| `tools/visualize_schema.py` | Generate visualizations | After analysis |
