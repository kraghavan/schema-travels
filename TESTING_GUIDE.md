# Testing Schema Travels with Real & Synthetic Data

This guide shows you how to test Schema Travels with various datasets.

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
```
