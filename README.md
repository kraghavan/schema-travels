# Schema Travels

**Access Pattern Analyzer for SQL → NoSQL Schema Migration**

Analyzes your SQL database query patterns and recommends optimal NoSQL (MongoDB/DynamoDB) schema designs based on actual usage data.

## Features

- **Query Log Analysis**: Parse PostgreSQL/MySQL query logs to understand access patterns
- **Hot Join Detection**: Identify frequently executed, expensive JOINs
- **Mutation Pattern Analysis**: Track read/write ratios per table
- **AI-Powered Recommendations**: Claude-powered schema recommendations with reasoning
- **Migration Simulation**: Estimate storage, latency, and cost impact before migrating
- **Local Persistence**: Track analysis history in local SQLite database

## Installation

```bash
# Clone the repository
git clone git@github.com:kraghavan/schema-travels.git
cd schema-travels

# Create virtual environment
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install dependencies
pip install -e .

# Set up environment variables
cp .env.example .env
# Edit .env and add your ANTHROPIC_API_KEY
```

## Quick Start

```bash
# Analyze PostgreSQL query logs
schema-travels analyze \
    --logs-dir ./my-db-logs \
    --schema-file ./schema.sql \
    --db-type postgres \
    --target mongodb

# View analysis results
schema-travels report --analysis-id <id>

# List all analyses
schema-travels history

# Run simulation
schema-travels simulate --analysis-id <id>
```

## Configuration

Create a `.env` file or set environment variables:

```bash
ANTHROPIC_API_KEY=your-api-key-here
ANTHROPIC_MODEL=claude-sonnet-4-20250514  # or claude-opus-4-5-20250514
LOG_LEVEL=INFO
```

## Input Requirements

### Query Logs

**PostgreSQL**: Enable query logging in `postgresql.conf`:
```
log_statement = 'all'
log_duration = on
log_line_prefix = '%t [%p] %u@%d '
```

**MySQL**: Enable slow query log with threshold 0:
```
slow_query_log = 1
slow_query_log_file = /var/log/mysql/slow.log
long_query_time = 0
log_queries_not_using_indexes = 1
```

### Schema Files

Provide your schema as SQL DDL:
```sql
CREATE TABLE users (
    id SERIAL PRIMARY KEY,
    email VARCHAR(255) NOT NULL,
    created_at TIMESTAMP DEFAULT NOW()
);

CREATE TABLE orders (
    id SERIAL PRIMARY KEY,
    user_id INTEGER REFERENCES users(id),
    total DECIMAL(10,2),
    created_at TIMESTAMP DEFAULT NOW()
);
```

## Output

The tool generates:

1. **Analysis Report**: JSON/Markdown report with recommendations
2. **Target Schema**: Suggested MongoDB/DynamoDB schema
3. **Simulation Results**: Estimated performance and cost impact

## Project Structure

```
schema-travels/
├── src/
│   └── schema_travels/
│       ├── collector/      # Log parsing and schema extraction
│       ├── analyzer/       # Query pattern analysis
│       ├── recommender/    # AI-powered recommendations
│       ├── simulator/      # Migration impact simulation
│       ├── persistence/    # SQLite storage
│       └── cli/            # Command-line interface
├── tests/
│   ├── datasets/           # Test datasets (Spider, TPC-H)
│   └── synthetic/          # Synthetic workload generators
└── examples/               # Example schemas and logs
```

## Development

```bash
# Install dev dependencies
pip install -e ".[dev]"

# Run tests
pytest

# Run linting
ruff check src/

# Format code
ruff format src/
```

## License

MIT License - see LICENSE file for details.
