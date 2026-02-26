# Contributing to Schema Travels

Thank you for your interest in contributing! This document provides guidelines for contributing to Schema Travels.

## Getting Started

### 1. Fork and Clone

```bash
# Fork on GitHub, then:
git clone https://github.com/YOUR_USERNAME/schema-travels.git
cd schema-travels
```

### 2. Set Up Development Environment

```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
pip install -e ".[dev]"
```

### 3. Create a Branch

```bash
git checkout -b feature/your-feature-name
# or
git checkout -b fix/your-bug-fix
```

## Development Workflow

### Running Tests

```bash
# Run all tests
pytest

# Run with coverage
pytest --cov=schema_travels

# Run specific test file
pytest tests/test_analyzer.py -v
```

### Code Style

We use `ruff` for linting and formatting:

```bash
# Check for issues
ruff check src/

# Auto-fix issues
ruff check --fix src/

# Format code
ruff format src/
```

### Type Checking

```bash
mypy src/schema_travels
```

### Pre-Commit Checklist

Before committing, ensure:

- [ ] All tests pass (`pytest`)
- [ ] Code is formatted (`ruff format src/`)
- [ ] No lint errors (`ruff check src/`)
- [ ] Type hints are correct (`mypy src/schema_travels`)
- [ ] New features have tests
- [ ] Documentation is updated

## Making Changes

### Code Style Guidelines

1. **Type Hints**: All functions should have type annotations
   ```python
   def analyze(self, queries: list[QueryLog]) -> AnalysisResult:
   ```

2. **Docstrings**: Use Google-style docstrings
   ```python
   def parse_file(self, path: Path) -> SchemaDefinition:
       """Parse a SQL schema file.
       
       Args:
           path: Path to the SQL file
           
       Returns:
           Parsed schema definition
           
       Raises:
           FileNotFoundError: If file doesn't exist
       """
   ```

3. **Imports**: Let `ruff` organize imports (stdlib → third-party → local)

4. **Line Length**: 100 characters max

### Commit Messages

Use clear, descriptive commit messages:

```
feat: add MySQL 8.0 log format support
fix: handle empty query logs gracefully  
docs: update installation instructions
test: add tests for schema parser edge cases
refactor: simplify hot join detection logic
```

Prefixes:
- `feat`: New feature
- `fix`: Bug fix
- `docs`: Documentation only
- `test`: Adding tests
- `refactor`: Code change that doesn't fix a bug or add a feature
- `chore`: Maintenance tasks

## Pull Request Process

### 1. Update Your Branch

```bash
git fetch upstream
git rebase upstream/main
```

### 2. Push Your Changes

```bash
git push origin feature/your-feature-name
```

### 3. Create Pull Request

1. Go to GitHub and create a PR
2. Fill out the PR template
3. Link any related issues

### 4. PR Requirements

- [ ] Tests pass in CI
- [ ] Code review approved
- [ ] No merge conflicts
- [ ] Documentation updated (if needed)

## Project Structure

```
schema-travels/
├── src/schema_travels/
│   ├── __init__.py          # Package version
│   ├── config.py             # Configuration management
│   ├── collector/            # Log and schema parsing
│   │   ├── log_parser.py     # PostgreSQL/MySQL log parsing
│   │   ├── schema_parser.py  # SQL DDL parsing
│   │   └── models.py         # Data models
│   ├── analyzer/             # Pattern analysis
│   │   ├── hot_joins.py      # Join pattern detection
│   │   ├── mutations.py      # Read/write analysis
│   │   └── pattern_analyzer.py
│   ├── recommender/          # Recommendation engine
│   │   ├── claude_advisor.py # AI integration
│   │   └── schema_generator.py
│   ├── simulator/            # Migration simulation
│   └── cli/                  # CLI interface
├── tests/                    # Test suite
├── tools/                    # Development tools
└── examples/                 # Example data
```

## Adding Features

### Adding a New Database Source

1. Create parser in `src/schema_travels/collector/`:
   ```python
   # oracle_log_parser.py
   class OracleLogParser(LogParser):
       def parse(self) -> list[QueryLog]:
           ...
   ```

2. Register in `log_parser.py`:
   ```python
   def get_parser(db_type: str, logs_dir: Path) -> LogParser:
       parsers = {
           "postgres": PostgresLogParser,
           "mysql": MySQLLogParser,
           "oracle": OracleLogParser,  # Add here
       }
   ```

3. Add CLI option in `cli/main.py`

4. Add tests in `tests/test_collector.py`

### Adding a New Target Database

1. Add to `recommender/models.py`:
   ```python
   class TargetDatabase(Enum):
       MONGODB = "mongodb"
       DYNAMODB = "dynamodb"
       CASSANDRA = "cassandra"  # Add here
   ```

2. Create generator method in `schema_generator.py`

3. Add cost model in `simulator/cost_model.py`

## Getting Help

- **Questions**: Open a [Discussion](https://github.com/kraghavan/schema-travels/discussions)
- **Bugs**: Open an [Issue](https://github.com/kraghavan/schema-travels/issues)
- **Security**: Email maintainers directly (don't open public issue)

## Code of Conduct

Be respectful and constructive. We're all here to build something useful.

---

Thank you for contributing! 🎉