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

# Install with all providers for testing
pip install -e ".[dev,all-providers]"
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

# Run LLM provider tests
pytest tests/test_llm_providers.py -v
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
│   ├── __init__.py           # Package version
│   ├── config.py             # Configuration management
│   ├── llm/                  # LLM provider abstraction (v2.3.0)
│   │   ├── __init__.py       # Public API
│   │   ├── provider.py       # LLMProvider protocol
│   │   ├── factory.py        # Provider factory
│   │   └── providers/        # Provider implementations
│   │       ├── claude.py
│   │       ├── openai.py
│   │       ├── gemini.py
│   │       ├── grok.py
│   │       └── ollama.py
│   ├── collector/            # Log and schema parsing
│   │   ├── log_parser.py
│   │   ├── schema_parser.py
│   │   └── models.py
│   ├── analyzer/             # Pattern analysis
│   │   ├── hot_joins.py
│   │   ├── mutations.py
│   │   └── pattern_analyzer.py
│   ├── recommender/          # Recommendation engine
│   │   ├── advisor.py        # Provider-agnostic advisor
│   │   ├── claude_advisor.py # Backwards-compat alias
│   │   └── schema_generator.py
│   ├── simulator/            # Migration simulation
│   └── cli/                  # CLI interface
├── tests/                    # Test suite
├── tools/                    # Development tools
└── examples/                 # Example data
```

## Adding Features

### Adding a New LLM Provider (v2.3.0)

1. Create provider in `src/schema_travels/llm/providers/`:
   ```python
   # newprovider.py
   from schema_travels.llm.provider import LLMProviderError, APIKeyMissingError
   
   class NewProvider:
       """New LLM provider implementation."""
       
       def __init__(self, model: str | None = None, **kwargs):
           self._model = model or "default-model"
           self._client = None
           
       @property
       def provider_name(self) -> str:
           return "newprovider"
       
       @property
       def model(self) -> str:
           return self._model
       
       def _get_client(self):
           if self._client is None:
               import os
               api_key = os.environ.get("NEW_API_KEY")
               if not api_key:
                   raise APIKeyMissingError(
                       "newprovider",
                       ["NEW_API_KEY"],
                       "pip install new-sdk"
                   )
               from new_sdk import Client
               self._client = Client(api_key=api_key)
           return self._client
       
       def complete(self, prompt: str, **kwargs) -> str:
           client = self._get_client()
           try:
               response = client.generate(prompt=prompt, model=self._model)
               return response.text
           except Exception as e:
               raise LLMProviderError(f"NewProvider error: {e}")
       
       def chat(self, messages: list[dict], **kwargs) -> str:
           # Convert messages to provider format
           client = self._get_client()
           # ... implementation
   ```

2. Register in `llm/factory.py`:
   ```python
   _PROVIDER_REGISTRY = {
       # ... existing providers
       "newprovider": {
           "class": "schema_travels.llm.providers.newprovider:NewProvider",
           "default_model": "default-model",
           "env_vars": ["NEW_API_KEY"],
           "install": "pip install new-sdk",
       },
   }
   ```

3. Export in `llm/providers/__init__.py`:
   ```python
   from schema_travels.llm.providers.newprovider import NewProvider
   ```

4. Add optional dependency in `pyproject.toml`:
   ```toml
   [project.optional-dependencies]
   newprovider = ["new-sdk>=1.0.0"]
   ```

5. Add tests in `tests/test_llm_providers.py`:
   ```python
   def test_newprovider_initialization():
       provider = NewProvider(model="test-model")
       assert provider.provider_name == "newprovider"
       assert provider.model == "test-model"
   ```

6. Update documentation in `README.md`, `CLAUDE.md`, `ARCHITECTURE.md`

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

2. Create design models in `recommender/{target}_models.py`

3. Create designer in `recommender/{target}_designer.py`

4. Add generator method in `schema_generator.py`

5. Add cost model in `simulator/cost_model.py`

6. Update CLI in `cli/main.py`

## Testing LLM Providers

### Unit Tests (No API calls)

```python
def test_provider_initialization():
    """Test provider can be created without API key."""
    provider = OpenAIProvider(model="gpt-4o")
    assert provider.provider_name == "openai"
    assert provider.model == "gpt-4o"

def test_missing_api_key():
    """Test proper error when API key missing."""
    with pytest.raises(APIKeyMissingError) as exc:
        provider = OpenAIProvider()
        provider.complete("test")
    assert "OPENAI_API_KEY" in str(exc.value)
```

### Integration Tests (Requires API keys)

```python
@pytest.mark.skipif(not os.environ.get("OPENAI_API_KEY"), reason="No API key")
def test_openai_complete():
    """Test actual API call."""
    provider = OpenAIProvider()
    response = provider.complete("Say 'test'")
    assert len(response) > 0
```

### Mock Tests

```python
def test_advisor_with_mock_provider(mocker):
    """Test advisor with mocked provider."""
    mock_provider = mocker.Mock()
    mock_provider.complete.return_value = '{"recommendations": []}'
    
    advisor = Advisor(provider=mock_provider)
    # Test advisor logic without API calls
```

## Getting Help

- **Questions**: Open a [Discussion](https://github.com/kraghavan/schema-travels/discussions)
- **Bugs**: Open an [Issue](https://github.com/kraghavan/schema-travels/issues)
- **Security**: Email maintainers directly (don't open public issue)

## Code of Conduct

Be respectful and constructive. We're all here to build something useful.

---

Thank you for contributing! 🎉
