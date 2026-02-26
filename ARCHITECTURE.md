# Architecture

## System Overview

```mermaid
flowchart LR
    subgraph Inputs
        LOGS[("📄 Query Logs")]
        SCHEMA[("📋 SQL Schema")]
    end

    subgraph Core["Schema Travels"]
        direction TB
        C["🔍 Collector"]
        A["📊 Analyzer"]
        R["🤖 Recommender"]
        S["⚡ Simulator"]
        
        C --> A
        A --> R
        R --> S
    end

    subgraph External
        CLAUDE["Claude API"]
        CACHE[("🔄 Cache")]
        DB[("💾 SQLite")]
    end

    subgraph Outputs
        MONGO["MongoDB Schema"]
        REPORT["📈 Migration Report"]
    end

    LOGS --> C
    SCHEMA --> C
    R <--> CLAUDE
    R <--> CACHE
    Core --> DB
    S --> REPORT
    R --> MONGO
```

## Detailed Data Flow

```mermaid
flowchart TB
    subgraph Inputs["📥 Inputs"]
        LOGS["Database Query Logs<br/>PostgreSQL/MySQL"]
        SCHEMA["SQL Schema File<br/>DDL"]
    end

    subgraph Collector["🔍 Collector Module"]
        LP["Log Parser"]
        SP["Schema Parser"]
        LP --> QD["QueryLog Objects"]
        SP --> SD["SchemaDefinition"]
    end

    subgraph Analyzer["📊 Analyzer Module"]
        HJ["Hot Join Analyzer"]
        MA["Mutation Analyzer"]
        PA["Pattern Analyzer"]
        
        HJ --> JP["Join Patterns"]
        MA --> MP["Mutation Patterns"]
        JP --> PA
        MP --> PA
        PA --> AR["AnalysisResult"]
    end

    subgraph Recommender["🤖 Recommender Module"]
        HASH["compute_input_hash()"]
        CC["Cache Check"]
        CA["Claude Advisor"]
        CS["Cache Store"]
        SG["Schema Generator"]
        
        HASH --> CC
        CC -->|Cache Hit| REC
        CC -->|Cache Miss| CA
        CA --> REC["Recommendations<br/>EMBED / REFERENCE"]
        CA --> CS
        REC --> SG
        SG --> TS["Target Schema"]
    end

    subgraph Simulator["⚡ Simulator Module"]
        CM["Cost Model"]
        MS["Migration Simulator"]
        
        CM --> MS
        MS --> SR["SimulationResult"]
    end

    subgraph Persistence["💾 Persistence"]
        DB[("SQLite Database")]
        CACHE[("Cache Files")]
        REPO["Repository"]
        REPO <--> DB
        CC <--> CACHE
        CS --> CACHE
    end

    LOGS --> LP
    SCHEMA --> SP
    
    QD --> HJ
    QD --> MA
    SD --> PA
    SD --> SG
    
    AR --> HASH
    AR --> MS
    TS --> MS
    
    AR --> REPO
    REC --> REPO
    TS --> REPO
    SR --> REPO

    CLAUDE_API["Claude API"]
    CA <--> CLAUDE_API
```

## Module Details

### Collector (`collector/`)

| File | Purpose |
|------|---------|
| `log_parser.py` | Parse PostgreSQL/MySQL query logs |
| `schema_parser.py` | Parse SQL DDL schemas |
| `models.py` | Data models (QueryLog, SchemaDefinition) |

### Analyzer (`analyzer/`)

| File | Purpose |
|------|---------|
| `hot_joins.py` | Detect frequently joined tables |
| `mutations.py` | Track read/write ratios per table |
| `pattern_analyzer.py` | Combine patterns, calculate co-access |

### Recommender (`recommender/`)

| File | Purpose |
|------|---------|
| `claude_advisor.py` | AI recommendations via Claude API |
| `schema_generator.py` | Generate MongoDB/DynamoDB schemas |
| `cache.py` | **v1.1.0** - Hash-based recommendation caching |
| `models.py` | Recommendation data models |

### Simulator (`simulator/`)

| File | Purpose |
|------|---------|
| `cost_model.py` | Storage/compute cost calculations |
| `simulator.py` | Migration impact estimation |

### Persistence (`persistence/`)

| File | Purpose |
|------|---------|
| `database.py` | SQLite connection management |
| `repository.py` | CRUD operations for analyses |

## Storage Layout

```
~/.schema-travels/
├── schema_travels.db           # Analysis history (SQLite)
└── cache/
    ├── index.json              # Cache metadata
    │   {
    │     "entries": {
    │       "a1b2c3d4": {
    │         "version": "1.0.0",
    │         "model": "claude-sonnet-4-20250514",
    │         "timestamp": "2025-02-26T10:30:00",
    │         "num_recommendations": 5
    │       }
    │     }
    │   }
    ├── a1b2c3d4.json           # Cached recommendations
    └── e5f6g7h8.json           # Another cached result
```

## Cache Flow (v1.1.0)

```mermaid
flowchart LR
    INPUT["Schema + Analysis + Target"]
    HASH["SHA256 Hash<br/>(first 16 chars)"]
    CHECK{"Cache<br/>exists?"}
    HIT["Return cached<br/>recommendations"]
    MISS["Call Claude API"]
    STORE["Store in cache"]
    OUTPUT["Recommendations"]

    INPUT --> HASH
    HASH --> CHECK
    CHECK -->|Yes| HIT
    CHECK -->|No| MISS
    MISS --> STORE
    HIT --> OUTPUT
    STORE --> OUTPUT
```

### Cache Invalidation

Cache entries are invalidated when:
1. `RECOMMENDATION_VERSION` is bumped in `cache.py`
2. User passes `--no-cache` flag
3. User passes `--clear-cache` flag
4. Cache file is manually deleted
