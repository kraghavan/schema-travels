```mermaid
flowchart TB
    subgraph Inputs["📥 Inputs"]
        LOGS["Database Query Logs<br/>(PostgreSQL/MySQL)"]
        SCHEMA["SQL Schema File<br/>(DDL)"]
    end

    subgraph Collector["🔍 Collector Module"]
        LP["Log Parser<br/><code>log_parser.py</code>"]
        SP["Schema Parser<br/><code>schema_parser.py</code>"]
        LP --> QD["QueryLog Objects"]
        SP --> SD["SchemaDefinition"]
    end

    subgraph Analyzer["📊 Analyzer Module"]
        HJ["Hot Join Analyzer<br/><code>hot_joins.py</code>"]
        MA["Mutation Analyzer<br/><code>mutations.py</code>"]
        PA["Pattern Analyzer<br/><code>pattern_analyzer.py</code>"]
        
        HJ --> JP["Join Patterns<br/>Co-access Matrix"]
        MA --> MP["Mutation Patterns<br/>Read/Write Ratios"]
        JP --> PA
        MP --> PA
        PA --> AR["AnalysisResult"]
    end

    subgraph Recommender["🤖 Recommender Module"]
        CA["Claude Advisor<br/><code>claude_advisor.py</code>"]
        SG["Schema Generator<br/><code>schema_generator.py</code>"]
        
        CA --> REC["SchemaRecommendations<br/>EMBED / REFERENCE"]
        REC --> SG
        SG --> TS["Target Schema<br/>(MongoDB/DynamoDB)"]
    end

    subgraph Simulator["⚡ Simulator Module"]
        CM["Cost Model<br/><code>cost_model.py</code>"]
        MS["Migration Simulator<br/><code>simulator.py</code>"]
        
        CM --> MS
        MS --> SR["SimulationResult<br/>Storage/Latency/Cost"]
    end

    subgraph Persistence["💾 Persistence"]
        DB[("SQLite Database<br/><code>~/.schema-travels/</code>")]
        REPO["Analysis Repository<br/><code>repository.py</code>"]
        REPO <--> DB
    end

    subgraph CLI["⌨️ CLI Interface"]
        ANALYZE["schema-travels analyze"]
        REPORT["schema-travels report"]
        HISTORY["schema-travels history"]
        SIMULATE["schema-travels simulate"]
    end

    subgraph Outputs["📤 Outputs"]
        JSON["JSON Report"]
        CONSOLE["Console Output<br/>(Rich Tables)"]
        MONGO["MongoDB Schema"]
        DYNAMO["DynamoDB Schema"]
    end

    %% Flow connections
    LOGS --> LP
    SCHEMA --> SP
    
    QD --> HJ
    QD --> MA
    SD --> PA
    SD --> SG
    
    AR --> CA
    AR --> MS
    TS --> MS
    
    %% CLI connections
    ANALYZE --> Collector
    ANALYZE --> Analyzer
    ANALYZE --> Recommender
    SIMULATE --> Simulator
    REPORT --> REPO
    HISTORY --> REPO
    
    %% Output connections
    AR --> REPO
    REC --> REPO
    TS --> REPO
    SR --> REPO
    
    TS --> JSON
    TS --> MONGO
    TS --> DYNAMO
    SR --> CONSOLE
    AR --> CONSOLE

    %% External API
    CLAUDE_API["Claude API<br/>claude-opus-4-5"]
    CA <--> CLAUDE_API

    %% Styling
    classDef input fill:#e1f5fe,stroke:#01579b
    classDef process fill:#fff3e0,stroke:#e65100
    classDef ai fill:#f3e5f5,stroke:#7b1fa2
    classDef storage fill:#e8f5e9,stroke:#2e7d32
    classDef output fill:#fce4ec,stroke:#c2185b
    
    class LOGS,SCHEMA input
    class LP,SP,HJ,MA,PA,CM,MS process
    class CA,CLAUDE_API ai
    class DB,REPO storage
    class JSON,CONSOLE,MONGO,DYNAMO output
```