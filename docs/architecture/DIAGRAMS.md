# QuartzDB Architecture Diagrams

## 1. System Architecture Overview

```mermaid
graph TB
    subgraph Client["Client Layer"]
        WebUI["Web UI / CLI"]
        API["HTTP/gRPC Clients"]
    end

    subgraph IngestLayer["Ingest Cluster"]
        Ingest1["Ingest Node 1"]
        Ingest2["Ingest Node 2"]
        Ingest3["Ingest Node 3"]
    end

    subgraph SearchLayer["Search Cluster"]
        Search1["Search Node 1"]
        Search2["Search Node 2"]
    end

    subgraph StorageLayer["Storage Cluster"]
        Storage1["Storage Node 1<br/>DataFusion Worker"]
        Storage2["Storage Node 2<br/>DataFusion Worker"]
        Storage3["Storage Node 3<br/>DataFusion Worker"]
    end

    subgraph MetastoreLayer["Metastore Cluster"]
        Meta1["Metastore Node 1"]
        Meta2["Metastore Node 2"]
    end

    subgraph ExternalStorage["External Storage"]
        S3["S3 Object Store<br/>Splits in Parquet"]
    end

    DataStream["NDJSON<br/>InfluxLine<br/>Prometheus"]

    DataStream -->|"Byte Stream"| Ingest1
    DataStream -->|"Byte Stream"| Ingest2
    DataStream -->|"Byte Stream"| Ingest3

    Client -->|"SQL/LogsQL/MetricsQL"| Search1
    Client -->|"SQL/LogsQL/MetricsQL"| Search2

    Ingest1 -->|"DocumentBatch<br/>Rendez-vous Hash"| Storage1
    Ingest1 -->|"DocumentBatch"| Storage2
    Ingest1 -->|"DocumentBatch"| Storage3

    Ingest2 -->|"DocumentBatch"| Storage1
    Ingest2 -->|"DocumentBatch"| Storage2
    Ingest2 -->|"DocumentBatch"| Storage3

    Ingest3 -->|"DocumentBatch"| Storage1
    Ingest3 -->|"DocumentBatch"| Storage2
    Ingest3 -->|"DocumentBatch"| Storage3

    Storage1 -->|"RegisterSplit<br/>UpdateMetadata"| Meta1
    Storage2 -->|"RegisterSplit<br/>UpdateMetadata"| Meta1
    Storage3 -->|"RegisterSplit<br/>UpdateMetadata"| Meta2

    Storage1 -->|"Upload Splits<br/>Parquet Format"| S3
    Storage2 -->|"Upload Splits<br/>Parquet Format"| S3
    Storage3 -->|"Upload Splits<br/>Parquet Format"| S3

    Search1 -->|"GetSplits<br/>GetMetadata"| Meta1
    Search2 -->|"GetSplits<br/>GetMetadata"| Meta2

    Search1 -->|"ExecutePlan<br/>DataFusion Tasks"| Storage1
    Search1 -->|"ExecutePlan"| Storage2
    Search1 -->|"ExecutePlan"| Storage3

    Storage1 -->|"QueryResult"| Search1
    Storage2 -->|"QueryResult"| Search1
    Storage3 -->|"QueryResult"| Search1

    Meta1 -->|"Replication"| Meta2
```

## 2. Data Flow: Ingestion and Query Paths

```mermaid
graph TD
    subgraph IngestionFlow["DATA INGESTION PATH"]
        I1["Client Data<br/>NDJSON/InfluxLine/Prometheus"]
        I2["Ingest Node<br/>1. Parse<br/>2. Validate<br/>3. Schema Check"]
        I3["Document Batch"]
        I4["Rendez-vous Hash<br/>hash(index_name + doc_id)"]
        I5["Route to Storage Node"]
        I6["Storage Node<br/>Receive Batch"]
        I7["Convert to Split<br/>Columnar Format"]
        I8["Compress & Upload<br/>to S3"]
        I9["Register Split<br/>in Metastore"]
        I10["Metastore Update<br/>Index Statistics"]
        I11["Split Available<br/>for Queries"]
    end

    subgraph QueryFlow["QUERY EXECUTION PATH"]
        Q1["Client Query<br/>SQL/LogsQL/MetricsQL"]
        Q2["Search Node<br/>Parse Query"]
        Q3["Query Metastore<br/>Find Relevant Splits"]
        Q4["Filter Splits by<br/>Time Range & Labels"]
        Q5["Build DataFusion<br/>Execution Plan"]
        Q6["Partition Plan<br/>Across Storage Nodes"]
        Q7["Storage Node<br/>Execute Local Tasks<br/>Scan Splits"]
        Q8["Apply Predicates<br/>Projection"]
        Q9["Return Intermediate<br/>Results"]
        Q10["Search Node<br/>Merge Results<br/>Sort/Aggregate"]
        Q11["Return Final<br/>Results to Client"]
    end

    I1 --> I2
    I2 --> I3
    I3 --> I4
    I4 --> I5
    I5 --> I6
    I6 --> I7
    I7 --> I8
    I8 --> I9
    I9 --> I10
    I10 --> I11

    Q1 --> Q2
    Q2 --> Q3
    Q3 --> Q4
    Q4 --> Q5
    Q5 --> Q6
    Q6 --> Q7
    Q7 --> Q8
    Q8 --> Q9
    Q9 --> Q10
    Q10 --> Q11

    I11 -.->|"Available for"| Q7
```

## 3. Component Responsibilities and Interactions

```mermaid
graph LR
    subgraph Ingest["INGEST NODES"]
        direction TB
        IN1["Input<br/>Formats"]
        IN2["Parser"]
        IN3["Validator"]
        IN4["Batcher"]
        IN5["Router"]
        IN1 --> IN2
        IN2 --> IN3
        IN3 --> IN4
        IN4 --> IN5
    end

    subgraph Metastore["METASTORE NODES"]
        direction TB
        MS1["Index<br/>Registry"]
        MS2["Split<br/>Registry"]
        MS3["Event<br/>Log"]
        MS4["Metadata<br/>Store"]
        MS1 -.->|"queries"| MS4
        MS2 -.->|"queries"| MS4
        MS3 -.->|"logs"| MS4
    end

    subgraph Storage["STORAGE NODES"]
        direction TB
        ST1["Batch<br/>Receiver"]
        ST2["Split<br/>Creator"]
        ST3["Compressor"]
        ST4["S3<br/>Uploader"]
        ST5["Local<br/>Cache"]
        ST6["DataFusion<br/>Worker"]
        ST1 --> ST2
        ST2 --> ST3
        ST3 --> ST4
        ST3 --> ST5
        ST5 --> ST6
    end

    subgraph Search["SEARCH NODES"]
        direction TB
        SN1["Query<br/>Parser"]
        SN2["Split<br/>Selector"]
        SN3["Plan<br/>Builder"]
        SN4["Task<br/>Scheduler"]
        SN5["Result<br/>Merger"]
        SN1 --> SN2
        SN2 --> SN3
        SN3 --> SN4
        SN4 --> SN5
    end

    IN5 -->|"DocumentBatch"| ST1
    ST1 -->|"RegisterSplit"| MS2
    SN1 -->|"GetMetadata"| MS1
    SN2 -->|"GetSplits"| MS2
    SN4 -->|"ExecutePlan"| ST6
    ST6 -->|"Results"| SN5
```

## 4. Request/Response Communication Matrix

```mermaid
graph TD
    subgraph RequestTypes["Communication Patterns"]
        Ingest["<b>Ingest → Storage</b><br/>DocumentBatch"]
        StorageMeta["<b>Storage → Metastore</b><br/>RegisterSplit<br/>UpdateMetadata"]
        SearchMeta["<b>Search → Metastore</b><br/>GetSplits<br/>GetMetadata"]
        SearchStorage["<b>Search → Storage</b><br/>ExecutePlan<br/>DataFusion Tasks"]
        StorageSearch["<b>Storage → Search</b><br/>QueryResult<br/>Intermediate Results"]
    end

    Ingest -.-> StorageMeta
    SearchMeta -.-> SearchStorage
    SearchStorage -.-> StorageSearch
```

## 5. Distributed Query Execution Timeline

```mermaid
sequenceDiagram
    participant Client
    participant SearchNode as Search Node
    participant MetastoreNode as Metastore
    participant StorageNode1 as Storage Node 1
    participant StorageNode2 as Storage Node 2
    participant StorageNode3 as Storage Node 3

    Client->>SearchNode: Query (SQL/LogsQL/MetricsQL)
    SearchNode->>MetastoreNode: GetSplits(index, time_range)
    MetastoreNode-->>SearchNode: [Split Metadata List]
    SearchNode->>SearchNode: Build Execution Plan
    SearchNode->>SearchNode: Partition Plan Across Nodes
    
    par Parallel Execution
        SearchNode->>StorageNode1: ExecuteTask(splits=[S1,S2])
        SearchNode->>StorageNode2: ExecuteTask(splits=[S3,S4])
        SearchNode->>StorageNode3: ExecuteTask(splits=[S5,S6])
    end
    
    StorageNode1-->>SearchNode: Partial Results
    StorageNode2-->>SearchNode: Partial Results
    StorageNode3-->>SearchNode: Partial Results
    
    SearchNode->>SearchNode: Merge Results<br/>Sort/Aggregate
    SearchNode-->>Client: Final Results
```

## 6. Storage Node Data Processing Pipeline

```mermaid
graph TD
    Input["Incoming<br/>DocumentBatch"]
    
    Parse["Parse<br/>Schema Match"]
    Buffer["Buffer<br/>in Memory"]
    Convert["Convert to<br/>Columnar Format"]
    Compress["Compress<br/>Splits"]
    Upload["Upload to<br/>S3"]
    Register["Register in<br/>Metastore"]
    Cache["Cache<br/>Locally"]
    Serve["Serve to<br/>Query Workers"]
    
    Input --> Parse
    Parse --> Buffer
    Buffer --> Convert
    Convert --> Compress
    Compress --> Upload
    Compress --> Cache
    Upload --> Register
    Register --> Serve
    Cache --> Serve
```

## 7. Rendez-vous Hashing Distribution

```mermaid
graph LR
    Doc1["Document 1<br/>index_name: logs<br/>id: 12345"]
    Doc2["Document 2<br/>index_name: logs<br/>id: 67890"]
    Doc3["Document 3<br/>index_name: metrics<br/>id: 54321"]
    
    Hash1["Hash Function<br/>hash(logs+12345)"]
    Hash2["Hash Function<br/>hash(logs+67890)"]
    Hash3["Hash Function<br/>hash(metrics+54321)"]
    
    Mod1["Mod NumNodes"]
    Mod2["Mod NumNodes"]
    Mod3["Mod NumNodes"]
    
    Doc1 --> Hash1
    Doc2 --> Hash2
    Doc3 --> Hash3
    
    Hash1 --> Mod1
    Hash2 --> Mod2
    Hash3 --> Mod3
    
    Mod1 -->|"→ Node 0"| Storage0["Storage<br/>Node 0"]
    Mod2 -->|"→ Node 1"| Storage1["Storage<br/>Node 1"]
    Mod3 -->|"→ Node 2"| Storage2["Storage<br/>Node 2"]
    
    style Doc1 fill:#e1f5ff
    style Doc2 fill:#e1f5ff
    style Doc3 fill:#e1f5ff
    style Storage0 fill:#fff9c4
    style Storage1 fill:#fff9c4
    style Storage2 fill:#fff9c4
```

## Key Architectural Principles

1. **Horizontal Scalability**: Each component type can scale independently
2. **Data Locality**: Documents routed to same node via deterministic hashing
3. **Fault Tolerance**: Metastore replication, S3 durability, split redundancy
4. **Separation of Concerns**: Each node type has specific, well-defined responsibilities
5. **Eventual Consistency**: Async updates between components coordinated via Metastore
6. **Query Distribution**: DataFusion enables flexible, distributed query execution
