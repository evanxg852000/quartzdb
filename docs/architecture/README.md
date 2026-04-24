# QuartzDB Architecture

## Overview

QuartzDB is a distributed log and metrics storage system with four main node types working together to provide a scalable, fault-tolerant storage and query platform. The architecture follows a separation of concerns pattern with specialized components for metadata management, data ingestion, storage, and query execution.

```
┌─────────────────────────────────────────────────────────────────┐
│                     QuartzDB Cluster                             │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐           │
│  │   Ingest     │  │   Search     │  │   Metastore  │           │
│  │   Nodes      │  │   Nodes      │  │   Nodes      │           │
│  │              │  │              │  │              │           │
│  │ • Parse      │  │ • SQL API    │  │ • Index Meta │           │
│  │ • Validate   │  │ • LogsQL API │  │ • Split Reg. │           │
│  │ • Batch      │  │ • MetricsQL  │  │ • Snapshots  │           │
│  └──────────────┘  └──────────────┘  └──────────────┘           │
│        │                   │                   ▲                 │
│        │                   │                   │                 │
│        └───────────────────┼───────────────────┘                 │
│                    ┌───────┴────────┐                             │
│                    │                │                             │
│              ┌─────▼────────┐       │                             │
│              │   Storage    │       │                             │
│              │    Nodes     │       │                             │
│              │              │       │                             │
│              │ • Splits     │       │                             │
│              │ • S3 Uploads │       │                             │
│              │ • DataFusion │       │                             │
│              │   Workers    │       │                             │
│              └──────────────┘       │                             │
│                    │                │                             │
│                    └────────────────┘                             │
│                                                                  │
│              ┌─────────────────────────────────┐                 │
│              │      External Storage (S3)      │                 │
│              │                                 │                 │
│              │ • Split Files (Parquet)         │                 │
│              │ • Split Metadata                │                 │
│              └─────────────────────────────────┘                 │
│                                                                  │
└─────────────────────────────────────────────────────────────────┘
```

## Component Details

### 1. **Metastore Nodes**

**Responsibility**: Central metadata management layer

- **Index Management**: Stores metadata about all indexes (schemas, retention policies, settings)
- **Split Registry**: Tracks all splits (partitioned data files) and their locations
- **Event Log**: Maintains an audit trail of all cluster operations
- **Consistency Point**: Single source of truth for cluster state

**Key Operations**:
```
- Register new indexes
- Record split creation and lifecycle
- Track index mutations
- Provide metadata queries to search nodes
```

**Data Storage**:
- Local (file-based) or PostgreSQL backend
- Stores in `quartzdb_data/metastore/` directory

### 2. **Ingest Nodes**

**Responsibility**: Data entry point with validation and routing

**Input Formats Supported**:
- NDJSON (Newline Delimited JSON)
- InfluxLine Protocol
- Prometheus format

**Processing Pipeline**:
1. **Parse**: Convert incoming bytestream into structured documents
2. **Validate**: Verify documents conform to index schema
3. **Batch**: Collect documents into batches
4. **Route**: Use rendez-vous hashing to determine target storage node

**Rendez-vous Hashing**: 
- Deterministic routing of documents to storage nodes
- Same document always goes to same node
- Enables distributed processing without coordination

**Output**:
- Sends batches to designated Storage nodes
- Acknowledges receipt to clients

### 3. **Storage Nodes**

**Responsibility**: Persistent storage and query execution

**Ingest Path**:
1. Receive batches from Ingest nodes
2. Convert batches into **splits** (compressed columnar format)
3. Upload splits to S3 for durability
4. Register splits in Metastore

**Split Format**:
- Apache Parquet columnar storage
- Compressed for efficient I/O
- Contains full row data plus statistics for filtering

**Query Execution**:
- Acts as DataFusion distributed worker
- Receives query plans from Search nodes
- Executes local computation on splits
- Returns results to Search nodes

**Data Locality**:
- Splits cached locally for fast access
- S3 provides durability
- Transparent failover to S3 on node loss

### 4. **Search Nodes**

**Responsibility**: Query interface and distributed query orchestration

**Query Interfaces**:
- **SQL**: Standard SQL queries
- **LogsQL**: Domain-specific language for logs (similar to Grafana LogQL)
- **MetricsQL**: Domain-specific language for metrics

**Query Processing**:
1. Parse query in native language
2. Query Metastore for relevant splits
3. Apply filtering predicates to reduce split set
4. Build distributed DataFusion execution plan
5. Schedule tasks to Storage nodes
6. Aggregate results and return to client

**Key Responsibilities**:
- Split selection based on time ranges and labels
- Query optimization
- Result aggregation
- Error handling and retry logic

## Data Flow

### Ingestion Path

```
Client Data Stream
       ↓
   [NDJSON/InfluxLine/Prometheus]
       ↓
┌──────────────────┐
│ Ingest Node      │
├──────────────────┤
│ • Parse          │
│ • Validate       │
│ • Batch          │
│ • Route          │
└──────────────────┘
       ↓
(Rendez-vous Hash: index_name + doc_id → storage_node_id)
       ↓
┌──────────────────┐
│ Storage Node     │
├──────────────────┤
│ • Convert Batch  │
│   to Split       │
│ • Compress       │
│ • Upload to S3   │
└──────────────────┘
       ↓
    [S3 Storage]
       ↓
┌──────────────────┐
│ Metastore Node   │
├──────────────────┤
│ • Register Split │
│ • Update Index   │
│   Statistics     │
└──────────────────┘
```

### Query Path

```
┌──────────────┐
│ Client Query │
│ (SQL/LogsQL) │
└──────────────┘
       ↓
┌──────────────────┐
│ Search Node      │
├──────────────────┤
│ • Parse Query    │
│ • Query Metastore│
│   for Splits     │
└──────────────────┘
       ↓
┌──────────────────┐
│ Metastore Node   │
├──────────────────┤
│ Return relevant  │
│ splits + metadata│
└──────────────────┘
       ↓
┌──────────────────────────┐
│ Search Node              │
├──────────────────────────┤
│ • Build DataFusion Plan  │
│ • Apply Filters          │
│ • Select Storage Nodes   │
└──────────────────────────┘
       ↓
┌─────────────────────────────────┐
│ Storage Nodes (Workers)         │
├─────────────────────────────────┤
│ • Execute local tasks           │
│ • Scan relevant splits          │
│ • Apply predicates              │
│ • Return intermediate results   │
└─────────────────────────────────┘
       ↓
┌──────────────────┐
│ Search Node      │
├──────────────────┤
│ • Merge Results  │
│ • Final Sort/Agg │
│ • Format Output  │
└──────────────────┘
       ↓
   Results to Client
```

## Communication Patterns

### Node-to-Node Communication

| From → To | Protocol | Message Type | Purpose |
|-----------|----------|--------------|---------|
| Ingest → Storage | gRPC/HTTP | DocumentBatch | Send parsed documents |
| Storage → Metastore | gRPC/HTTP | RegisterSplit | Register new split |
| Search → Metastore | gRPC/HTTP | GetSplits | Query split metadata |
| Search → Storage | gRPC/HTTP | ExecutePlan | Distributed query task |
| Storage → Search | gRPC/HTTP | QueryResult | Return execution results |

### Data Persistence

- **Metastore**: File-based (local) or PostgreSQL database
- **Storage**: S3 for split files + local cache
- **Write-ahead Logging**: Event log in Metastore for durability

## Deployment Topology

### Single Node (Development)
```
localhost:8000 - Ingest
localhost:8001 - Search
localhost:8002 - Storage
localhost:8003 - Metastore
```

### Multi-Node (Production)
```
Ingest Cluster:     3+ nodes (replicated)
Search Cluster:     2+ nodes (load-balanced)
Storage Cluster:    3+ nodes (distributed)
Metastore Cluster:  2+ nodes (HA setup)
S3:                 External object store
```

## Key Design Decisions

1. **Rendez-vous Hashing**: Deterministic routing avoids coordination overhead
2. **Columnar Storage**: Parquet format enables efficient filtering and compression
3. **Separation of Concerns**: Each node type specialized for its role
4. **Eventual Consistency**: Metastore-as-source-of-truth with async updates
5. **DataFusion for Compute**: Industry-standard query engine for flexibility
6. **S3 for Durability**: Decouples compute from storage for elasticity

## API Endpoints

### Ingest API
- `POST /ingest/:index_name` - Send data to index

### Search APIs
- `POST /query/sql` - Execute SQL query
- `POST /query/logsql` - Execute LogsQL query
- `POST /query/metricsql` - Execute MetricsQL query

### Admin APIs
- `POST /index/create` - Create new index
- `POST /index/delete/:name` - Delete index
- `GET /index/list` - List all indexes
- `POST /index/update` - Update index configuration
