# QuartzDB Architecture Quick Reference

## Component Quick Guide

| Component | Responsibility | Key Features | Scaling |
|-----------|-----------------|--------------|---------|
| **Ingest** | Entry point, parsing, validation, routing | • Multi-format input (NDJSON, InfluxLine, Prometheus)<br/>• Schema validation<br/>• Rendez-vous hashing<br/>• Batching | Horizontal - Load balance across nodes |
| **Storage** | Persistence, S3 upload, query worker | • Batch→Split conversion<br/>• Parquet compression<br/>• S3 integration<br/>• DataFusion executor | Horizontal - Hash-based distribution |
| **Search** | Query interface, execution planning | • SQL/LogsQL/MetricsQL<br/>• Split selection<br/>• Plan building<br/>• Result merging | Horizontal - Load balance queries |
| **Metastore** | Central metadata, source of truth | • Index registry<br/>• Split tracking<br/>• Event log<br/>• Consistency point | Vertical + Replication |

## Data Flow Summary

### Ingest Pipeline
```
ByteStream → Parse → Validate → Batch → Hash → Route → Storage Node
```

### Query Pipeline
```
Query → Parse → SelectSplits → BuildPlan → Execute(Parallel) → Merge → Results
```

## Message Types

### Ingest → Storage
- **DocumentBatch**: Collection of parsed documents with schema

### Storage → Metastore
- **RegisterSplit**: New split metadata (location, statistics, time range)
- **UpdateMetadata**: Index statistics updates

### Search → Metastore
- **GetSplits**: Query splits for index and time range
- **GetMetadata**: Fetch index schema and configuration

### Search ↔ Storage
- **ExecutePlan**: DataFusion task execution request
- **QueryResult**: Partial results from task execution

## Configuration Locations

- **Index Config**: `configs/index-config.yaml`
- **Server Config**: `configs/quartz.yml`
- **Data Directory**: `quartzdb_data/`
  - Metastore: `quartzdb_data/metastore/`
  - Storage logs: `quartzdb_data/storage/`

## Port Map (Default Single-Node)

| Service | Port | Protocol |
|---------|------|----------|
| Ingest | 8000 | HTTP/gRPC |
| Search | 8001 | HTTP/gRPC |
| Storage | 8002 | HTTP/gRPC |
| Metastore | 8003 | HTTP/gRPC |

## Key Algorithms

### Rendez-vous Hashing
```
target_node = hash(index_name + document_id) % num_storage_nodes
```
- Deterministic: Same document always → same node
- No coordination needed for routing
- Handles node additions gracefully

### Split Selection
```
relevant_splits = metastore.get_splits(
    index_name,
    time_range=[start, end],
    labels={filter_key: filter_value}
)
```
- Push-down predicates reduce I/O
- Time-based partitioning for quick filtering
- Label indexes for fast lookup

### Query Execution
```
1. Parse query in native language
2. Get candidate splits from metastore
3. Apply filter predicates (reduce splits)
4. Build DataFusion physical plan
5. Partition plan across storage nodes
6. Execute in parallel
7. Stream results back and merge
```

## Consistency Model

- **Metastore**: Strong consistency (source of truth)
- **Storage Nodes**: Eventual consistency
  - Batch received, processed locally first
  - Split uploaded asynchronously
  - Registration triggers on upload complete
- **Search**: Reads from metastore (sees latest registered splits)

## Failure Scenarios

| Failure | Impact | Recovery |
|---------|--------|----------|
| **Ingest Node Down** | New writes queued elsewhere | Auto-rebalance to other nodes |
| **Storage Node Down** | Splits still in S3 | Read from S3, redownload locally |
| **Search Node Down** | Queries fail | Retry on other search node |
| **Metastore Down** | No new operations possible | Failover to replica |
| **S3 Unavailable** | Can't upload new splits | Buffered locally, retry on restore |

## Performance Tuning

### Ingest Throughput
- Increase batch size (larger = higher latency, better compression)
- Increase number of ingest nodes (horizontal scale)
- Optimize parser for input format

### Query Speed
- Reduce time range in queries (fewer splits)
- Add label filters (faster split selection)
- Increase storage nodes (parallel execution)
- Cache frequently accessed splits

### Storage Efficiency
- Tune compression level (CPU vs storage trade-off)
- Optimize split size (time range vs file count)
- Enable predicate pushdown in queries

## Integration Points

### Input Interfaces
- HTTP REST `/ingest/:index_name`
- gRPC `ingest.proto::IngestService`

### Query Interfaces
- HTTP REST `/query/sql`
- HTTP REST `/query/logsql`
- HTTP REST `/query/metricsql`
- gRPC `search.proto::SearchService`

### Admin Interfaces
- CLI: `quartzdb index create|update|delete|list`
- HTTP REST: `/index/*`
- gRPC: `metastore.proto::MetastoreService`

## External Dependencies

- **S3**: Object storage for splits (required for production)
- **PostgreSQL**: Optional Metastore backend (defaults to local)
- **DataFusion**: Distributed query execution
- **Arrow**: In-memory columnar format
- **Protobuf**: Service definitions and serialization

## Monitoring Points

1. **Ingest Node**: Parse errors, batch latency, routing distribution
2. **Storage Node**: Split creation time, S3 upload latency, registration lag
3. **Search Node**: Query latency, split selection count, merge time
4. **Metastore**: Request latency, registration rate, consistency checks

## Links to Detailed Documentation

- [Full Architecture Overview](./README.md)
- [Mermaid Diagrams](./DIAGRAMS.md)
- [API Documentation](../user/README.md)
- [Development Guide](../dev/README.md)
