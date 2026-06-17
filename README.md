# QuartzDB

> **⚠ Experimental — Work in Progress**
> QuartzDB is in early-stage development (v0.0.1). Most components are minimum viable implementations or barely fleshed out — expect incomplete features, rough edges, and breaking changes to the API, configuration format, and storage layout at any time. It is not yet suitable for production use.

A high-performance distributed database for storing and querying logs and metrics. QuartzDB ingests structured time-series data, stores it in compressed columnar splits, and queries it via SQL with built-in full-text search.

## Features

- **Multi-format ingestion** — NDJSON, InfluxDB Line Protocol, Prometheus (NDJSON currently available)
- **SQL queries with full-text search** — powered by [Apache DataFusion](https://datafusion.apache.org/) and [Tantivy](https://github.com/quickwit-oss/tantivy) via the `qtz_search` table function
- **Columnar storage** — data stored as compressed Apache Parquet splits for efficient I/O
- **Object storage backend** — local filesystem, AWS S3, GCP, or Azure Blob Storage
- **Flexible metastore** — filesystem (default), SQLite, PostgreSQL, or remote gRPC
- **Distributed architecture** — ingest, search, storage, and metastore nodes scale independently
- **Single-binary deployment** — all node types can run in one process for development

## Quick Start

### Prerequisites

- [Rust](https://rustup.rs/) toolchain (edition 2024)

### Run the server

```bash
git clone https://github.com/quartzdb/quartzdb.git
cd quartzdb
cargo run -- run
```

The server starts on `127.0.0.1:8080` (HTTP) and `127.0.0.1:8081` (gRPC) by default.

To use a custom config file:

```bash
cargo run -- run --config ./quartzdb.yaml
```

### Ingest and query data

```bash
# Create a table
cargo run -- table put --file ./tests/data/logs-table-config.yaml

# List tables
cargo run -- table list

# Ingest NDJSON data
cargo run -- ingest --name logs --file ./path/to/logs.ndjson

# Query with SQL
cargo run -- search --name logs --query "SELECT * FROM qtz_search(logs, 'foo:*ali') AS t LIMIT 5"

# Aggregate query
cargo run -- search --name logs --query "SELECT COUNT(*) FROM qtz_search(logs, 'hostname:*mobi')"

# Delete a table
cargo run -- table delete --name logs
```

### Example output

```
./quartzdb search --name logs --query "SELECT * FROM qtz_search(logs, 'hostname:*mobi') WHERE appname = 'Scarface'"
+----------+--------------------------+----------+----------+----------+---------+
| __qtz_id | __qtz_timestamp          | appname  | facility | severity | version |
+----------+--------------------------+----------+----------+----------+---------+
| 7383     | 2025-03-14T20:00:49.618Z | Scarface | syslog   | emerg    | 1       |
| 6        | 2025-03-14T19:59:35.849Z | Scarface | local2   | crit     | 2       |
+----------+--------------------------+----------+----------+----------+---------+
```

## Configuration

Copy the example config and adjust for your environment:

```bash
cp configs/schema/quartzdb.yaml ./quartzdb.yaml
```

```yaml
# quartzdb.yaml
id: "node1"
address: "127.0.0.1:8080"   # HTTP on this port, gRPC on port+1

storage:
  directory: "./qtzdb_data"  # local cache directory
  uri: s3://my-bucket/       # object storage URI (local path or s3://)
  cache:
    policy: lru
    capacity: 3              # GB

metastore:
  type: fs                   # fs | memory | sqlite | postgres | remote
  # type: postgres
  # uri: "postgresql://user:password@localhost:5432/quartzdb"

ingester:
  enable: true

searcher:
  enable: true

storer:
  enable: true
```

Configuration is loaded in priority order: defaults → `quartzdb.yaml` in the working directory → environment variables prefixed with `QUARTZDB_`.

## Table Schema

Tables are defined in YAML. Each table has a name, a schema config, and optional per-table settings.

```yaml
# logs-table-config.yaml
name: logs

config:
  timestamp: timestamp       # field to use as the document timestamp
  labels:                    # full-text search indexed fields
    - hostname
    - service
  tags:                      # bloom-filter indexed fields (fast equality filters)
    - host
  fields:                    # typed structured fields
    - name: appname
      type: string
    - name: severity
      type: string
    - name: version
      type: int

settings:
  ingester:
    num_worker: 3
    batch_size: 1000
    commit_timeout_secs: 5000
  retention:
    period: "90 days"
    schedule: daily
```

| Field type | Description |
|------------|-------------|
| `labels` | Free-text fields indexed with Tantivy; searchable via `qtz_search` |
| `tags` | Low-cardinality fields with bloom-filter index for fast equality pruning |
| `fields` | Typed structured columns stored in Parquet |

## Querying

QuartzDB uses Apache DataFusion as its SQL engine. The `qtz_search` table function applies a full-text filter on `labels` fields before the SQL predicate is evaluated.

```sql
-- Full-text search across all label fields
SELECT * FROM qtz_search(logs, '*error*') LIMIT 100;

-- Wildcard search on a specific label field
SELECT * FROM qtz_search(logs, 'hostname:web-*') LIMIT 50;

-- Combine full-text search with SQL predicates
SELECT appname, COUNT(*) AS cnt
FROM qtz_search(logs, 'hostname:*mobi') AS t
WHERE severity = 'emerg'
GROUP BY appname
ORDER BY cnt DESC;

-- Aggregate over time
SELECT COUNT(*) FROM qtz_search(logs, 'service:auth');
```

## HTTP API

All endpoints are served under `/api/v1/`.

| Method | Path | Description |
|--------|------|-------------|
| `POST` | `/api/v1/ingest/ndjson/{table_name}` | Ingest NDJSON data into a table |
| `POST` | `/api/v1/search/{table_name}` | Execute a SQL query against a table |
| `GET` | `/api/v1/metastore/tables` | List all tables |
| `PUT` | `/api/v1/metastore/tables` | Create or update a table |
| `DELETE` | `/api/v1/metastore/tables/{name}` | Delete a table |

## Architecture

QuartzDB has four logical node types that can run on one machine or be distributed across a cluster.

```
┌─────────────────────────────────────────────────────────────────┐
│                        QuartzDB Cluster                         │
│                                                                 │
│  ┌─────────────┐   ┌─────────────┐   ┌─────────────┐            │
│  │   Ingest    │   │   Search    │   │  Metastore  │            │
│  │   Nodes     │   │   Nodes     │   │   Nodes     │            │
│  │ • Parse     │   │ • SQL API   │   │ • Table meta│            │
│  │ • Validate  │   │ • Prunning  │   │ • Split reg │            │
│  │ • Batch     │   │ • Planning  │   │ • Event log │            │
│  └─────────────┘   └─────────────┘   └─────────────┘            │
│         │                 │                  ▲                  │
│         └─────────────────┼──────────────────┘                  │
│                    ┌──────┴──────┐                              │
│                    │   Storage   │                              │
│                    │    Nodes    │                              │
│                    │ • Splits    │                              │
│                    │ • S3 upload │                              │
│                    │ • Workers   │                              │
│                    └─────────────┘                              │
│                           │                                     │
│              ┌────────────────────────┐                         │
│              │   Object Storage (S3)  │                         │
│              │   Split files (Parquet)│                         │
│              └────────────────────────┘                         │
└─────────────────────────────────────────────────────────────────┘
```

| Node | Role |
|------|------|
| **Ingest** | Parses incoming data, validates against the table schema, batches documents, and routes them to storage nodes using rendez-vous hashing |
| **Storage** | Converts batches into compressed Parquet splits, uploads them to S3, and acts as a DataFusion worker for distributed query execution |
| **Search** | Receives SQL queries, selects relevant splits from the metastore, builds a distributed DataFusion execution plan, and merges results |
| **Metastore** | Single source of truth for table schemas, split locations, and the cluster event log |

For a deeper dive see [`docs/architecture/README.md`](docs/architecture/README.md).

## Building a Release Binary

```bash
cargo build --release
./target/release/quartzdb run
```

## License

QuartzDB is licensed under the [GNU Affero General Public License v3.0](LICENSE).
