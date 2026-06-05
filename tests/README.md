# Test Commands

```bash
# start server
cargo run -- run

# upsert table
cargo run -- table put --file ./tests/data/logs-index-config.yaml

# list tables
cargo run -- table list

# ingest data
cargo run -- ingest --name logs --file ./tests/data/sample-logs.ndjson

# query data
cargo run -- query --

# delete table
cargo run -- table delete --name logs
