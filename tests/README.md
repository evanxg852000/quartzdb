# Test Commands

```bash
# start server
cargo run -- run

# create index
cargo run -- index create --file ./tests/data/logs-index-config.yaml

# update index
cargo run -- index update --file ./tests/data/logs-index-config.yaml

# list indexes
cargo run -- index list

# ingest data
cargo run -- ingest --name logs --file ./tests/data/sample-logs.ndjson

# query data
cargo run -- query --

# delete index
cargo run -- index delete --name logs
