

# QuartzDB
An experimental time series database for learning


## Quick start commands
```bash
# start server
cargo run -- run

# upsert table
cargo run -- table put --file ./tests/data/logs-table-config.yaml

# list tables
cargo run -- table list

# ingest data
cargo run -- ingest --name logs --file ./tests/data/sample-logs.ndjson

# query table (with sql)
cargo run -- search --name logs --query "SELECT * from qtz_search(logs, 'foo:*ali') as t limit 5"
cargo run -- search --name logs --query "SELECT COUNT(*) from qtz_search(logs, 'hostname:*mobi')"

# delete table
cargo run -- table delete --name logs
```
