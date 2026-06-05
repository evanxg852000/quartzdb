

# QuartzDB
An experimental time series database for learning


## Quick start commands
```bash
# start server
cargo run -- run

# create index
cargo run -- table put --file ./configs/schema/table-config.yaml

# update index
cargo run -- index update --file ./configs/index-config.yaml

# list indexes
cargo run -- table list

# insert data
cargo run -- ingest --name github_events --file ./tests/data/

# delete index
cargo run -- table delete --name github_events

cargo run -- query --name foo --query "select * from foo"
```
