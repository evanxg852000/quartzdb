

# QuartzDB
An experimental time series database for learning


## Quick start commands
```bash
# start server
cargo run -- run

# create table
cargo run -- table put --file ./configs/schema/table-config.yaml

# update table
cargo run -- index update --file ./configs/index-config.yaml

# list tables
cargo run -- table list

# insert data
cargo run -- ingest --name github_events --file ./tests/data/

# delete table
cargo run -- table delete --name github_events

# query table (with sql)
cargo run -- search --name logs --query "select * from qtz_search(logs, 'foo:*ali') as t limit 5"
```
