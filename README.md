# QuartzDB
An high performance database tailored for storng logs and metrics.


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

```bash
./quartzdb search --name logs --query "select* from qtz_search(logs, 'hostname:*mobi') where appname = 'Scarface'"
+----------+--------------------------+----------+----------+----------+---------+
| __qtz_id | __qtz_timestamp          | appname  | facility | severity | version |
+----------+--------------------------+----------+----------+----------+---------+
| 7383     | 2025-03-14T20:00:49.618Z | Scarface | syslog   | emerg    | 1       |
| 6        | 2025-03-14T19:59:35.849Z | Scarface | local2   | crit     | 2       |
| 6        | 2025-03-14T19:59:35.849Z | Scarface | local2   | crit     | 2       |
| 7383     | 2025-03-14T20:00:49.618Z | Scarface | syslog   | emerg    | 1       |
+----------+--------------------------+----------+----------+----------+---------+
```
