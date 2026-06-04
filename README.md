

# QuartzDB
An experimental time series database for learning

https://github.com/datafusion-contrib/datafusion-distributed
https://datafusion-contrib.github.io/datafusion-distributed/user-guide/concepts.html

https://datafusion.apache.org/blog/2025/09/21/custom-types-using-metadata/

## node types
metastore
ingester
searcher
storer (store, query, compact, retention)

https://github.com/cloudflare/pingora



## few commands
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

TODO:
- [x] fix storer
- [x] split writter
- [ ] basic query with datafusion-distributed (searcher)
- [ ] tests
- [ ] MVP
- [ ] SQL -> QuestDB(SQL)/InfluxQL
- [ ] QtzQL -> combine (LogsQL-parser, MetricsQL) from VitoriMetrics
- [ ] caching mechanism

https://www.bitsxpages.com/p/how-metrics-are-stored-and-queried



Datafusion:
- https://datafusion-contrib.github.io/datafusion-distributed/
- https://datafusion.apache.org/

TODO:
- FtsIndex
- FsStorage
- QInsert/QStorage/QSelect

- ClickHouse Storage

## References

- InfluxDB iox: https://github.com/influxdata/influxdb_iox
- QuestDB: https://questdb.io/docs/introduction
- Apache Arrow: https://docs.rs/arrow/latest/arrow/
- DataFusion: https://arrow.apache.org/datafusion/index.html
- https://leanpub.com/how-query-engines-work
- https://howqueryengineswork.com/
- https://github.com/GreptimeTeam/greptimedb


https://github.com/dpgil/tstorage-rs

compression
Delta
DoubleDelta
Gorilla

https://www.timescale.com/blog/time-series-compression-algorithms-explained
https://docs.rs/delta-encoding/latest
https://github.com/udoprog/gorilla

https://github.com/andresilva/cask
https://www.meilisearch.com/blog/how-full-text-search-engines-work


-----
how it should look like in parquet:
_source: JSON(binary) -> BSON
_timestamp: created_at
field1: int
field2: string
field3: float
