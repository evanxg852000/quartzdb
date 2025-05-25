# QuartzDB
An experimental time series database for learning

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


Table Banc: 450.000 GNF
Bois Fournis: 180.000 GNF
Bois-Rouge(Akajou)
longueur:2m10 -> 30*30
avec-casier -> 50 madrier -> 655.000
sans-casier -> 40 madrier

porte-en-bois: 100.000 gnf
porte-en-fer: 

32.750
40.
-----

Fassou-Macon:
Soudeur:

one log_id belong to a stream_id
one log_id can produce many series_id
[labels]_name:method ->  timestamp value
[labels]_name:request_time:  timestamp value

series_id, log_id(nullable log_id from which this series entry is from)
 stream_id(nullable), 

series_table: [series_id, name, labels, __stream_id:3? ]
time_series_data_table: group_by(series_id) -> [ts, value]

stream_table: [stream_id, name, labels]
log_store: group_by(stream_id) -> [ts, log_content]



Quartz-indexing
    - FullTextSearchIndex(Tantivy)
        -> build, list-term keys, values
        -> all tantivy query, 
        -> victoria-metrics filters
    - InvertedIndex(FST)
        -> build, list-term keys, values
        -> search, boolean, regex, fuzzy, 
    - BloomFilterIndex
    - BitmapIndex
    - SkipListIndex


Storage Model:
.meta: store split info for pruning {bloom, min-ts..max-ts}
.idx/: stores the index files
.offsets: maps series_id/stream_id to their location in .values/.store files
.values: stores timeseries data [ts, v]
.store: stores log data [ts, msgpack]

index -> series_id/stream_id
    series_id/stream_id -> info{id, group, tags, type(metric/log)} from .idx/
    series_id -> .offsets -> .values {[ts, v]}
    stream_id -> .offsets -> .store {[ts, msgpack]}

