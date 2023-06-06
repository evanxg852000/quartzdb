# QuartzDB
An experimental time series database for learning


## Referrence

- InfluxDB iox: https://github.com/influxdata/influxdb_iox
- QuestDB: https://questdb.io/docs/introduction
- Apache Arrow: https://docs.rs/arrow/latest/arrow/
- DataFusion: https://arrow.apache.org/datafusion/index.html
- https://leanpub.com/how-query-engines-work

loaded_in_ram
part_meta:
    bloom_filter: all-metrics(__name__)
    start_ts
    end_ts

Global FST -> __name__ -> part_num

part
 fst: label -> id

# example of log
```json
{
    "timestamp": 123332342,
    "line": "log_raw_data", # stored once in a bin file
    "labels": {
        "app": "api-service",
        "env": "prod",
        "node": "dcpe-2",
    },
    "values": {
        "cpu_usage": 69.0,
        "num_concurrent_request": 300,
        "total_num_request": 3049876,
        "log_level": "INFO",
    }
}
```
4 entries should be drawn from this LOG submission
```json
[
    {
        "labels": {
            "__name__": "cpu_usage",
            "app": "api-service",
            "env": "prod",
            "node": "dcpe-2",
        },
        "timestamp": 123332342,
        "line": 12, # can be null if not log
        "value": 69.0,
    },
    {
        "labels": {
            "__name__": "num_concurrent_request",
            "app": "api-service",
            "env": "prod",
            "node": "dcpe-2",
        },
        "timestamp": 123332342,
        "value": 300
    },
    {
        "labels": {
            "__name__": "total_num_request",
            "app": "api-service",
            "env": "prod",
            "node": "dcpe-2",
        },
        "timestamp": 123332342,
        "value": 3049876
    },
    {
        "labels": {
            "__name__": "log_level",
            "app": "api-service",
            "env": "prod",
            "node": "dcpe-2",
        },
        "timestamp": 123332342,
        "value": "INFO"
    }
]
```
