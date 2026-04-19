time-series: are meant for analytics
logs/traces: are meant for exploration, listing 

series_id: is a time series identifier
stream_id: is a log stream identifier

time-series:
Is a set of data that is meant for analytics. it is identified by a series_id.
A time-series has labels  or tags, with a special label called __name__.
The series_id is obtained by computing a hash value of the sorted list of labels.   

In storage, time-series data are store as (timestamp, value) pairs in chunks of parquet encoded bytes.


log stream: 
Is a set of logs coming from the same machine & the same app instance, user decides how to group them.
log stream also have labels useful during search.
During ingestion of logs, users specifies the attributes that should be considered as:
- labels: become the set of labels or tags for the log_stream, which help compute the stream_id the same way series_id is computed.
- metrics: as set of field for which values are extracted and used to create time-series 

example: 
```json
{
    "_timestamp": 123445834857,
    "_msg": "a log sample log line",
    "instance": "k8s-service",
    "region": "us-west",
    "cpu_usage": 0.6,
    "request_count": 123,
}
```
User can specify that:
labels: [instance, region]
metrics: [cpu_usage, request_count]

This will create the following objects:
- log_stream: 
    - stream_id: 4
    - labels[instance:k8s-service, region:us-west]
- log_entry: (inside a log_entries group corresponding to stream_id:4)
    - _timestamp: 123445834857
    - _msg: "a log sample log line"

- time-series:
    - series_id: 5
    - labels: {__name__:cpu_usage, __stream_id:4}
    - time-series-entry: (inside series_entries group corresponding to series_id:5)
        - _timestamp: 123445834857
        - value: 0.6

- time-series:
    - series_id: 6
    - labels: {__name__:request_count, __stream_id:4}
    - time-series-entry: (inside series_entries corresponding to series_id:5)
        - _timestamp: 123445834857
        - value: 123

Another log of this kind (same stream) will produce the same number of entries in log_entries and series_entries.

Simple Query:
1. with a simple log query (listing), we perform full-text search on the log, collect the stream_ids.
2. With the stream_ids, we select all series_ids by _stream_id label `Query::AnyOf(__stream_id, [stream_ids...])`

3. From our example, we will have: stream_id:4 & series_id[5,6], in order to fetch and merge the data, we place the cursor at the start_timestamp on all data tracks:
 - log entries: 4
 - series entries: 5
 - series entries: 6
We fetch each row and combine as log entry for sending to the requester.

Analytic Query:
1. For analytic query, we proceed the same way as simple query in step 1.
2. We select only the series_ids that our analytic query is interested in.
if for instance we are only interested in computing the average of `cpu_usage`, then our query might 
look like this: `Query::And(Query::AnyOf(__stream_id, [stream_ids...]), Query::TermEq("__name__", "cpu_usage") )`
3. with the series_ids at hand we can now fetch the data from parquet storage chunk and perform the computation (aggregation).


https://docs.pola.rs/

