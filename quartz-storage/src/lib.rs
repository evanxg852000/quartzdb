use std::any::Any;

mod engine;


// https://docs.greptime.com/contributor-guide/datanode/data-persistence-indexing/#inverted-index-format
// https://iceberg.apache.org/puffin-spec/#blobmetadata

// https://docs.influxdata.com/influxdb/cloud/reference/key-concepts/data-elements/#sample-data
struct Database {
    name: String,
    directory: String,
    tables: HashMap<String, Table>,
}

struct Table {
    name: String,
    schema: TableSchema,
    segments: Vec<Segment>,
}



enum ColumnType {
    Bool,
    Int32,
    Int64,
    Float32,
    Float64,
    String,
}

enum ColumnValue {
    Bool(bool),
    Int32(i32),
    Int64(i64),
    Float32(f32),
    Float64(f64),
    String(String),
}

enum ColumnSematic {
    Timestamp, // timestamp column
    Metric,  // metric column
    Message, // log message column
    Tag, // tag column
}

enum ColumnIndexType {
    InvertedIndex,
    FullTextIndex,
}


struct Column {
    name: String,
    column_type: ColumnType,
    sematic: ColumnSematic,
    is_nullable: bool,
    default_value: Option<ColumnValue>,
    index: Option<ColumnIndexType>,
}

struct TableSchema {
    // list of columns
    columns: Vec<Column>,
    // index of the columns that make up the primary_key
    // the order of the columns in the primary key is important
    // it's the same as partition keys & sort keys
    primary_keys: Vec<usize>, 
    // index of the timestamp column
    timestamp_key: usize, 
    // indices of tag columns
    tag_keys: Vec<usize>,
    // indices of metric columns
    metric_keys: Vec<usize>,
    // index of message column
    message_key: Option<usize>,
    // other table options
    // e.g. compression, partitioning, retention, compaction etc.
    options: HashMap<String, Box<dyn Any>>,
}

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        let result = 2 + 2;
        assert_eq!(result, 4);
    }
}
