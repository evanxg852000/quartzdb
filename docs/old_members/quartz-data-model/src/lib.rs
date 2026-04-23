// https://docs.rs/datafusion/latest/datafusion/index.html
// https://docs.rs/arrow/latest/arrow/

use arrow::{array::Int64Builder, ipc::Bool};
use bincode::de;
use datafusion::arrow::{array::{BooleanBuilder, Float32Builder, Float64Builder, Int32Builder, PrimitiveBuilder, RecordBatch, StringBuilder}, datatypes::Schema};


// https://docs.greptime.com/contributor-guide/datanode/data-persistence-indexing/#inverted-index-format
// https://iceberg.apache.org/puffin-spec/#blobmetadata

// https://docs.influxdata.com/influxdb/cloud/reference/key-concepts/data-elements/#sample-data
#[derive(Debug)]
struct Database {
    name: String,
    tables: HashMap<String, TableRef>,
}

struct Table {
    name: String,
    schema: SchemaRef,
}

type TableRef = Arc<Table>;


enum ColumnType {
    Bool,
    UInt32,
    UInt64,
    Int32,
    Int64,
    Float32,
    Float64,
    String,
    DateTime
}

enum ColumnValue {
    Bool(bool),
    UInt32(u32),
    UInt64(u64),
    Int32(i32),
    Int64(i64),
    Float32(f32),
    Float64(f64),
    String(String),
    DateTime(u64), // timestamp in milliseconds
    Null,
}

enum ColumnSematic {
    Timestamp, // timestamp column
    Metric,  // metric column
    FullTextSearchIndex, // full-text search index column
    TokenizedFullTextSearchIndex, // tokenized full-text search index column
    Tag, // tag column
}


struct Column {
    name: String,
    column_type: ColumnType,
    semantic: ColumnSematic,
    is_nullable: bool,
    default_value: Option<ColumnValue>,
}

type ColumnRef = Arc<Column>;

struct Schema {
    // list of columns
    columns: Vec<Column>,
    // index of the columns that make up the primary_key
    // This is the tag columns but sorted in alphabetical order by name
    // it's also called partition keys & sort keys
    sort_columns: Vec<usize>, 
    // index of the timestamp column
    timestamp_column: usize, 
    // fts-index columns
    fts_columns: Vec<usize>,
    // indices of metric columns
    metric_columns: Vec<usize>,
    // indices of tag columns
    tag_columns: Vec<usize>,

    // options: e.g. compression, retention_policy, compaction_policy etc.
    options: HashMap<String, Box<dyn Any>>,
}

type  SchemaRef = Arc<Schema>;


impl Schema {
    pub fn get_schema() -> Schema {
        todo!()
    }



    pub fn validate_row(&self, row: Vec<ColumnValue>) -> Result<(), String> {
        if row.len() != self.columns.len() {
            return Err(format!("Row length {} does not match schema length {}", row.len(), self.columns.len()));
        }
        for (i, column) in self.columns.iter().enumerate() {
            let value = &row[i];
            if !self.validate_column_value(column, value) {
                return Err(format!("Invalid value {:?} for column {:?}", value, column));
            }
        }
        Ok(())
    }

    pub fn validate_column_value(&self, column: &Column, value: &ColumnValue) -> bool {
        match (column.column_type, value) {
            (ColumnType::Bool, ColumnValue::Bool(_)) => true,
            (ColumnType::UInt32, ColumnValue::UInt32(_)) => true,
            (ColumnType::UInt64, ColumnValue::UInt64(_)) => true,
            (ColumnType::Int32, ColumnValue::Int32(_)) => true,
            (ColumnType::Int64, ColumnValue::Int64(_)) => true,
            (ColumnType::Float32, ColumnValue::Float32(_)) => true,
            (ColumnType::Float64, ColumnValue::Float64(_)) => true,
            (ColumnType::String, ColumnValue::String(_)) => true,
            (ColumnType::DateTime, ColumnValue::DateTime(_)) => true,
            _ => false,
        }
    }

    pub fn get_column(&self, name: &str) -> Option<ColumnRef> {
        self.columns.iter().find(|c| c.name == name).cloned()
    }
    
    pub fn get_column_by_index(&self, index: usize) -> Option<ColumnRef> {
        self.columns.get(index).cloned()
    }



}

struct SchemaBuilder {
    columns: Vec<Column>,
    primary_columns: Vec<usize>,
    timestamp_column: usize,
    fts_columns: Vec<usize>,
    metric_columns: Vec<usize>,
    tag_columns: Vec<usize>,
}
impl SchemaBuilder {
    pub fn new() -> Self {
        Self {
            columns: Vec::new(),
            primary_columns: Vec::new(),
            timestamp_column: 0,
            fts_columns: Vec::new(),
            metric_columns: Vec::new(),
            tag_columns: Vec::new(),
        }
    }

    pub fn add_column(&mut self, column: Column) -> &mut Self {
        self.columns.push(column);
        self
    }

    pub fn set_primary_columns(&mut self, primary_columns: Vec<usize>) -> &mut Self {
        self.primary_columns = primary_columns;
        self
    }

    pub fn set_timestamp_column(&mut self, timestamp_column: usize) -> &mut Self {
        self.timestamp_column = timestamp_column;
        self
    }

    pub fn set_fts_columns(&mut self, fts_columns: Vec<usize>) -> &mut Self {
        self.fts_columns = fts_columns;
        self
    }

    pub fn set_metric_columns(&mut self, metric_columns: Vec<usize>) -> &mut Self {
        self.metric_columns = metric_columns;
        self
    }

    pub fn set_tag_columns(&mut self, tag_columns: Vec<usize>) -> &mut Self {
        self.tag_columns = tag_columns;
        self
    }

    pub fn build(self) -> Schema {
        Schema {
            columns: self.columns,
            primary_columns: self.primary_columns,
            timestamp_column: self.timestamp_column,
            fts_columns: self.fts_columns,
            metric_columns: self.metric_columns,
            tag_columns: self.tag_columns,
            options: HashMap::new(),
        }
    }
}







enum PrimitiveColumnBuilder {
    Bool(BooleanBuilder),
    Int32(Int32Builder),
    Int64(Int64Builder),
    Float32(Float32Builder),
    Float64(Float64Builder),
    String(StringBuilder),
}

struct DataBlockBuilder {
    table_schema: Arc<TableSchema>,
    data: Vec<PrimitiveColumnBuilder>,
    min_timestamp: u64,
    max_timestamp: u64,
}

struct DataBlock {
    data: RecordBatch,
    min_timestamp: u64,
    max_timestamp: u64,
}



#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        let result = 2 + 2;
        assert_eq!(result, 4);
    }
}
