use std::{path::PathBuf, sync::Arc};

use common::schema::{QUARTZDB_LABELS_FIELD_NAME, QUARTZDB_ROW_ID_FIELD_NAME};
use datafusion::{arrow::datatypes::SchemaRef, error::{DataFusionError, Result}, execution::{SendableRecordBatchStream, context::SessionContext, options::ParquetReadOptions}, logical_expr::Expr, physical_plan::EmptyRecordBatchStream, };
use fastbloom::BloomFilter;
use datafusion::prelude::*;
// use storage::Storage;

use tantivy::query::QueryParser;

use crate::{search::context::TableSearchContext, split::index_store::{fast_field_collector::U64FastFieldCollector, packed_directory::PackedDirectory, packed_file::PackedFileReader}};

pub struct SplitReader {
    split_id: String,
    split_dir: PathBuf,
    /// decoded bloom filter
    tag_filter: BloomFilter,   
    /// openned tantivy index 
    index_store: tantivy::Index,
    column_store_file: String, 
}

impl SplitReader {

    pub async fn try_new(context: Arc<TableSearchContext>, split_id: &str) -> anyhow::Result<Self> {
        let storage = context.get_storage().clone();
        let table_name = context.get_table_meta().name.clone();

        //TODO: download split & wait to finish
        let _ = storage.exists("foo.txt").await;
        
        let split_dir = storage
            .root()
            .join(&table_name)
            .join(&split_id);
        
        let index_file_path = split_dir.join("index.qtz");
        let packed_index_file = PackedFileReader::new(index_file_path).await?;
        let bloom_filter_data = packed_index_file.get("bloom.qtz").await?;
        let tag_filter = bitcode::deserialize::<BloomFilter>(&bloom_filter_data)?;
        
        let packed_directory = PackedDirectory::new(packed_index_file);
        let index_store = tantivy::Index::open(packed_directory)?;

        let column_store_file =  split_dir.join("data.qtz")
            .to_string_lossy().to_string();
        
        Ok(Self{
            split_id: split_id.to_string(),
            split_dir,
            tag_filter,
            index_store,
            column_store_file,
        })
    }

    pub fn contain_tags(&self, tags: Vec<&str>) -> bool {
        tags.iter().any(|tag| self.tag_filter.contains(tag))
    }

    pub async fn search(
        &self, 
        schema: SchemaRef,
        projection: Option<Vec<usize>>,
        // filters: &[Expr], // data , timestamp 
        fts_expr: Option<String>,
        limit: Option<usize>,
    ) -> Result<SendableRecordBatchStream> {
        let fts_matched_ids = self.search_index_store(fts_expr, limit)
            .await
            .map_err(|err|DataFusionError::Execution(err.to_string()))?;

        self.fetch_column_store(schema, projection, fts_matched_ids, limit).await
    }

    async fn search_index_store(&self, fts_expr: Option<String>,  limit: Option<usize>) -> anyhow::Result<Option<Vec<u64>>> {
        match fts_expr {
            None => Ok(None),
            Some(fts_expr) if fts_expr == "*" => Ok(None),
            Some(fts_expr) =>  {
                let reader = self.index_store.reader()?;
                let searcher = reader.searcher();

                let fts_schema = self.index_store.schema();
                let labels_field = fts_schema.get_field(QUARTZDB_LABELS_FIELD_NAME)?;
                let query_parser = QueryParser::for_index(&self.index_store, vec![labels_field]);
                let tantivy_query = query_parser.parse_query(&fts_expr)?;
                // let (tantivy_query, _) = query_parser.parse_query_lenient(&fts_expr);

                let rows_collector = U64FastFieldCollector::new(QUARTZDB_ROW_ID_FIELD_NAME);
                //let collector_with_limit = &(TopDocs::with_limit(10), rows_collector)
                _ = limit;
                let rows = searcher.search(&tantivy_query, &rows_collector)?;
                Ok(Some(rows))
            }
        }
    }

    async fn fetch_column_store(
        &self, 
        schema: SchemaRef,
        projection: Option<Vec<usize>>,
        fts_matched_ids: Option<Vec<u64>>,
        limit: Option<usize>,
    ) -> Result<SendableRecordBatchStream> {
        let ctx = SessionContext::new();
        let read_options = ParquetReadOptions::default()
            .file_extension("")
            .schema(&*schema);
        let mut df = ctx.read_parquet(&self.column_store_file, read_options).await?;

        if let Some(fts_matched_ids) = fts_matched_ids  {
            if fts_matched_ids.is_empty() {
                return Ok(Self::empty_stream(schema));
            }

            df = df.filter(
            col(QUARTZDB_ROW_ID_FIELD_NAME).in_list(
                fts_matched_ids
                    .iter()
                    .map(|id|lit(*id))
                    .collect::<Vec<Expr>>(), 
                false,
            ))?;
        }

        if let Some(indices) = projection {
            let field_names = indices
                .iter()
                .map(|i| schema.field(*i).name().clone())
                .collect::<Vec<_>>();
            let df_projection = field_names
                .iter()
                .map(|v| v.as_str())
                .collect::<Vec<_>>();
            df = df.select_columns(&df_projection)?;
        }

        if let Some(limit) = limit {
            df = df.limit(0, Some(limit as usize))?;
        }

        // df = df.drop_columns(&[QUARTZDB_ROW_ID_FIELD_NAME])?;
        df.execute_stream().await
    }

    fn empty_stream(schema: SchemaRef) -> SendableRecordBatchStream {
        Box::pin(EmptyRecordBatchStream::new(schema))
    }
}


mod fixture {
    use std::sync::Arc;
    use datafusion::arrow::{array::{BooleanArray, Int32Array, RecordBatch, StringArray}, datatypes::{DataType, Field, Schema}};
    
    pub fn get_sample_batch() -> RecordBatch {
        // Define the arrays (columns)
        let id_array = Int32Array::from(vec![1, 2, 3]);
        let name_array = StringArray::from(vec![Some("Alice"), Some("Bob"), None]);
        let active_array = BooleanArray::from(vec![true, false, true]);

        // Define the schema matching the columns
        let schema = Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, true),
            Field::new("active", DataType::Boolean, false),
        ]);

        // Create the RecordBatch
        RecordBatch::try_new(
            Arc::new(schema),
            vec![
                Arc::new(id_array),
                Arc::new(name_array),
                Arc::new(active_array),
            ],
        ).unwrap()
    }

}


