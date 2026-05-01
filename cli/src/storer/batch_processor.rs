use std::sync::Arc;

use anyhow::{Result, anyhow};
use datafusion::arrow::array::{
    ArrayRef, BooleanArray, Float64Array, Int64Array, LargeStringBuilder, RecordBatch, StringArray,
    TimestampNanosecondBuilder, UInt64Array,
};
use datafusion::parquet::arrow::AsyncArrowWriter;
use proto::quartzdb::{FieldValue, ProtoDocumentBatch};
use storage::Storage;
use storage::remote_storage::RemoteStorage;
use tantivy::query::QueryParser;
use tantivy::{Directory, doc};
use tokio::sync::oneshot;

use crate::common::index::{FieldType, IndexMeta, SplitMeta};
use crate::metastore::client::MetastoreClient;
use crate::storer::split::index_store::fast_field_collector::U64FastFieldCollector;
use crate::storer::split::index_store::packed_directory::PackedDirectory;
use crate::storer::split::index_store::packed_file::PackedFileWriter;
use crate::storer::split::writter::SplitWriter;
use crate::{
    common::{
        processors::Processor,
        schema::{QUARTZDB_LABELS_FIELD_NAME, QUARTZDB_ROW_INDEX_FIELD_NAME, Schema},
    },
    storer::storage_impl::StorageImpl,
};

#[derive(Debug, Clone)]
pub struct StorerContext {
    index_meta: Arc<IndexMeta>,
    storage: Arc<dyn Storage>,
    metastore_client: MetastoreClient,
}

impl StorerContext {
    pub async fn new(index_meta: Arc<IndexMeta>, storage: Arc<dyn Storage>, metastore_client: MetastoreClient) -> Result<Self> {
        let mut index_storage = storage;
        if let Some(index_storage_settings) = &index_meta.settings.storage {
            index_storage = index_storage.derive_remote(&index_storage_settings.uri).await?;
        }
        Ok(Self {
            index_meta,
            storage: index_storage,
            metastore_client,
        })
    }
}

#[derive(Debug)]
pub struct BatchProcessor {
    context: Arc<StorerContext>,
}

impl Processor for BatchProcessor {}

impl BatchProcessor {
    pub fn new(context: Arc<StorerContext>) -> Self {
        Self { context }
    }

    pub async fn put_batch(
        &self,
        batch: ProtoDocumentBatch,
        reply_sender: oneshot::Sender<()>,
    ) -> Result<()> {
        put_batch(self.context.clone(), batch).await?;
        reply_sender
            .send(())
            .map_err(|_| anyhow!("Failed to send on reply mailbox"))?;
        Ok(())
    }
}

async fn put_batch(context: Arc<StorerContext>, batch: ProtoDocumentBatch) -> Result<()> {
    let storage = context.storage.clone();
    let index_name = context.index_meta.name.clone();
    let index_config = &context.index_meta.config;

    // build split & upload it
    let mut split_writer = SplitWriter::try_new(index_name, storage.clone()).await?;
    split_writer.write(batch, index_config).await?;
    let split_meta = split_writer.finalize().await?;

    // publish it
    context.metastore_client.put_split(split_meta).await?;
    Ok(())
}

/*

    // try to register split in metastore

    let storage = context.storage.clone();
    let index_name = context.index_meta.name.clone();
    let index_config = &context.index_meta.config;
    //TODO: perform the parquet & tantivy magic
    println!("Storing split for {}", index_name);
    println!("DocBatch legth: {}", batch.documents.len());

    // IndexBuilder(with bloom_filter), DataBuilder, SplitBuilder

    let index_dir = storage.directory.join(index_name);
    tokio::fs::create_dir_all(&index_dir.join("index")).await?;

    // tantivy
    let fts_schema = Schema::get_fts_schema();
    let index = tantivy::Index::create_in_dir(index_dir.join("index"), fts_schema.clone());
    if let Err(err) = &index {
        println!("{}", err.to_string());
    }
    let index = index?;
    println!("index created");
    let mut index_writer = index.writer(50_000_000)?;
    let row_index_field = fts_schema.get_field(QUARTZDB_ROW_INDEX_FIELD_NAME).unwrap();
    let labels_field = fts_schema.get_field(QUARTZDB_LABELS_FIELD_NAME).unwrap();
    for (index, proto_doc) in batch.documents.iter().enumerate() {
        let labels_object = serde_json::from_str::<serde_json::Value>(&proto_doc.labels)?;
        index_writer
            .add_document(tantivy::doc!(
                row_index_field => index as u64,
                labels_field => labels_object,
            ))
            .unwrap();

        proto_doc.tags
    }
    index_writer.commit().unwrap();
    let segment_ids = index
        .searchable_segments()?
        .iter()
        .map(|s| s.id())
        .collect::<Vec<_>>();
    index_writer.merge(&segment_ids).await?;
    index_writer.wait_merging_threads()?;
    println!("end tantivy index");

    //pack index
    let index_dir_manager = index.directory();
    let mut file_packer = PackedFileWriter::new(index_dir.join("data.idx")).await?;
    for f in index_dir_manager.list_managed_files() {
        let moved_dir = index_dir_manager.clone();
        let moved_f = f.clone();
        let data = tokio::task::spawn_blocking(move || {
            let data = moved_dir.atomic_read(moved_f.as_path())?;
            anyhow::Ok(data)
        })
        .await??;
        file_packer.add(f, data).await?;
    }
    file_packer.finilize().await?;
    println!("end packing index");

    println!("search inside the packed index");
    let packed_file = index_dir.join("data.idx");
    let packed_directory = PackedDirectory::new(packed_file.as_path()).await?;
    let new_index = tantivy::Index::open(packed_directory)?;
    let reader = new_index.reader()?;
    let searcher = reader.searcher();

    let query_parser = QueryParser::for_index(&index, vec![labels_field]);
    let query = query_parser.parse_query(r#"hostname:"some.casa""#)?;
    let collector = U64FastFieldCollector::new(QUARTZDB_ROW_INDEX_FIELD_NAME);
    let row_indices = searcher.search(&query, &collector)?;
    for row_indexe in row_indices {
        println!("->: {:?}", batch.documents[row_indexe as usize]);
    }

    // build data file (parquet)
    let data_schema = Schema::get_primary_schema(&index_config);
    let capacity = batch.documents.len();
    let mut timestamps_builder =
        TimestampNanosecondBuilder::with_capacity(capacity).with_timezone("UTC");
    let mut sources_builder = LargeStringBuilder::with_capacity(capacity, capacity * 200);

    let num_dynamic_columns = index_config.fields.len();
    let dynamic_colums_types = index_config
        .fields
        .iter()
        .map(|f| f.field_type)
        .collect::<Vec<_>>();
    let mut dynamic_columns: Vec<Vec<FieldValue>> = Vec::with_capacity(num_dynamic_columns);
    for _ in 0..num_dynamic_columns {
        dynamic_columns.push(Vec::with_capacity(capacity));
    }

    // build columnar values
    for proto_doc in batch.documents {
        timestamps_builder.append_value(proto_doc.timestamp);
        sources_builder.append_value(proto_doc.source);

        // put all columns values toghether
        for (i, v) in proto_doc.values.into_iter().enumerate() {
            dynamic_columns[i].push(v);
        }
    }

    let mut column_data: Vec<ArrayRef> = vec![
        Arc::new(timestamps_builder.finish()),
        Arc::new(sources_builder.finish()),
    ];
    for (i, dynamic_column) in dynamic_columns.into_iter().enumerate() {
        let column_array: ArrayRef = match &dynamic_colums_types[i] {
            FieldType::Uint => Arc::new(UInt64Array::from_iter(
                dynamic_column.into_iter().map(|fv| fv.as_u64()),
            )),
            FieldType::Int => Arc::new(Int64Array::from_iter(
                dynamic_column.into_iter().map(|fv| fv.as_i64()),
            )),
            FieldType::Float => Arc::new(Float64Array::from_iter(
                dynamic_column.into_iter().map(|fv| fv.as_f64()),
            )),
            FieldType::String => Arc::new(StringArray::from_iter(
                dynamic_column.into_iter().map(|fv| fv.as_string()),
            )),
            FieldType::Bool => Arc::new(BooleanArray::from_iter(
                dynamic_column.into_iter().map(|fv| fv.as_bool()),
            )),
        };
        column_data.push(column_array);
    }

    // let props = WriterProperties::builder()
    //     // .set_compression(parquet::basic::Compression::SNAPPY)
    //     .set_max_row_group_row_count(Some(1024 * 2))
    //     .set_data_page_row_count_limit(value)
    //     .build();

    let batch = RecordBatch::try_new(data_schema, column_data).unwrap();
    let file = tokio::fs::File::create(index_dir.join("data.parquet")).await?;
    let mut writer = AsyncArrowWriter::try_new(file, batch.schema(), None)?;
    writer.write(&batch).await?;
    writer.close().await?;

*/
