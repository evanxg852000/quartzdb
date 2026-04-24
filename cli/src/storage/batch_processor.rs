use std::sync::Arc;

use anyhow::{Result, anyhow};
use datafusion::arrow::array::{ArrayRef, BooleanArray, Float64Array, Int64Array, LargeStringArray, LargeStringBuilder, RecordBatch, StringArray, TimestampNanosecondBuilder, UInt64Array, UInt64Builder};
use datafusion::parquet::arrow::AsyncArrowWriter;
use proto::quartzdb::{FieldValue, ProtoDocumentBatch};
use tokio::sync::oneshot;
use tantivy::doc;

use crate::common::index::FieldType;
use crate::{
    common::{index::IndexConfig, processors::Processor, schema::{QUARTZDB_ID_FIELD_NAME, QUARTZDB_VALUE_FIELD_NAME, Schema}},
    storage::{self, storage_impl::StorageImpl},
};

#[derive(Debug, Clone)]
pub struct BatchProcessor {
    storage: Arc<StorageImpl>,
    index_name: String,
    index_config: Arc<IndexConfig>,
}

impl Processor for BatchProcessor {}

impl BatchProcessor {
    pub fn new(
        storage: Arc<StorageImpl>,
        index_name: String,
        index_config: Arc<IndexConfig>,
    ) -> Self {
        Self {
            storage,
            index_name,
            index_config,
        }
    }

    pub async fn put_batch(
        &self,
        batch: ProtoDocumentBatch,
        reply_sender: oneshot::Sender<()>,
    ) -> Result<()> {
        put_batch(
            self.storage.clone(),
            self.index_name.clone(),
            &self.index_config,
            batch,
        )
        .await?;
        reply_sender
            .send(())
            .map_err(|_| anyhow!("Failed to send on reply mailbox"))?;
        Ok(())
    }
}

async fn put_batch(
    storage: Arc<StorageImpl>,
    index_name: String,
    index_config: &IndexConfig,
    batch: ProtoDocumentBatch,
) -> Result<()> {
    //TODO: perform the parquet & tantivy magic
    println!("Storing split for {}", index_name);

    let index_dir = storage.directory
        .join(index_name);
    tokio::fs::create_dir_all(&index_dir.join("index")).await?;

    // tantivy
    let fts_schema =Schema::get_fts_schema();
    let index = tantivy::Index::create_in_dir(index_dir.join("index"), fts_schema.clone());
    if let Err(err) = &index {
        println!("{}", err.to_string());
    }
    let index = index?;
    println!("index created");
    let mut index_writer = index.writer(50_000_000)?;
    let id_field = fts_schema.get_field(QUARTZDB_ID_FIELD_NAME).unwrap();
    let obj_field = fts_schema.get_field(QUARTZDB_VALUE_FIELD_NAME).unwrap();
    for proto_doc in &batch.documents {
        index_writer.add_document(tantivy::doc!(
            id_field => proto_doc.id,
            obj_field => proto_doc.labels,
        )).unwrap();
    }
    index_writer.commit().unwrap();
    println!("end tantivy index");
    
    //parquet
    let data_schema = Schema::get_primary_schema(index_config);
    let capacity = batch.documents.len();
    let mut ids_builder = UInt64Builder::with_capacity(capacity);
    let mut timestamps_builder = TimestampNanosecondBuilder::with_capacity(capacity).with_timezone("UTC");
    let mut sources_builder = LargeStringBuilder::with_capacity(capacity, capacity*200);

    let num_dynamic_columns = index_config.fields.len();
    let dynamic_colums_types = index_config.fields.iter().map(|f|f.field_type).collect::<Vec<_>>();
    let mut dynamic_columns: Vec<Vec<FieldValue>> = Vec::with_capacity(num_dynamic_columns);
    for _ in 0..num_dynamic_columns {
        dynamic_columns.push(Vec::with_capacity(capacity));
    }

    // build columnar values
    for proto_doc in batch.documents {
        ids_builder.append_value(proto_doc.id);
        timestamps_builder.append_value(proto_doc.timestamp);
        sources_builder.append_value(proto_doc.source);

        // put all columns values toghether
        for (i, v) in proto_doc.values.into_iter().enumerate() {
            dynamic_columns[i].push(v);
        }
    }

    let mut column_data: Vec<ArrayRef> = vec![
            Arc::new(ids_builder.finish()),
            Arc::new(timestamps_builder.finish()),
            Arc::new(sources_builder.finish()),
    ];
    for (i, dynamic_column) in dynamic_columns.into_iter().enumerate() {
        let column_array: ArrayRef = match &dynamic_colums_types[i] {
            FieldType::Uint => Arc::new(UInt64Array::from_iter(dynamic_column.into_iter().map(|fv| fv.as_u64()))),
            FieldType::Int => Arc::new(Int64Array::from_iter(dynamic_column.into_iter().map(|fv| fv.as_i64()))),
            FieldType::Float => Arc::new(Float64Array::from_iter(dynamic_column.into_iter().map(|fv| fv.as_f64()))),
            FieldType::String => Arc::new(StringArray::from_iter(dynamic_column.into_iter().map(|fv| fv.as_string()))),
            FieldType::Bool => Arc::new(BooleanArray::from_iter(dynamic_column.into_iter().map(|fv| fv.as_bool()))),
        };
        column_data.push(column_array);
    }

    let batch = RecordBatch::try_new(data_schema, column_data).unwrap();
    let file = tokio::fs::File::create(index_dir.join("data.parquet")).await?;
    let mut writer = AsyncArrowWriter::try_new(file, batch.schema(), None)?;
    writer.write(&batch).await?;
    writer.close().await?;


    // build the split in temporary scratch folder

    // upload it or move it to storage folder

    // publish it

    Ok(())
}
