use std::{path::PathBuf, sync::Arc};

use anyhow::Result;
use datafusion::{
    arrow::array::{
        ArrayRef, BooleanArray, Float64Array, Int64Array, LargeStringBuilder, RecordBatch,
        StringArray, TimestampNanosecondBuilder, UInt64Array,
    },
    parquet::{
        arrow::AsyncArrowWriter, basic::Compression, file::properties::WriterProperties,
    },
};
use fastbloom::BloomFilter;
use proto::quartzdb::{FieldValue, ProtoDocumentBatch};
use storage::Storage;
use tempfile::TempDir;

use tantivy::{Directory, doc};

use crate::{
    common::{
        index::{FieldType, IndexConfig, SplitMeta},
        schema::{QUARTZDB_LABELS_FIELD_NAME, QUARTZDB_ROW_INDEX_FIELD_NAME, Schema},
    },
    storer::{split::index_store::packed_file::PackedFileWriter, storage_impl::StorageImpl},
};

const INDEXING_MEMORY_BUDGET: usize = 50 * 1024 * 1024;

pub struct SplitWriter {
    split_id: String,
    split_dir: PathBuf,
    index_name: String,
    storage: Arc<dyn Storage>,
    scratch_dir: TempDir,
    min_timestamp: i64,
    max_timestamp: i64,
}

impl SplitWriter {
    pub async fn try_new(index_name: String, storage: Arc<dyn Storage>) -> Result<Self> {
        let split_id = uuid::Uuid::now_v7().to_string();
        let split_dir = storage.root()
            .join(&index_name)
            .join(&split_id);
        let scratch_dir = storage.tempdir()?;
        Ok(Self {
            split_id,
            split_dir,
            index_name,
            storage,
            scratch_dir,
            min_timestamp: 0,
            max_timestamp: 0,
        })
    }

    pub async fn write(
        &mut self,
        batch: ProtoDocumentBatch,
        index_config: &IndexConfig,
    ) -> Result<()> {
        self.min_timestamp = batch.min_timestamp();
        self.max_timestamp = batch.max_timestamp();
        self.write_index(&batch).await?;
        self.write_data(batch, index_config).await?;
        Ok(())
    }

    /// The drop of self here removes the sractch dir
    pub async fn finalize(self) -> Result<SplitMeta> {
        // Move index & data into storage /index/<index_name>/{split.idx, split.bin}
        // aka upload to storage
        // let index_file_path = self.index_dir.join(format!("{}.idx", self.split_id));
        // self.storage.put(index_file_path, index_file_path).await?;

        let split_meta = SplitMeta {
            split_id: self.split_id,
            index_name: self.index_name,
            min_timestamp: self.min_timestamp,
            max_timestamp: self.max_timestamp,
        };
        Ok(split_meta)
    }

    async fn write_index(&self, batch: &ProtoDocumentBatch) -> Result<()> {
        // create tantivy index & bloom filter
        let fts_schema = Schema::get_fts_schema();
        let index = tantivy::Index::create_in_dir(&self.scratch_dir, fts_schema.clone())?;
        let mut index_writer = index.writer_with_num_threads(2, INDEXING_MEMORY_BUDGET)?;
        let mut bloom_filter = BloomFilter::with_false_pos(0.001).expected_items(batch.len());

        let row_index_field = fts_schema.get_field(QUARTZDB_ROW_INDEX_FIELD_NAME)?;
        let labels_field = fts_schema.get_field(QUARTZDB_LABELS_FIELD_NAME)?;
        for (index, proto_doc) in batch.documents.iter().enumerate() {
            let labels_json_object = serde_json::from_str::<serde_json::Value>(&proto_doc.labels)?;
            index_writer.add_document(tantivy::doc!(
                row_index_field => index as u64,
                labels_field => labels_json_object,
            ))?;
            for tag in &proto_doc.tags {
                bloom_filter.insert(tag);
            }
        }
        index_writer.commit()?;
        // compact index by mergings small segements
        let segment_ids = index
            .searchable_segments()?
            .iter()
            .map(|s| s.id())
            .collect::<Vec<_>>();
        index_writer.merge(&segment_ids).await?;
        index_writer.wait_merging_threads()?;

        // pack resulting index files & bloom filter file into split_id.idx
        let index_file_path = self.split_dir.join("index.qtz");
        let index_dir_manager = index.directory();
        let mut file_packer = PackedFileWriter::new(index_file_path).await?;
        for path in index_dir_manager.list_managed_files() {
            let moved_dir = index_dir_manager.clone();
            let moved_path = path.clone();
            let data = tokio::task::spawn_blocking(move || {
                let data = moved_dir.atomic_read(moved_path.as_path())?;
                anyhow::Ok(data)
            })
            .await??;
            file_packer.add(path, data).await?;
        }

        // add bloom filter file as filter.bin
        let bloom_fileter_data: Vec<u8> = bincode::serialize(&bloom_filter)?;
        file_packer.add("bloom.qtz", bloom_fileter_data).await?;

        // finalize index file packing
        file_packer.finilize().await?;
        Ok(())
    }

    async fn write_data(
        &self,
        batch: ProtoDocumentBatch,
        index_config: &IndexConfig,
    ) -> Result<()> {
        let data_schema = Schema::get_primary_schema(index_config);
        let capacity = batch.documents.len();
        let mut timestamps_builder =
            TimestampNanosecondBuilder::with_capacity(capacity).with_timezone("UTC");
        let mut sources_builder = LargeStringBuilder::with_capacity(capacity, capacity * 200);

        let num_dynamic_columns = index_config.fields.len();
        let dynamic_colums_types = index_config
            .fields
            .iter()
            .map(|field_config| field_config.field_type)
            .collect::<Vec<_>>();
        let mut dynamic_columns: Vec<Vec<FieldValue>> = Vec::with_capacity(num_dynamic_columns);
        for _ in 0..num_dynamic_columns {
            dynamic_columns.push(Vec::with_capacity(capacity));
        }

        // group columnar values while building timestamp & source arrays
        for proto_doc in batch.documents {
            timestamps_builder.append_value(proto_doc.timestamp);
            sources_builder.append_value(proto_doc.source);

            // Put all remainign columns values toghether
            for (dynamic_column_index, field_value) in proto_doc.values.into_iter().enumerate() {
                dynamic_columns[dynamic_column_index].push(field_value);
            }
        }

        let mut column_data: Vec<ArrayRef> = vec![
            Arc::new(timestamps_builder.finish()),
            Arc::new(sources_builder.finish()),
        ];
        for (i, dynamic_column) in dynamic_columns.into_iter().enumerate() {
            let column_array: ArrayRef = match &dynamic_colums_types[i] {
                FieldType::Uint => Arc::new(UInt64Array::from_iter(
                    dynamic_column
                        .into_iter()
                        .map(|field_value| field_value.as_u64()),
                )),
                FieldType::Int => Arc::new(Int64Array::from_iter(
                    dynamic_column
                        .into_iter()
                        .map(|field_value| field_value.as_i64()),
                )),
                FieldType::Float => Arc::new(Float64Array::from_iter(
                    dynamic_column
                        .into_iter()
                        .map(|field_value| field_value.as_f64()),
                )),
                FieldType::String => Arc::new(StringArray::from_iter(
                    dynamic_column
                        .into_iter()
                        .map(|field_value| field_value.as_string()),
                )),
                FieldType::Bool => Arc::new(BooleanArray::from_iter(
                    dynamic_column
                        .into_iter()
                        .map(|field_value| field_value.as_bool()),
                )),
            };
            column_data.push(column_array);
        }

        // try clickHouse pros to see
        let parquet_opts = WriterProperties::builder()
            .set_compression(Compression::SNAPPY)
            .set_max_row_group_row_count(Some(1024 * 2))
            .build();
        let data_file_path = self.split_dir.join("data.qtz");
        let batch = RecordBatch::try_new(data_schema, column_data).unwrap();
        let data_file = tokio::fs::File::create(data_file_path).await?;
        let mut parquet_writer =
            AsyncArrowWriter::try_new(data_file, batch.schema(), Some(parquet_opts))?;
        parquet_writer.write(&batch).await?;
        parquet_writer.close().await?;
        Ok(())
    }
}
