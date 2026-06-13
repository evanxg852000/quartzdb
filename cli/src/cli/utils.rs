use std::path::Path;

use anyhow::Result;
use common::schema::QUARTZDB_SOURCE_FIELD_NAME;
use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::compute::concat_batches;
use serde::de::DeserializeOwned;
use tokio::fs;

use std::io::Cursor;
use std::sync::Arc;
use arrow_json::reader::infer_json_schema;
use arrow_json::ReaderBuilder;

pub async fn read_as_object<T: DeserializeOwned>(file_path: &Path) -> Result<T> {
    let file_extension = file_path
        .extension()
        .map(|os_str| os_str.to_string_lossy().into_owned())
        .ok_or_else(|| anyhow::anyhow!("File must have extention to know the type"))?;

    let data = fs::read(file_path).await?;
    match file_extension.as_str() {
        "yaml" => serde_norway::from_slice::<T>(&data).map_err(|err| anyhow::anyhow!(err)),
        "json" => serde_json::from_slice::<T>(&data).map_err(|err| anyhow::anyhow!(err)),
        _ => Err(anyhow::anyhow!("Supported file formats are: `json, yamal`")),
    }
}

pub fn json_array_record_batch(json_array: &serde_json::Value) -> Result<RecordBatch> {
    let ndjson_data = json_array.as_array()
        .unwrap()
        .iter()
        .map(|item| item.to_string())
        .collect::<Vec<_>>()
        .join("\n");
    let mut cursor = Cursor::new(ndjson_data);
    let (inferred_schema, _records_read) = infer_json_schema(&mut cursor, None)?;
    let schema_ref = Arc::new(inferred_schema);

    let columns_to_keep: Vec<usize> = schema_ref
        .fields()
        .iter()
        .enumerate()
        .filter(|(_, field)| field.name() != QUARTZDB_SOURCE_FIELD_NAME)
        .map(|(index, _)| index)
        .collect();

    std::io::Seek::seek(&mut cursor, std::io::SeekFrom::Start(0))?;
    let mut reader = ReaderBuilder::new(schema_ref.clone())
        .with_batch_size(1024)
        .build(cursor)?;
    let mut result = RecordBatch::new_empty(schema_ref.clone());
    while let Some(batch_result) = reader.next() {
        let batch: RecordBatch = batch_result?;
        result = concat_batches(&schema_ref, [&result, &batch])?;
    }
    Ok(result.project(&columns_to_keep).unwrap())
}
