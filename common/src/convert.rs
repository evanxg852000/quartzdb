use anyhow::Result;
use bytes::Bytes;
use datafusion::arrow::{array::RecordBatch, ipc::{reader::StreamReader, writer::StreamWriter}};
use std::io::Cursor;
use serde_json::Value;

pub fn record_batch_to_bytes(record_batch: RecordBatch) -> Result<Bytes> {
    let mut data = Vec::new();
    let mut writer = StreamWriter::try_new(&mut data, &record_batch.schema())?;
    writer.write(&record_batch)?;
    writer.finish()?;
    Ok(Bytes::from(data))
}

pub fn record_batch_to_json(record_batch: RecordBatch) -> Result<Vec<Value>> {
    let mut buffer = Vec::new();
    // ArrayWriter formats the output as a JSON array
    let mut writer = arrow_json::ArrayWriter::new(&mut buffer);
    writer.write_batches(&[&record_batch])?;
    writer.finish()?;
    let value = serde_json::from_slice(&buffer)?;
    Ok(value)
}

pub fn record_batch_from_bytes(data: Bytes) -> Result<RecordBatch> {
    let cursor = Cursor::new(data);
    let mut reader = StreamReader::try_new(cursor, None)?;
    let record_batch = reader.next()
        .ok_or_else(||anyhow::anyhow!(""))??;
    Ok(record_batch)
}
