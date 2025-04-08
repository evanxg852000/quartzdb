use std::{collections::HashMap, fs::OpenOptions, io::Write, path::Path};

use memmap2::Mmap;
use quartz_common::{DataBlock, LogRecordBatch, LogStream, ObjectId, TimeSeries, TimeSeriesRecordBatch};

use super::DATA_FILE;


#[derive(Debug)]
pub enum ChunkType {
    TimeSeries,
    LogStream,
}

#[derive(Debug)]
struct ChunkInfo {
    object_id: ObjectId,
    chunk_type: ChunkType,
    offset: usize,
    length: usize,
}


#[derive(Debug)]
pub struct SegmentData {
    chunk_entries: HashMap<ObjectId, ChunkInfo>,
    data_file: Mmap,
}

impl SegmentData {
    pub fn create(
        directory: impl AsRef<Path>,
        time_series_data: Vec<&TimeSeriesRecordBatch>,
        log_data: Vec<&LogRecordBatch>,
    ) -> Self {
        let data_file_path = directory.as_ref().join(DATA_FILE);
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(data_file_path)
            .unwrap();
        
        //TODO: write & encode chunks
        let mut chunk_entries = HashMap::new();
        let mut offset = 0;
        for batch in time_series_data {
            let mut data_block = DataBlock::try_from(batch.get_rows()).unwrap();
            let data= data_block.serialize_to_parquet_buffer();
            file.write(&data).unwrap();
            let time_series_id = batch.get_time_series().time_series_id;
            let chunk_info = ChunkInfo {
                object_id: time_series_id,
                chunk_type: ChunkType::TimeSeries,
                offset: offset,
                length: data.len(),
            };
            chunk_entries.insert(time_series_id, chunk_info);
            offset += data.len();
        }

        for batch in log_data {
            let mut data_block = DataBlock::try_from(batch.get_rows()).unwrap();
            let data= data_block.serialize_to_parquet_buffer();
            file.write(&data).unwrap();
            let log_stream_id = batch.get_log_stream().log_stream_id;
            let chunk_info = ChunkInfo {
                object_id: log_stream_id,
                chunk_type: ChunkType::LogStream,
                offset: offset,
                length: data.len(),
            };
            chunk_entries.insert(log_stream_id, chunk_info);
            offset += data.len();
        }

        // write chunk entries as footer
        let footer_offset = offset as u32;
        let num_chunks = chunk_entries.len() as u32;
        file.write(&num_chunks.to_be_bytes()).unwrap();
        for (object_id, chunk_info) in chunk_entries.iter() {
            file.write(&object_id.to_be_bytes()).unwrap();
            let chunk_type = match chunk_info.chunk_type {
                ChunkType::TimeSeries => 0u16,
                ChunkType::LogStream => 1u16,
            };
            file.write(&chunk_type.to_be_bytes()).unwrap();
            file.write(&(chunk_info.offset as u32).to_be_bytes()).unwrap();
            file.write(&(chunk_info.length as u32).to_be_bytes()).unwrap();
        }
        file.write(&footer_offset.to_be_bytes()).unwrap();
        file.flush().unwrap();

        let data_file = unsafe { Mmap::map(&file).unwrap() };
        Self {
            chunk_entries: HashMap::new(),
            data_file,
        }
    }


    pub fn open(directory: impl AsRef<Path>) -> Self {
        let log_file_path = directory.as_ref().join(DATA_FILE);
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(log_file_path)
            .unwrap();
        let data_file = unsafe { Mmap::map(&file).unwrap() };

        let mut chunk_entries = HashMap::new();
        
        
        let mut offset = u32::from_be_bytes(data_file[data_file.len() - 4..].try_into().unwrap()) as usize;
        let num_chunks = u32::from_be_bytes(data_file[offset..offset + 4].try_into().unwrap());
        offset += 4;
        for _ in 0..num_chunks {
            let object_id = u64::from_be_bytes(data_file[offset..offset + 8].try_into().unwrap());
            offset += 8;
            let chunk_type = u16::from_be_bytes(data_file[offset..offset + 2].try_into().unwrap());
            offset += 2;
            let chunk_type = match chunk_type {
                0 => ChunkType::TimeSeries,
                1 => ChunkType::LogStream,
                _ => panic!("invalid chunk type"),
            };
           
            let target_offset = u32::from_be_bytes(data_file[offset..offset + 4].try_into().unwrap()) as usize;
            offset += 4;
            let length = u32::from_be_bytes(data_file[offset..offset + 4].try_into().unwrap()) as usize;
            offset += 4;
            chunk_entries.insert(object_id, ChunkInfo {
                object_id,
                chunk_type,
                offset: target_offset,
                length,
            });
        }

        Self {
            chunk_entries,
            data_file,
        }
    }

    pub fn fetch_block(&self, object_id: ObjectId) -> Result<DataBlock, String> {
        let chunk_info = self.chunk_entries.get(&object_id)
            .ok_or("chunk not found")?;
        let data = &self.data_file[chunk_info.offset..chunk_info.offset + chunk_info.length];
        let data_block = DataBlock::deserialize_from_parquet_buffer(data).unwrap();
        Ok(data_block)
    }
    
}
