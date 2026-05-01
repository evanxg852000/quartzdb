use std::{
    fs::OpenOptions, io::Write, path::Path
};

use memmap2::MmapMut;
use sorted_vec::SortedVec;

use crate::block::{BlockBuilder, BlockMeta, CompressedBlock};

const LOG_SEGMENT_FILE_EXTENSION: &str = "log";

#[derive(Debug)]
pub struct LogSegmentInner {
    id: String,
    log_file: MmapMut,
    block_size: usize,
    blocks: SortedVec<BlockMeta>,
    block_builder: BlockBuilder,
}

impl PartialEq for LogSegmentInner {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
    }
}

impl Eq for LogSegmentInner {}

impl PartialOrd for LogSegmentInner {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        self.id.partial_cmp(&other.id)
    }
}

impl Ord for LogSegmentInner {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.id.cmp(&other.id)
    } 
}

impl LogSegmentInner {
    pub fn open(directory: impl AsRef<Path>, segment_id: &str, block_size: usize) -> Self {
        let log_file_name = format!("{}.{}", segment_id, LOG_SEGMENT_FILE_EXTENSION);
        let log_file_path = directory.as_ref().join(log_file_name);
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(log_file_path)
            .unwrap();
        let file_size = file.metadata().unwrap().len();
        let block_count = file_size / block_size as u64;

        let log_file = unsafe { MmapMut::map_mut(&file).unwrap() };
        
        let blocks = match block_count {
            0 => SortedVec::new(),
            _ => {
                let mut blocks = SortedVec::with_capacity(block_count as usize);
                let mut offset = 0;
                for _ in 0..block_count {
                    let block_meta = BlockMeta::new(offset as u32, &log_file[offset..]);
                    offset += block_size;
                    blocks.push(block_meta);
                }
                blocks
            }
        };

        // let log_id_counter = start_log_id.map_or_else(|| {
        //     blocks.last().map_or(1, |meta| meta.max_log_id + 1)
        // }, |id| id);
        LogSegmentInner {
            id: segment_id.to_string(),
            log_file, 
            block_size,
            blocks,
            block_builder: BlockBuilder::new(block_size),
        }
    }

    pub fn get_min_log_id(&self) -> u64 {
        self.blocks.first().map_or(0, |meta| meta.min_log_id)
    }

    pub fn get_max_log_id(&self) -> u64 {
        self.blocks.last().map_or(0, |meta| meta.max_log_id)
    }

    pub fn get_log_file_size(&self) -> u64 {
        self.log_file.len() as u64
    }

    // Append a log entry to the log store
    // log_id is the id of the log entry.
    // It should be globally unique and monotonically increasing
    pub fn append(&mut self, log_id: u64, log_data: &[u8]) -> Result<(), String>  {
        let accepted = self.block_builder.append(log_id, log_data).unwrap();
        if accepted {
            return Ok(());
        }

        // block is full, compress & write to disk
        {
            let current_block_builder = std::mem::replace(&mut self.block_builder, BlockBuilder::new(self.block_size));
            let compressed_block = current_block_builder.finish();
            self.write_compressed_block(compressed_block);
        }

        // insert incoming log in new block builder by recursion
        self.append(log_id, log_data)
    }

    pub fn get(&self, log_id: u64) -> Option<Vec<u8>> {
        // find the block that contains the log_id
        let block_index = self.blocks.binary_search_by(|block_meta| {
            log_id.cmp(&block_meta.max_log_id) 
        }).ok()?;

        // extract the data from the block
        let block_meta = &self.blocks[block_index];
        let start = block_meta.offset as usize;
        let end = start + self.block_size;
        let block_data = &self.log_file[start..end];
        //TODO: use block cache
        let block = CompressedBlock::new(block_data.to_vec()).into_block();

        block.get_entry(log_id)
    }

    fn write_compressed_block(&mut self, compressed_block: CompressedBlock) {
        let mut data = compressed_block.into_bytes();
        // do we need padding?
        let padding_len = self.block_size - data.len();
        if padding_len != 0 {
            data.extend_from_slice(&vec![0; padding_len]);
        }

        let start = self.log_file.len();
        (&mut self.log_file[start..]).write_all(&data).unwrap();
        self.log_file.flush().unwrap();

        let block_meta = BlockMeta::new(start as u32, &self.log_file[start..]);
        self.blocks.push(block_meta);
    }
    
}



