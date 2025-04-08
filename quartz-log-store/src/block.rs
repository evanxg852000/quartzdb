use std::io::{Cursor, Read, Write};

use lz4_flex::{
    block::get_maximum_output_size,
    frame::{FrameDecoder, FrameEncoder},
};

#[derive(Debug)]
pub enum BlockSize {
    KB16,
    KB32,
    KB64,
}

impl BlockSize {
    pub fn count(&self) -> u64 {
        match self {
            BlockSize::KB16 => 16 * 1024,
            BlockSize::KB32 => 32 * 1024,
            BlockSize::KB64 => 64 * 1024,
        }
    }
}


#[derive(Debug, Eq, PartialEq, Ord, PartialOrd)]
pub struct BlockMeta {
    pub min_log_id: u64,
    pub max_log_id: u64,
    pub offset: u32,
}

impl BlockMeta {
    pub fn new(block_offset: u32, block_data: &[u8]) -> Self {
        let min_log_id = u64::from_le_bytes(block_data[0..8].try_into().unwrap());
        let max_log_id = u64::from_le_bytes(block_data[8..16].try_into().unwrap());
        BlockMeta {min_log_id,  max_log_id, offset: block_offset }
    }

}

#[derive(Debug)]
pub struct BlockHeader {
    min_log_id: u64,
    max_log_id: u64,
    offsets: Vec<usize>,
}

impl BlockHeader {
    pub fn new() -> Self {
        BlockHeader {
            min_log_id: 0,
            max_log_id: 0,
            offsets: Vec::new(),
        }
    }

    pub fn from_bytes(data: &[u8]) -> Self {
        let min_log_id = u64::from_le_bytes(data[0..8].try_into().unwrap());
        let max_log_id = u64::from_le_bytes(data[8..16].try_into().unwrap());
        let entry_count = u16::from_le_bytes(data[16..18].try_into().unwrap()) as usize;
        let mut offsets = Vec::with_capacity(entry_count as usize);

        for i in 0..entry_count {
            let start = 18 + i * 2;
            let end = 18 + (i + 1) * 2;
            let offset = u16::from_le_bytes(data[start..end].try_into().unwrap());
            offsets.push(offset as usize);
        }

        BlockHeader {
            min_log_id: min_log_id,
            max_log_id: max_log_id,
            offsets: offsets,
        }
    }

    pub fn into_bytes(&self) -> Vec<u8> {
        let mut data = Vec::with_capacity(self.encoded_size());
        data.extend_from_slice(&self.min_log_id.to_le_bytes());
        data.extend_from_slice(&self.max_log_id.to_le_bytes());
        data.extend_from_slice(&(self.offsets.len() as u16).to_le_bytes());
        for offset in &self.offsets {
            data.extend_from_slice(&offset.to_le_bytes());
        }
        data
    }

    pub fn add_entry(&mut self, entry_id: u64 , entry_offset: usize) {
        assert!(self.max_log_id < entry_id, "entry_id should be monotonically increasing");
        if self.offsets.is_empty() {
            self.min_log_id = entry_id;
        }
        self.offsets.push(entry_offset);
        self.max_log_id = entry_id;
    }

    // Compute the size of the header when encoded
    // - 8 bytes for min_log_id
    // - 8 bytes for max_log_id
    // - 2 bytes for entry count
    // - 2 bytes for each entry offset
    pub fn encoded_size(&self) -> usize {
        2 * std::mem::size_of::<u64>()
            + std::mem::size_of::<u16>()
            + self.offsets.len() * std::mem::size_of::<u16>()
    }
}

#[derive(Debug)]
pub struct Block {
    header: BlockHeader,
    data: Vec<u8>,
}

impl Block {
    pub fn from_bytes(data: Vec<u8>) -> Self {
        let header = BlockHeader::from_bytes(&data);
        Block { header, data }
    }

    pub fn get_entry(&self, log_id: u64) -> Option<Vec<u8>> {
        let index = (log_id - self.header.min_log_id) as usize;
        if index >= self.header.offsets.len() {
            return None;
        }

        let start = self.header.offsets[index] as usize;
        let end = if index + 1 < self.header.offsets.len() {
            self.header.offsets[index + 1] as usize
        } else {
            self.data.len()
        };

        Some(self.data[start..end].to_vec())
    }
}

struct BlockIterator<'a> {
    block: &'a Block,
    index: usize,
}

// This is the block that is written to the log file
#[derive(Debug)]
pub struct CompressedBlock {
    data: Vec<u8>,
}

impl CompressedBlock {
    pub fn new(data: Vec<u8>) -> Self {
        CompressedBlock { data }
    }

    pub fn into_bytes(self) -> Vec<u8> {
        self.data
    }

    pub fn into_block(self) -> Block {
        let header = BlockHeader::from_bytes(&self.data);
        let header_len = header.encoded_size();
        let bytes_reader = Cursor::new(&self.data[header_len..]);
        let mut decompressor = FrameDecoder::new(bytes_reader);
        let mut data = Vec::new();
        decompressor.read_to_end(&mut data).unwrap();
        Block { header, data }
    }
}

#[derive(Debug)]
pub struct BlockBuilder {
    block_size: usize,
    header: BlockHeader,
    compressor: FrameEncoder<Vec<u8>>,
}

impl BlockBuilder {
    pub fn new(block_size: usize) -> Self {
        BlockBuilder {
            block_size: block_size,
            header: BlockHeader::new(),
            compressor: FrameEncoder::new(Vec::with_capacity(block_size)),
        }
    }

    pub fn append(&mut self, entry_id: u64, entry_data: &[u8]) -> Result<bool, String> {
        let header_len = self.header.encoded_size();
        let compressed_data_len = self.compressor.get_ref().len();
        let compressed_entry_len = get_maximum_output_size(entry_data.len());

        let final_len = header_len + compressed_data_len + compressed_entry_len;
        if final_len > self.block_size {
            return Ok(false);
        }

        self.header.add_entry(entry_id, compressed_data_len);
        self.compressor.write_all(entry_data).unwrap();
        Ok(true)
    }

    pub fn finish(self) -> CompressedBlock {
        let mut data = self.compressor.finish().unwrap();
        let header = self.header.into_bytes();
        data.splice(0..0, header);
        CompressedBlock::new(data)
    }
}
