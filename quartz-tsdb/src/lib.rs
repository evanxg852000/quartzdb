use std::{collections::{HashMap, btree_map::Range, BTreeMap}, sync::Arc};

use crossbeam::channel as channel;
use ingester::Value;

mod series_info;
mod ingester;
mod encoding;
mod int_mapping;


enum DataType{
    Int,
    Float,
}

// unique_id of time series
type SeriesID = Vec<u8>;

// type RawChunk = Vec<(u64, f64)>;

/// RawChunk
struct RawChunk {
    data_type: DataType,
    first_timestamp: i64,
    first_value: Value,

    // Time series compressed data 
    // data.0(timestamp): monotonic increasing (double delta encoded, varint)
    // data.1(value): converted to i64 or f64 & gorilla encoded
    data: Vec<(u64, f64)>,
}

// a RawBlock is a data ingestion block destined
// to be used by a single ingestion thread. 
struct RawBlock {
    chunks: HashMap<SeriesID, RawChunk>
}


// After ingestion period this is constructed
// by collecting raw blocks from ingestion thread
// and grouping them by series unique name. 
// This is finally compressed and built as disk format block
// that becomes queryable.
struct Block {
    index: (),
    bloom_filter: (),
    //TODO: find a very fast hasher
    chunks: HashMap<SeriesID, RawChunk>
}




struct DiskBlockMeta {
    
}

struct DiskBlock {
    // string sorted uuid 
    block_id: String,
    // merged from other block
    sources: Vec<String>,
    // number of time this block has been merged
    num_merge: usize,

    index: (), // FST of tags -> sorted [ series_ordinal_ids ]
    // Be smart when creating/configuring this because the time 
    //we know exactly the terms it will contain
    // based on tags/labels
    // metric name is also a tag with special label __name__
    bloom_filter: (),   
    start_timestamp: u64,
    start_timestamp: u64,

    //TODO: map of label name to (num-values, start position of values)
    // maybe the fst can give this back this
    // - return all tags with prefix `foo:*` (OK)
    // - return all prefix `*:any` (check tantivy)
    //  when building the fst
    labels: BTreeMap<String, usize>,

    //map series_id to chunk (start_position, byte-size)
    // ordered by ordinal_ids
    // OPTIMIZATION: we could cache the uncompressed version of this data
    // with key being {block_id}-{ordinal-id}
    chunk_positions: Vec<(u64, usize)>, 
    mmap_file: // block file mmaped
}

impl DiskBlock {
    //Matcher: see prometheus
    fn scan(matcher: Matcher) -> Iter<item=RecordBatch> {
        todo!()
    }
} 


trait IngestionCommitter {
    fn can_commit(block: &RawBlock) -> bool;
}

trait BlockOptimizer {
    // Useful for moving a chunk to another location via 
    // arrow flight.
    fn optimize(raw_blocks: Vec<(SeriesID, RawBlock)>) -> Block;
}

/* example

let tsdb = TsDbOptions::default()
        .compaction_check_frequency(1200)
        .committer() //commit policy
        .blockOptimizer() // block optimization policy
        .compacter() // compaction
        .wal() // set wal interface
        .open("my-time-series.db")?;

let ingester = tsdb.writer()?;

// just grab what is available as blocks 
// (at the time the searcher is created) no lock at all
let search = tsdb.searcher()?;

 */

type OperationBatch = Vec<(u64, f64)>;

struct TsDB {
    blocks: Vec<Arc<DiskBlock>>,
    // maybe add chunk cache of uncompressed chunks
    // chunk_cache: Cache<Chunk>,
    batch_receiver: channel::Receiver<OperationBatch>,
}

impl TsDB {


    fn search(&self) -> Searcher {
        Searcher { blocks: self.blocks.iter().cloned() }
    }

    fn search(&self) -> Searcher {
        Searcher { blocks: self.blocks.iter().cloned() }
    }
}


struct Searcher {
    blocks: Vec<Arc<DiskBlock>>,
}

impl Searcher {
    fn search(matcher: Matcher) -> Iter<item=RecordBatch> {
        
    }
}

struct Writer {
    sender: channel::Sender<OperationBatch>
}

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        let result = 2 + 2;
        assert_eq!(result, 4);
    }
}
