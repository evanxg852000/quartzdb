use fst::Map;

smaples -> [chunks] -> get merged ->  split -> block



// Batch of documents.
// Most of the time, users will send operation one-by-one, but it can be useful to
// send them as a small block to ensure that
// - all docs in the operation will happen on the same segment and continuous doc_ids.
// - all operations in the group are committed at the same time, making the group
// atomic.
type OperationBatch = SmallVec<[Operation; 4]>;
type AddBatchSender = crossbeam_channel::channel::Sender<AddBatch>;
type AddBatchReceiver = crossbeam_channel::channel::Receiver<AddBatch>;

type SampleBatchSender = channel::Sender<OperationBatch>;
type SampleBatchReceiver = channel::Receiver<OperationBatch>; 

struct SampleWritter {
    sample_batch_sender: SampleBatchSender,
}

// type SampleWritter = mpsc::Sender<Smaple>;


struct DatabaseConfig {
    num_writter: usize,
    writer_memory_budget: usize, 
}

struct Database {
    /// metadata of all split, should be a highly concurrent hasmap 
    /// ordered by [start_ts,end_ts] so that we can time prune split early
    /// TODO: find a way to rapidly prune out split not in the range
    splits: Arc<Mutex<BTreeSet<SplitMetadata>>>,
    /// a cache loading split finite-state-transducer
    /// should be fast and big enougth to always host latest 
    /// splits
    series_cache: SeriesCache,

    // When a writer is complete by timeout or size treshold reached,
    // it should send its chunk to this channel for merger to pick it up
    chunk_merger_receiver: mpsc::Receiver<Chunk>,

    // When active merge is complete, the split is sent to this channel
    // to be published in series_cache and splits
    split_publisher_receiver: spsc::Receiver<Split>, 

}

impl Database {

    pub fn open(path: PathBuf) -> Self {
        
    }

    // Will return a clone of 
    fn writter(&self) -> SampleWritter {
        SampleWritter {
            sample_batch_sender: self.queue_tx,
        }
    }


}






struct SplitMetadata {
    id: String,
    min_timestamp: i64,
    max_timestamp: i64,

    num_series: usize,
    num_sample: usize,
    size_bytes: usize,
}


/// A cache for the fst map of splits
/// Cache policy is more about keeping recently created split
struct SeriesCache<CP: CachePolicy> {
    // A fast, concurrent cache of split_id -> split_fst
    time_series: Cache<CP, fst::Map>,
}


/// Chunk is a part of  a time series split still receiving sample 
/// when it is completed, it will merged with other concurrent 
/// chunks and called a split
struct Chunk {
    min_timestamp: i64,
    max_timestamp: i64,

    num_series: usize,
    num_sample: usize,
    size_bytes: usize,

    // a fast hashtable series_name_label => series_id
    // will help assign new time_series comming in to give it new id
    time_series: HashMap<String, u64>,
}


// a cheap
struct 
} 
