mod segment;
mod catalog;
mod block_cache;
mod segment_list;


use std::{path::{Path, PathBuf}, sync::Arc};

use block_cache::BlockCache;
use catalog::Catalog;
use hashbrown::HashMap;
use lru::LruCache;
use quartz_common::{DataBlock, LogRowIndex, LogStream, LogStreamId, ObjectId, TimeSeries, TimeSeriesId};
use quartz_index::Query;
use segment::Segment;
use segment_list::SegmentList;


struct StorageEngine {
    directory: PathBuf,
    catalog: Arc<Catalog>,
    segments: SegmentList,
    block_cache: Arc<BlockCache>,
}

impl StorageEngine {
    pub fn open(directory: impl AsRef<Path>) {
        std::fs::create_dir_all(directory.as_ref()).unwrap();
        // fetch all segments from the directory
        let segments_list = SegmentList::new();
        let entries = std::fs::read_dir(".")?;
        for entry in entries {
            let entry = entry.unwrap();
            let path = entry.path();
            if path.is_dir() {
                // check if the directory is a segment
                let meta_file_path = path.join("meta.json");
                if !meta_file_path.exists() {
                    continue;
                }
                // open the segment
                let segment = Segment::open(path);
                segments_list.add_segment(segment);
            } 
        }

        let catalog= Catalog::open(directory);
        Self {
            directory: directory.as_ref().to_path_buf(),
            catalog: Arc::new(catalog),
            segments: segments_list,
            block_cache: Arc::new(BlockCache::new(500)),
        }
    }

    pub fn add_segment(&self, segment: Segment) {
        self.segments.add_segment(segment);
    }

    pub fn query(&self, query: Query, min_timestamp: u64, max_timestamp: u64) -> impl Iterator<Item = DataBlock> {
        // segment pruning (timestamp, bloom-filter)
        let relevant_segments = self.segments.get_relevant_segments_for_query(min_timestamp, max_timestamp);
        
        match &query {
            Query::InvertedIndex(..) => {
                let mut matching_set = Vec::new();
                for segment in relevant_segments {
                    let series_ids = segment.search_inverted_index(query);
                    matching_set.push((segment, series_ids));
                }
                TimeSeriesBlockIterator {
                    matching_set,
                    current_index: 0,
                    catalog: self.catalog.clone(),
                    block_cache: self.block_cache.clone(),
                }
            }
            Query::Fts(..) => {
                let mut matching_set = Vec::new();
                for segment in relevant_segments {
                    let log_ids = segment.search_fts(query);
                    matching_set.push((segment, log_ids));
                }
                LogStreamBlockIterator {
                    matching_set,
                    current_segment_index: 0,
                    current_log_stream_index: 0,
                    catalog: self.catalog.clone(),
                    block_cache: self.block_cache.clone(),
                }
            }   
        }
    }


}

struct TimeSeriesBlockIterator {
    matching_set: Vec<(Arc<Segment>, Vec<TimeSeriesId>)>,
    current_index: usize,
    catalog: Arc<Catalog>,
    block_cache: BlockCache,
}

impl Iterator for TimeSeriesBlockIterator {
    type Item = DataBlock;

    fn next(&mut self) -> Option<Self::Item> {
        if self.current_index >= self.matching_set.len() {
            return None;
        }
        let (current_segment, time_series_ids) = &self.matching_set.get(self.current_index)?;
        self.current_index += 1;

        // get all the series data as bloc

        todo!("implement")
    }
}

struct LogStreamBlockIterator{
    matching_set: Vec<(Arc<Segment>, Vec<(LogStreamId, Vec<LogRowIndex>)>)>,
    current_segment_index: usize,
    current_log_stream_index: usize,
    catalog: Arc<Catalog>,
    block_cache: BlockCache,
}

impl Iterator for LogStreamBlockIterator {
    type Item = DataBlock;

    fn next(&mut self) -> Option<Self::Item> {
        if self.current_segment_index >= self.matching_set.len() {
            return None;
        }

        let (current_segment, log_record_set) = &self.matching_set.get(self.current_segment_index)?;
        if self.current_log_stream_index >= log_record_set.len() {
            self.current_segment_index += 1;
            self.current_log_stream_index = 0;
            return self.next();
        }
        

        let (current_log_stream_id, log_row_indices) = &log_record_set.get(self.current_log_stream_index)?;
        self.current_log_stream_index += 1;


        // fetch other columns (metrics)
        let log_stream = self.catalog.get_log_stream(*current_log_stream_id).unwrap();
        let log_line_column = self.block_cache.fetch_block(current_segment, current_log_stream_id).unwrap();
        let mut columns = Vec::new();
        columns.push(log_line_column.get_column_data_as(column_name, &log_stream.stream_name).unwrap());

        let series_ids = current_segment.search_inverted_index(Query::InvertedIndex(quartz_index::Filter::Equal(format!("__stream_id:{}", current_log_stream_id))));
        for series_id in series_ids {
            let time_series = self.catalog.get_time_series(series_id).unwrap();
            let data_column = current_segment.fetch_block(series_id).unwrap();
            let column = data_column.get_column_data_as(column_name, &time_series.measurement).unwrap();
            columns.push(column);
        }

        // combine columns into a single DataBlock
        let data_frame = DataFrame::new(columns).unwrap();
        Some( DataBlock::new(data_frame))
    }
}
