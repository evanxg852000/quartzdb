mod segment;
mod system_catalog;
mod block_cache;



use std::{path::{Path, PathBuf}, sync::Arc};

use hashbrown::HashMap;
use lru::LruCache;
use quartz_common::{DataBlock, LogRowIndex, LogStream, LogStreamId, ObjectId, TimeSeries, TimeSeriesId};
use quartz_index::Query;
use segment::Segment;


struct Storage {
    directory: PathBuf,
    segments: Vec<Arc<Segment>>,
    block_cache: LruCache<DataBlock>,
}

impl Storage {
    pub fn create(directory: impl AsRef<Path>) -> Self {
        Self {
            directory: directory.as_ref().to_path_buf(),
            segments: Vec::new(),
            block_cache: LruCache::new(100),
        }
    }

    pub fn open(directory: impl AsRef<Path>) {
        todo!("implement")
    }

    pub fn query(&self, query: Query, min_timestamp: u64, max_timestamp: u64) -> impl Iterator<Item = DataBlock> {
        // segment pruning (timestamp, bloom-filter)
        let segments: Vec<Arc<Segment>> = self
            .segments
            .iter()
            .filter(|segment| segment.get_metadata().min_timestamp <= max_timestamp && segment.get_metadata().max_timestamp >= min_timestamp)
            .cloned()
            .collect();

        match &query {
            Query::InvertedIndex(..) => {
                let mut matching_set = Vec::new();
                for segment in segments {
                    let series_ids = segment.search_inverted_index(query);
                    matching_set.push((segment, series_ids));
                }
                TimeSeriesBlockIterator {
                    matching_set,
                    current_index: 0,
                }
            }
            Query::Fts(..) => {
                let mut matching_set = Vec::new();
                for segment in segments {
                    let log_ids = segment.search_fts(query);
                    matching_set.push((segment, log_ids));
                }
                LogStreamBlockIterator {
                    matching_set,
                    current_segment_index: 0,
                    current_log_stream_index: 0,
                }
            }   
        }
    }

    fn query_inverted_index(&self, query: Query, segments: Vec<&Segment>) -> Vec<(&Segment, Vec<ObjectId>)> {   
    }

}

struct TimeSeriesBlockIterator {
    matching_set: Vec<(Arc<Segment>, Vec<TimeSeriesId>)>,
    current_index: usize,
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
        let log_line_column = current_segment.fetch_block(*current_log_stream_id).unwrap();
        let mut columns = Vec::new();
        columns.push(log_line_column.get_column_data(column_name, "message").unwrap());

        let series_ids = current_segment.search_inverted_index(Query::InvertedIndex(quartz_index::Filter::Equal(format!("__stream_id:{}", current_log_stream_id))));
        for series_id in series_ids {
            // TODO get series info to display columns name
            let series_name = format!("__series_{}", series_id);
            let data_column = current_segment.fetch_block(series_id).unwrap();
            let column = data_column.get_column_data(column_name, series_name).unwrap();
            columns.push(column);
        }

        // combine columns into a single DataBlock
        let data_frame = DataFrame::new(columns).unwrap();
        Some( DataBlock::new(data_frame))
    }
}
