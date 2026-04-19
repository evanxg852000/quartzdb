use crate::{TimeSeriesId, TagSet};

// https://docs.rs/fast-str/latest/fast_str/ 

#[derive(Debug)]
pub enum TimeSeriesDataType {
    I64,
    F64,
}

impl TimeSeriesDataType {
    pub fn infer_from_str_value(value: &str) -> Result<TimeSeriesDataType, String> {
        //TODO: fast_float::parse
        match value.parse::<f64>() {
            Ok(_) => {
                match value.parse::<i64>() {
                    Ok(_) => Ok(TimeSeriesDataType::I64),
                    Err(_) => Ok(TimeSeriesDataType::F64),
                }
            },
            Err(err) => Err(format!("Failed to parse value as a number: {}", err)),
        }
    }
}

#[derive(Debug, Eq)]
pub struct TimeSeries {
    pub time_series_id: TimeSeriesId,
    pub data_type: TimeSeriesDataType,
    pub measurement: String, // __name__:{measurement}
    pub tags: TagSet,
}

#[derive(Debug)]
pub enum TimeSeriesRows {
    I64(Vec<(u64, i64)>), // (timestamp, value)  sorted by timestamp ASC
    F64(Vec<(u64, f64)>),
}

impl TimeSeriesRows {

    pub fn min_timestamp(&self) -> u64 {
        match self {
            TimeSeriesRows::I64(rows) => rows[0].0,
            TimeSeriesRows::F64(rows) => rows[0].0,
        }
    }

    pub fn max_timestamp(&self) -> u64 {
        match self {
            TimeSeriesRows::I64(rows) => rows[rows.len() - 1].0,
            TimeSeriesRows::F64(rows) => rows[rows.len() - 1].0,
        }
    }

    pub fn extend(&mut self, other: TimeSeriesRows) {
        match (self, other) {
            (TimeSeriesRows::I64(rows), TimeSeriesRows::I64(other_rows)) => {
                rows.extend(other_rows.into_iter());
                rows.sort_by(|a, b| a.0.cmp(&b.0));
            }
            (TimeSeriesRows::F64(rows), TimeSeriesRows::F64(other_rows)) => {
                rows.extend(other_rows.into_iter());
                rows.sort_by(|a, b| a.0.cmp(&b.0));
            }
            (TimeSeriesRows::I64(_), TimeSeriesRows::F64(_)) => panic!("cannot extend i64 rows with f64 rows"),
            (TimeSeriesRows::F64(_), TimeSeriesRows::I64(_)) => panic!("cannot extend f64 rows with i64 rows"),
        }
    }

    pub fn estimate_memory_size(&self) -> u64 {
        match self {
            TimeSeriesRows::I64(rows) => {
                rows.iter()
                    .fold(0usize, |acc, (timestamp, value)| {
                        acc + value.len() + 8 // timestamp size
                    }) as u64
            }
            TimeSeriesRows::F64(rows) => {
                rows.iter()
                    .fold(0usize, |acc, (timestamp, value)| {
                        acc + value.len() + 8 // timestamp size
                    }) as u64
            }
            
        }
    }

}


#[derive(Debug)]
pub struct TimeSeriesRecordBatch {
    time_series: TimeSeries,
    min_timestamp: u64,
    max_timestamp: u64,
    rows: TimeSeriesRows,
}

impl TimeSeriesRecordBatch {
    pub fn new(time_series: TimeSeries) -> Self {
        let rows = match time_series.data_type {
            TimeSeriesDataType::I64 => TimeSeriesRows::I64(Vec::new()),
            TimeSeriesDataType::F64 => TimeSeriesRows::F64(Vec::new()),
        };
        TimeSeriesRecordBatch {
            time_series,
            min_timestamp: 0,
            max_timestamp: 0,
            rows,
        }
    }

    pub fn get_time_series(&self) -> &TimeSeries {
        &self.time_series
    }

    pub fn get_min_timestamp(&self) -> u64 {
        self.min_timestamp
    }

    pub fn get_max_timestamp(&self) -> u64 {
        self.max_timestamp
    }

    pub fn get_rows(&self) -> &TimeSeriesRows {
        &self.rows
    }

    pub fn insert_f64_row(&mut self, timestamp: u64, value: f64) {
        match &mut self.rows {
            TimeSeriesRows::I64(_) => panic!("cannot insert f64 row into i64 rows"),
            TimeSeriesRows::F64(rows) => {
                if rows.is_empty() {
                    self.min_timestamp = timestamp;
                    self.max_timestamp = timestamp;
                }
                rows.push((timestamp, value));
            }
        }
        self.min_timestamp = std::cmp::min(self.min_timestamp, timestamp);
        self.max_timestamp = std::cmp::max(self.max_timestamp, timestamp);
    }

    pub fn insert_f64_rows(&mut self, rows: Vec<(u64, f64)>) {
        match &mut self.rows {
            TimeSeriesRows::I64(_) => panic!("cannot insert f64 rows into i64 rows"),
            TimeSeriesRows::F64(self_rows) => {
                self_rows.extend(rows.into_iter());
                self_rows.sort_by(|a, b| a.0.cmp(&b.0));
            }
        }
        self.min_timestamp = self.rows.min_timestamp();
        self.max_timestamp = self.rows.max_timestamp();
    }

    pub fn insert_i64_row(&mut self, timestamp: u64, value: i64) {
        match &mut self.rows {
            TimeSeriesRows::F64(_) => panic!("cannot insert i64 row into f64 rows"),
            TimeSeriesRows::I64(rows) => {
                if rows.is_empty() {
                    self.min_timestamp = timestamp;
                    self.max_timestamp = timestamp;
                }
                rows.push((timestamp, value));
            }
        }
        self.min_timestamp = std::cmp::min(self.min_timestamp, timestamp);
        self.max_timestamp = std::cmp::max(self.max_timestamp, timestamp);
    }

    pub fn insert_i64_rows(&mut self, rows: Vec<(u64, i64)>) {
        match &mut self.rows {
            TimeSeriesRows::F64(_) => panic!("cannot insert i64 rows into f64 rows"),
            TimeSeriesRows::I64(self_rows) => {
                self_rows.extend(rows.into_iter());
                self_rows.sort_by(|a, b| a.0.cmp(&b.0));
            }
        }
        self.min_timestamp = self.rows.min_timestamp();
        self.max_timestamp = self.rows.max_timestamp();
    }

    pub fn extend(&mut self, other: TimeSeriesRecordBatch) {
        self.rows.extend(other.rows);
        self.min_timestamp = self.rows.min_timestamp();
        self.max_timestamp = self.rows.max_timestamp();
    }

    pub fn len(&self) -> usize {
        match &self.rows {
            TimeSeriesRows::I64(rows) => rows.len(),
            TimeSeriesRows::F64(rows) => rows.len(),
        }
    }
}

impl TimeSeriesRecordBatch {
    pub fn encode(&self) -> Result<Vec<u8>, String> {
        Ok(vec![])
    }

    pub fn decode(buffer: &[u8]) -> Self {
        todo!("implement decoding");
    }
}
