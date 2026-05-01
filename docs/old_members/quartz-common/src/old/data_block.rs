use polars::{frame::DataFrame, prelude::ParquetCompression};
use polars::io::parquet::read::ParquetReader;
use polars::io::SerReader;
use polars::prelude::{Column, ParquetWriter};

use crate::{LogRecordBatch, TimeSeriesRecordBatch, TimeSeriesRows};


#[derive(Debug)]
pub struct DataBlock {
    data_frame: DataFrame,
}

impl DataBlock {
    pub fn new(data_frame: DataFrame) -> Self {
        DataBlock { data_frame }
    }
 
    pub fn get_column_data_as(&self, column_name: &str, new_name: &str) -> Option<Column> {
        //TODO rework 
        self.data_frame.column(column_name).ok().map(|col| {
            Column::new(new_name.to_string(), col.to_vec())
        })
    }

    pub fn serialize_to_parquet_buffer(&mut self) -> Vec<u8> {
        let mut buffer = Vec::new();
        let mut writer = ParquetWriter::new(buffer);
        writer.with_compression(ParquetCompression::Snappy);
        writer.finish(&mut self.data_frame).unwrap();
        buffer
    }

    pub fn deserialize_from_parquet_buffer(data: &[u8]) -> Result<Self, polars::prelude::PolarsError> {
        let reader = ParquetReader::new(data);
        let data_frame = reader.finish()?;
        Ok(Self { data_frame })
    }

}


impl TryFrom<TimeSeriesRecordBatch> for DataBlock {
    type Error = PolarsError;

    fn try_from(batch: TimeSeriesRecordBatch) -> Result<Self, Self::Error> {
        let rows =  batch.get_rows();
        let (timestamp_col, value_col) = match rows {
            TimeSeriesRows::I64(rows) => {
                let timestamps = rows.iter().map(|(ts, _)| ts).collect::<Vec<_>>();
                let values = rows.iter().map(|(_, v)| v).collect::<Vec<_>>();

                let timestamp_col = Column::new("timestamp".into(), timestamps);
                let value_col = Column::new("value".into(), values);
                (timestamp_col, value_col)
            }
            TimeSeriesRows::F64(rows) => {
                let timestamps = rows.iter().map(|(ts, _)| ts).collect::<Vec<_>>();
                let values = rows.iter().map(|(_, v)| v).collect::<Vec<_>>();

                let timestamp_col = Column::new("timestamp".into(), timestamps);
                let value_col = Column::new("value".into(), values);

                (timestamp_col, value_col)
            }
        };

        let data_frame = DataFrame::new(vec![timestamp_col, value_col])?;
        Ok(DataBlock { data_frame })
    }
}

impl TryFrom<LogRecordBatch> for DataBlock {
    type Error = PolarsError;

    fn try_from(batch: LogRecordBatch) -> Result<Self, Self::Error> {
        let timestamps = batch.get_rows().iter().map(|(ts, _)| ts).collect::<Vec<_>>();
        let values = batch.get_rows().iter().map(|(_, v)| v).collect::<Vec<_>>();

        let timestamp_col = Column::new("timestamp".into(), timestamps);
        let value_col = Column::new("value".into(), values);

        let data_frame = DataFrame::new(vec![timestamp_col, value_col])?;
        Ok(DataBlock { data_frame })
    }
    
}
