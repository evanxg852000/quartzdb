

// InfluxDB line protocol:
// https://docs.influxdata.com/influxdb/v1.8/write_protocols/line_protocol_tutorial/
// weather,location=us-midwest,season=summer temperature=82 1465839830100400200

// Prometheus:
// https://github.com/prometheus/docs/blob/main/content/docs/instrumenting/exposition_formats.md#text-based-format

use serde::{Serialize, Deserialize};

use crate::series_info::Label;


//TODO:  Schema specification
// Timestamp precision: sec, millis, micros, nanos
// Data Type: int, float
// Example: weather_humidity{ @type=int, @precision=sec, city=conakry} 25 1395066363000 
// if no schema, default to inferred data_type and timestamp_precision of milliseconds 
// schema is taken into account only for the first item in a chunk, subsequent are ignored
// Important: change of schema (type or precision) means change of SeriesID
// Special Label
// __name__
// __type__ -> @type
// __precision__ -> @precision

pub enum Value {
    Int(i64),
    Float(f64),
}

#[derive(Debug, Serialize, Deserialize)]
pub struct InsertionItem{
    name: String,
    tags: Vec<Label>,
    value: Value,
    timestamp: i64,
}





#[derive(Debug, Serialize, Deserialize)]
struct InfluxLine {
    name: String,
    tags: Vec<Label>,
    measurements: Vec<(String, Value)>,
    timestamp: i64,
}

impl InfluxLine {
    pub fn into_insertion_items(self) -> impl IntoIterator<Item = InsertionItem> {
        self.measurements
            .into_iter()
            .map(|(measurement, value)| {
                InsertionItem { 
                    // TODO: make this an options:
                    // 1. concat with series name 
                    // 2. add label source_type=influx
                    name: format!("{}_{}", self.name, measurement), 
                    tags: self.tags.clone(), 
                    value, 
                    timestamp: self.timestamp 
                }
            })
    }
}
