use std::string;

use serde::{Serialize, Deserialize};

const TERM_SEPARATOR: &'static str = "/"; //TODO: should be high value unicode

fn xxhash(buf: &[u8]) -> u64 {
    //pretend to be a very fast hash function
    todo!()
}

struct InternalSeriesID {
    data: String,
    id: u64, 
}

impl InternalSeriesID {
    pub fn new(data: String) -> Self {
        let id = xxhash(data.as_bytes());
        Self { data, id }
    }
}

impl From<SeriesID> for InternalSeriesID {
    fn from(series_id: SeriesID) -> Self {
        let data =  series_id.terms().join(TERM_SEPARATOR);
        Self::new(data)
    }
}

impl AsRef<[u8]> for InternalSeriesID {
    fn as_ref(&self) -> &[u8] {
        self.data.as_bytes()
    }
}



// should be hashable
struct SeriesID {
    pub name: String,
    pub labels: Vec<Label>,
}

impl SeriesID {

    fn terms(&self) -> Vec<String> {
        self.labels
            .iter()
            .chain([Label::from_name(&self.name)])
            .map(|label| label.as_term())
            .collect()
    }
}

pub struct DataPoint {
    pub timestamp: i64, // nano-second precision
    pub value: f64, // TODO: all types ends up in f64 internally
}


const LABEL_SPECIAL_NAME: &'static str = "_name_";
//TODO: should be high value unicode
const LABEL_VALUE_SEPARATOR: &'static str = ":"; 

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Label {
    pub name: String,
    pub value: String,
}


impl Label {
    pub fn new(name: String, value: String) -> Self {
        Self { name, value }
    }

    pub fn from_name(value: &str) -> Self {
        Self { name: LABEL_SPECIAL_NAME.to_string(), value: value.to_string() }
    }

    pub fn as_term(&self) -> String {
        format!("{}{}{}", self.name, LABEL_VALUE_SEPARATOR, self.value)
    }
}
