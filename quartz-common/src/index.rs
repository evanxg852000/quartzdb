use serde::{Deserialize, Serialize};


#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize, Hash, PartialEq, Eq, Debug)]  
pub enum FieldType {
    String,
    Int,
    Float,
}


#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct FieldConfig {
    pub name: String,
    pub field_type: FieldType,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct IndexConfig {
    pub timestamp: String,
    pub labels: Vec<String>,
    pub tags: Vec<String>,
    pub fields: Vec<FieldConfig>,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct IndexMeta {
    pub name: String,
    pub config: IndexConfig,
    pub settings: IndexSettings,
    pub splits: Vec<SplitMeta>,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct IndexSettings {
    pub ingest: String,
    pub search: String,
    pub retention: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct SplitMeta {
    pub split_id: String,
    pub index_id: String,
    pub start_time: u64,
    pub end_time: u64,
    // pub bloom_filter: Vec<u8>,
}
