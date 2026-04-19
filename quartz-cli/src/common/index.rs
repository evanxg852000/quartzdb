

use url::Url;
use serde::{Deserialize, Serialize};


#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize, Hash)]  
#[serde(rename_all = "lowercase")]
pub enum FieldType {
    String,
    Int,
    Float,
}


#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct FieldConfig {
    pub name: String, //JSON Path that escape dot if needed
    #[serde(rename = "type")]
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
pub struct LocalCacheSettings {
    pub max_size_bytes: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(tag = "type", rename_all = "lowercase")]
pub enum StorageSettings {
    // local file system
    Local,
    // S3 (aws, gcp, minio)
    Remote {
        bucket: Url,
        local_cache: Option<LocalCacheSettings>,
    }
}

// #[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
// pub struct  {
//     #[serde(rename = "type")]
//     pub storage_type: StorageType,
// }

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct IngestSettings {
    pub batch_size: u64,
    pub commit_timeout_secs: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct SearchSettings {
    pub todo: String,
}


#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct RetentionSettings {
    pub period: String,
    pub schedule: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct IndexSettings {
    pub storage: StorageSettings,
    pub ingest: IngestSettings,
    pub search: SearchSettings,
    pub retention: Option<RetentionSettings>,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct SplitMeta {
    pub split_id: String,
    pub index_id: String,
    pub start_time: u64,
    pub end_time: u64,
}


#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct IndexMeta {
    pub name: String,
    pub config: IndexConfig,
    pub settings: IndexSettings,
    #[serde(default)]
    pub splits: Vec<SplitMeta>,
}
