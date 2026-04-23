use std::path::PathBuf;
use std::sync::Arc;

use anyhow::{Result, anyhow};
use hashbrown::{HashMap, hash_map::Entry};
use tokio::{fs, sync::Mutex};

use crate::common::index::IndexMeta;

const METASTORE_DIR: &str = "metastore";

#[derive(Debug)]
pub struct LocalMetastore {
    directory: PathBuf,
    indexes: Arc<Mutex<HashMap<String, IndexMeta>>>,
}

impl LocalMetastore {
    pub fn new(data_dir: PathBuf) -> Self {
        LocalMetastore {
            directory: data_dir.join(METASTORE_DIR),
            indexes: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub async fn load_indexes(&self) -> Result<()> {
        let mut indexes = self.indexes.lock().await;
        for entry in std::fs::read_dir(&self.directory)? {
            let entry = entry?;
            if entry.file_type()?.is_dir() {
                let metastore_file_path = entry.path().join("meta.json");
                let data = fs::read(&metastore_file_path).await?;
                let index_meta: IndexMeta = serde_json::from_slice(&data)?;
                indexes.insert(index_meta.name.clone(), index_meta);
            }
        }
        Ok(())
    }

    pub async fn put_index(&self, index_meta: IndexMeta) -> Result<()> {
        let index_path = self.directory.join(&index_meta.name);
        fs::create_dir_all(&index_path).await?;
        let meta_json = serde_json::to_string(&index_meta)?;
        fs::write(index_path.join("meta.json"), meta_json).await?;

        let mut indexes = self.indexes.lock().await;
        match indexes.entry(index_meta.name.clone()) {
            Entry::Occupied(mut entry) => {
                entry.insert(index_meta);
            }
            Entry::Vacant(entry) => {
                entry.insert(index_meta);
            }
        };
        Ok(())
    }

    pub async fn delete_index(&self, index_name: &str) -> Result<()> {
        let mut indexes = self.indexes.lock().await;
        if !indexes.contains_key(index_name) {
            return Err(anyhow!("Index '{}' does not exist", index_name));
        }
        indexes.remove(index_name);
        let index_path = self.directory.join(index_name);
        fs::remove_dir_all(&index_path).await?;
        Ok(())
    }

    pub async fn list_indexes(&self) -> Result<Vec<IndexMeta>> {
        let mut indexes = Vec::new();
        let indexes_guard = self.indexes.lock().await;
        for index_meta in indexes_guard.values() {
            indexes.push(index_meta.clone());
        }
        Ok(indexes)
    }
}
