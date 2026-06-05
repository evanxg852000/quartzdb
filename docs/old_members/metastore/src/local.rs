use std::sync::{Arc, Mutex};
use std::path::PathBuf;

use hashbrown::HashMap;
use tokio::fs;

use quartz_common::index::IndexMeta;

pub struct LocalMetastore{
    directory: PathBuf,
    indexes: Arc<Mutex<HashMap<String, IndexMeta>>>,
}

impl LocalMetastore {
    pub fn new(directory: PathBuf) -> Self {
        LocalMetastore { 
            directory, 
            indexes: Arc::new(Mutex::new(HashMap::new())),
        }
    }
    
    pub async fn create_index(&self, index_meta: IndexMeta) -> anyhow::Result<()> {
        let index_path = self.directory.join(&index_meta.name);
        fs::create_dir_all(&index_path)?;
        let meta_json = serde_json::to_string(&index_meta)?;    
        fs::write(index_path.join("meta.json"), meta_json).await?;
        let mut indexes = self.indexes.lock().unwrap();
        indexes.insert(index_meta.name.clone(), index_meta);
        Ok(())
    }

    pub async fn list_indices(&self) -> anyhow::Result<Vec<String>> {
        let mut indices = Vec::new();
        for entry in std::fs::read_dir(&self.directory)? {
            let entry = entry?;
            if entry.file_type()?.is_dir() {
                if let Some(name) = entry.file_name().to_str() {
                    indices.push(name.to_string());
                }
            }
        }
        Ok(indices)
    }

    pub async fn delete_index(&self, index_name: &str) -> anyhow::Result<()> {
        let mut indexes_guard = self.indexes.lock().unwrap();
        if !indexes_guard.contains_key(index_name) {
            anyhow::bail!("Index '{}' does not exist", index_name);
        }
        indexes_guard.remove(index_name);
        let index_path = self.directory.join(index_name);
        fs::remove_dir_all(&index_path).await?;
        Ok(())
    }




}
