use std::path::PathBuf;

use anyhow::Result;
use tokio::fs;

const STORAGE_DIR: &str = "storage";

#[derive(Debug)]
pub struct StorageImpl {
    directory: PathBuf,
}

impl StorageImpl {
    pub fn new(data_dir: PathBuf) -> Self {
        Self {
            directory: data_dir.join(STORAGE_DIR),
        }
    }

    pub async fn init(&self) -> Result<()> {
        fs::create_dir_all(&self.directory).await?;
        Ok(())
    }
}
