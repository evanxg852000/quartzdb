use std::path::{Path, PathBuf};

use anyhow::Result;
use tokio::fs;

const STORAGE_DIR: &str = "storage";

#[derive(Debug)]
pub struct StorageImpl {
    pub directory: PathBuf,
}

impl StorageImpl {
    pub fn new(data_dir: PathBuf) -> Self {
        Self {
            directory: data_dir.join(STORAGE_DIR),
        }
    }

    pub async fn init(&self) -> Result<()> {
        fs::create_dir_all(&self.directory).await?;
        //TODO: check underlying s3 connect if any
        // try to list
        Ok(())
    }

    pub async fn create_dir_all(&self, path: impl AsRef<Path>) -> Result<()> {
        fs::create_dir_all(self.directory.join(path)).await?;
        Ok(())
    }

    pub async fn remove_dir_all(&self, path: impl AsRef<Path>) -> Result<()> {
        fs::remove_dir_all(self.directory.join(path)).await?;
        Ok(())
    }
}
