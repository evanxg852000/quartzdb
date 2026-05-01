use std::fmt::{self, Debug};
use std::io;
use std::path::Path;
use std::sync::Arc;

use anyhow::Result;
use tantivy::Directory;
use tantivy::directory::error::OpenReadError;
use tantivy::directory::{FileHandle, FileSlice, OwnedBytes};

use crate::storer::split::index_store::packed_file::PackedFileReader;

#[derive(Clone)]
pub struct PackedDirectory {
    reader: Arc<PackedFileReader>,
}

impl Debug for PackedDirectory {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "PackedDirectory")
    }
}

impl PackedDirectory {
    pub async fn new(path: impl AsRef<Path>) -> Result<Self> {
        let reader = Arc::new(PackedFileReader::new(path).await?);
        Ok(Self { reader })
    }
}

impl Directory for PackedDirectory {
    fn get_file_handle(&self, path: &Path) -> Result<Arc<dyn FileHandle>, OpenReadError> {
        let file_slice = self.open_read(path)?;
        Ok(Arc::new(file_slice))
    }

    fn open_read(&self, path: &Path) -> Result<FileSlice, OpenReadError> {
        let reader = self.reader.clone();
        let data = tokio::task::block_in_place(move || {
            let handle = tokio::runtime::Handle::current();
            handle.block_on(async { reader.get(path).await })
        })
        .map_err(|_| OpenReadError::FileDoesNotExist(path.to_path_buf()))?;
        Ok(FileSlice::new(Arc::new(OwnedBytes::new(data))))
    }

    fn atomic_read(&self, path: &Path) -> Result<Vec<u8>, OpenReadError> {
        let file_slice = self.open_read(path)?;
        let payload = file_slice
            .read_bytes()
            .map_err(|io_error| OpenReadError::wrap_io_error(io_error, path.to_path_buf()))?;
        Ok(payload.to_vec())
    }

    fn exists(&self, path: &Path) -> Result<bool, OpenReadError> {
        let reader = self.reader.clone();
        let exists = tokio::task::block_in_place(move || {
            let handle = tokio::runtime::Handle::current();
            handle.block_on(async { reader.exists(path).await })
        })
        .map_err(|_| OpenReadError::FileDoesNotExist(path.to_path_buf()))?;
        Ok(exists)
    }

    fn atomic_write(&self, _path: &Path, _data: &[u8]) -> io::Result<()> {
        unimplemented!("read-only")
    }

    fn delete(&self, _path: &Path) -> Result<(), tantivy::directory::error::DeleteError> {
        unimplemented!("read-only")
    }

    fn open_write(
        &self,
        _path: &Path,
    ) -> Result<tantivy::directory::WritePtr, tantivy::directory::error::OpenWriteError> {
        unimplemented!("read-only")
    }

    fn sync_directory(&self) -> io::Result<()> {
        unimplemented!("read-only")
    }

    fn watch(
        &self,
        _watch_callback: tantivy::directory::WatchCallback,
    ) -> tantivy::Result<tantivy::directory::WatchHandle> {
        Ok(tantivy::directory::WatchHandle::empty())
    }

    fn acquire_lock(
        &self,
        _lock: &tantivy::directory::Lock,
    ) -> Result<tantivy::directory::DirectoryLock, tantivy::directory::error::LockError> {
        Ok(tantivy::directory::DirectoryLock::from(Box::new(|| {})))
    }
}
