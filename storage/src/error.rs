use std::io;

pub type Result<T> = core::result::Result<T, StorageError>;

pub enum StorageError {
    IoError(io::Error),
}
