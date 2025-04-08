use std::io;

use thiserror::Error;

pub type IndexResult<T> = std::result::Result<T, IndexError>;

#[derive(Error, Debug)]
pub enum IndexError {
    #[error("IO error")]
    Io(#[from] io::Error),
    #[error("Fst error")]
    Fst(#[from] tantivy_fst::Error),
    #[error("bincode encode error")]
    Encode(#[from] bincode::error::EncodeError),
    #[error("bincode decode error")]
    Decode(#[from] bincode::error::DecodeError),
    #[error("Query error")]
    InvalidQuery(String),
    #[error("Query not supported.")]
    QueryNotSupported,
    #[error("IndexWriter error")]
    TantivyError(#[from] tantivy::TantivyError),

    #[error("IndexReader error")]
    IndexReader,
    #[error("Document not found")]
    DocNotFound,
    #[error("Other error")]
    Other(String),
}
