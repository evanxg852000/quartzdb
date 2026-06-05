
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum MetastoreError {
    #[error("Index with id '{0}' already exists.")]
    IndexAlreadyExists(String),

    #[error("Index with id '{0}' not found.")]
    IndexNotFound(String),

    #[error("Failed to read index configuration: {0}")]
    IndexConfigReadError(String),

    #[error("Failed to write index configuration: {0}")]
    IndexConfigWriteError(String),

    #[error("Invalid index configuration: {0}")]
    InvalidIndexConfig(String),
}
