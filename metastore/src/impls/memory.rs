use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions};
use tonic::{Request, Response, Status};
use proto::quartzdb::{
    HelloRequest, HelloResponse,
    metastore_service_server::MetastoreService,
};
use std::str::FromStr;

use crate::impls::sqlite::SqliteMetastoreServiceImpl;

pub struct MemoryMetastoreServiceImpl{
    inner_in_memory_sqite: SqliteMetastoreServiceImpl,
}

impl MemoryMetastoreServiceImpl {
    pub async fn try_new() -> anyhow::Result<Self> {
        let opts = SqliteConnectOptions::from_str("sqlite::memory:")?
            .create_if_missing(true);
        let conn = SqlitePoolOptions::new()
            .max_connections(1) // Ensures all queries share the same in-memory DB
            .connect_with(opts)
            .await?;
        Ok(Self { inner_in_memory_sqite: SqliteMetastoreServiceImpl::new(conn).await? })
    }
}

#[tonic::async_trait]
impl MetastoreService for MemoryMetastoreServiceImpl {
    async fn hello(
        &self,
        request: Request<HelloRequest>,
    ) -> Result<Response<HelloResponse>, Status> {
        self.inner_in_memory_sqite.hello(request).await
    }
}
