use tonic::{Request, Response, Status};
use proto::quartzdb::{
    HelloRequest, HelloResponse,
    metastore_service_server::MetastoreService,
};
use sqlx::{SqlitePool, sqlite::{SqliteConnectOptions, SqlitePoolOptions}};
use std::str::FromStr;


const MIGRATION_SCRIPT: &str = r#"
CREATE TABLE IF NOT EXISTS indexes (
    name TEXT PRIMARY KEY
);

CREATE TABLE IF NOT EXISTS splits (
    id TEXT PRIMARY KEY,
    index_name TEXT,
    data TEXT,
    FOREIGN KEY (index_name) REFERENCES indexes(name) ON DELETE CASCADE
);
"#;



pub struct SqliteMetastoreServiceImpl {
    conn: sqlx::SqlitePool,
}

impl SqliteMetastoreServiceImpl {
    pub(crate) async fn new(conn: SqlitePool) -> anyhow::Result<Self> {
        //TODO: run migrations
        sqlx::query(MIGRATION_SCRIPT)
        .execute(&conn)
        .await?;
        Ok(Self { conn })
    }

    pub async fn try_new(path: &str) -> anyhow::Result<Self> {
        let opts = SqliteConnectOptions::from_str(&format!("sqlite:{}", path))?
            .create_if_missing(true);
        let conn = SqlitePoolOptions::new()
            .max_connections(10) // Ensures all queries share the same in-memory DB
            .connect_with(opts)
            .await?;
        Self::new(conn).await
    }
}

#[tonic::async_trait]
impl MetastoreService for SqliteMetastoreServiceImpl {
    async fn hello(
        &self,
        request: Request<HelloRequest>,
    ) -> Result<Response<HelloResponse>, Status> {
        //TODO implement actual logic
        let message = format!("Hello, {}!", request.into_inner().name);
        Ok(Response::new(HelloResponse { message }))
    }
}
