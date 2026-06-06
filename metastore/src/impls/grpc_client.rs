use std::sync::Arc;

use anyhow::Result;
use common::catalog::{SplitMeta, TableMeta};
use common::proto::grpc_metastore_service_client::GrpcMetastoreServiceClient;
use common::proto::{DeleteTableRequest, GetTableRequest, ListTablesRequest, PutTableRequest};
use common::proto::{FetchEventsRequest, PutSplitRequest};
use tokio::sync::Mutex;
use tonic::transport::Channel;

use crate::{events::MetastoreEvent, service::MetastoreService};

pub struct GrpcClientMetastoreServiceImpl {
    service_client: Arc<Mutex<GrpcMetastoreServiceClient<Channel>>>,
}

impl GrpcClientMetastoreServiceImpl {
    pub async fn try_new(uri: String) -> anyhow::Result<Self> {
        //TODO: retry with exponential backoff because
        // we should wait here till metastore node is ready
        // A node will not start without a metastore
        let channel = Channel::from_shared(uri)?.connect().await?;
        let client = GrpcMetastoreServiceClient::new(channel);
        Ok(Self {
            service_client: Arc::new(Mutex::new(client)),
        })
    }
}

#[tonic::async_trait]
impl MetastoreService for GrpcClientMetastoreServiceImpl {
    async fn fetch_events(&self, last_checkin: Option<u64>) -> Result<Vec<MetastoreEvent>> {
        let request = FetchEventsRequest { last_checkin };
        let events = {
            let mut client = self.service_client.lock().await;
            let response = client.fetch_events(request).await?;
            response.into_inner().events
        };
        let events = bitcode::deserialize(&events)?;
        Ok(events)
    }

    async fn list_tables(&self) -> Result<Vec<TableMeta>> {
        let request = ListTablesRequest {};
        let tables = {
            let mut client = self.service_client.lock().await;
            let response = client.list_tables(request).await?;
            response.into_inner().tables
        };
        let tables = bitcode::deserialize(&tables)?;
        Ok(tables)
    }

    async fn put_table(&self, table_meta: TableMeta) -> Result<()> {
        let table = bitcode::serialize(&table_meta)?;
        let request = PutTableRequest { table };
        let mut client = self.service_client.lock().await;
        client.put_table(request).await?;
        Ok(())
    }

    async fn get_table(&self, table_name: &str) -> Result<TableMeta> {
        let request = GetTableRequest {
            name: table_name.to_string(),
        };
        let table = {
            let mut client = self.service_client.lock().await;
            let response = client.get_table(request).await?;
            response.into_inner().table
        };
        let table = bitcode::deserialize(&table)?;
        Ok(table)
    }

    async fn delete_table(&self, table_name: &str) -> Result<()> {
        let request = DeleteTableRequest {
            name: table_name.to_string(),
        };
        let mut client = self.service_client.lock().await;
        client.delete_table(request).await?;
        Ok(())
    }

    async fn put_split(&self, split_meta: SplitMeta) -> Result<()> {
        let split = bitcode::serialize(&split_meta)?;
        let request = PutSplitRequest { split };
        let mut client = self.service_client.lock().await;
        client.put_split(request).await?;
        Ok(())
    }
}
