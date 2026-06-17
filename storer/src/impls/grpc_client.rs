use std::sync::Arc;

use arrow_flight::FlightDescriptor;
use arrow_flight::encode::FlightDataEncoderBuilder;
use common::convert::{record_batch_from_bytes, record_batch_to_bytes};
use common::proto::{PutRequest, SearchRequest};
use common::proto::grpc_storer_service_client::GrpcStorerServiceClient;
use datafusion::arrow::array::RecordBatch;
use datafusion_distributed::WorkerServiceClient;
use futures::StreamExt;
use tokio::sync::Mutex;
use tonic::transport::Channel;

use crate::service::StorerService;


pub struct GrpcClientStorerServiceImpl {
    service_client: Arc<Mutex<GrpcStorerServiceClient<Channel>>>,
}

impl GrpcClientStorerServiceImpl {
    pub async fn try_new(url: String) -> anyhow::Result<Self> {
        let channel = Channel::from_shared(url)?.connect().await?;
        let service_client = GrpcStorerServiceClient::new(channel);
        Ok(Self {
            service_client: Arc::new(Mutex::new(service_client)),
        })
    }
}

#[tonic::async_trait]
impl StorerService for GrpcClientStorerServiceImpl {
    async fn put(&self, table_name: &str, record_batch: RecordBatch) -> anyhow::Result<()> {        
        let request = PutRequest{
            table_name: table_name.to_string(),
            record_batch: record_batch_to_bytes(record_batch)?,
        };
        let mut client = self.service_client.lock().await;
        client.put(request).await?;
        Ok(())
    }

    async fn search(&self, table_name: &str, query: &str) -> anyhow::Result<RecordBatch> {
        let request = SearchRequest{
            table_name: table_name.to_string(),
            query: query.to_string(),
        };
        let mut client = self.service_client.lock().await;
        let response = client.search(request).await?;
        let record_batch = record_batch_from_bytes(response.into_inner().record_batch)?;
        Ok(record_batch)
    }
}
