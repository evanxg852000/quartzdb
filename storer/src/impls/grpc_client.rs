use std::sync::Arc;

use arrow_flight::{FlightDescriptor, client};
use arrow_flight::encode::FlightDataEncoderBuilder;
// use common::proto::metastore_service_client::MetastoreServiceClient;
use common::proto::{
    DeleteTableRequest, DeleteTableResponse, GetTableRequest, GetTableResponse, ListTablesRequest,
    ListTablesResponse, PutTableRequest, PutTableResponse,
};
use futures::StreamExt;
use tokio::sync::Mutex;
use tonic::transport::Channel;
use tonic::{Request, Response, Status};

use arrow_flight::flight_service_client::FlightServiceClient;

use crate::service::{StorerPutRequest, StorerQueryRequest, StorerService};

pub struct GrpcClientStorerServiceImpl {
    service_client: Arc<Mutex<FlightServiceClient<tonic::transport::Channel>>>,
}

impl GrpcClientStorerServiceImpl {
    pub async fn try_new(url: String) -> anyhow::Result<Self> {
        let channel = Channel::from_shared(url)?
            .connect()
            .await?;
        let client = FlightServiceClient::new(channel);
        Ok(Self {
            service_client: Arc::new(Mutex::new(client)),
        })
    }
}

#[tonic::async_trait]
impl StorerService for GrpcClientStorerServiceImpl {
    async fn put(&self, request: StorerPutRequest) -> anyhow::Result<()> {
        println!("Received Put request: {:?}", request);
        let StorerPutRequest{ info, data} = request;
        let descriptor = FlightDescriptor::new_cmd("info".as_bytes());
        let flight_data_stream = FlightDataEncoderBuilder::new()
            .with_flight_descriptor(Some(descriptor))
            .build(futures::stream::iter(vec![Ok::<_, arrow_flight::error::FlightError>(data)]))
            .map(|result| result.unwrap());

        let response = {
            let mut client = self.service_client.lock().await;
            client.do_put(flight_data_stream).await?
        };
        let mut ack_stream = response.into_inner();
        while let Some(ack) = ack_stream.next().await {
            let put_result = ack?;
            // The server can optionally send custom metadata back in the 'app_metadata' field
            println!("Server acknowledged batch. Metadata len: {:?}", put_result.app_metadata.len());
        }
        Ok(())
    }

    async fn query(&self, query: StorerQueryRequest) -> anyhow::Result<()> {
        println!("Received Query request: {:?}", query);
        //TODO: forward internal call to datafusion-distributed
        Ok(())
    }
}
