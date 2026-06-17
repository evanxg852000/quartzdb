use std::sync::Arc;

use common::convert::{record_batch_from_bytes, record_batch_to_bytes};
use common::proto::{PutRequest, PutResponse, SearchRequest, SearchResponse};
use common::proto::grpc_storer_service_server::GrpcStorerService;
use tonic::{Request, Response, Status};

use crate::service::StorerService;

#[derive(Clone)]
pub struct GrpcServerStorerServiceImpl {
    inner: Arc<dyn StorerService>,
}

impl GrpcServerStorerServiceImpl {
    pub fn new(inner: Arc<dyn StorerService>) -> Self {
        Self { inner }
    }
}

#[tonic::async_trait]
impl GrpcStorerService for GrpcServerStorerServiceImpl {

    async fn put(
        &self,
        request: Request<PutRequest>,
    ) -> Result<Response<PutResponse>, Status> {
        let PutRequest { table_name, record_batch } = request.into_inner();
        let record_batch = record_batch_from_bytes(record_batch)
            .map_err(|e| Status::internal(e.to_string()))?;
        self
            .inner
            .put(&table_name, record_batch)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;
        Ok(Response::new(PutResponse::default()))
    }

    async fn search(
        &self,
        request: Request<SearchRequest>,
    ) -> Result<Response<SearchResponse>, Status> {
        let SearchRequest { table_name, query } = request.into_inner();
        let record_batch = self
            .inner
            .search(&table_name, &query)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;
        let record_batch  = record_batch_to_bytes(record_batch)
            .map_err(|e| Status::internal(e.to_string()))?;
        Ok(Response::new(SearchResponse{ record_batch }))
    }

}



// #[tonic::async_trait]
// impl FlightService for GrpcServerStorerServiceImpl {
//     type HandshakeStream = BoxStream<'static, Result<HandshakeResponse, Status>>;
//     type ListFlightsStream = BoxStream<'static, Result<FlightInfo, Status>>;
//     type DoGetStream = BoxStream<'static, Result<FlightData, Status>>;
//     type DoPutStream = BoxStream<'static, Result<PutResult, Status>>;
//     type DoActionStream = BoxStream<'static, Result<arrow_flight::Result, Status>>;
//     type ListActionsStream = BoxStream<'static, Result<ActionType, Status>>;
//     type DoExchangeStream = BoxStream<'static, Result<FlightData, Status>>;

//     async fn do_put(
//         &self,
//         request: Request<Streaming<FlightData>>,
//     ) -> Result<Response<Self::DoPutStream>, Status> {
//         // 1. Grab the raw gRPC stream
//         let mut inner_stream = request.into_inner();

//         // 1. Extract the first message which contains the descriptor and schema
//         let first_msg = match inner_stream.next().await {
//             Some(Ok(data)) => data,
//             Some(Err(e)) => return Err(Status::internal(e.to_string())),
//             None => return Err(Status::invalid_argument("Empty stream received")),
//         };

//         // Read the descriptor (e.g., path or command string identifying the upload)
//         if let Some(descriptor) = &first_msg.flight_descriptor {
//             println!("Receiving data for path: {:?}", descriptor);
//         }

//         // 2. Extract the schema from the first message metadata
//         let schema =
//             Schema::try_from(&first_msg).map_err(|e| Status::invalid_argument(e.to_string()))?;
//         let schema_ref = std::sync::Arc::new(schema);

//         // 3. Chain the first message's data payload with the rest of the stream
//         let remaining_stream = futures::stream::once(async { Ok(first_msg) }).chain(
//             inner_stream
//                 .map(|r| r.map_err(|e| arrow_flight::error::FlightError::Tonic(Box::new(e)))),
//         );

//         // 4. Use FlightRecordBatchStream to automatically parse raw bytes into RecordBatches
//         let mut batch_stream = FlightRecordBatchStream::new_from_flight_data(remaining_stream);

//         while let Some(batch_result) = batch_stream.next().await {
//             let record_batch = batch_result.map_err(|e| Status::internal(e.to_string()))?;
//             println!("Received batch with {} rows", record_batch.num_rows());

//             // TODO: Persist or process your record_batch here
//             self.inner
//                 .put(StorerPutRequest {
//                     info: StorerPutRequestInfo {
//                         table_name: "foo".into(),
//                     },
//                     data: record_batch,
//                 })
//                 .await
//                 .unwrap();
//         }

//         // 5. Respond back to the client acknowledging success
//         // let output_stream = futures::stream::empty();
//         let response_payload = PutResult {
//             app_metadata: Bytes::from("Put operation completed successfully"),
//         };
//         let output_stream = futures::stream::iter(vec![Ok(response_payload)]);
//         Ok(Response::new(Box::pin(output_stream)))
//     }

//     // --- Stub out other mandatory trait methods with unimplemented ---
//     async fn handshake(
//         &self,
//         _request: Request<Streaming<HandshakeRequest>>,
//     ) -> Result<Response<Self::HandshakeStream>, Status> {
//         Err(Status::unimplemented("Implement handshake"))
//     }

//     async fn list_flights(
//         &self,
//         _request: Request<Criteria>,
//     ) -> Result<Response<Self::ListFlightsStream>, Status> {
//         Err(Status::unimplemented("Implement list_flights"))
//     }

//     async fn get_flight_info(
//         &self,
//         _request: Request<FlightDescriptor>,
//     ) -> Result<Response<FlightInfo>, Status> {
//         Err(Status::unimplemented("Implement get_flight_info"))
//     }

//     async fn poll_flight_info(
//         &self,
//         _request: Request<FlightDescriptor>,
//     ) -> Result<Response<PollInfo>, Status> {
//         Err(Status::unimplemented("Implement poll_flight_info"))
//     }

//     async fn get_schema(
//         &self,
//         _request: Request<FlightDescriptor>,
//     ) -> Result<Response<SchemaResult>, Status> {
//         Err(Status::unimplemented("Implement get_schema"))
//     }

//     async fn do_get(
//         &self,
//         _request: Request<Ticket>,
//     ) -> Result<Response<Self::DoGetStream>, Status> {
//         Err(Status::unimplemented("Implement do_get"))
//     }

//     async fn do_action(
//         &self,
//         _request: Request<Action>,
//     ) -> Result<Response<Self::DoActionStream>, Status> {
//         Err(Status::unimplemented("Implement do_action"))
//     }

//     async fn list_actions(
//         &self,
//         _request: Request<Empty>,
//     ) -> Result<Response<Self::ListActionsStream>, Status> {
//         Err(Status::unimplemented("Implement list_actions"))
//     }

//     async fn do_exchange(
//         &self,
//         _request: Request<Streaming<FlightData>>,
//     ) -> Result<Response<Self::DoExchangeStream>, Status> {
//         Err(Status::unimplemented("Implement do_exchange"))
//     }
// }
