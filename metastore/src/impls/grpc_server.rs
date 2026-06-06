use std::sync::Arc;

use common::proto::{
    DeleteTableRequest, DeleteTableResponse, FetchEventsRequest, FetchEventsResponse,
    GetTableRequest, GetTableResponse, ListTablesRequest, ListTablesResponse, PutSplitRequest,
    PutSplitResponse, PutTableRequest, PutTableResponse,
    grpc_metastore_service_server::GrpcMetastoreService,
};
use tonic::{Request, Response, Status};

use crate::service::MetastoreService;

pub struct GrpcServerMetastoreServiceImpl {
    inner: Arc<dyn MetastoreService>,
}

impl GrpcServerMetastoreServiceImpl {
    pub fn new(inner: Arc<dyn MetastoreService>) -> Self {
        Self { inner }
    }
}

#[tonic::async_trait]
impl GrpcMetastoreService for GrpcServerMetastoreServiceImpl {
    async fn fetch_events(
        &self,
        request: Request<FetchEventsRequest>,
    ) -> Result<Response<FetchEventsResponse>, Status> {
        let last_checkin = request.into_inner().last_checkin;
        let events = self
            .inner
            .fetch_events(last_checkin)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;
        let events = bitcode::serialize(&events).map_err(|e| Status::internal(e.to_string()))?;
        Ok(Response::new(FetchEventsResponse { events }))
    }

    async fn list_tables(
        &self,
        _request: Request<ListTablesRequest>,
    ) -> Result<Response<ListTablesResponse>, Status> {
        let tables = self
            .inner
            .list_tables()
            .await
            .map_err(|e| Status::internal(e.to_string()))?;
        let tables = bitcode::serialize(&tables).map_err(|e| Status::internal(e.to_string()))?;
        Ok(Response::new(ListTablesResponse { tables }))
    }

    async fn put_table(
        &self,
        request: Request<PutTableRequest>,
    ) -> Result<Response<PutTableResponse>, Status> {
        let table = request.into_inner().table;
        let table_meta =
            bitcode::deserialize(&table).map_err(|e| Status::internal(e.to_string()))?;
        self.inner
            .put_table(table_meta)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;
        Ok(Response::new(PutTableResponse {}))
    }

    async fn get_table(
        &self,
        request: Request<GetTableRequest>,
    ) -> Result<Response<GetTableResponse>, Status> {
        let table_name = request.into_inner().name;
        let table_meta = self
            .inner
            .get_table(&table_name)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;
        let table = bitcode::serialize(&table_meta).map_err(|e| Status::internal(e.to_string()))?;
        Ok(Response::new(GetTableResponse { table }))
    }

    async fn delete_table(
        &self,
        request: Request<DeleteTableRequest>,
    ) -> Result<Response<DeleteTableResponse>, Status> {
        let table_name = request.into_inner().name;
        self.inner
            .delete_table(&table_name)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;
        Ok(Response::new(DeleteTableResponse {}))
    }

    async fn put_split(
        &self,
        request: Request<PutSplitRequest>,
    ) -> Result<Response<PutSplitResponse>, Status> {
        let split = request.into_inner().split;
        let split_meta =
            bitcode::deserialize(&split).map_err(|e| Status::internal(e.to_string()))?;
        self.inner
            .put_split(split_meta)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;
        Ok(Response::new(PutSplitResponse {}))
    }
}
