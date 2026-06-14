use std::sync::Arc;

use common::catalog::TableMeta;
use common::schema::Schema;
use bytes::Bytes;
use datafusion::execution::TaskContext;
use datafusion::physical_plan::ExecutionPlan;
use datafusion_proto::physical_plan::PhysicalExtensionCodec;
use prost::Message;
use datafusion_proto::protobuf::proto_error;
use datafusion::common::{Result, internal_err};

use crate::search::context::{SearchContext, TableSearchContext};
use crate::search::execution_plan::SplitSearchExec;

#[derive(Clone, PartialEq, prost::Message)]
pub struct SplitSearchExecProto {
    /// Serialized version of common::TableMeta
    #[prost(bytes = "bytes", tag = "1")]
    pub table_meta: ::prost::bytes::Bytes,
    #[prost(string, tag = "2")]
    pub split_id: prost::alloc::string::String,
    #[prost(uint64, repeated, tag = "3")]
    pub projection: prost::alloc::vec::Vec<u64>,
    #[prost(string, optional, tag = "4")]
    pub fts_expr: Option<prost::alloc::string::String>,
    #[prost(uint64, optional, tag = "5")]
    pub limit: Option<u64>,
}


#[derive(Debug)]
pub struct SplitSearchExecCodec {
    context: Arc<SearchContext>,
}

impl SplitSearchExecCodec {
    pub fn new(context: Arc<SearchContext>) -> Self {
        Self { context }
    }
}

impl PhysicalExtensionCodec for SplitSearchExecCodec {
    fn try_decode(
        &self,
        buf: &[u8],
        inputs: &[Arc<dyn ExecutionPlan>],
        _ctx: &TaskContext,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        println!("EVAN!! {:?}", self.context);
        if !inputs.is_empty() {
            return internal_err!("SplitSearchExec should have no children, got {}", inputs.len());
        }

        let proto = SplitSearchExecProto::decode(buf)
            .map_err(|err| proto_error(format!("Failed to decode SplitSearchExecProto: {err}")))?;
        let table_meta = bitcode::deserialize::<TableMeta>(&proto.table_meta)
            .map_err(|e| proto_error(format!("Failed to decode TableMeta: {e}")))?;
        
        let schema = Schema::get_primary_schema(&table_meta.config);
        let context = TableSearchContext::try_new(Arc::new(table_meta), self.context.clone())
            .map_err(|e| proto_error(format!("Failed to create TableSearchContext: {e}")))?;
        let projection = match proto.projection.is_empty() {
            true => None,
            false => Some(proto.projection.into_iter().map(|v| v as usize).collect::<Vec<_>>())
        };
        
        Ok(Arc::new(SplitSearchExec::new(
            Arc::new(context),
            schema, 
            proto.split_id,
            projection,
            proto.fts_expr,
            proto.limit.map(|v| v as usize),
        )))
    }

    fn try_encode(&self, node: Arc<dyn ExecutionPlan>, buf: &mut Vec<u8>) -> Result<()> {
        let Some(exec) = node.as_any().downcast_ref::<SplitSearchExec>() else {
            return internal_err!("codec: Expected plan to be SplitSearchExec, but was {}", node.name());
        };

        let data = bitcode::serialize(exec.get_context().get_table_meta())
            .map_err(|err| proto_error(format!("Failed to encode TableMeta: {err}")))?;
        let projection = match exec.get_projection().clone() {
            None => vec![],
            Some(indices) => indices.into_iter().map(|v| v as u64).collect(),
        };
        let proto = SplitSearchExecProto {
            table_meta: Bytes::from(data),
            split_id: exec.get_split_id().to_string(),
            projection,
            fts_expr: exec.get_fts_expr().clone(),
            limit: exec.get_limit().clone().map(|v| v as u64),
        };

        proto
            .encode(buf)
            .map_err(|err| proto_error(format!("Failed to encode SplitSearchExec: {err}")))
    }
}
