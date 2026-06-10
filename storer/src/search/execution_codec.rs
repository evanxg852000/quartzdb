use std::sync::Arc;

use datafusion::{arrow::datatypes::Schema, execution::TaskContext};
use datafusion::physical_plan::ExecutionPlan;
use datafusion_proto::physical_plan::PhysicalExtensionCodec;
use prost::Message;
use datafusion_proto::protobuf;
use datafusion_proto::protobuf::proto_error;
use datafusion::common::{Result, internal_err};
use storage::Storage;

use crate::search::execution_plan::SplitSearchExec;

#[derive(Clone, PartialEq, prost::Message)]
pub struct SplitSearchExecProto {
    #[prost(string, tag = "1")]
    pub table_name: prost::alloc::string::String,
    #[prost(string, tag = "2")]
    pub split_id: prost::alloc::string::String,
    #[prost(message, optional, tag = "3")]
    pub schema: Option<protobuf::Schema>,
    #[prost(uint64, repeated, tag = "4")]
    pub projection: prost::alloc::vec::Vec<u64>,
    #[prost(string, optional, tag = "5")]
    pub fts_expr: Option<prost::alloc::string::String>,
}


#[derive(Debug)]
pub struct SplitSearchExecCodec {
    storage: Arc<dyn Storage>
}

impl SplitSearchExecCodec {
    pub fn new(storage: Arc<dyn Storage>) -> Self {
        Self { storage }
    }
}

impl PhysicalExtensionCodec for SplitSearchExecCodec {
    fn try_decode(
        &self,
        buf: &[u8],
        inputs: &[Arc<dyn ExecutionPlan>],
        _ctx: &TaskContext,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        println!("EVAN!! {:?}", self.storage);

        if !inputs.is_empty() {
            return internal_err!("NumbersExec should have no children, got {}", inputs.len());
        }

        let proto = SplitSearchExecProto::decode(buf)
            .map_err(|e| proto_error(format!("Failed to decode SplitSearchExecProto: {e}")))?;

        let schema: Schema = proto
            .schema
            .as_ref()
            .map(|s| s.try_into())
            .ok_or(proto_error("NetworkShuffleExec is missing schema"))??;

        Ok(Arc::new(SplitSearchExec::new(
            proto.table_name,
            self.storage.clone(),
            Arc::new(schema), 
            proto.split_id,
            proto.projection,
            proto.fts_expr,
        )))
    }

    fn try_encode(&self, node: Arc<dyn ExecutionPlan>, buf: &mut Vec<u8>) -> Result<()> {
        let Some(exec) = node.as_any().downcast_ref::<SplitSearchExec>() else {
            return internal_err!("codec: Expected plan to be SplitSearchExec, but was {}", node.name());
        };

        let proto = SplitSearchExecProto {
            table_name: exec.get_table_name().to_string(),
            schema: Some(node.schema().try_into()?),
            split_id: exec.get_split_id().to_string(),
            projection: exec.get_projection().clone(),
            fts_expr: exec.get_fts_expr().clone(),
        };

        proto
            .encode(buf)
            .map_err(|err| proto_error(format!("Failed to encode SplitSearchExec: {err}")))
    }
}
