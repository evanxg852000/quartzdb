use std::sync::Arc;

use datafusion_distributed::WorkerSessionBuilder;



struct QueryWorkerSessionBuilder {
    // Thread-safe handle containing the gRPC client and cache
    shared_schema_provider: Arc<MetastoreSchemaProvider>,
}

impl WorkerSessionBuilder for QueryWorkerSessionBuilder {
    fn create_session_ctx(&self, _session_id: &str) -> SessionContext {
        let config = SessionConfig::new().with_target_partitions(2);
        let ctx = SessionContext::new_with_config(config);

        // Fetch the default catalog (usually "datafusion")
        let catalog = ctx.catalog("datafusion").unwrap();
        catalog.register_schema("public", self.shared_schema_provider.clone()).unwrap();
        ctx
    }
} 
