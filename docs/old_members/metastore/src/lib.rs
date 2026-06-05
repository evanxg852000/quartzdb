mod local;
mod error;

use quartz_common::index::Index;

use crate::local::LocalMetastore;




///
/// create index, update index, list indexes, delete index
/// JSON file-based metastore for now, can be replaced with a real database later
/// gRPC: heartbeat, list-splits, cluster-state-events
///
/// No handoff, each node is responsible for its thing.
///


pub struct MetastoreService {
    local: LocalMetastore
}



