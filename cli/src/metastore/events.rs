use serde::{Deserialize, Serialize};
use tokio::sync::broadcast::Receiver;

use crate::common::index::IndexConfig;

pub type MetastoreEventStream = Receiver<MetastoreEvent>;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum MetastoreEvent {
    IndexPut { name: String, config: IndexConfig },
    IndexDeleted { name: String },
}
