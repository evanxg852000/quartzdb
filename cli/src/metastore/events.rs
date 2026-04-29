use serde::{Deserialize, Serialize};
use tokio::sync::broadcast::Receiver;

use crate::common::index::IndexMeta;

pub type MetastoreEventStream = Receiver<MetastoreEvent>;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum MetastoreEvent {
    IndexPut { name: String, index_meta: IndexMeta },
    IndexDeleted { name: String },
}
