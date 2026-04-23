use crate::common::index::SplitMeta;

struct ColumnStore {
    ctx: SessionContext // datafusion
}

#[derive(Debug)]
pub struct Split {
    split_meta: SplitMeta,
    column_store: SessionContext
    fst_index: tantivy::Index,


}

impl Split {
    pub fn new(split_meta: SplitMeta) -> Self {
        Self { split_meta }
    }

    pub fn pack() {

    }
}
