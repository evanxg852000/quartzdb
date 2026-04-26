pub mod columm_store;
pub mod index_store;
pub mod reader;
pub mod writter;


use crate::common::index::SplitMeta;


#[derive(Debug)]
pub struct Split {
    split_meta: SplitMeta,
    // column_store: SessionContext
    // fst_index: tantivy::Index,


}

impl Split {
    pub fn new(split_meta: SplitMeta) -> Self {
        Self { split_meta }
    }

    pub fn pack() {

    }
}
