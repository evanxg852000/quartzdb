use std::path::Path;

use hashbrown::HashMap;
use roaring::RoaringTreemap;
use serde::{Deserialize, Serialize};

use crate::{IndexResult, ObjectId, postings::Postings, query::Matcher};

#[derive(Debug, Serialize, Deserialize)]
pub struct InvertedIndexSettings {}

#[derive(Debug)]
pub struct InvertedIndex {
    postings: Postings,
}

impl InvertedIndex {
    pub fn create(
        directory: impl AsRef<Path>,
        term_dict: HashMap<String, Vec<ObjectId>>,
    ) -> IndexResult<Self> {
        let postings = Postings::create(directory, term_dict)?;
        Ok(Self { postings })
    }

    pub fn open(directory: impl AsRef<Path>) -> IndexResult<Self> {
        let postings = Postings::open(directory)?;
        Ok(Self { postings })
    }

    pub fn terms(&self, from: &str, to: &str) -> IndexResult<Vec<String>> {
        self.postings.terms(from, to)
    }

    pub fn search(&self, matcher: Matcher) -> IndexResult<RoaringTreemap> {
        self.postings.search(matcher)
    }
}

// Allows us to index time series and log streams.
//
// pub struct InvertedIndexBuilder {
//     /// Index id (ulid).
//     id: String,
//     /// Term dictionary mapping (term -> vec<doc_ids>)
//     terms: HashMap<String, Vec<ObjectId>>,
//     /// Document store
//     documents: HashMap<ObjectId, Bytes>,
//     /// Accumulated document size in bytes
//     num_bytes: usize,
// }

// impl InvertedIndexBuilder {
//     pub fn create() -> InvertedIndexBuilder {
//         InvertedIndexBuilder {
//             id: Ulid::new().to_string(),
//             terms: HashMap::new(),
//             documents: HashMap::new(),
//             num_bytes: 0,
//         }
//     }

//     pub fn build(self) -> InvertedIndex {
//         InvertedIndex {
//             id: self.id,
//             terms: self.terms,
//             documents: self.documents,
//             num_bytes: self.num_bytes,
//         }
//     }

// }
