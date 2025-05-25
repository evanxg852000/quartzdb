mod automaton;
mod core;
mod error;
mod fts_index;
mod inverted_index;
mod postings;
mod query;

pub use core::*;
pub use error::{IndexError, IndexResult};
pub use query::{Filter, Query};

use fts_index::FtsIndex;
use hashbrown::HashMap;
use inverted_index::InvertedIndex;
use roaring::RoaringTreemap;

use std::{
    collections::BTreeMap,
    path::{Path, PathBuf},
};

// An identifier referencing an indexed object.
// Can be one of: TimeSeriesId, LogStreamId, LogId.
pub type ObjectId = u64;

// The series_id is a unique identifier for a time series.
pub type TimeSeriesId = u64;

// The stream_id is a unique identifier for a log stream.
pub type LogStreamId = u64;

// The log_id is a unique identifier for a log entry.
pub type LogId = u64;

// A document is a unit of indexing and search.
// It is a collection of fields, each with a name and a value.
#[derive(Debug)]
pub struct Index {
    inverted_index: InvertedIndex,
    fst_index: FtsIndex,
}

impl Index {
    pub fn create(
        directory: impl AsRef<Path>,
        term_dict: HashMap<String, Vec<ObjectId>>,
        documents: Vec<(&str, ObjectId, ObjectId)>,
    ) -> IndexResult<Self> {
        // ensure directory exists
        std::fs::create_dir_all(&directory)?;

        // create inverted index
        let inverted_index = InvertedIndex::create(&directory, term_dict)?;

        // create fts index
        let fts_index_path = IndexComponent::path(&directory, IndexComponent::FtsIndex);
        let fst_index = FtsIndex::create(fts_index_path)?;
        fst_index.insert_docs(documents)?;

        Ok(Self {
            inverted_index,
            fst_index,
        })
    }

    pub fn open(directory: impl AsRef<Path>) -> IndexResult<Self> {
        // open inverted index
        let inverted_index = InvertedIndex::open(&directory)?;

        // open fts index
        let fts_index_dir = IndexComponent::path(&directory, IndexComponent::FtsIndex);
        let fst_index = FtsIndex::open(&fts_index_dir)?;

        Ok(Self {
            inverted_index,
            fst_index,
        })
    }

    pub fn terms(&self, from: &str, to: &str) -> IndexResult<Vec<String>> {
        self.inverted_index.terms(from, to)
    }

    // InvertedIndex query returns object_ids that point to block of data
    // use the ObjectId to fetch the data from the global doc store.
    pub fn search_inverted_index(&self, query: Query) -> IndexResult<Vec<ObjectId>> {
        let Query::InvertedIndex(filter) = query else {
            return Err(IndexError::InvalidQuery(
                "only Query::InvertedIndex query supported".to_string(),
            ));
        };
        let object_ids = self.inverted_index.search(filter.matcher()?)?;
        Ok(object_ids.into_iter().collect())
    }

    // Log query returns a list of ObjectId (stream_id) with corresponding ObjectId (log_ids)
    // that point to individual log entry.
    // - use the first ObjectId (stream_id) to fetch the Log Info from the global doc store.
    // - use the corresponding list of ObjectId (log_ids) to fetch the log entries from the  LogStore.
    pub fn search_fts(&self, query: Query) -> IndexResult<Vec<(LogStreamId, Vec<LogId>)>> {
        let Query::Fts(phrase, slop, filter, limit) = query else {
            return Err(IndexError::InvalidQuery(
                "only Query::Fts query supported".to_string(),
            ));
        };

        // inverted index object_ids
        let left_ids_set = self.inverted_index.search(filter.matcher()?)?;

        // full text search metadata_items
        let metadata_items = self.fst_index.search(&phrase, slop, limit)?;
        let right_ids_set = metadata_items
            .iter()
            .map(|(first, _)| first)
            .collect::<RoaringTreemap>();
        // intersect object_ids
        let object_ids = left_ids_set & right_ids_set;

        let mut result = BTreeMap::new();
        for (first, second) in metadata_items {
            if object_ids.contains(first) {
                result
                    .entry(first)
                    .or_insert_with(RoaringTreemap::new)
                    .insert(second); //.push(second);
            }
        }

        let items = result
            .into_iter()
            .map(|(key, ids)| (key, ids.into_iter().collect::<Vec<_>>()))
            .collect();
        Ok(items)
    }
}

// Buffer of documents to be written to the index.
#[derive(Debug)]
pub struct IndexBuilder {
    directory: PathBuf,
    term_dict: HashMap<String, Vec<ObjectId>>,
    documents: Vec<(String, ObjectId, ObjectId)>,
}

impl IndexBuilder {
    pub fn new(directory: impl AsRef<Path>) -> Self {
        Self {
            directory: directory.as_ref().to_path_buf(),
            term_dict: HashMap::new(),
            documents: Vec::new(),
        }
    }

    pub fn memory_usage(&self) -> usize {
        let mut memory_usage = 0usize;
        for (term, doc_ids) in &self.term_dict {
            memory_usage += term.as_bytes().len() + doc_ids.len() * std::mem::size_of::<ObjectId>();
        }

        for (content, _, _) in &self.documents {
            memory_usage += content.as_bytes().len() + std::mem::size_of::<ObjectId>() * 2;
        }

        memory_usage
    }

    pub fn add_term(&mut self, term: String, object_id: ObjectId) {
        self.term_dict
            .entry(term)
            .or_insert_with(Vec::new)
            .push(object_id);
    }

    pub fn add_document(
        &mut self,
        content: String,
        first_metadata: ObjectId,
        second_metadata: ObjectId,
    ) {
        self.documents
            .push((content, first_metadata, second_metadata));
    }

    pub fn build(self) -> IndexResult<Index> {
        let documents = self.documents
            .iter()
            .map(|(content, first, second)| (content.as_str(), *first, *second))
            .collect();
        // let term_dict = self.term_dict
        //     .iter()
        //     .map(|(term, object_ids)| (term.as_str(), object_ids.clone()))
        //     .collect();
        Index::create(self.directory, self.term_dict, documents)
    }
}

#[cfg(test)]
mod tests {
    use tempdir::TempDir;

    use crate::{Filter, IndexBuilder, error::IndexResult, query::Query};

    fn sample_documents() -> Vec<(&'static str, u64, u64)> {
        vec![
            ("Take a breath, let it go, walk away", 1, 1),
            ("There's a breach in the warp core, captain", 1, 2),
            ("Maybe we just shouldn't use computers", 1, 3),
            ("I'm sorry, Dave. I'm afraid I can't do that", 1, 4),
            ("We're gonna need a bigger boat", 2, 5),
            (
                "A bug was encountered but not in Vector, which doesn't have bugs",
                2,
                6,
            ),
            ("Pretty pretty pretty good", 2, 7),
            ("You're not gonna believe what just happened", 3, 8),
            (
                "Great Scott! We're never gonna reach 88 mph with the flux capacitor in its current state!",
                3,
                9,
            ),
            ("#hugops to everyone who has to deal with this", 3, 10),
        ]
    }

    fn sample_terms() -> Vec<(&'static str, u64)> {
        vec![
            ("location:us-south", 1),
            ("request_method:delete", 1),
            ("status:500", 1),
            ("status:404", 1),
            ("device:sensor-7", 1),
            ("location:us-west", 2),
            ("status:200", 2),
            ("request_method:get", 2),
            ("device:sensor-4", 2),
            ("status:200", 2),
            ("request_method:get", 3),
            ("request_method:put", 3),
            ("request_method:delete", 3),
            ("request_method:post", 3),
            ("device:sensor-12", 3),
            ("location:us-west", 3),
        ]
    }

    #[test]
    fn usage() -> IndexResult<()> {
        let tmp_dir = TempDir::new("./data").unwrap();

        let mut index_builder = IndexBuilder::new(tmp_dir.path());

        let documents = sample_documents();
        for (content, first_metadata, second_metadata) in documents {
            index_builder.add_document(content.to_string(), first_metadata, second_metadata);
        }
        let terms = sample_terms();
        for (term, object_id) in terms {
            index_builder.add_term(term.to_string(), object_id);
        }
        let index = index_builder.build()?;

        {
            let query = Query::inverted_index(Filter::Equal("request_method:get".to_string()));
            let object_ids = index.search_inverted_index(query)?;
            assert_eq!(object_ids, vec![2, 3]);
        }

        {
            let query = Query::inverted_index(Filter::StartsWith("device".to_string()));
            let object_ids = index.search_inverted_index(query)?;
            assert_eq!(object_ids, vec![1, 2, 3]);
        }

        {
            let query = Query::fts("bug encountered", 1, Filter::All, None);
            let log_entries = index.search_fts(query)?;
            assert_eq!(log_entries, vec![(2, vec![6])]);
        }

        {
            let query = Query::fts("*", 1, Filter::All, None);
            let log_entries = index.search_fts(query)?;
            assert_eq!(
                log_entries,
                vec![
                    (1, vec![1, 2, 3, 4]),
                    (2, vec![5, 6, 7]),
                    (3, vec![8, 9, 10]),
                ]
            );
        }

        Ok(())
    }
}
