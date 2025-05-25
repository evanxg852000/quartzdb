use std::path::Path;

use tantivy::{
    Index, IndexSettings, Order, Term,
    collector::{FilterCollector, MultiCollector, TopDocs},
    directory::MmapDirectory,
    doc,
    query::{AllQuery, PhraseQuery, TermQuery},
    schema::{FAST, Field, FieldType, Schema, TEXT},
};

use crate::{ObjectId, error::IndexError};

const FIRST_METADATA_FIELD_NAME: &str = "__quartz_db_log_stream_id";
const SECOND_METADATA_FIELD_NAME: &str = "__quartz_db_log_id";
const CONTENT_FIELD_NAME: &str = "__quartz_db_log_content";

const INDEXER_MEMORY_BUDGET: usize = 500 * 1024 * 1024; // 500MB;

// Full text search index
#[derive(Debug)]
pub struct FtsIndex {
    content_field: Field,
    first_metadata_field: Field,
    second_metadata_field: Field,
    index: Index,
}

impl FtsIndex {
    pub fn create(directory: impl AsRef<Path>) -> Result<Self, IndexError> {
        std::fs::create_dir_all(&directory)?;
        let mut schema_builder = Schema::builder();
        let content_field = schema_builder.add_text_field(CONTENT_FIELD_NAME, TEXT);
        let first_metadata_field = schema_builder.add_u64_field(FIRST_METADATA_FIELD_NAME, FAST);
        let second_metadata_field = schema_builder.add_u64_field(SECOND_METADATA_FIELD_NAME, FAST);
        let schema = schema_builder.build();

        let managed_directory = MmapDirectory::open(directory).unwrap();
        let index =
            Index::create(managed_directory, schema.clone(), IndexSettings::default()).unwrap();

        Ok(Self {
            content_field,
            first_metadata_field,
            second_metadata_field,
            index,
        })
    }

    pub fn open(directory: impl AsRef<Path>) -> Result<Self, IndexError> {
        let managed_directory = MmapDirectory::open(directory).unwrap();
        let index = Index::open(managed_directory)?;
        let schema = index.schema();
        let content_field = schema.get_field(CONTENT_FIELD_NAME).unwrap();
        let first_metadata_field = schema.get_field(FIRST_METADATA_FIELD_NAME).unwrap();
        let second_metadata_field = schema.get_field(SECOND_METADATA_FIELD_NAME).unwrap();
        Ok(Self {
            content_field,
            first_metadata_field,
            second_metadata_field,
            index,
        })
    }

    pub fn insert_docs(
        &self,
        documents: Vec<(&str, ObjectId, ObjectId)>,
    ) -> Result<(), IndexError> {
        let mut writer = self.index.writer(INDEXER_MEMORY_BUDGET)?;
        for (content, first_metadata, second_metadata) in documents {
            writer.add_document(doc!(
                self.content_field => content,
                self.first_metadata_field => first_metadata,
                self.second_metadata_field => second_metadata,
            ))?;
        }
        writer.commit()?;
        Ok(())
    }

    pub fn search(
        &self,
        phrase: &str,
        slop: u32,
        limit: Option<usize>,
    ) -> Result<Vec<(ObjectId, ObjectId)>, IndexError> {
        let query = FtsIndex::parse_fts_query(phrase, slop, &self.index)?;
        let reader = self.index.reader()?;
        let searcher = reader.searcher();

        // filter
        let limit = limit.unwrap_or(100);
        let first_metadata_collector = TopDocs::with_limit(limit)
            .order_by_fast_field::<u64>(FIRST_METADATA_FIELD_NAME, Order::Desc);
        let first_metadata_filter_collector: FilterCollector<_, _, u64> = FilterCollector::new(
            FIRST_METADATA_FIELD_NAME.to_string(),
            |value| value > 0,
            first_metadata_collector,
        );

        let second_metadata_collector = TopDocs::with_limit(limit)
            .order_by_fast_field::<u64>(SECOND_METADATA_FIELD_NAME, Order::Desc);
        let second_metadata_filter_collector: FilterCollector<_, _, u64> = FilterCollector::new(
            SECOND_METADATA_FIELD_NAME.to_string(),
            |value| value > 0,
            second_metadata_collector,
        );

        let mut collectors = MultiCollector::new();
        let first_metadata_handle = collectors.add_collector(first_metadata_filter_collector);
        let second_metadata_handle = collectors.add_collector(second_metadata_filter_collector);
        let mut multi_fruit = searcher.search(&query, &collectors)?;
        let first_metadata_items = first_metadata_handle.extract(&mut multi_fruit);
        let second_metadata_items = second_metadata_handle.extract(&mut multi_fruit);
        let metadata_items = first_metadata_items
            .into_iter()
            .zip(second_metadata_items.into_iter())
            .map(|((first, _), (second, _))| (first, second))
            .collect::<Vec<_>>();
        Ok(metadata_items)
    }

    // Parses a full text search query into a tantivy query
    //
    // This is like a minimized version of tantivy's QueryParser that only supports:
    // AllQuery
    // TermQuery (single term only)
    // PhraseQuery
    //
    fn parse_fts_query(
        phrase: &str,
        slop: u32,
        index: &tantivy::Index,
    ) -> Result<Box<dyn tantivy::query::Query>, IndexError> {
        if phrase == "*" {
            return Ok(Box::new(AllQuery));
        }

        let schema = index.schema();
        let tokenizers = index.tokenizers();

        let field = schema.get_field(CONTENT_FIELD_NAME)?;
        let field_entry = schema.get_field_entry(field);
        let field_type = field_entry.field_type();

        let FieldType::Str(indexing_options) = field_type else {
            return Err(IndexError::InvalidQuery(
                "field must be of type text.".to_string(),
            ));
        };
        let option = indexing_options.get_indexing_options().unwrap();
        let mut text_analyzer = tokenizers.get(option.tokenizer()).unwrap();

        let mut terms: Vec<(usize, Term)> = Vec::new();
        let mut token_stream = text_analyzer.token_stream(phrase);
        token_stream.process(&mut |token| {
            let term = Term::from_field_text(field, &token.text);
            terms.push((token.position, term));
        });

        match terms.len() {
            0 => Err(IndexError::InvalidQuery(
                "no terms found in the query.".to_string(),
            )),
            1 => {
                let term = terms.first().unwrap().1.clone();
                let segment_postings_options = field_type.get_index_record_option().unwrap();
                Ok(Box::new(TermQuery::new(term, segment_postings_options)))
            }
            _ => Ok(Box::new(PhraseQuery::new_with_offset_and_slop(terms, slop))),
        }
    }
}

// #[derive(Debug, Serialize, Deserialize, Clone)]
// pub struct FtsSettings{
//     indexer_memory_budget: usize,
//     indexer_threads: u64,
//     first_metadata_field_name: String,
//     second_metadata_field_name: String,
//     content_field_name: String,
// }

// impl Default for FtsSettings{
//     fn default() -> Self {
//         Self{
//             indexer_memory_budget: 1024 * 1024 * 1024,
//             indexer_threads: 1,
//             first_metadata_field_name: "__log_stream_id".to_string(),
//             second_metadata_field_name: "__log_id".to_string(),
//             content_field_name: "__log_content".to_string(),
//         }
//     }
// }

// impl FtsSettings{
//     fn save_to(&self, path: impl AsRef<Path>) -> Result<(), IndexError> {
//         //TODO: use bincode to serialize
//         let data = serde_json::to_string(self).unwrap();
//         fs::write(path, data)?;
//         Ok(())
//     }

//     pub fn load_from(path: impl AsRef<Path>) -> Result<Self, IndexError> {
//         let data = fs::read_to_string(path)?;
//         let settings: FtsSettings = serde_json::from_str(&data).unwrap();
//         Ok(settings)
//     }
// }
