mod error;
mod fast_field_collector;
mod filters;
mod query;

use error::IndexError;
use fast_field_collector::FastFieldCollector;
use query::{Query, QueryBuilder};
use std::fs;
use std::path::PathBuf;

use tantivy::query::QueryParser;
use tantivy::schema::{Schema, FAST, INDEXED, STORED, STRING, TEXT};
use tantivy::{DateTime, Index, TantivyDocument}; 

const ID_FIELD: &str = "__qtz_id";
const TIMESTAMP_FIELD: &str = "__qtz_timestamp";
const MESSAGE_FIELD: &str = "__qtz_message";
const TAGS_FIELD: &str = "__qtz_tags";
const TAG_SEPARATOR: &str = ":";

pub struct IndexDocument {
    id: u64,
    timestamp: DateTime,
    messages: Vec<String>,
    tags: Vec<String>,
}

impl IndexDocument {
    pub fn new(id: u64, message: &str, unix_timestamp_seconds: i64) -> Self {
        Self {
            id,
            timestamp: DateTime::from_timestamp_secs(unix_timestamp_seconds),
            messages: Vec::from([message.to_string()]),
            tags: Vec::new(),
        }
    }

    pub fn set_timestamp_(&mut self, unix_timestamp_seconds: i64) {
        self.timestamp = DateTime::from_timestamp_secs(unix_timestamp_seconds)
    }

    pub fn add_tag(&mut self, label: &str, value: &str) {
        self.tags.push(format!("{}{}{}", label, TAG_SEPARATOR, value));
    }

    pub fn add_message(&mut self, message: &str) {
        self.messages.push(message.to_string());
    }

}


pub struct FtsIndex {
    path: PathBuf,
    index: Index,
}

impl FtsIndex {
    pub fn create(path: PathBuf) -> Result<Self, IndexError> {
        fs::create_dir_all(&path).expect("Failed to create index directory");

        let mut schema_builder = Schema::builder();
        schema_builder.add_u64_field(ID_FIELD, FAST | STORED);
        schema_builder.add_date_field(TIMESTAMP_FIELD, INDEXED);
        schema_builder.add_text_field(MESSAGE_FIELD, TEXT);
        schema_builder.add_text_field(TAGS_FIELD, STRING);
        let schema = schema_builder.build();
        let index = Index::create_in_dir(path.clone(), schema)?;
        
        Ok(Self {
            path,
            index,
        })
    }

    pub fn open(path: PathBuf) -> Result<Self, IndexError>  {
        let index = Index::open_in_dir(path.clone())?;
        Ok(Self {
            path,
            index,
        })
    }

    pub fn path(&self) -> &PathBuf {
        &self.path
    }

    pub fn insert_batch(&self, documents: Vec<IndexDocument>) -> Result<(), IndexError> {
        if documents.is_empty() {
            return Ok(());
        }
    
        let mut index_writer = self.index
            .writer(50_000_000)?; // 50MB memory budget

        let schema = self.index.schema();
        let id_field = schema.get_field(ID_FIELD)?;
        let timestamp_field = schema.get_field(TIMESTAMP_FIELD)?;
        let message_field = schema.get_field(MESSAGE_FIELD)?;
        let tags_field = schema.get_field(TAGS_FIELD)?;
        for doc in documents {
            let mut tantivy_doc = TantivyDocument::default();
            tantivy_doc.add_u64(id_field, doc.id);
            tantivy_doc.add_date(timestamp_field, doc.timestamp);
            for message in doc.messages.iter() {
                tantivy_doc.add_text(message_field, &message);
            }
            for tag in &doc.tags {
                tantivy_doc.add_text(tags_field, tag);
            }
            
            index_writer.add_document(tantivy_doc)?;
        }

        index_writer.commit()?;
        Ok(())
    }

    pub fn query_builder(&self) -> QueryBuilder {
        QueryBuilder::new(self.index.schema())
    }

    pub fn search(&self, query: Query) -> Result<Vec<u64>, IndexError> {
        let reader = self.index.reader()?;
        let searcher = reader.searcher();
        // let schema = self.index.schema();
        // let message_field = schema.get_field(MESSAGE_FIELD).unwrap();

        let Query{ tantivy_query, start_timestamp_secs, end_timestamp_secs} = query;

        let ids_collector = FastFieldCollector::<u64>::new(
            ID_FIELD,
            Some(TIMESTAMP_FIELD),
            start_timestamp_secs,
            end_timestamp_secs,
        );

        let doc_ids = searcher.search(&tantivy_query, &ids_collector)?;
        Ok(doc_ids)
    }
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use tempdir::TempDir;

    use crate::{error::FstResult, query::Query, Config, Document, Index};

    #[test]
    fn usage() -> FstResult<()> {
        let tmp_dir = TempDir::new("./data").unwrap();
        let config = Config::new(tmp_dir.path());

        // let tmp_dir = PathBuf::from("./data");
        // let config = Config::new(tmp_dir.as_path());

        let index = Index::open(config)?;

        {
            // write
            let writer = index.writer();
            writer.insert_doc(Document::new(1, "foo bar", &["foo", "bar"]));
            writer.insert_doc(Document::new(2, "foo baz", &["foo", "baz"]));
            writer.insert_doc(Document::new(3, "biz buz", &["biz", "buz"]));
            writer.commit(true)?;
        }

        {
            // read doc_store
            let doc_id = 2u64;
            let reader = index.reader();
            let data = reader.fetch_doc(doc_id).unwrap();
            let text = String::from_utf8(data.to_vec()).unwrap();
            assert_eq!(&text, "foo baz");
        }

        {
            // overwrite doc 3
            let writer = index.writer();
            writer.insert_doc(Document::new(3, "overwrite", &["biz", "buz"]));
            writer.commit(true)?;

            let reader = index.reader();
            let data = reader.fetch_doc(3).unwrap();
            let text = String::from_utf8(data.to_vec()).unwrap();
            assert_eq!(&text, "overwrite");
        }

        {
            // all terms
            let reader = index.reader();
            let terms = reader.terms(Query::All)?;
            assert_eq!(&terms, &["bar", "baz", "biz", "buz", "foo"]);
        }

        {
            // terms range query
            let reader = index.reader();
            let terms = reader.terms_range(None, None)?;
            assert_eq!(&terms, &["bar", "baz", "biz", "buz", "foo"]);

            let terms = reader.terms_range(Some("biz"), None)?;
            assert_eq!(&terms, &["biz", "buz", "foo"]);

            let terms = reader.terms_range(None, Some("buz"))?;
            assert_eq!(&terms, &["bar", "baz", "biz"]);
        }

        // { // search
        //     let reader = index.reader();
        //     let doc_ids = reader.query("foo AND bar")?;
        //     for doc_id in doc_ids {
        //         let content = reader.fetch_doc(doc_id).unwrap();
        //         let text = String::from_utf8(content.to_vec()).unwrap();
        //         println!("Found doc: {} -> {}", doc_id, text);
        //     }

        //     assert_eq!(&doc_ids, &[1]);
        // }

        index.close(false).unwrap();
        Ok(())
    }
}
