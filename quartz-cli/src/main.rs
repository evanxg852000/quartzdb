use tantivy::collector::Count;
use tantivy::query::RangeQuery;
use tantivy::schema::{STORED, Schema, TEXT};
use tantivy::{Index, IndexWriter, doc};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let logs: Vec<String> = vec![];
    let metrics: Vec<String> = vec![];

    // location:us-west
    // group:web
    // measruement:latency
    // tags: {host=web01, region=us-west, dc=us-west-1}

    // let mut schema_builder = Schema::builder();
    // let message_field = schema_builder
    //     .add_u64_field("_id", STORED)
    //     .add_text_field("_body", TEXT);
    // let schema = schema_builder.build();

    // Index::create(dir, schema, settings)
    // let index = Index::create_in_ram(schema);
    // let mut index_writer: IndexWriter = index.writer_with_num_threads(1, 20_000_000)?;
    // for message in logs {
    //     index_writer.add_document(doc!(message_field => message))?;
    // }
    // index_writer.commit()?;

    // let reader = index.reader()?;
    // let searcher = reader.searcher();
    // let docs_in_the_sixties = RangeQuery::new_u64("year".to_string(), 1960..1970);
    // let num_60s_books = searcher.search(&docs_in_the_sixties, &Count)?;
    // assert_eq!(num_60s_books, 2285);

    //TODO: create a ingest api and benchmark
    println!("Hello, world!");

    Ok(())
}
