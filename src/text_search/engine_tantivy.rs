use std::fs;
use std::path::Path;
use std::sync::{Arc, Mutex};

use tantivy::collector::TopDocs;
use tantivy::query::QueryParser;
use tantivy::schema::{Schema, STORED, TEXT};
use tantivy::{doc, Index, IndexReader, IndexWriter, ReloadPolicy, Term};
use uuid::Uuid;

use crate::text_search::Search;

#[derive(Clone)]
pub struct TantivyEngine {
  index: Index,
  writer: Arc<Mutex<IndexWriter>>,
  reader: Arc<Mutex<IndexReader>>,
}

impl TantivyEngine {
  pub fn new() -> Self {
    let mut schema_builder = Schema::builder();
    schema_builder.add_text_field("uuid", TEXT | STORED);
    schema_builder.add_text_field("name", TEXT);

    let schema = schema_builder.build();
    // schema_builder.build();

    let directory_path = "./data/tantivy";
    fs::create_dir_all(directory_path).unwrap();

    let index = if Path::new(directory_path).is_dir() {
      Index::open_in_dir(directory_path).unwrap()
    } else {
      Index::create_in_dir(directory_path, schema).unwrap()
    };

    let writer = Arc::new(Mutex::new(index.writer(3_000_000).unwrap()));
    let reader = Arc::new(Mutex::new(
      index.reader_builder().reload_policy(ReloadPolicy::OnCommit).try_into().unwrap(),
    ));

    Self { writer, reader, index }
  }
}

impl Search for TantivyEngine {
  fn insert(&mut self, id: Uuid, text: &str) {
    let schema = self.index.schema();
    let uuid = schema.get_field("uuid").unwrap();
    let name = schema.get_field("name").unwrap();

    let mut writer = self.writer.lock().unwrap();

    writer
      .add_document(doc! {
        uuid => id.to_string(),
        name => text,
      })
      .unwrap();

    writer.commit().unwrap();
  }

  fn delete(&mut self, id: Uuid) {
    let uuid = self.index.schema().get_field("uuid").unwrap();

    let mut writer = self.writer.lock().unwrap();

    writer.delete_term(Term::from_field_text(uuid, &id.to_string()));
    writer.commit().unwrap();
  }

  fn search(&self, input: &str) -> Vec<Uuid> {
    let reader = self.reader.lock().unwrap();
    let searcher = reader.searcher();

    let schema = self.index.schema();
    let uuid = schema.get_field("uuid").unwrap();
    let name = schema.get_field("name").unwrap();

    let parser = QueryParser::for_index(&self.index, vec![name]);
    let query = parser.parse_query(input).unwrap();

    let top_docs = searcher.search(&query, &TopDocs::with_limit(10)).unwrap();

    top_docs
      .iter()
      .map(|(_score, doc_address)| {
        let retrieved_doc = searcher.doc(*doc_address).unwrap();

        let id = retrieved_doc.get_first(uuid).unwrap();
        let id = id.as_text().unwrap();

        Uuid::parse_str(id).unwrap()
      })
      .collect()
  }
}
