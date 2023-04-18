use std::fs;
use std::path::Path;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tantivy::collector::TopDocs;
use tantivy::directory::MmapDirectory;
use tantivy::query::QueryParser;
use tantivy::schema::{Schema, STORED, TEXT};
use tantivy::{doc, Directory, Index, IndexReader, IndexWriter, ReloadPolicy, Term};
use uuid::Uuid;

use crate::text_search::Search;

const COMMIT_RATE: usize = 500;
const COMMIT_TIME: Duration = Duration::from_secs(1);

pub struct Writer {
  writer: Arc<Mutex<IndexWriter>>,
  added_events: usize,
  commit_timestamp: std::time::Instant,
}

impl Writer {
  pub fn new(writer: Arc<Mutex<IndexWriter>>) -> Self {
    Self {
      writer,
      added_events: 0,
      commit_timestamp: std::time::Instant::now(),
    }
  }

  pub fn add_event(&mut self, id: Uuid, text: &str) {
    let schema = TantivyEngine::new().index.schema();
    let uuid = schema.get_field("uuid").unwrap();
    let name = schema.get_field("name").unwrap();

    let mut writer = self.writer.lock().unwrap();

    writer
      .add_document(doc! {
        uuid => id.to_string(),
        name => text,
      })
      .unwrap();

    self.added_events += 1;

    if self.added_events >= COMMIT_RATE
      || self.commit_timestamp.elapsed() >= COMMIT_TIME
    {
      writer.commit().unwrap();
      self.added_events = 0;
      self.commit_timestamp = std::time::Instant::now();
    }
  }

  pub fn commit(&mut self) -> Result<bool, tantivy::TantivyError> {
    self.commit_helper(false)
  }

  pub fn force_commit(&mut self) -> Result<bool, tantivy::TantivyError> {
    self.commit_helper(true)?;
    Ok(true)
  }

  fn commit_helper(&mut self, force: bool) -> Result<bool, tantivy::TantivyError> {
    if self.added_events > 0
      && (force
        || self.added_events >= COMMIT_RATE
        || self.commit_timestamp.elapsed() >= COMMIT_TIME)
    {
      self.writer.lock().unwrap().commit().unwrap();
      self.added_events = 0;
      self.commit_timestamp = std::time::Instant::now();
      Ok(true)
    } else {
      Ok(false)
    }
  }
}

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

    let path = "./data/tantivy";
    fs::create_dir_all(path).unwrap();

    let directory: Box<dyn Directory> = Box::new(MmapDirectory::open(path).unwrap());
    let index = Index::open_or_create(directory, schema).unwrap();

    let writer = index.writer(3_000_000).unwrap();
    let reader = index
      .reader_builder()
      .reload_policy(ReloadPolicy::OnCommit)
      .try_into()
      .unwrap();

    Self {
      writer: Arc::new(Mutex::new(writer)),
      reader: Arc::new(Mutex::new(reader)),
      index,
    }
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
