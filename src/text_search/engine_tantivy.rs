use std::fs;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tantivy::collector::TopDocs;
use tantivy::directory::MmapDirectory;
use tantivy::query::QueryParser;
use tantivy::schema::{Schema, STORED, TEXT, Field};
use tantivy::{doc, Directory, Index, IndexReader, IndexWriter, ReloadPolicy, Term};
use uuid::Uuid;

const COMMIT_RATE: usize = 500;
const COMMIT_TIME: Duration = Duration::from_secs(1);

#[derive(Clone)]
pub struct TantivyEngine {
  index: Index,
  added_events: usize,
  writer: Arc<Mutex<IndexWriter>>,
  reader: Arc<Mutex<IndexReader>>,
  commit_timestamp: std::time::Instant,
}

impl TantivyEngine {
  pub fn new() -> Self {
    let mut schema_builder = Schema::builder();
    schema_builder.add_text_field("uuid", TEXT | STORED);
    schema_builder.add_text_field("name", TEXT);

    let schema = schema_builder.build();

    let path = "./data/tantivy";
    fs::create_dir_all(path).unwrap();

    let directory = MmapDirectory::open(path).unwrap();
    let directory: Box<dyn Directory> = Box::new(directory);
    let index = Index::open_or_create(directory, schema).unwrap();

    let writer = index.writer(3_000_000).unwrap();
    let reader = index
      .reader_builder()
      .reload_policy(ReloadPolicy::OnCommit)
      .try_into()
      .unwrap();

    Self {
      index,
      added_events: 0,
      commit_timestamp: std::time::Instant::now(),
      writer: Arc::new(Mutex::new(writer)),
      reader: Arc::new(Mutex::new(reader)),
    }
  }

  pub(crate) fn commit(&mut self) -> Result<bool, tantivy::TantivyError> {
    self.commit_helper(false)
  }

  pub(crate) fn force_commit(&mut self) -> Result<bool, tantivy::TantivyError> {
    self.commit_helper(true)
  }

  fn commit_helper(&mut self, force: bool) -> Result<bool, tantivy::TantivyError> {
    if force || (self.added_events > 0 &&
      (self.added_events >= COMMIT_RATE || self.commit_timestamp.elapsed() >= COMMIT_TIME))
    {
      println!("TantivyEngine: commit, {} {:?}", self.added_events, self.commit_timestamp.elapsed());
      self.writer.lock().unwrap().commit()?;
      self.added_events = 0;
      self.commit_timestamp = std::time::Instant::now();
      Ok(true)
    } else {
      self.added_events += 1;
      Ok(false)
    }
  }

  pub fn insert(&mut self, id: Uuid, text: &str) -> Result<bool, tantivy::TantivyError> {
    let (uuid, name) = self.schematic();

    {
      let writer = self.writer.lock()?;
      writer
        .add_document(doc! {
          uuid => id.to_string(),
          name => text,
        })
        .unwrap();
    }

    self.commit()
  }

  pub fn delete(&mut self, id: Uuid) -> Result<bool, tantivy::TantivyError> {
    let uuid = self.index.schema().get_field("uuid").unwrap();

    {
      let writer = self.writer.lock().unwrap();
      writer.delete_term(Term::from_field_text(uuid, &id.to_string()));
    }

    self.force_commit()
  }

  pub fn search(&self, input: &str) -> Vec<Uuid> {
    let (uuid, name) = self.schematic();
    
    let reader = self.reader.lock().unwrap();
    let searcher = reader.searcher();

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

  fn schematic(&self) -> (Field, Field) {
    let schema = self.index.schema();
    let uuid = schema.get_field("uuid").unwrap();
    let name = schema.get_field("name").unwrap();
    (uuid, name)
  }
}
