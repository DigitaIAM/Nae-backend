use super::*;

use std::fs;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tantivy::collector::TopDocs;
use tantivy::directory::MmapDirectory;
use tantivy::query::QueryParser;
use tantivy::schema::{Field, Schema, STORED, TEXT};
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
  pub fn new<S: AsRef<Path>>(folder: S) -> Self
  where
    PathBuf: From<S>,
  {
    let mut schema_builder = Schema::builder();
    schema_builder.add_text_field("uuid", TEXT | STORED);
    schema_builder.add_text_field("name", TEXT);

    let schema = schema_builder.build();

    let path = folder.as_ref();
    fs::create_dir_all(path).unwrap();

    let directory = MmapDirectory::open(path).unwrap();
    let directory: Box<dyn Directory> = Box::new(directory);
    let index = Index::open_or_create(directory, schema).unwrap();

    let writer = index.writer(3_000_000).unwrap();
    let reader = index.reader_builder().reload_policy(ReloadPolicy::OnCommit).try_into().unwrap();

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
    if force
      || (self.added_events > 0
        && (self.added_events >= COMMIT_RATE || self.commit_timestamp.elapsed() >= COMMIT_TIME))
    {
      // println!("TantivyEngine: commit, {} {:?}", self.added_events, self.commit_timestamp.elapsed());
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
      match writer.add_document(doc! {
        uuid => id.to_string(),
        name => text,
      }) {
        Ok(_) => {},
        Err(e) => return Err(e),
      }
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
    let query = match parser.parse_query(input) {
      Ok(q) => q,
      Err(e) => {
        eprintln!("error at parsing query: {input} {e}");
        return vec![];
      },
    };

    let top_docs = match searcher.search(
      // &query, &TopDocs::with_limit(page_size)
      &query,
      &TopDocs::with_limit(100),
    ) {
      Ok(r) => r,
      Err(e) => {
        eprintln!("error at query: {e}");
        return vec![];
      },
    };

    top_docs
      .iter()
      .map(|(_score, doc_address)| match searcher.doc(*doc_address) {
        Ok(doc) => match doc.get_first(uuid) {
          None => UUID_NIL,
          Some(id) => match id.as_text() {
            None => UUID_NIL,
            Some(id) => match Uuid::parse_str(id) {
              Ok(r) => r,
              Err(e) => {
                eprintln!("error at uuid parse: {id} {e}");
                UUID_NIL
              },
            },
          },
        },
        Err(_) => UUID_NIL,
      })
      .filter(|id| *id != UUID_NIL)
      .collect()
  }

  fn schematic(&self) -> (Field, Field) {
    let schema = self.index.schema();
    let uuid = schema.get_field("uuid").unwrap();
    let name = schema.get_field("name").unwrap();
    (uuid, name)
  }
}
