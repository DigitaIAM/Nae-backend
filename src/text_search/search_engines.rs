use simsearch::{SearchOptions, SimSearch};
use tantivy::schema::{Schema, STORED, TEXT, Value};
use tantivy::{Index, ReloadPolicy};
use tantivy::query::QueryParser;
use tantivy::collector::TopDocs;
use uuid::Uuid;

pub trait Search {
  // fn load();
  fn search(&self, input: &str) -> Vec<Uuid>;
}

// --------------------------------

#[derive(Clone)]
pub struct SimSearchEngine {
  pub engine: SimSearch<Uuid>,
}

impl SimSearchEngine {
  pub fn new() -> Self {
      Self {
          engine: SimSearch::new(),
      }
  }
}

impl Search for SimSearchEngine {
  // fn load()
  fn search(&self, input: &str) -> Vec<Uuid> {
      self.engine.search(input)
  }
}

// ------------------------------

#[derive(Clone)]
pub struct TantivySearch {
  // index: Index,
  pub engine: SimSearch<Uuid>,
}

impl TantivySearch {
  pub fn new() -> Self {
      Self {
          engine: SimSearch::new(),
      }
  }
}

impl Search for TantivySearch {
  fn search(&self, input: &str) -> Vec<Uuid> {
      self.engine.search(input)
  }
}
