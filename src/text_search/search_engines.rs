use simsearch::{SearchOptions, SimSearch};
// use tantivy::schema::{Schema, STORED, TEXT, Value};
use uuid::Uuid;

pub trait Search {
  // fn load();
  fn search(&self, input: &str) -> Vec<Uuid>;
}

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

// pub struct TantivySearch {}

// impl TantivySearch {
//   pub fn new() -> Self {}
// }

// impl Search for TantivySearch {}
