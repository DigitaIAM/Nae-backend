use simsearch::{SearchOptions, SimSearch};
use tantivy::schema::{Schema, STORED, TEXT, Value};
use tantivy::{Index, ReloadPolicy};
use tantivy::query::QueryParser;
use tantivy::collector::TopDocs;
use uuid::Uuid;

pub trait Search {
  fn load(&mut self, catalog: Vec<(Uuid, String)>);
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
  fn load(&mut self, catalog: Vec<(Uuid, String)>) {
    let mut schema_builder = Schema::builder();
    let text_field = schema_builder.add_text_field("text", TEXT | STORED);
    let schema = schema_builder.build();
    let index = Index::create_in_ram(schema);
    let mut index_writer = index.writer(50_000_000).unwrap();

    for (uuid, text) in catalog {
      let mut doc = tantivy::Document::default();
      doc.add_text(text_field, &text);
    }

  }

  fn search(&self, input: &str) -> Vec<Uuid> {
    vec![Uuid::new_v4()]
  }
}
