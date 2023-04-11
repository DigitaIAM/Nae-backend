use std::sync::Arc;

use simsearch::{SearchOptions, SimSearch};
use tantivy::schema::{Schema, STORED, TEXT, Value};
use tantivy::{Index, ReloadPolicy};
use tantivy::query::QueryParser;
use tantivy::collector::TopDocs;
use tantivy::Document;
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
  // schema: Schema,
  index: Index,
}

impl TantivySearch {
  pub fn new() -> Self {
      let mut schema_builder = Schema::builder();
      schema_builder.add_text_field("body", TEXT);
      schema_builder.add_text_field("id", TEXT | STORED);

      let schema = schema_builder.build();

      let path = "./tantivy";

      let index = Index::create_in_dir(&path, schema).unwrap();

      TantivySearch { index }
  }
}

impl Search for TantivySearch {
  fn load(&mut self, catalog: Vec<(Uuid, String)>) {
      let mut writer = self.index.writer(3_000_000).unwrap();

      let body = self.index.schema().get_field("body").unwrap();
      let id = self.index.schema().get_field("id").unwrap();

      for (uuid, text) in catalog {
        let mut rec = Document::default();
        rec.add_text(body, text);
        rec.add_text(id, uuid.to_string());
        writer.add_document(rec).unwrap();
      }

      writer.commit().unwrap();
  }

  fn search(&self, input: &str) -> Vec<Uuid> {
    let reader = self
      .index
      .reader_builder()
      .reload_policy(ReloadPolicy::OnCommit)
      .try_into()
      .unwrap();

    let searcher = reader.searcher();

    let id = self.index.schema().get_field("id").unwrap();
    let body = self.index.schema().get_field("body").unwrap();

    let parser = QueryParser::for_index(&self.index, vec![ body ]);
    let query = parser.parse_query(input).unwrap();

    let top_docs = searcher.search(&query, &TopDocs::with_limit(10)).unwrap();

    let result: Vec<Uuid> = vec![];
    
    result
  }
}