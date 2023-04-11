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

pub struct TantivySearch {
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

      let mut results = vec![];
      
      for (_score, doc_address) in top_docs {
          let retrieved_doc = searcher.doc(doc_address).unwrap();
          let id = retrieved_doc.get_first(id).unwrap();
          let id = id.as_text().unwrap();
          let id = Uuid::parse_str(id).unwrap();
          results.push(id);
      }

      results
  }
}
