use std::sync::{Arc, Mutex};

use std::path::Path;

// use simsearch::{SimSearch, SearchOptions};
use tantivy::schema::{
  Schema, STORED, TEXT, 
  // Value
};
use tantivy::{Index, ReloadPolicy, Term, doc, IndexWriter, IndexReader};
use tantivy::query::QueryParser;
use tantivy::collector::TopDocs;
// use tantivy::{doc, Document};
use uuid::Uuid;

pub trait Search {
  fn index_writer(&mut self) -> Arc<Mutex<IndexWriter>>;
  fn index_reader(&self) -> Arc<Mutex<IndexReader>>;
  fn insert(&mut self, id: Uuid, text: &str);
  fn delete(&mut self, id: Uuid);
  fn search(&self, input: &str) -> Vec<Uuid>;
}

// --------------------------------

// #[derive(Clone)]
// pub struct SimSearchEngine {
//   pub engine: SimSearch<Uuid>,
// }

// impl SimSearchEngine {
//   pub fn new() -> Self {
//       Self {
//         engine: SimSearch::new(),
//       }
//   }
// }

// ------------------------------

#[derive(Clone)]
pub struct TantivyEngine {
  // schema: Schema,
  index: Index,
}

impl TantivyEngine {
  pub fn new() -> Self {
    let mut schema_builder = Schema::builder();
    schema_builder.add_text_field("uuid", TEXT | STORED);
    schema_builder.add_text_field("name", TEXT);

    let schema = schema_builder.build();

    let directory_path = "./tantivy";
    // if Index::exists(directory_path)
    let index = if Path::new(directory_path).is_dir() {
      Index::open_in_dir(directory_path).unwrap()
    } else {
      Index::create_in_dir(directory_path, schema).unwrap()
    };

    Self {
      index,
    }
  }
}


impl Search for TantivyEngine {
  fn index_writer(&mut self) -> Arc<Mutex<IndexWriter>> {
    Arc::new(Mutex::new(self.index.writer(3_000_000).unwrap()))
  }

  fn index_reader(&self) -> Arc<Mutex<IndexReader>> {
    Arc::new(Mutex::new(self.index.reader_builder().reload_policy(ReloadPolicy::OnCommit).try_into().unwrap()))
  }

  fn insert(&mut self, id: Uuid, text: &str) {
    // let mut index_writer = self.index.writer(3_000_000).unwrap();

    let uuid = self.index.schema().get_field("uuid").unwrap();
    let name = self.index.schema().get_field("name").unwrap(); 

    // index_writer.add_document(doc!{
    //   uuid => id.to_string(),
    //   name => text,
    // }).unwrap();
    
    self.index_writer().lock().unwrap().commit().unwrap();
  }

  fn delete(&mut self, id: Uuid) {
    let mut index_writer = self.index.writer(3_000_000).unwrap();

    let uuid = self.index.schema().get_field("uuid").unwrap();

    index_writer.delete_term(Term::from_field_text(uuid, &id.to_string()));
    index_writer.commit().unwrap();
  }

  fn search(&self, input: &str) -> Vec<Uuid> {
    // let reader = self
    //   .index
    //   .reader_builder()
    //   .reload_policy(ReloadPolicy::OnCommit)
    //   .try_into()
    //   .unwrap();

    // let searcher = reader.searcher();
    let searcher = self.index_reader().lock().unwrap().searcher();

    let uuid = self.index.schema().get_field("uuid").unwrap();
    let name = self.index.schema().get_field("name").unwrap();

    let parser = QueryParser::for_index(&self.index, vec![ name ]);
    let query = parser.parse_query(input).unwrap();

    let top_docs = searcher.search(&query, &TopDocs::with_limit(10)).unwrap();

    top_docs.iter().map(|(_score, doc_address)| {

      let retrieved_doc = searcher.doc(*doc_address).unwrap();

      let id = retrieved_doc.get_first(uuid).unwrap();
      let id = id.as_text().unwrap();

      Uuid::parse_str(id).unwrap()
    }).collect()
  }
}