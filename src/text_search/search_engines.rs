use std::sync::Arc;

use simsearch::{SearchOptions, SimSearch};
use tantivy::schema::{Schema, STORED, TEXT, Value};
use tantivy::{Index, ReloadPolicy, Term};
use tantivy::query::QueryParser;
use tantivy::collector::TopDocs;
use tantivy::{doc, Document};
use uuid::Uuid;

pub trait Search {
  fn insert(&mut self, id: Uuid, text: &str);
  fn delete(&mut self, id: Uuid);
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
pub struct TantivyEngine {
  // schema: Schema,
  index: Index,
}

impl TantivyEngine {
// Создаем новый индекс в памяти с полями body и id
  pub fn new() -> Self {
// Создаем схему индекса с полями body и id
    let mut schema_builder = Schema::builder();
    schema_builder.add_text_field("uuid", TEXT | STORED);
    schema_builder.add_text_field("name", TEXT);
// Собираем схему индекса в индекс
    let schema = schema_builder.build();
// Создаем индекс в памяти 
    let directory_path = "./tantivy";
    let index = Index::create_in_dir(directory_path, schema).unwrap();
// Возвращаем структуру TantivySearch
    Self {
      index,
    }
  }
}

impl Search for TantivyEngine {
  fn insert(&mut self, id: Uuid, text: &str) {
    let mut index_writer = self.index.writer(3_000_000).unwrap();

    let uuid = self.index.schema().get_field("uuid").unwrap();
    let name = self.index.schema().get_field("name").unwrap();

    index_writer.add_document(doc!{
      uuid => id.to_string(),
      name => text,
    }).unwrap();

    index_writer.commit().unwrap();
  }

  fn delete(&mut self, id: Uuid) {
    let mut index_writer = self.index.writer(3_000_000).unwrap();

    let uuid = self.index.schema().get_field("uuid").unwrap();
    let name = self.index.schema().get_field("name").unwrap();

    index_writer.delete_term(Term::from_field_text(uuid, id));

    unimplemented!()
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
// получаем результат поиска по индексу searcher с ограничением в 10 документов
    let top_docs = searcher.search(&query, &TopDocs::with_limit(10)).unwrap();
// Преобразуем результат в вектор из Uuid
    top_docs.iter().map(|(_score, doc_address)| {
// Получаем документ по адресу из результата поиска по индексу searcher
      let retrieved_doc = searcher.doc(*doc_address).unwrap();

      let id = retrieved_doc.get_first(id).unwrap();
      let id = id.as_text().unwrap();
      Uuid::parse_str(id).unwrap()
    }).collect()
  }
}