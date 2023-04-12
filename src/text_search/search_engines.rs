use std::sync::Arc;

use simsearch::{SearchOptions, SimSearch};
use tantivy::schema::{Schema, STORED, TEXT, Value};
use tantivy::{Index, ReloadPolicy};
use tantivy::query::QueryParser;
use tantivy::collector::TopDocs;
use tantivy::Document;
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
    schema_builder.add_text_field("body", TEXT);
    schema_builder.add_text_field("id", TEXT | STORED);
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
    unimplemented!()  
  }

  fn delete(&mut self, id: Uuid) {
    unimplemented!()
  }
  
  fn search(&self, input: &str) -> Vec<Uuid> {
// Создаем reader для поиска по индексу и searcher для поиска по reader
    let reader = self
      .index
// Перезагружаем индекс при запуске поиска
      .reader_builder()
// Перезагружаем индекс при commit()
      .reload_policy(ReloadPolicy::OnCommit)
// Создаем reader из индекса 
      .try_into()
// Если reader не создался, то возвращаем пустой вектор Uuid
      .unwrap();

    let searcher = reader.searcher();

    let id = self.index.schema().get_field("id").unwrap();
    let body = self.index.schema().get_field("body").unwrap();
// Создаем парсер запросов для поиска по полю body
    let parser = QueryParser::for_index(&self.index, vec![ body ]);
// Парсим запрос
    let query = parser.parse_query(input).unwrap();
// получаем результат поиска по индексу searcher с ограничением в 10 документов
    let top_docs = searcher.search(&query, &TopDocs::with_limit(10)).unwrap();
// Преобразуем результат в вектор из Uuid
    let mut result: Vec<Uuid> = vec![];
    result.extend(top_docs.iter().map(|(_score, doc_address)| {
// Получаем документ по адресу из результата поиска по индексу searcher
      let retrieved_doc = searcher.doc(*doc_address).unwrap();
// Получаем поле id из документа и преобразуем его в Uuid
      let id = retrieved_doc.get_first(id).unwrap();
      let id = id.as_text().unwrap();
      Uuid::parse_str(id).unwrap()
    }));
    result
  }
}