// use std::sync::Arc;

// use simsearch::{SimSearch, SearchOptions};
use tantivy::schema::{
  Schema, STORED, TEXT, 
  // Value
};
use tantivy::{Index, ReloadPolicy, Term, doc};
use tantivy::query::QueryParser;
use tantivy::collector::TopDocs;
// use tantivy::{doc, Document};
use uuid::Uuid;

pub trait Search {
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
// Создаем схему индекса с полями body и id
    // let mut schema_builder = Schema::builder();
    // schema_builder.add_text_field("uuid", TEXT | STORED);
    // schema_builder.add_text_field("name", TEXT);
// Собираем схему индекса в индекс
    // let schema = schema_builder.build();
// Чиатем индекс с диска 
    let directory_path = "./tantivy";
    let index = Index::open_in_dir(directory_path).unwrap();

    Self {
      index,
    }
  }
}

impl Search for TantivyEngine {
  fn insert(&mut self, id: Uuid, text: &str) {
// Создаём штуку для записи в индекс
    let mut index_writer = self.index.writer(3_000_000).unwrap();
// Создаём поля как в ранее созданной схеме (она создалась при мервом запуске приложения)
    let uuid = self.index.schema().get_field("uuid").unwrap();
    let name = self.index.schema().get_field("name").unwrap();
// Добавляем в индекс документ с теми данными, с которыми мы вызвали эту функцию
    index_writer.add_document(doc!{
      uuid => id.to_string(),
      name => text,
    }).unwrap();
// Подверждаем изменения в индексе
    index_writer.commit().unwrap();
  }

  fn delete(&mut self, id: Uuid) {
    let mut index_writer = self.index.writer(3_000_000).unwrap();
// Получаем поле uuid из схемы индекса
    let uuid = self.index.schema().get_field("uuid").unwrap();
// Удаляем по условию: поле должно равняться нужному uuid
    index_writer.delete_term(Term::from_field_text(uuid, &id.to_string()));

    index_writer.commit().unwrap();
  }

  fn search(&self, input: &str) -> Vec<Uuid> {
// Создаём штуку для чтения из индекса
    let reader = self
      .index
      .reader_builder()
      .reload_policy(ReloadPolicy::OnCommit)
      .try_into()
      .unwrap();
// Создаём штуку для поиска по индексу
    let searcher = reader.searcher();
// Получаем поля из схемы индекса
    let uuid = self.index.schema().get_field("uuid").unwrap();
    let name = self.index.schema().get_field("name").unwrap();
// Создаём парсер запросов и передаём ему поля, по которым мы будем искать
    let parser = QueryParser::for_index(&self.index, vec![ name ]);

    let query = parser.parse_query(input).unwrap();
// получаем результат поиска по индексу searcher с ограничением в 10 документов
    let top_docs = searcher.search(&query, &TopDocs::with_limit(10)).unwrap();
// Преобразуем результат в вектор из Uuid
    top_docs.iter().map(|(_score, doc_address)| {
// Получаем документ по адресу из результата поиска по индексу searcher
      let retrieved_doc = searcher.doc(*doc_address).unwrap();
// Получаем значение поля uuid из документа retrieved_doc
      let id = retrieved_doc.get_first(uuid).unwrap();
      let id = id.as_text().unwrap();
// Парсим строку в Uuid и возвращаем вектор из Uuid
      Uuid::parse_str(id).unwrap()
    }).collect()
  }
}