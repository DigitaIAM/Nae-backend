mod engine_tantivy;
mod processing;
mod tantivy_search;

pub use processing::handle_mutation;
pub use processing::SearchEngine;
use tantivy::schema::Field;
pub use tantivy_search::TextSearch;

use uuid::Uuid;

pub trait Search {
  fn insert(&mut self, id: Uuid, text: &str);
  fn delete(&mut self, id: Uuid);
  fn search(&self, input: &str) -> Vec<Uuid>;
  fn schematic(&self) -> (Field, Field);
}
