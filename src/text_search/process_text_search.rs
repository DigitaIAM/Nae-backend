use std::io::Error;

use json::JsonValue;
use serde::{Deserialize, Serialize};

use crate::{commutator::Application, text_search::SimSearchEngine};
use crate::text_search::search_engines::Search;

#[derive(Debug, Deserialize, Serialize, PartialEq)]
struct JsonValueObject {
  name: String,
  manufacturer: String,
  // id: String,
  // uuid: String,
}

#[derive(Clone)]
pub struct SearchEngine {
  catalog: Vec<(String, String)>,
}

impl SearchEngine {
  pub fn new() -> Self {
    SearchEngine { catalog: vec![] }
  }
  pub fn create(&mut self, id: &str, text: &str) {
    self.catalog.push((id.to_string(), text.to_string()));
  }
  pub fn change(&mut self, id: &str, before: &str, after: &str) {
    self.delete(id, before);
    self.create(id, after);
  }

  pub fn delete(&mut self, id: &str, _text: &str) {
    if let Some(index) = self.catalog.iter().position(|(current_id, _current_text)| current_id == id)
    {
      self.catalog.remove(index);
    };
  }
  #[allow(unused)]
  pub fn search(&self, text: &str) -> Vec<String> {
    let engine = SimSearchEngine::new();
    let catalog = SearchEngine {
      catalog: load()
    };
    let search_test = engine.search(text);
    vec![]
  }
}

fn load() -> Vec<(String, String)> {
  vec![]
}

pub fn process_text_search(
  app: &Application,
  ctx: &Vec<String>,
  before: &JsonValue,
  data: &JsonValue,
) -> Result<(), Error> {
  if ctx == &vec!["drugs"] {
    let id = data["_uuid"].as_str().unwrap_or_default();
    let before_name = before["name"].as_str();
    let after_name = data["name"].as_str();

    if let Some(before_name) = before_name {
      if let Some(after_name) = after_name {
        if before_name == after_name {
          // IGNORE
        } else {
          let mut search = app.search.write().unwrap();
          search.change(id, before_name, after_name);
        }
      } else {
        let mut search = app.search.write().unwrap();
        search.delete(id, before_name);
      }
    } else {
      if let Some(after_name) = after_name {
        let mut search = app.search.write().unwrap();
        search.create(id, after_name);
      } else {
        // IGNORE
      }
    }
  }
  Ok(())
}
