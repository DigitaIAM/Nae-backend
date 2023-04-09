use std::io::Error;

use json::JsonValue;
use serde::{Deserialize, Serialize};
use simsearch::{SearchOptions, SimSearch};
use uuid::Uuid;

// use crate::{commutator::Application, text_search::SimSearchEngine};
use crate::commutator::Application;

#[derive(Debug, Deserialize, Serialize, PartialEq)]
struct JsonValueObject {
  name: String,
  manufacturer: String,
  // id: String,
  // uuid: String,
}

#[derive(Clone)]
pub struct SearchEngine {
  engine: SimSearch<Uuid>,
}

impl SearchEngine {
  pub fn new() -> Self {
    Self {
      engine: SimSearch::new(),
      // engine: SimSearch::new_with(),
    }
  }

  pub fn insert(&mut self, id: Uuid, text: &str) {
    self.engine.insert(id, text);
  }

  pub fn update(&mut self, id: Uuid, after: &str) {
    self.engine.delete(&id);
    self.engine.insert(id, after);
  }

  pub fn delete(&mut self, id: &Uuid) {
    self.engine.delete(id);
  }

  pub fn search(&self, text: &str) -> Vec<Uuid> {
    println!("-> {text}");
    let result = self.engine.search(text);
    println!("{}", result.len());
    result
  }
}

pub fn process_text_search(
  app: &Application,
  ctx: &Vec<String>,
  before: &JsonValue,
  data: &JsonValue,
) -> Result<(), Error> {
  dbg!(&ctx, &before, &data);
  if ctx == &vec!["drugs"] {
    let id = data["_uuid"].as_str().map(|data| Uuid::parse_str(data).unwrap()).unwrap();
    let before_name = before["name"].as_str();
    let after_name = data["name"].as_str();

    if let Some(before_name) = before_name {
      if let Some(after_name) = after_name {
        if before_name == after_name {
          // IGNORE
        } else {
          let mut search = app.search.write().unwrap();
          search.update(id, after_name);
        }
      } else {
        let mut search = app.search.write().unwrap();
        search.delete(&id);
      }
    } else {
      if let Some(after_name) = after_name {
        let mut search = app.search.write().unwrap();
        search.insert(id, after_name);
      } else {
        // IGNORE
      }
    }
  }
  Ok(())
}
