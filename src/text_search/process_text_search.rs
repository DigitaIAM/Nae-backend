use std::io::Error;

use serde::{Deserialize, Serialize};
use json::JsonValue;
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
  catalog: Vec<(String, String)>
}

impl SearchEngine {
  pub fn new() -> Self {
    SearchEngine { catalog: vec![], }
  }
  pub fn create(&mut self, id: &str, text: &str) -> Result<(), Error> {
    self.catalog.push((id.to_string(), text.to_string()));
    Ok(())
  }
  pub fn change(&self, id: &str, before: &str, after: &str) -> Result<(), Error> {
    todo!()
  }
  pub fn remove(&self, id: &str, text: &str) -> Result<(), Error> {
    Ok(())
  }
}

pub fn process_text_search(app: &Application,  ctx: &Vec<String>, before: &JsonValue, data: &JsonValue) -> Result<(), Error> {
  if ctx == &vec!["drugs"] {
    let id = data["_id"].as_str().unwrap_or_default();
    let before_name = before["name"].as_str();
    let after_name = data["name"].as_str();

    if let Some(before_name) = before_name {
      if let Some(after_name) = after_name {
        if before_name == after_name {
          todo!() // IGNORE
        } else {
          todo!() // CHANGE
          // app.search.change(id, before_name, after_name)
        }
      } else {
        todo!() // REMOVE
        // app.search.delete()
      }
    } else {
      if let Some(after_name) = after_name {
        todo!() // CREATE
        // app.search.delete(id, after_name)
      } else {
        todo!() // IGNORE
      }
    }

    todo!()
  }
  Ok(())
}