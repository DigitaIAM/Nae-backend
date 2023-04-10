use std::{io::Error, sync::RwLock};

use json::JsonValue;
use serde::{Deserialize, Serialize};
// use simsearch::{SearchOptions, SimSearch};
use simsearch::SimSearch;
use uuid::Uuid;

// use crate::{commutator::Application, text_search::SimSearchEngine};
use crate::{commutator::Application, storage::Workspaces};

#[derive(Debug, Deserialize, Serialize, PartialEq)]
struct JsonValueObject {
  name: String,
  manufacturer: String,
  // id: String,
  // uuid: String,
}

#[derive(Clone)]
pub struct SearchEngine {
  catalog: Vec<(Uuid, String)>,
  engine: SimSearch<Uuid>,
}

impl SearchEngine {
  pub fn new() -> Self {
    Self {
      catalog: vec![],
      engine: SimSearch::new(),
    }
  }

  fn load(&self, workspaces: Workspaces) -> Result<(), service::error::Error> {
    workspaces.list()?.iter().for_each(|ws| {
      let memories = ws.memories(vec!["drugs".to_string()]);
      // memories.list(None)?.iter().map(|doc| doc.json().unwrap()).map(|doc| (doc["name"].clone(), doc["_uuid"].clone()));
      let jsontuple: Vec<(JsonValue, JsonValue)> = memories.list(None).unwrap().iter().map(|doc| 
        doc.json().unwrap()).map(|doc| (doc["name"].clone(), doc["_uuid"].clone())
      ).collect();
      // jsontuple.iter().for_each(|(name, uuid)| {
      //   let name = name.as_str().unwrap();
      //   let uuid_str = uuid.as_str().unwrap();
      //   let uuid = Uuid::parse_str(uuid_str).unwrap();
      //   self.engine.insert(uuid, name);
      // });
      
      // Ok(())
    });
    Ok(())
  }

  pub fn create(&mut self, id: Uuid, text: &str) {
    self.catalog.push((id, text.to_string()));
    // self.engine.insert(id, text);
  }

  pub fn change(&mut self, id: Uuid, _before: &str, after: &str) {
    self.delete(&id);
    self.create(id, after);
    // self.engine.delete(&id);
    // self.engine.insert(id, after);
  }

  pub fn delete(&mut self, id: &Uuid) {
    if let Some(index) = self.catalog.iter().position(
      |(current_id, _current_text)| current_id == id
    ) {
      self.catalog.remove(index);
    };
    // self.engine.delete(id);
  }

  pub fn search(&self, text: &str) -> Vec<Uuid> {
    let ws = Workspaces::new("./data/companies/");
    self.load(ws).unwrap();

    // let ctx = vec!["drugs"];

    println!("-> {text}");
    let result = self.engine.search(text);
    println!("result.len() = {}", result.len());
    result
  }
}

pub fn process_text_search(
  app: &Application,
  ctx: &Vec<String>,
  before: &JsonValue,
  data: &JsonValue,
) -> Result<(), Error> {
  // dbg!(&ctx, &before, &data);
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
          search.change(id, before_name, after_name);
        }
      } else {
        let mut search = app.search.write().unwrap();
        search.delete(&id);
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
