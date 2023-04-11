use std::io::Error;

use json::JsonValue;
use serde::{Deserialize, Serialize};
use simsearch::SimSearch;
use uuid::Uuid;

// use crate::storage::organizations::Workspace;
use crate::{commutator::Application, storage::Workspaces, text_search::search_engines::SimSearchEngine};

#[derive(Debug, Deserialize, Serialize, PartialEq)]
struct JsonValueObject {
  name: String,
  manufacturer: String,
  // id: String,
  // uuid: String,
}

pub trait SearchTrait {
  fn load(&mut self, catalog: Vec<(Uuid, String)>);
  fn search(&self, input: &str) -> Vec<Uuid>;
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
      // engine: SimSearch::new(),
      engine: SimSearchEngine::new().engine,
    }
  }

  pub fn load(&mut self, workspaces: Workspaces) -> Result<(), service::error::Error> {
    for ws in workspaces.list()? {
      let memories = ws.memories(vec!["drugs".to_string()]);
      
      for mem in memories.list(None)? {
        let jdoc = mem.json()?;
        let name = jdoc["name"].as_str().unwrap();
        let uuid = jdoc["_uuid"].as_str().unwrap();
        let uuid = Uuid::parse_str(uuid).unwrap();
        self.engine.insert(uuid, name);
      }
    }

    Ok(())
  }

  pub fn create(&mut self, id: Uuid, text: &str) {
    self.catalog.push((id, text.to_string()));
  }

  pub fn change(&mut self, id: Uuid, _before: &str, after: &str) {
    self.delete(&id);
    self.create(id, after);
  }

  pub fn delete(&mut self, id: &Uuid) {
    if let Some(index) = self.catalog.iter().position(|(current_id, _current_text)| current_id == id)
    {
      self.catalog.remove(index);
    };
  }

  pub fn search(&mut self, text: &str) -> Vec<Uuid> {
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
