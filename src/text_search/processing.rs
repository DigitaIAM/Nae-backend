use json::JsonValue;
use simsearch::SimSearch;
use std::io::Error;
use uuid::Uuid;

use crate::{
  commutator::Application, storage::Workspaces, text_search::engine_tantivy::TantivyEngine,
  text_search::Search,
};

#[derive(Clone)]
pub struct SearchEngine {
  sim: SimSearch<Uuid>,
  tan: TantivyEngine,
}

impl SearchEngine {
  pub fn new() -> Self {
    Self { sim: SimSearch::new(), tan: TantivyEngine::new() }
  }

  pub fn load(&mut self, workspaces: Workspaces) -> Result<(), service::error::Error> {
    for ws in workspaces.list()? {
      let memories = ws.memories(vec!["drugs".to_string()]);

      for mem in memories.list(None)? {
        let jdoc = mem.json()?;
        let name = jdoc["name"].as_str().unwrap();
        let uuid = jdoc["_uuid"].as_str().unwrap();
        let uuid = Uuid::parse_str(uuid).unwrap();

        self.sim.insert(uuid, name);
      }
    }

    Ok(())
  }

  pub fn create(&mut self, id: Uuid, text: &str) {
    self.sim.insert(id, text);
    self.tan.insert(id, text);
  }

  pub fn change(&mut self, id: Uuid, _before: &str, after: &str) {
    self.delete(&id);
    self.create(id, after);
  }

  pub fn delete(&mut self, id: &Uuid) {
    self.sim.delete(id);
    self.tan.delete(*id);
  }

  pub fn search(&mut self, text: &str) -> Vec<Uuid> {
    let mut result_sim = self.sim.search(text);
    let result_tan = self.tan.search(text);

    // println!("result_tan.len() = {}", result_tan.len());
    // println!("result_sim.len() = {}", result_sim.len());

    result_sim.extend(result_tan);

    result_sim
  }
}

pub fn handle_mutation(
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
