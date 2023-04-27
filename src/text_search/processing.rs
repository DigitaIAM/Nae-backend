use json::JsonValue;
use simsearch::SimSearch;
use uuid::Uuid;

use crate::{
  commutator::Application, storage::Workspaces, 
  text_search::engine_tantivy::TantivyEngine,
};

#[derive(Debug)]
pub enum Error {
  Tantivy(tantivy::TantivyError),
  Service(service::error::Error),
  IO(std::io::Error),
}

impl From<tantivy::TantivyError> for Error {
  fn from(err: tantivy::TantivyError) -> Self {
    Error::Tantivy(err)
  }
}

impl From<service::error::Error> for Error {
  fn from(err: service::error::Error) -> Self {
    Error::Service(err)
  }
}
    
impl From<std::io::Error> for Error {
  fn from(err: std::io::Error) -> Self {
    Error::IO(err)
  }
}

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

        self.sim.insert(uuid, &name);
      }
    }

    Ok(())
  }

  pub fn create(&mut self, id: Uuid, text: &str) -> Result<(), tantivy::TantivyError> {
    self.sim.insert(id, text);
    self.tan.insert(id, text)?;
    Ok(())
  }

  pub fn change(&mut self, id: Uuid, _before: &str, after: &str) -> Result<(), tantivy::TantivyError> {
    self.delete(&id)?;
    self.create(id, after)?;
    Ok(())
  }

  pub fn delete(&mut self, id: &Uuid) -> Result<(), tantivy::TantivyError> {
    self.sim.delete(id);
    self.tan.delete(*id)?;
    Ok(())
  }

  pub fn search(&self, text: &str, page_size: usize, offset: usize) -> Vec<Uuid> {
    let text = &text.to_lowercase().replace("ё", "е")[..];

    let result_full = self.tan.search(&format!("\"{}\"", text));
    let result_tan = self.tan.search(text);
    let result_sim = self.sim.search(text);

    println!("result_full.len() = {}\tresult_tan.len() = {}\tresult_sim.len() = {}", 
      result_full.len(), result_tan.len(), result_sim.len()
    );

    let result_tan = remove_duplicates(result_tan, &result_full);
    let result_sim = remove_duplicates(result_sim, &result_tan);
    let result_sim = remove_duplicates(result_sim, &result_full);

    println!("result_full.len() = {}\tresult_tan.len() = {}\tresult_sim.len() = {}", 
      result_full.len(), result_tan.len(), result_sim.len()
    );

    let half_page = page_size / 2;

    let mut skip_full = 0;
    let mut skip_tan = 0;
    let mut skip_sim = 0;

    loop {
      if skip_full + skip_tan + skip_sim >= offset {
        break;
      }
      // TODO
    }

    let mut result: Vec<Uuid> = Vec::with_capacity(page_size);

    result.extend(result_full.iter().skip(skip_full).take(half_page));
    println!("skip_full = {skip_full}; result.len() = {}", result.len());
    result.extend(result_tan.iter().skip(skip_tan).take(half_page - result.len()));
    println!("skip_tan = {skip_tan}; result.len() = {}", result.len());
    result.extend(result_sim.iter().skip(skip_sim).take(page_size - result.len()));
    println!("skip_sim = {skip_sim}; result.len() = {}", result.len());
    result
  }

  pub fn commit(&mut self) -> Result<(), Error> {
    self.tan.force_commit()?;
    Ok(())
  }
}

fn remove_duplicates(mut result_sim: Vec<Uuid>, result_tan: &Vec<Uuid>) -> Vec<Uuid> {
  result_sim.retain(|e| !result_tan.contains(e));
  result_sim
}

pub fn handle_mutation(
  app: &Application,
  ctx: &Vec<String>,
  before: &JsonValue,
  data: &JsonValue,
) -> Result<(), Error> {
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
          let after_name = after_name.to_lowercase().replace("ё", "е");
          search.change(id, before_name, after_name.as_str())?;
        }
      } else {
        let mut search = app.search.write().unwrap();
        search.delete(&id)?;
      }
    } else {
      if let Some(after_name) = after_name {
        let mut search = app.search.write().unwrap();
        let after_name = after_name.to_lowercase().replace("ё", "е");
        search.create(id, after_name.as_str())?;
      } else {
        // IGNORE
      }
    }
  }
  Ok(())
}
