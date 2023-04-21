use json::JsonValue;
use simsearch::SimSearch;
use uuid::Uuid;
use regex::Regex;

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

  pub fn search(&mut self, text: &str) -> Vec<Uuid> {
    let mut result_tan = self.tan.search(text);
    let mut result_sim = self.sim.search(text);

// (+) убрать дубликаты и (+) объединить в сет
// метрика схожести
// сортировка по метрике

    println!("result_tan.len() = {}", result_tan.len());
    println!("result_sim.len() = {}", result_sim.len());

    let mut result_sim = remove_duplicates(&mut result_sim, result_tan.clone());
    if result_sim.len() > 5 {
      result_sim = result_sim.split_off(5);
    }
    if result_tan.len() > 5 {
      result_tan = result_tan.split_off(5);
    }

    result_tan.append(&mut result_sim);
    // result_tan.dedup();
    // println!("COMBINED = {}", result_tan.len());

    result_tan
  }

  pub fn commit(&mut self) -> Result<(), Error> {
    self.tan.force_commit()?;
    Ok(())
  }
}
use std::{collections::BTreeSet, iter::FromIterator};
fn remove_duplicates(result_sim: &mut Vec<Uuid>, result_tan: Vec<Uuid>) -> Vec<Uuid> {
  let to_remove = BTreeSet::from_iter(result_tan);
  result_sim.retain(|e| !to_remove.contains(e));
  result_sim.clone()
}

pub fn handle_mutation(
  app: &Application,
  ctx: &Vec<String>,
  before: &JsonValue,
  data: &JsonValue,
) -> Result<(), Error> {
  // dbg!(&ctx, &before, &data);ИМУРАН ТАБ 25 МГ №100
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
          search.change(id, before_name, after_name)?;
        }
      } else {
        let mut search = app.search.write().unwrap();
        search.delete(&id)?;
      }
    } else {
      if let Some(after_name) = after_name {
        let mut search = app.search.write().unwrap();
        // try replace_all
        // let re = Regex::new(r#"""#).unwrap();
        // after_name.replace_all(re, "");
        let letter_e_1 = Regex::new(r#"ё"#).unwrap();
        let letter_e_2 = Regex::new(r#"Ё"#).unwrap();
        let after_name = letter_e_1.replace_all(after_name, "е");
        let after_name = letter_e_2.replace_all(&after_name, "Е");
        search.create(id, after_name)?;
      } else {
        // IGNORE
      }
    }
  }
  Ok(())
}
