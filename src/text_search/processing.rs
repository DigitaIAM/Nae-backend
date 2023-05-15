use super::*;

use json::JsonValue;
use simsearch::SimSearch;
use uuid::Uuid;

use crate::{
  commutator::Application, storage::Workspaces, text_search::engine_tantivy::TantivyEngine,
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
    let text = text.to_lowercase().replace("ё", "е");

    self.sim.insert(id, text.as_str());
    self.tan.insert(id, text.as_str())?;
    Ok(())
  }

  pub fn change(
    &mut self,
    id: Uuid,
    _before: &str,
    after: &str,
  ) -> Result<(), tantivy::TantivyError> {
    self.delete(&id)?;
    self.create(id, after)?;
    Ok(())
  }

  pub fn delete(&mut self, id: &Uuid) -> Result<(), tantivy::TantivyError> {
    self.sim.delete(id);
    self.tan.delete(*id)?;
    Ok(())
  }

  pub fn search(&self, text: &str, page_size: usize, offset: usize) -> (usize, Vec<Uuid>) {
    // println!("page_size = {page_size}, offset = {offset}");
    let text = text.to_lowercase().replace("ё", "е");

    let result_full = self.tan.search(&format!("\"{}\"", text));
    let result_tan = self.tan.search(&text);
    let result_sim = self.sim.search(&text);

    let result_tan = remove_duplicates(result_tan, &result_full);
    let result_tan: Vec<_> = [result_tan, result_full].concat();

    let result_sim = remove_duplicates(result_sim, &result_tan);

    // PAGINATION
    let page_number = offset / page_size;

    let total = result_tan.len() + result_sim.len();

    if page_number > total / page_size + 1 {
      return (0, vec![]);
    }

    let half_page = page_size / 2;

    let full_page_tan = result_tan.len() / half_page;
    let full_page_sim = result_sim.len() / half_page;

    let full_page = full_page_tan.min(full_page_sim);

    // println!("full_page_tan = {full_page_tan} full_page_sim = {full_page_sim}");

    if page_number < full_page {
      let offset = page_number * half_page;
      let mut result: Vec<Uuid> =
        result_tan.iter().skip(offset).take(half_page).map(|s| *s).collect();
      result.extend(result_sim.into_iter().skip(offset).take(half_page));

      return (total, result);
    } else if page_number == full_page {
      if full_page == full_page_tan {
        // println!("min - tan");
        let offset = page_number * half_page;
        let take_tan = result_tan.len() - offset;
        let mut result: Vec<Uuid> =
          result_tan.iter().skip(offset).take(take_tan).map(|s| *s).collect();

        let take_sim = page_size - take_tan;
        result.extend(result_sim.into_iter().skip(offset).take(take_sim));

        return (total, result);
      } else {
        // println!("min - sim");
        // println!("page_number = {page_number} half_page = {half_page} full_page_tan = {full_page_tan} full_page_sim = {full_page_sim} {} {}", cat_0_and_1.len(), result_sim.len());
        //sim < tan
        let offset = page_number * half_page;
        let take = result_sim.len() - offset;
        let result_sim: Vec<Uuid> = result_sim.iter().skip(offset).take(take).map(|s| *s).collect();

        let take = page_size - take;
        let result_tan: Vec<Uuid> = result_tan.iter().skip(offset).take(take).map(|s| *s).collect();
        let result = [result_tan, result_sim].concat();

        return (total, result);
      }
    } else {
      if full_page == full_page_tan {
        let offset_a = full_page * half_page;
        let offset_b = page_size - (result_tan.len() - full_page * half_page);
        let offset_c = page_size * (page_number - full_page - 1);
        let offset_full = offset_a + offset_b + offset_c;

        // println!("offset_a = {offset_a}, offset_b = {offset_b}, offset_c = {offset_c}");

        let result: Vec<Uuid> =
          result_sim.iter().skip(offset_full).take(page_size).map(|s| *s).collect();

        return (total, result);
      } else {
        let offset_a = full_page * half_page;
        let offset_b = page_size - (result_sim.len() - full_page * half_page);
        let offset_c = page_size * (page_number - full_page - 1);
        let offset_full = offset_a + offset_b + offset_c;

        // println!("offset_a = {offset_a}, offset_b = {offset_b}, offset_c = {offset_c}");

        let result: Vec<Uuid> =
          result_tan.iter().skip(offset_full).take(page_size).map(|s| *s).collect();

        return (total, result);
      }
    }
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
    let id = data["_uuid"]
      .as_str()
      .map(|data| Uuid::parse_str(data).unwrap_or(UUID_NIL))
      .unwrap_or(UUID_NIL);
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
        search.create(id, after_name)?;
      } else {
        // IGNORE
      }
    }
  }
  Ok(())
}
