use crate::commutator::Application;
use crate::services::Data;
use crate::storage::organizations::Workspace;
use crate::storage::{load, save};
use chrono::{DateTime, Utc};
use json::JsonValue;

use service::error::Error;
use service::utils::time::time_to_string;

use std::path::PathBuf;

use crate::links::GetLinks;
use crate::memories::{Enrich, Resolve};
use crate::utils::substring::StringUtils;
use service::utils::json::JsonParams;
use std::collections::HashMap;
use std::sync::Mutex;
use store::elements::receive_data;
use uuid::Uuid;
use values::c;

static LOCK: Mutex<Vec<u8>> = Mutex::new(vec![]);

#[derive(Clone)]
pub struct Memories {
  pub ws: Workspace,

  // ./memories
  pub(crate) top_folder: PathBuf,

  // example: ['warehouse','receive']
  pub ctx: Vec<String>,

  // example: warehouse/receive/
  pub folder: PathBuf,
}

fn save_data(
  app: &Application,
  ws: &Workspace,
  top_folder: &PathBuf,
  folder: &PathBuf,
  ctx: &Vec<String>,
  _id: &String,
  _uuid: Option<Uuid>,
  time: DateTime<Utc>,
  mut data: JsonValue,
) -> Result<JsonValue, Error> {
  let mut stack: HashMap<String, (JsonValue, JsonValue)> = HashMap::new();

  let (before, after) = {
    let _lock = LOCK.lock().unwrap();

    // if data[_ID] != id {
    //   return Err(Error::IOError(format!("incorrect id {id} vs {}", data[_ID])));
    // }

    let time_str = time_to_string(time);

    let file_name = format!("{time_str}.json");
    let mut path_current = folder.clone();
    path_current.push(&file_name);

    // 2023/01/2023-01-06T12:43:15Z/latest.json
    let mut path_latest = folder.clone();
    path_latest.push("latest.json");

    // ["warehouse", "receive"]
    // ["warehouse", "issue"]
    // ["warehouse", "transfer"]
    // TODO handles[self.ctx].apply()
    // data = { _id: "", date: "2023-01-11", storage: "uuid", goods: [{goods: "", uom: "", qty: 0, price: 0, cost: 0, _tid: ""}, ...]}
    // cost = qty * price

    // println!("loading before {path_latest:?}");

    let before = match load(&path_latest) {
      Ok(b) => {
        //WORKAROUND: make sure that id & uuid stay same
        if !b[c::ID].is_null() {
          data[c::ID] = b[c::ID].clone();
        }
        if !b[c::UUID].is_null() {
          data[c::UUID] = b[c::UUID].clone();
        }
        b
      },
      Err(_) => JsonValue::Null,
    };

    // println!("loaded before {before:?}");

    let _ = crate::text_search::handle_mutation(app, ctx, &before, &data);
    // TODO .map_err(|e| IOError(e.to_string()))?;

    receive_data(app, ws.id.to_string().as_str(), before.clone(), data.clone(), ctx, &stack)
      .map_err(|e| Error::GeneralError(e.message()))?;

    app.links().save_links(ws, ctx, &data, &before)?;

    let uuid = data[c::UUID].as_str();

    save(&path_current, data.dump())?;

    // ignore error if file do not exist
    let _ = symlink::remove_symlink_file(&path_latest);
    symlink::symlink_file(&file_name, &path_latest)?;

    if let Some(uuid) = uuid {
      index_uuid(top_folder, folder, uuid)?;
    }

    (before, data.clone())
  };

  log::debug!("_Before {before:?}\n_After {after:?}");

  stack.insert(before[c::ID].string(), (before, after.clone()));

  let sources = app.links().get_source_links_without_ctx(data[c::UUID].uuid()?)?;

  log::debug!("_sources {sources:?}");

  let _ops: Vec<JsonValue> = sources
    .iter()
    .map(|uuid| uuid.resolve_to_json_object(&ws))
    .filter(|o| o.is_object())
    .filter(|o| o[c::STATUS].string().as_str() != c::DELETED)
    .map(|o| {
      let mut _ctx: Vec<String> = o[c::ID].string().split('/').map(|s| s.to_string()).collect();
      _ctx.pop();
      let _ = receive_data(app, ws.id.to_string().as_str(), o.clone(), o.clone(), &_ctx, &stack) // TODO
        .map_err(|e| Error::GeneralError(e.message()));
      o
    })
    .collect();

  Ok(after)
}

pub(crate) fn index_uuid(top_folder: &PathBuf, folder: &PathBuf, uuid: &str) -> Result<(), Error> {
  // let str = uuid.to_string();
  let mut path_folder = top_folder.clone();
  path_folder.push("uuid");
  path_folder.push(uuid.slice(0..4));

  std::fs::create_dir_all(&path_folder).map_err(|e| {
    Error::IOError(format!("can't create folder {}: {}", path_folder.to_string_lossy(), e))
  })?;

  let mut path_uuid = path_folder.clone();
  path_uuid.push(uuid);

  if let Some(folder) = pathdiff::diff_paths(folder.canonicalize()?, path_folder.canonicalize()?) {
    if !path_uuid.exists() {
      symlink::symlink_dir(folder, &path_uuid)?;
    }
  } else {
    todo!("raise error")
  }

  Ok(())
}

// remove context details
fn remove_prefix(id: &str) -> &str {
  if let Some(pos) = &id.rfind('/') {
    id.slice((*pos + 1)..)
  } else {
    id
  }
}

pub(crate) fn build_folder_path(id: &String, folder: &PathBuf) -> Option<PathBuf> {
  if id.is_empty() {
    return None;
  }
  let id = remove_prefix(id);

  if id.len() < 8 {
    return None;
  }

  let year = id.slice(0..4);
  let month = id.slice(5..7);

  // 2023/01/2023-01-06T12:43:15Z/
  let mut folder = folder.clone();
  folder.push(year);
  folder.push(month);
  folder.push(id);

  Some(folder)
}

impl Memories {
  pub(crate) fn create(&self, app: &Application, mut data: JsonValue) -> Result<JsonValue, Error> {
    let (id, time, folder) = {
      let _lock = LOCK.lock().unwrap();

      let mut count = 0;
      let mut time = Utc::now();
      loop {
        count += 1;
        if count > 1_000_000 {
          return Err(Error::IOError(format!("fail to allocate free id: {}", time_to_string(time))));
        }
        let id = format!("{}/{}", self.ctx.join("/"), time_to_string(time));
        // println!("id: {id}");

        // context/2023/01/2023-01-06T12:43:15Z/
        let folder = match build_folder_path(&id, &self.folder) {
          Some(f) => f,
          None => return Err(Error::IOError(format!("fail on folder path for id: {}", id))),
        };

        // println!("creating folder {folder:?}");

        std::fs::create_dir_all(&folder).map_err(|e| {
          Error::IOError(format!("can't create folder {}: {}", folder.to_string_lossy(), e))
        })?;

        let time_str = time_to_string(time);

        let file_name = format!("{time_str}.json");
        let mut path_current = folder.clone();
        path_current.push(&file_name);

        if path_current.exists() {
          time += chrono::Duration::milliseconds(1);
          continue;
        }

        // create because of lock releasing
        save(&path_current, "".into())?;

        break (id, time, folder);
      }
    };

    let uuid = Uuid::new_v4();

    data[c::ID] = id.clone().into();
    data[c::UUID] = uuid.to_string().into();

    let data =
      save_data(app, &self.ws, &self.top_folder, &folder, &self.ctx, &id, Some(uuid), time, data)?;

    Ok(data.enrich(&self.ws))
  }

  pub(crate) fn update(
    &self,
    app: &Application,
    id: String,
    data: Data,
  ) -> Result<JsonValue, Error> {
    let time = Utc::now();

    let folder = match build_folder_path(&id, &self.folder) {
      Some(f) => f,
      None => return Err(Error::IOError(format!("fail on folder path for id: {}", id))),
    };

    let data =
      save_data(app, &self.ws, &self.top_folder, &folder, &self.ctx, &id, None, time, data)?;

    Ok(data.enrich(&self.ws))
  }

  // TODO move to ???
  pub(crate) fn get(&self, id: &String) -> Option<Document> {
    if id.contains('/') {
      self.ws.resolve_id(id)
    } else {
      match Uuid::parse_str(id) {
        Ok(id) => self.ws.resolve_uuid(&id),
        Err(_) => None,
      }
    }
  }

  pub(crate) fn list(&self, reverse: Option<bool>) -> std::io::Result<Vec<Document>> {
    let mut result = Vec::new();

    // let mut folder = self.folder.clone();
    // folder.push(format!("{:0>4}/{:0>2}/", ts.year(), ts.month()));

    let years: Vec<PathBuf> = std::fs::read_dir(&self.folder)?
      .map(|res| res.map(|e| e.path()))
      .collect::<Result<Vec<PathBuf>, std::io::Error>>()?
      .into_iter()
      .filter(|y| y.is_dir())
      .filter(|y| {
        y.file_name()
          .map(|name| name.to_string_lossy().to_string().parse::<u32>().is_ok())
          .unwrap_or(false)
      })
      .collect();

    for year in years {
      let months: Vec<PathBuf> = std::fs::read_dir(&year)?
        .map(|res| res.map(|e| e.path()))
        .collect::<Result<Vec<PathBuf>, std::io::Error>>()?
        .into_iter()
        .filter(|y| y.is_dir())
        .collect();

      for month in months {
        let records: Vec<PathBuf> = std::fs::read_dir(&month)?
          .map(|res| res.map(|e| e.path()))
          .collect::<Result<Vec<PathBuf>, std::io::Error>>()?
          .into_iter()
          .filter(|y| y.is_dir())
          .collect();

        for record in records {
          let mut path = record.clone();
          path.push("latest.json");

          let id = record.file_name().unwrap_or_default().to_string_lossy().to_string();
          result.push(Document { mem: self.clone(), id: id.to_string(), path });
        }
      }
    }

    if let Some(reverse) = reverse {
      if reverse {
        result.sort_by(|a, b| a.id.cmp(&b.id));
      } else {
        result.sort_by(|a, b| b.id.cmp(&a.id));
      }
    }

    Ok(result)
  }
}

pub struct Document {
  pub mem: Memories,
  pub id: String,

  pub(crate) path: PathBuf,
}

impl Document {
  pub fn json(&self) -> Result<JsonValue, Error> {
    load(&self.path)
  }
}

#[cfg(test)]
mod tests {
  use super::*;
  use tempfile::tempdir;

  #[test]
  fn test_simple() {
    let tmp_dir = tempdir().unwrap();
    let folder = tmp_dir.path().into();

    let path = build_folder_path(&"context/2023-01-06T12:43:15Z".to_string(), &folder);

    assert!(path.is_some());
    assert!(path.unwrap().to_string_lossy().ends_with("/2023/01/2023-01-06T12:43:15Z"));
  }

  #[test]
  fn test_utf16() {
    let tmp_dir = tempdir().unwrap();
    let folder = tmp_dir.path().into();

    let path = build_folder_path(&"ГЛЮКОМЕТР GLUCODR AUTO A`".to_string(), &folder);
    println!("{path:?}")
  }

  // #[test]
  // fn test_() {
  //   let tmp_dir = tempdir().unwrap();
  //
  //   let wss = Workspaces::new(tmp_dir.into());
  //   let ws = wss.get(&ID::random());
  //   let memories = ws.memories(vec!["ctx".into()]);
  // }
}
