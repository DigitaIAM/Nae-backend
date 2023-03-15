use crate::animo::memory::ID;
use crate::commutator::Application;
use crate::services::Data;
use service::error::Error;
use crate::storage::{json, load, save};
use store::elements::{dt, receive_data, Batch, NumberForGoods, OpMutation};
use service::utils::{json::JsonParams,
                     time::time_to_string};
use chrono::{DateTime, Utc};
use json::{JsonValue, object};
use rust_decimal::Decimal;
use std::path::PathBuf;
use std::str::FromStr;
use service::Services;
use std::sync::Mutex;
use uuid::Uuid;
use std::fs;

static LOCK: Mutex<Vec<u8>> = Mutex::new(vec![]);

pub(crate) struct SMemories {
  pub(crate) oid: ID,
  pub(crate) ctx: Vec<String>,

  pub(crate) top_folder: PathBuf,

  // example: warehouse/receive/
  pub(crate) folder: PathBuf,
}

fn save_data(
  app: &Application,
  top_folder: &PathBuf,
  folder: &PathBuf,
  ctx: &Vec<String>,
  id: &String,
  uuid: Option<Uuid>,
  time: DateTime<Utc>,
  data: JsonValue,
) -> Result<JsonValue, Error> {
  let lock = LOCK.lock().unwrap();

  // if data["_id"] != id {
  //   return Err(Error::IOError(format!("incorrect id {id} vs {}", data["_id"])));
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

  println!("loading before {path_latest:?}");

  let before = match load(&path_latest) {
    Ok(x) => x,
    Err(_) => JsonValue::Null,
  };

  println!("loaded before {before:?}");

  let data = receive_data(app, time, data, ctx, before).map_err(|e| Error::GeneralError(e.message()))?;

  println!("saving");
  save(&path_current, data.dump())?;

  symlink::remove_symlink_file(&path_latest);
  symlink::symlink_file(&file_name, &path_latest)?;

  if let Some(uuid) = uuid {
    let str = uuid.to_string();
    let mut path_folder = top_folder.clone();
    path_folder.push("uuid");
    path_folder.push(&str[0..4]);

    std::fs::create_dir_all(&path_folder).map_err(|e| {
      Error::IOError(format!("can't create folder {}: {}", path_folder.to_string_lossy(), e))
    })?;

    let mut path_uuid = path_folder.clone();
    path_uuid.push(str);

    if let Some(folder) = pathdiff::diff_paths(folder.canonicalize()?, path_folder.canonicalize()?) {
      if !path_uuid.exists() {
        symlink::symlink_dir(&folder, &path_uuid)?;
      }
    } else {
      todo!("raise error")
    }
  }

  Ok(data)
}

pub fn memories_find(app: &Application, filter: JsonValue, ctx: Vec<&str>) -> Result<Vec<JsonValue>, Error> {
  let oid = ID::from("Midas-Plastics");
  let result = app.service("memories").find(object!{oid: oid.to_base64(), ctx: ctx, filter: filter})?;

  Ok(result["data"].members().map(|o|o.clone()).collect())
}

pub fn memories_create(app: &Application, data: JsonValue, ctx: Vec<&str>) -> Result<JsonValue, Error> {
  let oid = ID::from("Midas-Plastics");

  let result = app.service("memories").create(data, object!{oid: oid.to_base64(), ctx: ctx })?;

  // println!("create_result: {result:?}");

  Ok(result)
}

impl SMemories {
  // remove context details
  fn remove_prefix(&self, id: &String) -> String {
    if let Some(pos) = &id.rfind('/') {
      id[(*pos + 1)..].to_string()
    } else {
      id.to_string()
    }
  }

  fn folder(&self, id: &String) -> PathBuf {
    println!("before: {id}");
    let id = self.remove_prefix(id);
    println!("after: {id}");

    let year = &id[0..4];
    let month = &id[5..7];

    println!("create id {id} year {year} month {month}");

    // 2023/01/2023-01-06T12:43:15Z/
    let mut folder = self.folder.clone();
    folder.push(year);
    folder.push(month);
    folder.push(&id);

    folder
  }

  pub(crate) fn create(
    &self,
    app: &Application,
    mut data: JsonValue,
  ) -> Result<JsonValue, Error> {
    let (id, time, folder) = {
      let lock = LOCK.lock().unwrap();

      let mut count = 0;
      let mut time = chrono::Utc::now();
      loop {
        count += 1;
        if count > 1_000_000 {
          return Err(Error::IOError(format!("fail to allocate free id: {}", time_to_string(time))));
        }
        let id = format!("{}/{}", self.ctx.join("/"), time_to_string(time));
        println!("id: {id}");

        // context/2023/01/2023-01-06T12:43:15Z/
        let mut folder = self.folder(&id);

        println!("creating folder {folder:?}");

        std::fs::create_dir_all(&folder).map_err(|e| {
          Error::IOError(format!("can't create folder {}: {}", folder.to_string_lossy(), e))
        })?;

        let time_str = time_to_string(time);

        let file_name = format!("{time_str}.json");
        let mut path_current = folder.clone();
        path_current.push(&file_name);

        if path_current.exists() {
          time = time + chrono::Duration::milliseconds(1);
          continue;
        }

        // create because of lock releasing
        save(&path_current, "".into())?;

        break (id, time, folder);
      }
    };

    let uuid = Uuid::new_v4();

    data["_id"] = id.clone().into();
    data["_uuid"] = uuid.to_string().into();

    save_data(app, &self.top_folder, &folder, &self.ctx, &id, Some(uuid), time, data)
  }

  pub(crate) fn update(
    &self,
    app: &Application,
    id: String,
    data: Data,
  ) -> Result<JsonValue, Error> {
    let time = Utc::now();
    save_data(app, &self.top_folder, &self.folder(&id), &self.ctx, &id, None, time, data)
  }

  // TODO move to ???
  pub(crate) fn get(&self, id: &String) -> Option<SDoc> {
    let id = if id.contains("/") {
      // remove prefix (context)
      self.remove_prefix(id)
    } else {
      let mut path = self.top_folder.clone();
      path.push("uuid");
      path.push(&id[0..4]);
      path.push(id);

      let path = match fs::read_link(path) {
        Ok(r) => r,
        Err(_) => return None,
      };

      let mut id = path.to_string_lossy().to_string();
      while &id.as_str()[0..3] == "../" {
        id = id[3..].to_string();
      }

      // remove prefix (context)
      self.remove_prefix(&id)
    };

    let year = &id[..4];
    let month = &id[5..7];

    let mut path = self.folder.clone();
    path.push(format!("{:0>4}/{:0>2}/{}/latest.json", year, month, id));

    Some(SDoc { id: id.clone(), oid: self.oid.clone(), ctx: self.ctx.clone(), path })
  }

  pub(crate) fn list(&self, reverse: Option<bool>) -> std::io::Result<Vec<SDoc>> {
    let mut result = Vec::new();

    // let mut folder = self.folder.clone();
    // folder.push(format!("{:0>4}/{:0>2}/", ts.year(), ts.month()));

    let years: Vec<PathBuf> = std::fs::read_dir(&self.folder)?
      .map(|res| res.map(|e| e.path()))
      .collect::<Result<Vec<PathBuf>, std::io::Error>>()?
      .into_iter()
      .filter(|y| y.is_dir())
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
          result.push(SDoc { id: id.to_string(), oid: self.oid, ctx: self.ctx.clone(), path });
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

pub(crate) struct SDoc {
  id: String,

  oid: ID,
  ctx: Vec<String>,

  path: PathBuf,
}

impl SDoc {
  pub(crate) fn json(&self) -> Result<JsonValue, Error> {
    load(&self.path)
  }
}
