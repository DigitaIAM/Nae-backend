use crate::animo::memory::ID;
use crate::commutator::Application;
use crate::services::{Data, Error};
use crate::storage::{json, load, save};
use crate::store::{dt, receive_data, Batch, NumberForGoods, OpMutation};
use crate::utils::time::time_to_string;
use chrono::{DateTime, Utc};
use json::JsonValue;
use rust_decimal::Decimal;
use std::path::PathBuf;
use std::str::FromStr;

pub(crate) struct SMemories {
  pub(crate) oid: ID,
  pub(crate) ctx: Vec<String>,

  // example: warehouse/receive/
  pub(crate) folder: PathBuf,
}

fn save_data(
  app: &Application,
  folder: &PathBuf,
  ctx: &Vec<String>,
  id: &String,
  time: DateTime<Utc>,
  data: JsonValue,
) -> Result<JsonValue, Error> {
  // if data["_id"] != id {
  //   return Err(Error::IOError(format!("incorrect id {id} vs {}", data["_id"])));
  // }

  let time = time_to_string(time);

  let file_name = format!("{time}.json");
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

  let before = match load(&path_latest) {
    Ok(x) => x,
    Err(_) => JsonValue::Null,
  };

  let data = receive_data(app, time, data, ctx, before)?;

  println!("saving");
  save(&path_current, data.dump())?;

  println!("remove symlink_file ${path_latest:?}");
  symlink::remove_symlink_file(&path_latest);
  println!("create symlink_file ${file_name:?}");
  symlink::symlink_file(&file_name, &path_latest)?;
  println!("done");

  Ok(data)
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
    time: DateTime<Utc>,
    mut data: JsonValue,
  ) -> Result<JsonValue, Error> {
    let id = format!("{}/{}", self.ctx.join("/"), time_to_string(time));
    println!("id: {id}");

    // context/2023/01/2023-01-06T12:43:15Z/
    let mut folder = self.folder(&id);

    println!("creating folder {folder:?}");

    std::fs::create_dir_all(&folder).map_err(|e| {
      Error::IOError(format!("can't create folder {}: {}", folder.to_string_lossy(), e))
    })?;

    data["_id"] = id.clone().into();

    save_data(app, &folder, &self.ctx, &id, time, data)
  }

  pub(crate) fn update(
    &self,
    app: &Application,
    id: String,
    data: Data,
  ) -> Result<JsonValue, Error> {
    let time = Utc::now();
    save_data(app, &self.folder(&id), &self.ctx, &id, time, data)
  }

  pub(crate) fn get(&self, id: &String) -> SDoc {
    // remove prefix (context)
    let id = self.remove_prefix(id);

    let year = &id[..4];
    let month = &id[5..7];

    println!("get id {id} year {year} month {month}");

    let mut path = self.folder.clone();
    path.push(format!("{:0>4}/{:0>2}/{}/latest.json", year, month, id));

    SDoc { id: id.clone(), oid: self.oid.clone(), ctx: self.ctx.clone(), path }
  }

  pub(crate) fn list(&self) -> std::io::Result<Vec<SDoc>> {
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

    result.sort_by(|a, b| b.id.cmp(&a.id));

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
