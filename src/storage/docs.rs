use crate::animo::memory::ID;
use crate::services::{Data, Error};
use crate::storage::{json, load, save};
use crate::utils::time::time_to_string;
use chrono::{DateTime, Utc};
use json::JsonValue;
use std::path::PathBuf;

pub(crate) struct SDocs {
  pub(crate) oid: ID,
  pub(crate) ctx: Vec<String>,

  // example: warehouse/receive/
  pub(crate) folder: PathBuf,
}

impl SDocs {
  fn save(
    &self,
    id: String,
    create: bool,
    time: DateTime<Utc>,
    mut data: JsonValue,
  ) -> Result<JsonValue, Error> {
    let year = &id[0..4];
    let month = &id[6..8];

    println!("create id {id} year {year} month {month}");

    if data["_id"] != id {
      return Err(Error::IOError(format!(")")));
    }

    // 2023/01/2023-01-06T12:43:15Z/
    let mut folder = self.folder.clone();
    folder.push(year);
    folder.push(month);
    folder.push(&id);

    if create {
      std::fs::create_dir_all(&folder).map_err(|e| {
        Error::IOError(format!("can't create folder {}: {}", folder.to_string_lossy(), e))
      })?;
    }

    let mut path_current = folder.clone();
    path_current.push(format!("{id}.json"));

    // ["warehouse", "receive"]
    // ["warehouse", "issue"]
    // ["warehouse", "transfer"]
    // TODO handles[self.ctx].apply()
    // data = { _id: "", date: "2023-01-11", storage: "uuid", goods: [{goods: "", uom: "", qty: 0, price: 0, cost: 0, _tid: ""}]}
    // cost = qty * price

    save(&path_current, data.dump())?;

    // 2023/01/2023-01-06T12:43:15Z/latest.json
    let mut path_latest = folder.clone();
    path_latest.push("latest.json");

    symlink::symlink_file(path_current, path_latest)?;

    Ok(data)
  }

  pub(crate) fn create(&self, id: DateTime<Utc>, mut data: JsonValue) -> Result<JsonValue, Error> {
    let id = time_to_string(id);

    data["_id"] = id.clone().into();

    self.save(id, true, Utc::now(), data)
  }

  pub(crate) fn get(&self, id: &String) -> SDoc {
    let year = &id[..4];
    let month = &id[6..8];

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
  pub(crate) fn update(&self, data: Data) -> Result<JsonValue, Error> {
    todo!()
  }

  pub(crate) fn json(&self) -> Result<JsonValue, Error> {
    load(&self.path)
  }
}
