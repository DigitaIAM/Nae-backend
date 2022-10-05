use std::collections::{BTreeMap, HashMap};
use std::convert::Infallible;
use std::fmt::{Display, Formatter};
use std::io::Write;
use std::path::Component;
use std::str::FromStr;
use std::sync::{Arc, RwLock};
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::SystemTime;
use walkdir::WalkDir;
use actix_web::error::ParseError::Status;
use chrono::{Datelike, DateTime, ParseResult, SecondsFormat, Utc};
use dbase::FieldConversionError;
use json::JsonValue;
use json::object::Object;
use tantivy::HasLen;
use uuid::Uuid;

use crate::{Application, auth, Memory, Services, Transformation, TransformationKey, Value};
use crate::animo::error::DBError;
use crate::services::{Data, Error, Params, Service};
use crate::ws::error_general;

#[derive(Debug, Clone)]
struct ID {
  time: DateTime<Utc>,
  camera: crate::ID,
}

impl ID {
  fn time_to_string(&self) -> String {
    self.time.to_rfc3339_opts(SecondsFormat::Millis, true)
  }
}

impl TryFrom<String> for ID {
  type Error = Error;

  fn try_from(value: String) -> Result<Self, Self::Error> {
    let mut parts = value
      .split("_")
      .into_iter()
      .collect::<Vec<&str>>();

    let (camera, time) = if parts.len() == 2 {
      (parts.remove(0), parts.remove(0))
    } else {
      return Err(Error::GeneralError(format!("wrong id {:?}", value)));
    };

    let camera = crate::ID::from_base64(camera.as_bytes())
      .map_err(|e| Error::GeneralError(e.to_string()))?;

    let time = DateTime::parse_from_rfc3339(time)
      .map(|ts| ts.into())
      .map_err(|e| Error::GeneralError(e.to_string()))?;

    Ok(ID { time, camera })
  }
}

impl Display for ID {
  fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
    f.write_fmt(format_args!("{}_{}", self.camera.to_base64(), self.time_to_string()))
  }
}

impl Ord for ID {
  fn cmp(&self, other: &Self) -> std::cmp::Ordering {
    let cmp = self.camera.cmp(&other.camera);
    if cmp.is_eq() {
      self.time.cmp(&other.time).reverse()
    } else {
      cmp
    }
  }
}

impl PartialOrd for ID {
  fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
    Some(self.cmp(other))
  }
}

impl PartialEq for ID {
  fn eq(&self, other: &Self) -> bool {
    (self.time, &self.camera) == (other.time, &other.camera)
  }
}

impl Eq for ID { }

pub struct Events {
  app: Application,
  path: Arc<String>,
  folder: String,

  objs: Arc<RwLock<BTreeMap<ID, String>>>,
}

impl Events {
  pub(crate) fn new(app: Application, path: &str, folder: &str) -> Arc<dyn Service> {

    // make sure folder exist
    std::fs::create_dir_all(folder).unwrap();

    let mut data: BTreeMap<ID, String> = BTreeMap::new();
    // load data
    for entry in WalkDir::new(folder)
      .follow_links(true)
      .into_iter()
      .filter_map(|e| e.ok()) {

      let f_name = entry.file_name().to_string_lossy();
      if f_name.ends_with(".json") {
        if let Some(name) = f_name.strip_suffix(".json") {
          let time = string_to_time(name).unwrap();

          let mut comps = entry.path().components();
          comps.next_back();
          comps.next_back();
          comps.next_back();
          let folder_name = comps.as_path().file_name().unwrap().to_string_lossy();

          let camera = crate::ID::from_base64(folder_name.as_bytes()).unwrap();

          let path = entry.path().to_string_lossy().into();
          data.insert(ID { time, camera }, path);
        }
      }
    }

    Arc::new(Events {
      app,
      path: Arc::new(path.to_string()),
      folder: folder.to_string(),
      objs: Arc::new(RwLock::new(data))
    })
  }

  fn save(&self, id: &ID, obj: &JsonValue) -> Result<(), Error> {
    let year = id.time.year();
    let month = id.time.month();
    // let folder = format!("{}/{:0>4}/{:0>2}", self.folder, year, month);
    let folder = format!("{}/cameras/{}/{:0>4}/{:0>2}", self.folder, id.camera.to_base64(), year, month);

    // make sure folder exist
    std::fs::create_dir_all(&folder).map_err(|e| Error::IOError(e.to_string()))?;

    let path = format!("{folder}/{}.json", id.time_to_string());

    let mut file = std::fs::OpenOptions::new()
      .create(true)
      .write(true)
      .open(path.clone())
      .map_err(|e| Error::IOError(format!("fail to write file: {}", e)))?;

    let data = obj.dump();
      // .map_err(|e| Error::IOError(format!("fail to generate json")))?;

    file.write_all(data.as_bytes())
      .map_err(|e| Error::IOError(format!("fail to write file: {}", e)))?;

    // make sure folder exist
    std::fs::create_dir_all(&folder).map_err(|e| Error::IOError(e.to_string()))?;

    let mut objs = self.objs.write().unwrap();
    objs.insert(id.clone(), path.clone());
    Ok(())
  }

  fn load(path: String) -> crate::services::Result {
    // load from disk
    let contents = std::fs::read_to_string(path).unwrap();
    json::parse(contents.as_str())
      .map_err(|e| Error::IOError(e.to_string()))
  }
}

fn now_in_seconds() -> u128 {
  SystemTime::now()
    .duration_since(std::time::UNIX_EPOCH)
    .expect("system time is likely incorrect")
    .as_millis()
}

fn string_to_time<S: AsRef<str>>(data: S) -> Result<DateTime<Utc>,Error> {
  DateTime::parse_from_rfc3339(data.as_ref())
    .map(|ts| ts.into())
    .map_err(|e| Error::GeneralError(e.to_string()))
}

impl Service for Events {
  fn path(&self) -> &str {
    &self.path
  }

  fn find(&self, params: Params) -> crate::services::Result {
    let limit = self.limit(&params);
    let skip = self.skip(&params);

    let objs = self.objs.read().unwrap();
    let total = objs.len();

    let list = objs.iter()
      .skip(skip)
      .map(|(_,path)| {
        // load from disk
        let contents = std::fs::read_to_string(path).unwrap_or("".into());
        json::parse(contents.as_str()).unwrap_or(JsonValue::Null)
      })
      .filter(|o| o.is_object() && !o.is_empty())
      .take(limit)
      .collect();

    Ok(
      json::object! {
        data: JsonValue::Array(list),
        total: total,
        "$skip": skip,
      }
    )
  }

  fn get(&self, id: String, params: Params) -> crate::services::Result {
    let id = ID::try_from(id)?;

    let objs = self.objs.read().unwrap();
    match objs.get(&id) {
      None => Err(Error::NotFound(id.to_string())),
      Some(path) => Events::load(path.clone())
    }
  }

  fn create(&self, data: Data, params: Params) -> crate::services::Result {
    let array = vec![data.clone()]; // TODO rewrite
    let (single, total, it) = if data.is_array() {
      (true, data.len(), data.members())
    } else if data.is_object() {
      (false, 1, array.iter())
    } else {
      todo!("return error")
    };

    let mut result = Vec::with_capacity(total);
    for obj in it {
      let event = &obj["event"];
      if event["major"].as_usize().unwrap_or(0) != 5 {
        continue;
      }
      if event["minor"].as_usize().unwrap_or(0) != 75 {
        continue;
      }

      let time = event["time"].as_str().unwrap_or("").trim().to_string();
      let time = match string_to_time(time) {
        Ok(dt) => dt,
        Err(e) => {
          result.push(error_general(e.to_string()));
          continue;
        },
      };

      let camera = obj["cameraId"].as_str().unwrap_or("").trim().to_string();
      let camera = match crate::ID::from_base64(camera.as_bytes()) {
        Ok(id) => id,
        Err(e) => {
          result.push(error_general(e.to_string()));
          continue;
        },
      };

      let id = ID { time, camera };

      // TODO check that it unique

      let mut obj = obj.clone();

      obj["_id"] = JsonValue::String(id.to_string());

      match self.save(&id, &obj) {
        Ok(_) => result.push(obj),
        Err(e) => {
          result.push(error_general(format!("can't save json {}", id)));
        }
      }
    }

    Ok(
      if single {
        if result.is_empty() {
          JsonValue::Null
        } else {
          result.remove(0)
        }
      } else {
        JsonValue::Array(result)
      }
    )
  }

  fn update(&self, id: String, data: Data, params: Params) -> crate::services::Result{
    Err(Error::NotImplemented)
  }

  fn patch(&self, id: String, data: Data, params: Params) -> crate::services::Result{
    Err(Error::NotImplemented)
  }

  fn remove(&self, id: String, params: Params) -> crate::services::Result{
    Err(Error::NotImplemented)
  }
}