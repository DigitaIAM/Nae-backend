use std::collections::{BTreeMap, HashMap};
use std::convert::Infallible;
use std::io::Write;
use std::sync::{Arc, RwLock};
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::SystemTime;
use actix_web::error::ParseError::Status;
use dbase::FieldConversionError;
use json::JsonValue;
use json::object::Object;
use tantivy::HasLen;
use uuid::Uuid;

use crate::{Application, auth, ID, Memory, Services, Transformation, TransformationKey, Value};
use crate::animo::error::DBError;
use crate::services::{Data, Error, Params, Service};
use crate::ws::error_general;

pub struct InFiles {
  app: Application,
  path: Arc<String>,
  folder: String,

  objs: Arc<RwLock<BTreeMap<ID, JsonValue>>>,
}

impl InFiles {
  pub(crate) fn new(app: Application, path: &str, folder: &str) -> Arc<dyn Service> {

    // make sure folder exist
    std::fs::create_dir_all(folder).unwrap();

    let mut data = BTreeMap::new();
    // load data
    for entry in std::fs::read_dir(folder).unwrap() {
      let entry = entry.unwrap();
      let path = entry.path();
      if path.is_file() && path.ends_with(".json") {
        let contents = std::fs::read_to_string(path).unwrap();

        let obj: JsonValue = json::parse(contents.as_str()).unwrap();

        let id = obj["_id"].as_str().unwrap();
        let id = ID::from_base64(id.as_bytes()).unwrap();

        data.entry(id).or_insert(obj);
      }
    }

    Arc::new(InFiles {
      app,
      path: Arc::new(path.to_string()),
      folder: folder.to_string(),
      objs: Arc::new(RwLock::new(data))
    })
  }

  fn save(&self, id: &ID, obj: &JsonValue) -> Result<(), Error> {
    let path = format!("{}/{}.json", self.folder, id.to_base64());

    let mut file = std::fs::OpenOptions::new()
      .create(true)
      .write(true)
      .open(path.clone())
      .map_err(|e| Error::IOError(format!("fail to write file: {}", e)))?;

    let data = obj.dump();
      // .map_err(|e| Error::IOError(format!("fail to generate json")))?;

    file.write_all(data.as_bytes())
      .map_err(|e| Error::IOError(format!("fail to write file: {}", e)))?;

    let mut objs = self.objs.write().unwrap();
    objs.insert(id.clone(), obj.clone());
    Ok(())
  }
}

fn now_in_seconds() -> u128 {
  SystemTime::now()
    .duration_since(std::time::UNIX_EPOCH)
    .expect("system time is likely incorrect")
    .as_millis()
}

impl Service for InFiles {
  fn path(&self) -> &str {
    &self.path
  }

  fn find(&self, params: Params) -> crate::services::Result {
    let limit = self.limit(&params);
    let skip = self.skip(&params);

    let objs = self.objs.read().unwrap();
    let total = objs.len();

    let mut list = Vec::with_capacity(limit);
    for (_, obj) in objs.iter().skip(skip).take(limit) {
      list.push(obj.clone());
    }

    Ok(
      json::object! {
        data: JsonValue::Array(list),
        total: total,
        "$skip": skip,
      }
    )
  }

  fn get(&self, id: String, params: Params) -> crate::services::Result {
    let id = crate::services::string_to_id(id)?;
    let objs = self.objs.read().unwrap();
    Ok(
      match objs.get(&id) {
        None => JsonValue::Null,
        Some(obj) => obj.clone()
      }
    )
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
      let id = ID::random();
      let mut obj = obj.clone();

      obj["_id"] = JsonValue::String(id.to_base64());

      match self.save(&id, &obj) {
        Ok(_) => result.push(obj),
        Err(e) => result.push(error_general("can't save json"))
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