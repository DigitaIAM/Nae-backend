use crate::services::{string_to_id, Data, Params};
use crate::{animo::memory::ID, commutator::Application, storage::Workspaces};
use json::JsonValue;
use service::error::Error;
use service::Service;
use std::sync::Arc;

lazy_static::lazy_static! {
    pub(crate) static ref PEOPLE: ID = ID::for_constant("people");
}

// const PROPERTIES: [&str; 4] = ["organization", "first_name", "last_name", "email"];

pub struct People {
  app: Application,
  path: Arc<String>,

  ws: Workspaces,
}

impl People {
  pub fn new(app: Application, ws: Workspaces) -> Arc<dyn Service> {
    Arc::new(People { app, path: Arc::new("people".to_string()), ws })
  }
}

impl Service for People {
  fn path(&self) -> &str {
    &self.path
  }

  fn find(&self, params: Params) -> crate::services::Result {
    let oid = crate::services::oid(&params)?;

    let limit = self.limit(&params);
    let skip = self.skip(&params);

    let list = self.ws.get(&oid).people();

    let (total, list) = if let Some(search) = params[0]["$search"].as_str() {
      let search = search.to_lowercase();
      let list: Vec<JsonValue> = list
        .into_iter()
        .map(|o| o.json())
        .filter(|d| d["name"].as_str().unwrap_or_default().to_lowercase().contains(&search))
        .collect();
      (list.len(), list.into_iter().skip(skip).take(limit).collect())
    } else {
      (list.len(), list.into_iter().skip(skip).take(limit).map(|o| o.json()).collect())
    };

    Ok(json::object! {
      data: JsonValue::Array(list),
      total: total,
      "$skip": skip,
    })
  }

  fn get(&self, id: String, params: Params) -> crate::services::Result {
    let oid = crate::services::oid(&params)?;

    let id = crate::services::string_to_id(id)?;
    self.ws.get(&oid).person(&id).load()
  }

  fn create(&self, data: Data, _params: Params) -> crate::services::Result {
    let oid = crate::services::oid(&data)?;
    if !data.is_object() {
      Err(Error::GeneralError("only object allowed".into()))
    } else {
      let id = ID::random();

      let mut obj = data.clone();
      obj["_id"] = JsonValue::String(id.to_base64());

      self.ws.get(&oid).person(&id).save(obj.dump())?;

      Ok(obj)
    }
  }

  fn update(&self, id: String, data: Data, _params: Params) -> crate::services::Result {
    let oid = crate::services::oid(&data)?;
    if !data.is_object() {
      Err(Error::GeneralError("only object allowed".into()))
    } else {
      let id = crate::services::string_to_id(id)?;

      let mut obj = data.clone();
      obj["_id"] = id.to_base64().into();

      self.ws.get(&oid).person(&id).save(obj.dump())?;

      Ok(obj)
    }
  }

  fn patch(&self, id: String, data: Data, params: Params) -> crate::services::Result {
    let oid = crate::services::oid(&params)?;
    if !data.is_object() {
      Err(Error::GeneralError("only object allowed".into()))
    } else {
      let id = crate::services::string_to_id(id)?;

      let storage = self.ws.get(&oid).person(&id);

      let mut obj = storage.load()?;
      for (n, v) in data.entries() {
        if n != "_id" {
          obj[n] = v.clone();
        }
      }

      storage.save(obj.dump())?;

      Ok(obj)
    }
  }

  fn remove(&self, id: String, params: Params) -> crate::services::Result {
    let oid = crate::services::oid(&params)?;
    let id = string_to_id(id)?;

    self.ws.get(&oid).shift(id).delete()
  }
}
