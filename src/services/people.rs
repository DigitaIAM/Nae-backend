use crate::animo::error::DBError;
use crate::services::{string_to_id, Data, Params};
use service::{Service, Services};
use service::error::Error;
use crate::ws::error_general;
use crate::{
  auth, commutator::Application, animo::memory::ChangeTransformation, animo::memory::Memory, storage::SOrganizations, animo::memory::Transformation,
  animo::memory::TransformationKey, animo::memory::Value, animo::memory::ID,
};
use json::object::Object;
use json::JsonValue;
use std::sync::{Arc, RwLock};

lazy_static::lazy_static! {
    pub(crate) static ref PEOPLE: ID = ID::for_constant("people");
}

const PROPERTIES: [&str; 4] = ["organization", "first_name", "last_name", "email"];

pub(crate) struct People {
  app: Application,
  path: Arc<String>,

  orgs: SOrganizations,
}

impl People {
  pub(crate) fn new(app: Application, orgs: SOrganizations) -> Arc<dyn Service> {
    Arc::new(People { app, path: Arc::new("people".to_string()), orgs })
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

    let list = self.orgs.get(&oid).people();

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
    self.orgs.get(&oid).person(&id).load()
  }

  fn create(&self, data: Data, params: Params) -> crate::services::Result {
    let oid = crate::services::oid(&data)?;
    if !data.is_object() {
      Err(Error::GeneralError("only object allowed".into()))
    } else {
      let id = ID::random();

      let mut obj = data.clone();
      obj["_id"] = JsonValue::String(id.to_base64());

      self.orgs.get(&oid).person(&id).save(obj.dump())?;

      Ok(obj)
    }
  }

  fn update(&self, id: String, data: Data, params: Params) -> crate::services::Result {
    let oid = crate::services::oid(&data)?;
    if !data.is_object() {
      Err(Error::GeneralError("only object allowed".into()))
    } else {
      let id = crate::services::string_to_id(id)?;

      let mut obj = data.clone();
      obj["_id"] = id.to_base64().into();

      self.orgs.get(&oid).person(&id).save(obj.dump())?;

      Ok(obj)
    }
  }

  fn patch(&self, id: String, data: Data, params: Params) -> crate::services::Result {
    let oid = crate::services::oid(&params)?;
    if !data.is_object() {
      Err(Error::GeneralError("only object allowed".into()))
    } else {
      let id = crate::services::string_to_id(id)?;

      let storage = self.orgs.get(&oid).person(&id);

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

    self.orgs.get(&oid).shift(id).delete()
  }
}
