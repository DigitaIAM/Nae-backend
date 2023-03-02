use actix_web::error::ParseError::Status;
use dbase::FieldConversionError;
use json::object::Object;
use json::JsonValue;
use std::collections::{BTreeMap, HashMap};
use std::convert::Infallible;
use std::io::Write;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, RwLock};
use std::time::SystemTime;
use tantivy::HasLen;
use uuid::Uuid;

use crate::animo::error::DBError;
use crate::services::JsonData;
use crate::services::{Data, Params};
use service::{Service, Services};
use service::error::Error;
use crate::warehouse::turnover::Organization;
use crate::ws::error_general;
use crate::{
  auth, commutator::Application, storage::SOrganizations, animo::memory::{Memory, Transformation, TransformationKey, Value, ID},
};
pub(crate) struct Companies {
  app: Application,
  name: String,

  orgs: SOrganizations,
}

impl Companies {
  pub(crate) fn new(app: Application, orgs: SOrganizations) -> Arc<dyn Service> {
    Arc::new(Companies { app, name: "companies".to_string(), orgs })
  }
}

impl Service for Companies {
  fn path(&self) -> &str {
    &self.name
  }

  fn find(&self, params: Params) -> crate::services::Result {
    let limit = self.limit(&params);
    let skip = self.skip(&params);

    let list = self.orgs.list()?;
    let total = list.len();

    let list = list.into_iter().skip(skip).take(total).map(|o| o.json()).collect();

    Ok(json::object! {
      data: JsonValue::Array(list),
      total: total,
      "$skip": skip,
    })
  }

  fn get(&self, id: String, params: Params) -> crate::services::Result {
    let id = crate::services::string_to_id(id)?;
    self.orgs.get(&id).load()
  }

  fn create(&self, data: Data, params: Params) -> crate::services::Result {
    if !data.is_object() {
      Err(Error::GeneralError("only object allowed".into()))
    } else {
      let id = ID::random();

      let mut obj = data.clone();
      obj["_id"] = JsonValue::String(id.to_base64());

      self.orgs.create(id)?.save(obj.dump())?;

      Ok(obj)
    }
  }

  fn update(&self, id: String, data: Data, params: Params) -> crate::services::Result {
    if !data.is_object() {
      Err(Error::GeneralError("only object allowed".into()))
    } else {
      let id = crate::services::string_to_id(id)?;

      let mut obj = data.clone();
      obj["_id"] = id.to_base64().into();

      self.orgs.get(&id).save(obj.dump())?;

      Ok(obj)
    }
  }

  fn patch(&self, id: String, data: Data, params: Params) -> crate::services::Result {
    if !data.is_object() {
      Err(Error::GeneralError("only object allowed".into()))
    } else {
      let id = crate::services::string_to_id(id)?;

      let storage = self.orgs.get(&id);

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
    let id = ID::from_base64(id.as_bytes()).map_err(|e| Error::GeneralError(e.to_string()))?;

    self.orgs.get(&id).delete()
  }
}
