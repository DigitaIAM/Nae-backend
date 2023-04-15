use json::JsonValue;

use std::sync::Arc;

use crate::services::JsonData;
use crate::services::{Data, Params};

use crate::{animo::memory::ID, commutator::Application, storage::Workspaces};
use service::error::Error;
use service::Service;
pub struct Companies {
  app: Application,
  name: String,

  ws: Workspaces,
}

impl Companies {
  pub(crate) fn new(app: Application, ws: Workspaces) -> Arc<dyn Service> {
    Arc::new(Companies { app, name: "companies".to_string(), ws })
  }
}

impl Service for Companies {
  fn path(&self) -> &str {
    &self.name
  }

  fn find(&self, params: Params) -> crate::services::Result {
    let _limit = self.limit(&params);
    let skip = self.skip(&params);

    let list = self.ws.list()?;
    let total = list.len();

    let list = list.into_iter().skip(skip).take(total).map(|o| o.json()).collect();

    Ok(json::object! {
      data: JsonValue::Array(list),
      total: total,
      "$skip": skip,
    })
  }

  fn get(&self, id: String, _params: Params) -> crate::services::Result {
    let id = crate::services::string_to_id(id)?;
    self.ws.get(&id).load()
  }

  fn create(&self, data: Data, _params: Params) -> crate::services::Result {
    if !data.is_object() {
      Err(Error::GeneralError("only object allowed".into()))
    } else {
      let id = ID::random();

      let mut obj = data.clone();
      obj["_id"] = JsonValue::String(id.to_base64());

      self.ws.create(id)?.save(obj.dump())?;

      Ok(obj)
    }
  }

  fn update(&self, id: String, data: Data, _params: Params) -> crate::services::Result {
    if !data.is_object() {
      Err(Error::GeneralError("only object allowed".into()))
    } else {
      let id = crate::services::string_to_id(id)?;

      let mut obj = data.clone();
      obj["_id"] = id.to_base64().into();

      self.ws.get(&id).save(obj.dump())?;

      Ok(obj)
    }
  }

  fn patch(&self, id: String, data: Data, _params: Params) -> crate::services::Result {
    if !data.is_object() {
      Err(Error::GeneralError("only object allowed".into()))
    } else {
      let id = crate::services::string_to_id(id)?;

      let storage = self.ws.get(&id);

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

  fn remove(&self, id: String, _params: Params) -> crate::services::Result {
    let id = ID::from_base64(id.as_bytes()).map_err(|e| Error::GeneralError(e.to_string()))?;

    self.ws.get(&id).delete()
  }
}
