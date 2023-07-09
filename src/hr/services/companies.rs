use json::JsonValue;
use std::sync::Arc;

use crate::commutator::Application;
use crate::services::{Data, Params};
use service::error::Error;
use service::{Context, Service};
use values::ID;

pub struct Companies {
  app: Application,
  name: String,
}

impl Companies {
  pub(crate) fn new(app: Application) -> Arc<dyn Service> {
    Arc::new(Companies { app, name: "companies".to_string() })
  }
}

impl Service for Companies {
  fn path(&self) -> &str {
    &self.name
  }

  fn find(&self, _ctx: Context, params: Params) -> crate::services::Result {
    let _limit = self.limit(&params);
    let skip = self.skip(&params);

    let list = self.app.wss.list()?;
    let total = list.len();

    let list = list.into_iter().skip(skip).take(total).map(|o| o.json()).collect();

    Ok(json::object! {
      data: JsonValue::Array(list),
      total: total,
      "$skip": skip,
    })
  }

  fn get(&self, _ctx: Context, id: String, _params: Params) -> crate::services::Result {
    let id = crate::services::string_to_id(id)?;
    self.app.wss.get(&id).load()
  }

  fn create(&self, _ctx: Context, data: Data, _params: Params) -> crate::services::Result {
    if !data.is_object() {
      Err(Error::GeneralError("only object allowed".into()))
    } else {
      let id = ID::random();

      let mut obj = data.clone();
      obj["_id"] = JsonValue::String(id.to_base64());

      self.app.wss.create(id)?.save(obj.dump())?;

      Ok(obj)
    }
  }

  fn update(
    &self,
    _ctx: Context,
    id: String,
    data: Data,
    _params: Params,
  ) -> crate::services::Result {
    if !data.is_object() {
      Err(Error::GeneralError("only object allowed".into()))
    } else {
      let id = crate::services::string_to_id(id)?;

      let mut obj = data.clone();
      obj["_id"] = id.to_base64().into();

      self.app.wss.get(&id).save(obj.dump())?;

      Ok(obj)
    }
  }

  fn patch(
    &self,
    _ctx: Context,
    id: String,
    data: Data,
    _params: Params,
  ) -> crate::services::Result {
    if !data.is_object() {
      Err(Error::GeneralError("only object allowed".into()))
    } else {
      let id = crate::services::string_to_id(id)?;

      let storage = self.app.wss.get(&id);

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

  fn remove(&self, _ctx: Context, id: String, _params: Params) -> crate::services::Result {
    let id = ID::from_base64(id.as_bytes()).map_err(|e| Error::GeneralError(e.to_string()))?;

    self.app.wss.get(&id).delete()
  }
}
