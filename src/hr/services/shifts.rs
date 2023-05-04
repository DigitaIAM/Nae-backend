use json::JsonValue;
use std::sync::Arc;

use crate::services::string_to_id;
use crate::services::{Data, Params};
use crate::{commutator::Application, storage::Workspaces};
use service::error::Error;
use service::{Context, Service};
use values::ID;

pub(crate) struct Shifts {
  app: Application,
  name: String,

  ws: Workspaces,
}

impl Shifts {
  pub(crate) fn new(app: Application, ws: Workspaces) -> Arc<dyn Service> {
    Arc::new(Shifts { app, name: "shifts".to_string(), ws })
  }
}

impl Service for Shifts {
  fn path(&self) -> &str {
    &self.name
  }

  fn find(&self, _ctx: Context, params: Params) -> crate::services::Result {
    let oid = crate::services::oid(&params)?;

    let limit = self.limit(&params);
    let skip = self.skip(&params);

    let list = self.ws.get(&oid).shifts(); //.list(skip, limit)?;
    let total = list.len();

    let list = list.into_iter().skip(skip).take(limit).map(|o| o.json()).collect();

    Ok(json::object! {
      data: JsonValue::Array(list),
      total: total,
      "$skip": skip,
    })
  }

  fn get(&self, _ctx: Context, id: String, params: Params) -> crate::services::Result {
    let oid = crate::services::oid(&params)?;

    let id = crate::services::string_to_id(id)?;
    self.ws.get(&oid).shift(id).load()
  }

  fn create(&self, _ctx: Context, data: Data, _params: Params) -> crate::services::Result {
    let oid = crate::services::oid(&data)?;
    if !data.is_object() {
      Err(Error::GeneralError("only object allowed".into()))
    } else {
      let id = ID::random();

      let mut obj = data.clone();
      obj["_id"] = JsonValue::String(id.to_base64());

      self.ws.get(&oid).shift(id).save(obj.dump())?;

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
    let oid = crate::services::oid(&data)?;
    if !data.is_object() {
      Err(Error::GeneralError("only object allowed".into()))
    } else {
      let id = crate::services::string_to_id(id)?;

      let mut obj = data.clone();
      obj["_id"] = id.to_base64().into();

      self.ws.get(&oid).shift(id).save(obj.dump())?;

      Ok(obj)
    }
  }

  fn patch(&self, _ctx: Context, id: String, data: Data, params: Params) -> crate::services::Result {
    let oid = crate::services::oid(&params)?;
    if !data.is_object() {
      Err(Error::GeneralError("only object allowed".into()))
    } else {
      let id = crate::services::string_to_id(id)?;

      let storage = self.ws.get(&oid).shift(id);

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

  fn remove(&self, _ctx: Context, id: String, params: Params) -> crate::services::Result {
    let oid = crate::services::oid(&params)?;
    let id = string_to_id(id)?;

    self.ws.get(&oid).shift(id).delete()
  }
}
