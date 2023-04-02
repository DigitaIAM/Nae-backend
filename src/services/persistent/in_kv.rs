use crate::services::{Data, Params};
use crate::{
  animo::memory::{ChangeTransformation, Memory, TransformationKey, Value, ID},
  commutator::Application,
};
use json::object::Object;
use json::JsonValue;
use service::error::Error;
use service::Service;
use std::sync::Arc;

pub(crate) struct InKV {
  app: Application,
  path: Arc<String>,

  zone: ID,
  properties: Arc<Vec<String>>,
}

impl InKV {
  pub(crate) fn new(
    app: Application,
    path: &str,
    zone: ID,
    properties: Vec<String>,
  ) -> Arc<dyn Service> {
    Arc::new(InKV { app, path: Arc::new(path.to_string()), zone, properties: Arc::new(properties) })
  }

  fn save(&self, id: ID, data: Data, params: Params) -> crate::services::Result {
    let mut result = Object::with_capacity(self.properties.len() + 1);

    // prepare changes
    let mutations = self
      .properties
      .iter()
      .map(|name| {
        let value = match data[name].as_str() {
          None => Value::Nothing,
          Some(str) => Value::String(str.trim().to_string()),
        };
        (name, value)
      })
      .filter(|(n, v)| v.is_string())
      .map(|(name, value)| {
        result.insert(&name, value.as_string().unwrap_or_default().into());
        ChangeTransformation::create(self.zone, id, &name, value)
      })
      .collect();

    // store
    self.app.db.modify(mutations).map_err(|e| Error::GeneralError(e.to_string()))?;

    result.insert("_id", id.to_base64().into());
    Ok(JsonValue::Object(result))
  }
}

impl Service for InKV {
  fn path(&self) -> &str {
    &self.path
  }

  fn find(&self, params: Params) -> crate::services::Result {
    let limit = self.limit(&params);
    let skip = self.skip(&params);

    todo!()

    // let objs = self.objs.read().unwrap();
    // let total = objs.len();
    //
    // let mut list = Vec::with_capacity(limit);
    // for (_, obj) in objs.iter().skip(skip).take(limit) {
    //   list.push(obj.clone());
    // }
    //
    // Ok(
    //   json::object! {
    //     data: JsonValue::Array(list),
    //     total: total,
    //     "$skip": skip,
    //   }
    // )
  }

  fn get(&self, id: String, params: Params) -> crate::services::Result {
    let id = crate::services::string_to_id(id)?;

    let keys = self.properties.iter().map(|name| TransformationKey::simple(id, name)).collect();
    match self.app.db.query(keys) {
      Ok(records) => {
        let mut obj = Object::with_capacity(self.properties.len() + 1);

        self
          .properties
          .iter()
          .zip(records.iter())
          .filter(|(n, v)| v.into != Value::Nothing)
          .for_each(|(n, v)| obj.insert(n, v.into.to_json()));

        if obj.len() == 0 {
          Err(Error::NotFound(id.to_base64()))
        } else {
          obj.insert("_id", id.to_base64().into());
          Ok(JsonValue::Object(obj))
        }
      },
      Err(msg) => Err(Error::IOError(msg.to_string())),
    }
  }

  fn create(&self, data: Data, params: Params) -> crate::services::Result {
    let id = ID::random();
    self.save(id, data, params)
  }

  fn update(&self, id: String, data: Data, params: Params) -> crate::services::Result {
    let id = ID::from_base64(id.as_bytes()).map_err(|e| Error::GeneralError(e.to_string()))?;

    // TODO check that record exist

    self.save(id, data, params)
  }

  fn patch(&self, id: String, data: Data, params: Params) -> crate::services::Result {
    let id = ID::from_base64(id.as_bytes()).map_err(|e| Error::GeneralError(e.to_string()))?;

    // TODO check that record exist

    self.save(id, data, params)
  }

  fn remove(&self, id: String, params: Params) -> crate::services::Result {
    Err(Error::NotImplemented)
  }
}
