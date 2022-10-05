use std::sync::{Arc, RwLock};
use json::JsonValue;
use json::object::Object;
use crate::{Application, auth, ChangeTransformation, ID, Memory, Services, Transformation, TransformationKey, Value};
use crate::animo::error::DBError;
use crate::services::{Data, Error, Params, Service};
use crate::ws::error_general;

lazy_static::lazy_static! {
    pub(crate) static ref PEOPLE: ID = ID::for_constant("people");
}

const PROPERTIES: [&str; 4] = ["organization", "first_name", "last_name", "email"];

pub(crate) struct People {
  app: Application,
  path: Arc<String>,
}

impl People {
  pub(crate) fn new(app: Application, path: &str) -> Arc<dyn Service> {
    Arc::new(People { app, path: Arc::new(path.to_string()) })
  }

  fn save(&self, id: ID, data: Data, params: Params) -> crate::services::Result {
    let mut result = Object::with_capacity(PROPERTIES.len() + 1);

    // prepare changes
    let mutations = PROPERTIES.into_iter()
      .map(|name| {
        let value = match data[name].as_str() {
          None => Value::Nothing,
          Some(str) => Value::String(str.trim().to_string()),
        };
        (name, value)
      })
      .filter(|(n,v)| v.is_string())
      .map(|(name, value)| {
        result.insert(name, value.as_string().unwrap_or_default().into());
        ChangeTransformation::create(*PEOPLE, id, name, value)
      })
      .collect();

    // store
    self.app.db.modify(mutations)
      .map_err(|e| Error::GeneralError(e.to_string()))?;

    result.insert("_id", id.to_base64().into());
    Ok(JsonValue::Object(result))
  }
}

impl Service for People {
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

    let keys = PROPERTIES.iter()
      .map(|name|TransformationKey::simple(id, name))
      .collect();
    match self.app.db.query(keys) {
      Ok(records) => {
        let mut obj = Object::with_capacity(PROPERTIES.len() + 1);

        PROPERTIES.iter()
          .zip(records.iter())
          .filter(|(n, v)| v.into != Value::Nothing)
          .for_each(|(n, v)| obj.insert(n, v.into.to_json()));

        if obj.len() == 0 {
          Err(Error::NotFound(id.to_base64()))
        } else {
          obj.insert("_id", id.to_base64().into());
          Ok(JsonValue::Object(obj))
        }
      }
      Err(msg) => Err(Error::IOError(msg.to_string()))
    }
  }

  fn create(&self, data: Data, params: Params) -> crate::services::Result {
    let id = ID::random();
    self.save(id, data, params)
  }

  fn update(&self, id: String, data: Data, params: Params) -> crate::services::Result {
    let id = ID::from_base64(id.as_bytes())
      .map_err(|e| Error::GeneralError(e.to_string()))?;

    // TODO check that record exist

    self.save(id, data, params)
  }

  fn patch(&self, id: String, data: Data, params: Params) -> crate::services::Result {
    let id = ID::from_base64(id.as_bytes())
      .map_err(|e| Error::GeneralError(e.to_string()))?;

    // TODO check that record exist

    self.save(id, data, params)
  }

  fn remove(&self, id: String, params: Params) -> crate::services::Result {
    Err(Error::NotImplemented)
  }
}