use std::sync::{Arc, RwLock};
use json::JsonValue;
use json::object::Object;
use crate::{Application, auth, ID, Memory, Services, Transformation, TransformationKey, Value};
use crate::animo::error::DBError;
use crate::services::{Data, Error, Params, Service};
use crate::ws::error_general;

pub(crate) struct People {
  app: Application,
  path: Arc<String>,
}

impl People {
  pub(crate) fn new(app: Application, path: &str) -> Arc<dyn Service> {
    Arc::new(People { app, path: Arc::new(path.to_string()) })
  }
}

impl Service for People {
  fn path(&self) -> &str {
    &self.path
  }

  fn find(&self, params: Params) -> crate::services::Result {
    // if let Some(email) = params["query"]["email"].as_str() {
    //   todo!()
    // }
    Err(Error::NotImplemented)
  }

  fn get(&self, id: ID, params: Params) -> crate::services::Result {
    let names = ["first_name", "last_name", "email", "photo"];
    let keys = names.iter()
      .map(|name|TransformationKey::simple(id, name))
      .collect();
    match self.app.db.query(keys) {
      Ok(records) => {
        let mut obj = Object::with_capacity(names.len() + 1);

        names.iter()
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
    let email = data["email"].as_str().unwrap_or("").to_string();

    Err(Error::NotImplemented)
  }

  fn update(&self, id: ID, data: Data, params: Params) -> crate::services::Result {
    Err(Error::NotImplemented)
  }

  fn patch(&self, id: ID, data: Data, params: Params) -> crate::services::Result {
    Err(Error::NotImplemented)
  }

  fn remove(&self, id: ID, params: Params) -> crate::services::Result {
    Err(Error::NotImplemented)
  }
}