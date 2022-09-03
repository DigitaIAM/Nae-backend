use std::sync::{Arc, RwLock};
use json::JsonValue;
use json::object::Object;
use crate::{Application, auth, ID, Memory, Services, Transformation, TransformationKey, Value};
use crate::animo::error::DBError;
use crate::services::{Data, Params, Service};
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

  fn find(&self, params: Params) -> JsonValue {
    if let Some(email) = params["query"]["email"].as_str() {
      todo!()
    }
    JsonValue::Null
  }

  fn get(&self, id: ID, params: Params) -> JsonValue {
    let names = ["label", "email", "avatar"];
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
          error_general("not found")
        } else {
          obj.insert("_id", id.to_base64().into());
          JsonValue::Object(obj)
        }
      }
      Err(msg) => {
        error_general("can't process request")
      }
    }
  }

  fn create(&self, data: Data, params: Params) -> JsonValue {
    let email = data["email"].as_str().unwrap_or("").to_string();
    let password = data["password"].as_str().unwrap_or("").to_string();

    let signup = crate::auth::SignUpRequest { email: email.clone(), password };

    match auth::signup_procedure(&self.app, signup) {
      Ok((account, token)) => {
        json::object! {
          _id: account.to_base64(),
          accessToken: token,
          email: email,
        }
      }
      Err(msg) => {
        error_general("can't process request")
      }
    }
  }

  fn update(&self, id: ID, data: Data, params: Params) -> JsonValue{
    todo!()
  }

  fn patch(&self, id: ID, data: Data, params: Params) -> JsonValue{
    todo!()
  }

  fn remove(&self, id: ID, params: Params) -> JsonValue{
    todo!()
  }
}