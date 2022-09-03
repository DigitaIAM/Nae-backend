use std::sync::{Arc, RwLock};
use json::JsonValue;
use crate::{Application, ID, Services};
use crate::services::{Data, Params, Service};

pub(crate) struct Authentication {
  app: Application,
  path: Arc<String>,
}

impl Authentication {
  pub(crate) fn new(app: Application, path: &str) -> Arc<dyn Service> {
    Arc::new(Authentication { app, path: Arc::new(path.to_string()) })
  }
}

impl Service for Authentication {
  fn path(&self) -> &str {
    &self.path
  }

  fn find(&self, params: Params) -> JsonValue {
    todo!()
  }

  fn get(&self, id: ID, params: Params) -> JsonValue {
    todo!()
  }

  fn create(&self, data: Data, params: Params) -> JsonValue {
    let strategy = data["strategy"].as_str().unwrap_or("local").to_string();
    let email = data["email"].as_str().unwrap_or("").to_string();
    let password = data["password"].as_str().unwrap_or("").to_string();

    let request = crate::auth::LoginRequest {
      password,
      email: email.clone(),
      remember_me: false
    };

    match crate::auth::login_procedure(&self.app, request) {
      Ok((account, token)) => {
        let user = (&self.app as (&dyn Services))
          .service("people")
          .get(account, JsonValue::Null);

        JsonValue::Array(vec![
          // error
          JsonValue::Null,
          // data
          json::object! {
            accessToken: token,
            user: user
          }
        ])
      },
      Err(msg) => json::object!{
        className: "not-authenticated",
        code: 401,
        message: "Invalid login",
        name: "NotAuthenticated",
      }
    }
  }

  fn update(&self, id: ID, data: Data, params: Params) -> JsonValue {
    todo!()
  }

  fn patch(&self, id: ID, data: Data, params: Params) -> JsonValue {
    todo!()
  }

  fn remove(&self, id: ID, params: Params) -> JsonValue {
    todo!()
  }
}