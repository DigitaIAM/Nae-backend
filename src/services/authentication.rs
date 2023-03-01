use crate::services::{Data, Params};
use service::{Service, Services};
use errors::Error;
use crate::{commutator::Application, animo::memory::ID};
use json::JsonValue;
use std::sync::{Arc, RwLock};

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

  fn find(&self, params: Params) -> crate::services::Result {
    Err(Error::NotImplemented)
  }

  fn get(&self, id: String, params: Params) -> crate::services::Result {
    Err(Error::NotImplemented)
  }

  fn create(&self, data: Data, params: Params) -> crate::services::Result {
    let strategy = data["strategy"].as_str().unwrap_or("local").to_string();
    match strategy.as_str() {
      "jwt" => {
        let token = data["accessToken"].as_str().unwrap_or("");
        let account = crate::auth::Account::jwt(&self.app, token)
          .map_err(|e| Error::GeneralError(e.to_string()))?;

        let user = self.app.service("users").get(account.id.to_base64(), JsonValue::Null)?;

        let data = json::object! {
          accessToken: token,
          user: user
        };
        Ok(data)
      },
      "local" => {
        let email = data["email"].as_str().unwrap_or("").trim().to_lowercase();
        let password = data["password"].as_str().unwrap_or("").to_string();

        let request =
          crate::auth::LoginRequest { password, email: email.clone(), remember_me: false };

        match crate::auth::login_procedure(&self.app, request) {
          Ok((account, token)) => {
            let user = self.app.service("users").get(account.to_base64(), JsonValue::Null)?;

            let data = json::object! {
              accessToken: token,
              user: user
            };
            Ok(data)
          },
          Err(msg) => Err(Error::GeneralError(msg)),
        }
      },
      _ => Err(Error::GeneralError(format!("unknown strategy '{strategy}'"))),
    }
  }

  fn update(&self, id: String, data: Data, params: Params) -> crate::services::Result {
    Err(Error::NotImplemented)
  }

  fn patch(&self, id: String, data: Data, params: Params) -> crate::services::Result {
    Err(Error::NotImplemented)
  }

  fn remove(&self, id: String, params: Params) -> crate::services::Result {
    Err(Error::NotImplemented)
  }
}
