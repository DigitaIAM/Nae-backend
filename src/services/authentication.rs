use crate::auth::JWTAuth;
use crate::commutator::Application;
use crate::services::{Data, Params};
use json::JsonValue;
use service::error::Error;
use service::{Account, Context, Service, Services};
use std::sync::Arc;

pub struct Authentication {
  app: Application,
  path: Arc<String>,
}

impl Authentication {
  pub fn new(app: Application, path: &str) -> Arc<dyn Service> {
    Arc::new(Authentication { app, path: Arc::new(path.to_string()) })
  }
}

impl Service for Authentication {
  fn path(&self) -> &str {
    &self.path
  }

  fn find(&self, _ctx: Context, _params: Params) -> crate::services::Result {
    Err(Error::NotImplemented)
  }

  fn get(&self, _ctx: Context, _id: String, _params: Params) -> crate::services::Result {
    Err(Error::NotImplemented)
  }

  fn create(&self, ctx: Context, data: Data, _params: Params) -> crate::services::Result {
    let strategy = data["strategy"].as_str().unwrap_or("local").to_string();
    match strategy.as_str() {
      "jwt" => {
        let token = data["accessToken"].as_str().unwrap_or("");
        let account =
          Account::jwt(&self.app, token).map_err(|e| Error::GeneralError(e.to_string()))?;

        let user = self.app.service("users").get(
          Context::local(),
          account.id.to_base64(),
          JsonValue::Null,
        )?;

        let data = json::object! {
          accessToken: token,
          user: user
        };

        let mut ctx_account = ctx.account.write().unwrap();
        *ctx_account = account;

        Ok(data)
      },
      "local" => {
        let email = data["email"].as_str().unwrap_or("").trim().to_lowercase();
        let password = data["password"].as_str().unwrap_or("").to_string();

        let request = crate::auth::LoginRequest { password, email, remember_me: false };

        match crate::auth::login_procedure(&self.app, request) {
          Ok((account, token)) => {
            let user = self.app.service("users").get(
              Context::local(),
              account.to_base64(),
              JsonValue::Null,
            )?;

            let data = json::object! {
              accessToken: token,
              user: user
            };

            let mut ctx_account = ctx.account.write().unwrap();
            *ctx_account = Account { id: account, email: "-".to_string() };

            Ok(data)
          },
          Err(msg) => Err(Error::GeneralError(msg)),
        }
      },
      _ => Err(Error::GeneralError(format!("unknown strategy '{strategy}'"))),
    }
  }

  fn update(
    &self,
    _ctx: Context,
    _id: String,
    _data: Data,
    _params: Params,
  ) -> crate::services::Result {
    Err(Error::NotImplemented)
  }

  fn patch(
    &self,
    _ctx: Context,
    _id: String,
    _data: Data,
    _params: Params,
  ) -> crate::services::Result {
    Err(Error::NotImplemented)
  }

  fn remove(&self, ctx: Context, _id: String, _params: Params) -> crate::services::Result {
    let account = { ctx.account.read().unwrap().clone() };
    match crate::auth::logout_procedure(&self.app, account) {
      Ok(_) => Ok(JsonValue::Null),
      Err(msg) => Err(Error::GeneralError(msg)),
    }
  }
}
