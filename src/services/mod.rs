mod authentication;
mod people;
pub(crate) mod persistent;
mod users;

use crate::ID;
use actix_web::web::Json;
use chrono::{DateTime, Utc};
use json::JsonValue;
use serde_json::Value;
use std::collections::HashMap;
use std::sync::Arc;

use crate::animo::error::DBError;
pub(crate) use authentication::Authentication;
pub(crate) use people::People;
pub(crate) use users::Users;

use crate::ws::error_not_found;

pub(crate) type Result = std::result::Result<JsonValue, Error>;
pub(crate) type Data = JsonValue;
pub(crate) type Params = JsonValue;

#[derive(Debug)]
pub enum Mutation {
  // service name, data and etc
  Create(String, Data, Params),
  Update(String, String, Data, Params),
  Patch(String, String, Data, Params),
  Remove(String, String, Params),
}

#[derive(Debug)]
pub enum Event {
  // service name, data
  Created(String, Data),
  Updated(String, Data),
  Patched(String, Data),
  Removed(String, Data),
}

pub trait Services: Send + Sync {
  fn register(&mut self, service: Arc<dyn Service>);
  fn service<S: AsRef<str> + ToString>(&self, name: S) -> Arc<dyn Service>;
}

pub trait Service: Send + Sync {
  fn path(&self) -> &str;

  fn find(&self, params: Params) -> Result;
  fn get(&self, id: String, params: Params) -> Result;
  fn create(&self, data: Data, params: Params) -> Result;
  fn update(&self, id: String, data: Data, params: Params) -> Result;
  fn patch(&self, id: String, data: Data, params: Params) -> Result;
  fn remove(&self, id: String, params: Params) -> Result;

  fn id(&self, name: &str, params: &Params) -> std::result::Result<ID, Error> {
    if let Some(id) = params[name].as_str() {
      ID::from_base64(id.as_bytes()).map_err(|e| Error::GeneralError(e.to_string()))
    } else {
      Err(Error::GeneralError(format!("{name} not found")))
    }
  }

  fn oid(&self, params: &Params) -> std::result::Result<ID, Error> {
    if params.is_array() {
      self.id("oid", &params[0])
    } else {
      self.id("oid", params)
    }
  }

  fn cid(&self, params: &Params) -> std::result::Result<ID, Error> {
    if params.is_array() {
      self.id("cid", &params[0])
    } else {
      self.id("cid", params)
    }
  }

  fn pid(&self, params: &Params) -> std::result::Result<ID, Error> {
    if params.is_array() {
      self.id("pid", &params[0])
    } else {
      self.id("pid", params)
    }
  }

  fn date(&self, params: &Params) -> std::result::Result<DateTime<Utc>, Error> {
    let params = {
      if params.is_array() {
        &params[0]
      } else {
        params
      }
    };

    if let Some(date) = params["date"].as_str() {
      if date == "today" {
        Ok(Utc::now())
      } else {
        todo!()
      }
    } else {
      Err(Error::GeneralError("date not found".into()))
    }
  }

  fn limit(&self, params: &Params) -> usize {
    let params = {
      if params.is_array() {
        &params[0]
      } else {
        params
      }
    };

    if let Some(limit) = params["$limit"].as_number() {
      usize::try_from(limit).unwrap_or(10)
    } else {
      10
    }
  }

  fn skip(&self, params: &Params) -> usize {
    let params = {
      if params.is_array() {
        &params[0]
      } else {
        params
      }
    };

    if let Some(skip) = params["$skip"].as_number() {
      usize::try_from(skip).unwrap_or(0)
    } else {
      0
    }
  }
}

pub fn string_to_id(data: String) -> std::result::Result<ID, Error> {
  ID::from_base64(data.as_bytes()).map_err(|e| Error::GeneralError(e.to_string()))
}

pub(crate) struct NoService(pub(crate) String);

impl NoService {
  fn error(&self) -> Result {
    Err(Error::NotFound(format!("service {}", self.0)))
  }
}

impl Service for NoService {
  fn path(&self) -> &str {
    self.0.as_str()
  }

  fn find(&self, params: Params) -> Result {
    self.error()
  }

  fn get(&self, id: String, params: Params) -> Result {
    self.error()
  }

  fn create(&self, data: Data, params: Params) -> Result {
    self.error()
  }

  fn update(&self, id: String, data: Data, params: Params) -> Result {
    self.error()
  }

  fn patch(&self, id: String, data: Data, params: Params) -> Result {
    self.error()
  }

  fn remove(&self, id: String, params: Params) -> Result {
    self.error()
  }
}

//     400: BadRequest
//     401: NotAuthenticated
//     402: PaymentError
//     403: Forbidden
//     404: NotFound
//     405: MethodNotAllowed
//     406: NotAcceptable
//     408: Timeout
//     409: Conflict
//     411: LengthRequired
//     422: Unprocessable
//     429: TooManyRequests
//     500: GeneralError
//     501: NotImplemented
//     502: BadGateway
//     503: Unavailable

quick_error! {
  #[derive(Debug)]
  pub enum Error {
    NotAuthenticated(error: String) {
      display("{}", error)
    }
    NotFound(error: String) {
      display("{}", error)
    }
    IOError(error: String) {
      display("{}", error)
    }
    GeneralError(error: String) {
      display("{}", error)
      // from(e: crate::hik::error::Error) -> (e.to_string())
    }
    CameraError(error: crate::hik::error::Error) {
      display("{}", error.to_string())
    }
    NotImplemented
  }
}

impl Error {
  fn to_code(&self) -> usize {
    match self {
      Error::NotAuthenticated(_) => 401,
      Error::NotFound(_) => 404,
      Error::NotImplemented => 501,
      _ => 500,
    }
  }

  fn to_class_name(&self) -> &str {
    match self {
      Error::NotAuthenticated(_) => "not-authenticated",
      Error::NotFound(_) => "not-found",
      Error::IOError(_) => "io-error",
      Error::GeneralError(_) => "general-error",
      Error::CameraError(_) => "general-error",
      Error::NotImplemented => "not-implemented",
    }
  }

  fn to_name(&self) -> &str {
    match self {
      Error::NotAuthenticated(_) => "NotAuthenticated",
      Error::NotFound(_) => "NotFound",
      Error::IOError(_) => "IOError",
      Error::GeneralError(_) => "GeneralError",
      Error::CameraError(_) => "GeneralError",
      Error::NotImplemented => "NotImplemented",
    }
  }

  pub fn to_json(&self) -> JsonValue {
    json::object! {
      className: self.to_class_name(),
      code: self.to_code(),
      message: self.to_string(),
      name: self.to_name(),
    }
  }
}

pub trait JsonData {
  fn json(&self) -> Result;
}

impl JsonData for String {
  fn json(&self) -> Result {
    json::parse(self.as_str()).map_err(|e| Error::IOError(e.to_string()))
  }
}
