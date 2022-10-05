pub(crate) mod persistent;
mod authentication;
mod users;
mod people;

use std::collections::HashMap;
use std::sync::Arc;
use actix_web::web::Json;
use json::JsonValue;
use serde_json::Value;
use crate::ID;

pub(crate) use authentication::Authentication;
pub(crate) use users::Users;
pub(crate) use people::People;
use crate::animo::error::DBError;

use crate::ws::error_not_found;

pub(crate) type Result = std::result::Result<JsonValue, Error>;
pub(crate) type Data = JsonValue;
pub(crate) type Params = JsonValue;

#[derive(Debug)]
pub enum Mutation {
  // service name, data and etc
  Create(String,Data,Params),
  Update(String,String,Data,Params),
  Patch(String,String,Data,Params),
  Remove(String,String,Params),
}

#[derive(Debug)]
pub enum Event {
  // service name, data
  Created(String,Data),
  Updated(String,Data),
  Patched(String,Data),
  Removed(String,Data),
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

  fn limit(&self, params: &Params) -> usize {
    if let Some(limit) = params[0]["$limit"].as_number() {
      usize::try_from(limit).unwrap_or(10)
    } else {
      10
    }
  }

  fn skip(&self, params: &Params) -> usize {
    if let Some(skip) = params[0]["$skip"].as_number() {
      usize::try_from(skip).unwrap_or(0)
    } else {
      0
    }
  }
}

pub fn string_to_id(data: String) -> std::result::Result<ID, Error> {
  ID::from_base64(data.as_bytes())
    .map_err(|e| Error::GeneralError(e.to_string()))
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
      Error::NotImplemented => "not-implemented",
    }
  }

  fn to_name(&self) -> &str {
    match self {
      Error::NotAuthenticated(_) => "NotAuthenticated",
      Error::NotFound(_) => "NotFound",
      Error::IOError(_) => "IOError",
      Error::GeneralError(_) => "GeneralError",
      Error::NotImplemented => "NotImplemented",
    }
  }

  pub fn to_json(&self) -> JsonValue {
    json::object!{
      className: self.to_class_name(),
      code: self.to_code(),
      message: self.to_string(),
      name: self.to_name(),
    }
  }
}