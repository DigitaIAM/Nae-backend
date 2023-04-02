mod authentication;
mod people;
pub(crate) mod persistent;
mod users;

use crate::animo::memory::ID;
pub use authentication::Authentication;
use json::JsonValue;
pub use people::People;
use service::error::Error;
pub use users::Users;

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

pub fn id(name: &str, params: &Params) -> std::result::Result<ID, Error> {
  if let Some(id) = params[name].as_str() {
    ID::from_base64(id.as_bytes()).map_err(|e| Error::GeneralError(e.to_string()))
  } else {
    Err(Error::GeneralError(format!("id `{name}` not found")))
  }
}

pub fn uuid(name: &str, params: &Params) -> std::result::Result<uuid::Uuid, Error> {
  if params.is_array() {
    if let Some(id) = params[0][name].as_str() {
      uuid::Uuid::parse_str(id).map_err(|e| Error::GeneralError(e.to_string()))
    } else {
      Err(Error::GeneralError(format!("uuid `{name}` not found")))
    }
  } else {
    if let Some(id) = params[name].as_str() {
      uuid::Uuid::parse_str(id).map_err(|e| Error::GeneralError(e.to_string()))
    } else {
      Err(Error::GeneralError(format!("uuid `{name}` not found")))
    }
  }
}

pub fn oid(params: &Params) -> std::result::Result<ID, Error> {
  if params.is_array() {
    id("oid", &params[0])
  } else {
    id("oid", params)
  }
}

pub fn cid(params: &Params) -> std::result::Result<ID, Error> {
  if params.is_array() {
    id("cid", &params[0])
  } else {
    id("cid", params)
  }
}

pub fn pid(params: &Params) -> std::result::Result<ID, Error> {
  if params.is_array() {
    id("pid", &params[0])
  } else {
    id("pid", params)
  }
}

pub fn string_to_id(data: String) -> std::result::Result<ID, Error> {
  ID::from_base64(data.as_bytes()).map_err(|e| Error::GeneralError(e.to_string()))
}

pub trait JsonData {
  fn json(&self) -> Result;
}

impl JsonData for String {
  fn json(&self) -> Result {
    json::parse(self.as_str()).map_err(|e| Error::IOError(e.to_string()))
  }
}
