use chrono::ParseError;
use json::JsonError;
use std::string::FromUtf8Error;

#[derive(Debug)]
pub struct WHError {
  message: String,
}

impl WHError {
  pub fn new(e: &str) -> Self {
    WHError { message: e.to_string() }
  }

  pub fn message(&self) -> String {
    self.message.clone()
  }
}

impl From<service::error::Error> for WHError {
  fn from(e: service::error::Error) -> Self {
    WHError { message: e.to_string() }
  }
}

impl From<rocksdb::Error> for WHError {
  fn from(e: rocksdb::Error) -> Self {
    WHError { message: e.to_string() }
  }
}

impl From<serde_json::Error> for WHError {
  fn from(e: serde_json::Error) -> Self {
    WHError { message: e.to_string() }
  }
}

impl From<ciborium::ser::Error<std::io::Error>> for WHError {
  fn from(e: ciborium::ser::Error<std::io::Error>) -> Self {
    WHError { message: e.to_string() }
  }
}

impl From<ciborium::de::Error<std::io::Error>> for WHError {
  fn from(e: ciborium::de::Error<std::io::Error>) -> Self {
    WHError { message: e.to_string() }
  }
}

impl From<ParseError> for WHError {
  fn from(e: ParseError) -> Self {
    WHError { message: e.to_string() }
  }
}

impl From<FromUtf8Error> for WHError {
  fn from(e: FromUtf8Error) -> Self {
    WHError { message: e.to_string() }
  }
}

impl From<rust_decimal::Error> for WHError {
  fn from(e: rust_decimal::Error) -> Self {
    WHError { message: e.to_string() }
  }
}

impl From<uuid::Error> for WHError {
  fn from(e: uuid::Error) -> Self {
    WHError { message: e.to_string() }
  }
}

// impl From<service::error::Error> for WHError {
//   fn from(e: service::error::Error) -> Self {
//     WHError { message: e.to_string() }
//   }
// }

impl From<JsonError> for WHError {
  fn from(e: JsonError) -> Self {
    WHError { message: e.to_string() }
  }
}
