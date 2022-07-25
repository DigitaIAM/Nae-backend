use std::collections::HashMap;
use std::sync::Arc;
use actix_web::web::Json;
use json::JsonValue;
use serde_json::Value;
use crate::ID;

mod authentication;
pub(crate) use authentication::Authentication;
use crate::ws::error_not_found;

type Data = JsonValue;
type Params = JsonValue;

pub trait Services: Send + Sync {
  fn register(&mut self, service: Arc<dyn Service>);
  fn service(&self, name: &str) -> Arc<dyn Service>;
}

pub trait Service: Send + Sync {
  fn path(&self) -> &str;

  fn find(&self, params: Params) -> JsonValue;
  fn get(&self, id: ID, params: Params) -> JsonValue;
  fn create(&self, data: Data, params: Params) -> JsonValue;
  fn update(&self, id: ID, data: Data, params: Params) -> JsonValue;
  fn patch(&self, id: ID, data: Data, params: Params) -> JsonValue;
  fn remove(&self, id: ID, params: Params) -> JsonValue;
}

pub(crate) struct NoService(pub(crate) String);

impl NoService {
  fn error(&self) -> JsonValue {
    error_not_found(format!("can't find service {:?}", self.0).as_str())
  }
}

impl Service for NoService {
  fn path(&self) -> &str {
    self.0.as_str()
  }

  fn find(&self, params: Params) -> JsonValue {
    self.error()
  }

  fn get(&self, id: ID, params: Params) -> JsonValue {
    self.error()
  }

  fn create(&self, data: Data, params: Params) -> JsonValue {
    self.error()
  }

  fn update(&self, id: ID, data: Data, params: Params) -> JsonValue {
    self.error()
  }

  fn patch(&self, id: ID, data: Data, params: Params) -> JsonValue {
    self.error()
  }

  fn remove(&self, id: ID, params: Params) -> JsonValue {
    self.error()
  }
}