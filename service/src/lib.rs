extern crate actix_web;
extern crate json;
extern crate chrono;

pub mod utils;
pub mod error;

use actix_web::web::{Json};
use chrono::{DateTime, NaiveDate, NaiveDateTime, NaiveTime, ParseResult, Utc};
use json::JsonValue;
use serde_json::Value;
use std::collections::HashMap;
use std::sync::Arc;
use std::convert::TryFrom;

use error::Error;
use utils::json::JsonParams;
use utils::time::DateRange;

#[macro_use] extern crate quick_error;

pub type Result = std::result::Result<JsonValue, Error>;
pub(crate) type Data = JsonValue;
pub(crate) type Params = JsonValue;

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

  fn ctx(&self, params: &Params) -> Vec<String> {
    self.params(params)["ctx"]
        .members()
        .map(|j| j.string_or_none())
        .filter(|v| v.is_some())
        .map(|v| v.unwrap_or_default())
        .collect()
  }

  fn parse_date(&self, str: &str) -> std::result::Result<DateTime<Utc>, Error> {
    match NaiveDate::parse_from_str(str, "%Y-%m-%d") {
      Ok(d) => Ok(DateTime::<Utc>::from_utc(NaiveDateTime::new(d, NaiveTime::default()), Utc)),
      Err(e) => Err(Error::GeneralError(format!("invalid date '{str}'"))),
    }
  }

  fn date(&self, name: &str, params: &Params) -> std::result::Result<Option<DateTime<Utc>>, Error> {
    let params = {
      if params.is_array() {
        &params[0]
      } else {
        params
      }
    };

    if let Some(date) = params[name].as_str() {
      // if date == "today" {
      //   todo!() // Ok(Utc::now().into())
      // } else {
      let date = self.parse_date(date)?;
      Ok(Some(date))
      // }
    } else {
      Ok(None)
    }
  }

  fn date_range(&self, params: &Params) -> std::result::Result<Option<DateRange>, Error> {
    let dates = &params["dates"];

    if let Some(date) = dates["from"].as_str() {
      let from = self.parse_date(date)?;
      // println!("FN_DATE_RANGE {date:?}");
      if let Some(date) = dates["till"].as_str() {
        let till = self.parse_date(date)?;

        Ok(Some(DateRange(from, till)))
      } else {
        return Err(Error::GeneralError("dates require `till`".into()));
      }
    } else {
      Ok(None)
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
      usize::try_from(limit).unwrap_or(10).max(100)
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

  fn params<'a>(&self, params: &'a Params) -> &'a JsonValue {
    if params.is_array() {
      &params[0]
    } else {
      params
    }
  }
}

pub struct NoService(pub String);

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