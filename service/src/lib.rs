extern crate actix_web;
extern crate chrono;
extern crate json;

pub mod error;
pub mod utils;

use chrono::{DateTime, NaiveDate, NaiveDateTime, NaiveTime, Utc};
use json::JsonValue;
use std::convert::TryFrom;
use std::sync::{Arc, RwLock};
use std::time::Duration;

use error::Error;
use utils::json::JsonParams;
use utils::time::DateRange;

#[macro_use]
extern crate quick_error;

use values::{ID, ID_MIN};

pub type Result = std::result::Result<JsonValue, Error>;
pub(crate) type Data = JsonValue;
pub(crate) type Params = JsonValue;

#[derive(Debug, Clone)] //, serde::Serialize, serde::Deserialize)]
pub struct Account {
  pub id: ID,
  // first_name: String,
  // last_name: String,
  pub email: String,
  // data: Option<JsonValue>,
}

#[derive(Debug, Clone)]
pub struct Context {
  pub request: Option<actix_web::dev::RequestHead>,
  pub account: Arc<RwLock<Account>>,
  pub timestamp: Duration,
}

impl Context {
  fn guest() -> Arc<RwLock<Account>> {
    Arc::new(RwLock::new(Account { id: ID_MIN, email: "".to_string() }))
  }

  pub fn local() -> Self {
    Self { request: None, timestamp: Context::since_the_epoch(), account: Self::guest() }
  }

  pub fn rest(request: actix_web::dev::RequestHead) -> Self {
    Self { request: Some(request), timestamp: Context::since_the_epoch(), account: Self::guest() }
  }

  pub fn websocket(request: actix_web::dev::RequestHead) -> Self {
    Self { request: Some(request), timestamp: Context::since_the_epoch(), account: Self::guest() }
  }

  fn since_the_epoch() -> Duration {
    let start = std::time::SystemTime::now();
    start.duration_since(std::time::UNIX_EPOCH).expect("Time went backwards")
  }
}

pub trait Services: Send + Sync {
  fn register(&mut self, service: Arc<dyn Service>);
  fn service<S: AsRef<str> + ToString>(&self, name: S) -> Arc<dyn Service>;
}

pub trait Service: Send + Sync {
  fn path(&self) -> &str;

  fn find(&self, ctx: Context, params: Params) -> Result;
  fn get(&self, ctx: Context, id: String, params: Params) -> Result;
  fn create(&self, ctx: Context, data: Data, params: Params) -> Result;
  fn update(&self, ctx: Context, id: String, data: Data, params: Params) -> Result;
  fn patch(&self, ctx: Context, id: String, data: Data, params: Params) -> Result;
  fn remove(&self, ctx: Context, id: String, params: Params) -> Result;

  fn enrich(&self, params: &Params) -> bool {
    self.params(params)["enrich"].as_bool().unwrap_or(true)
  }

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
      Err(_) => Err(Error::GeneralError(format!("invalid date '{str}'"))),
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
      usize::try_from(limit).unwrap_or(10).min(100)
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

  fn find(&self, _ctx: Context, _params: Params) -> Result {
    self.error()
  }

  fn get(&self, _ctx: Context, _id: String, _params: Params) -> Result {
    self.error()
  }

  fn create(&self, _ctx: Context, _data: Data, _params: Params) -> Result {
    self.error()
  }

  fn update(&self, _ctx: Context, _id: String, _data: Data, _params: Params) -> Result {
    self.error()
  }

  fn patch(&self, _ctx: Context, _id: String, _data: Data, _params: Params) -> Result {
    self.error()
  }

  fn remove(&self, _ctx: Context, _id: String, _params: Params) -> Result {
    self.error()
  }
}
