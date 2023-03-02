use std::str::FromStr;

use chrono::{DateTime, Utc};
use json::JsonValue;
use rust_decimal::Decimal;
use uuid::Uuid;

use crate::error::Error;

pub trait JsonParams {
  fn string(&self) -> String;

  fn string_or_none(&self) -> Option<String>;

  fn uuid(&self) -> Result<Uuid, Error>;

  // fn uuid_from_datetime(&self) -> Uuid;

  fn uuid_or_none(&self) -> Option<Uuid>;

  fn number(&self) -> Decimal;
  fn number_or_none(&self) -> Option<Decimal>;

  fn date(&self) -> Result<DateTime<Utc>, Error>;
  fn datetime(&self) -> Result<DateTime<Utc>, Error>;
}

impl JsonParams for JsonValue {
  fn string(&self) -> String {
    self.as_str().unwrap_or("").to_string()
  }

  fn string_or_none(&self) -> Option<String> {
    self.as_str().map(|s| s.to_string())
  }

  fn uuid(&self) -> Result<Uuid, Error> {
    Ok(Uuid::try_parse(&self.string())?)
  }

  // fn uuid_from_datetime(&self) -> Uuid {
  //   let dt: DateTime<Utc> = DateTime::parse_from_rfc3339(format!("{}", self.string()).as_str()).unwrap_or_default().into();
  //   // Uuid::try_from(dt).unwrap_or_default()
  // }

  fn uuid_or_none(&self) -> Option<Uuid> {
    if let Some(s) = self.string_or_none() {
      if &s == "null" || &s == "" {
        // log::debug!("FN_UUID_OR_NONE EMPTY");
        None
      } else {
        if let Ok(res) = Uuid::try_parse(&s) {
          // log::debug!("FN_UUID_OR_NONE: {res:?}");
          Some(res)
        } else {
          None
        }
      }
    } else {
      // log::debug!("FN_UUID_OR_NONE FAILED TO PARSE STRING");
      None
    }
  }

  fn number(&self) -> Decimal {
    Decimal::from_str(&self.to_string()).unwrap_or_default()
  }

  fn number_or_none(&self) -> Option<Decimal> {
    if let Some(number) = self.as_number() {
      let number = number.to_string();
      match Decimal::from_str(&self.to_string()) {
        Ok(number) => Some(number),
        Err(_) => None,
      }
    } else {
      None
    }
  }

  fn date(&self) -> Result<DateTime<Utc>, Error> {
    let s = self.string();
    let dt = DateTime::parse_from_rfc3339(format!("{s}T00:00:00Z").as_str())?.into();

    Ok(dt)
  }

  fn datetime(&self) -> Result<DateTime<Utc>, Error> {
    let s = self.string();
    let dt = DateTime::parse_from_rfc3339(&s)?.into();

    Ok(dt)
  }
}

pub trait JsonMerge {
  fn merge(&self, patch: &JsonValue) -> JsonValue;
}

impl JsonMerge for JsonValue {
  fn merge(&self, patch: &JsonValue) -> JsonValue {
    if !patch.is_object() {
      return patch.clone();
    }

    let mut obj = self.clone();
    if !obj.is_object() {
      obj = JsonValue::new_object();
    }
    for (key, value) in patch.entries() {
      if value.is_null() {
        obj.remove(key);
      } else {
        // data.entry(key).or_insert(JsonValue::Null).merge(value);
        obj[key] = obj[key].merge(value);
      }
    }

    obj
  }
}
