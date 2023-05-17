use crate::elements::{dt, Goods, ToJson, WHError, UUID_MAX, UUID_NIL};
use chrono::{DateTime, Utc};
use json::{object, JsonValue};
use serde::{Deserialize, Serialize};
use service::utils::json::JsonParams;
use uuid::Uuid;

#[derive(Debug, Clone, PartialEq, Eq, Ord, PartialOrd, Hash, Serialize, Deserialize)]
pub struct Batch {
  pub id: Uuid,
  pub date: DateTime<Utc>,
}

impl Batch {
  pub(crate) fn new() -> Self {
    Batch { id: Uuid::new_v4(), date: DateTime::<Utc>::MAX_UTC }
  }

  // TODO make constant
  pub fn no() -> Self {
    Batch { id: UUID_NIL, date: dt("1970-01-01").unwrap() }
  }

  pub(crate) fn MIN() -> Self {
    Batch { id: UUID_NIL, date: dt("1970-01-01").unwrap() }
  }

  pub(crate) fn MAX() -> Self {
    Batch { id: UUID_MAX, date: DateTime::<Utc>::MAX_UTC }
  }

  pub fn is_empty(&self) -> bool {
    self.id == UUID_NIL
  }

  fn from_json(json: &JsonValue) -> Result<Self, WHError> {
    if json.is_object() {
      Ok(Batch { id: json["id"].uuid()?, date: json["date"].date_with_check()? })
    } else {
      Err(WHError::new("fn from_json for Batch failed"))
    }
  }

  pub(crate) fn to_barcode(&self) -> String {
    let date = self.date.to_string();
    let mut id: String = self.id.to_string().chars().filter(|c| *c >= '0' && *c <= '9').collect();
    while id.len() < 5 {
      id.push('0');
    }
    format!("2{}{}{}{}", &date[2..4], &date[5..7], &date[8..10], &id[0..5])
  }

  pub(crate) fn to_bytes(&self, goods: &Goods) -> Vec<u8> {
    let dt = self.date.timestamp() as u64;

    goods
      .as_bytes()
      .iter()
      .chain(dt.to_be_bytes().iter())
      .chain(self.id.as_bytes().iter())
      .map(|b| *b)
      .collect()
  }
}

impl ToJson for Batch {
  fn to_json(&self) -> JsonValue {
    let barcode = self.to_barcode();
    object! {
      barcode: barcode.to_json(),
      id: self.id.to_json(),
      date: self.date.to_json()
    }
  }
}

pub(crate) fn min_batch() -> Vec<u8> {
  UUID_NIL
    .as_bytes()
    .iter()
    .chain(u64::MIN.to_be_bytes().iter())
    .chain(UUID_NIL.as_bytes().iter())
    .map(|b| *b)
    .collect()
}

pub(crate) fn max_batch() -> Vec<u8> {
  UUID_MAX
    .as_bytes()
    .iter()
    .chain(u64::MAX.to_be_bytes().iter())
    .chain(UUID_MAX.as_bytes().iter())
    .map(|b| *b)
    .collect()
}
