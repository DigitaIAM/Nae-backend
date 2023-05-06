// #![allow(dead_code, unused_variables, unused_imports)]

use chrono::{DateTime, Datelike, Month, NaiveDate, Utc};
use json::{object, JsonValue};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use uuid::{uuid, Uuid};

pub use super::error::WHError;
use service::utils::time::{date_to_string, time_to_string};

use crate::GetWarehouse;
use service::{Context, Services};

use crate::agregations::{AggregationStore, AgregationStoreGoods};
use crate::balance::{BalanceDelta, BalanceForGoods, Cost};
use crate::batch::Batch;
use crate::operations::{InternalOperation, Op, OpMutation};
use service::utils::json::JsonParams;

pub type Goods = Uuid;
pub type Store = Uuid;
pub type Qty = Decimal;

pub(crate) const UUID_NIL: Uuid = uuid!("00000000-0000-0000-0000-000000000000");
pub(crate) const UUID_MAX: Uuid = uuid!("ffffffff-ffff-ffff-ffff-ffffffffffff");

pub trait ToJson {
  fn to_json(&self) -> JsonValue;
}

impl ToJson for Uuid {
  fn to_json(&self) -> JsonValue {
    JsonValue::String(self.to_string())
  }
}

impl ToJson for DateTime<Utc> {
  fn to_json(&self) -> JsonValue {
    JsonValue::String(date_to_string(*self))
  }
}

impl ToJson for Decimal {
  fn to_json(&self) -> JsonValue {
    JsonValue::String(self.to_string())
  }
}

impl ToJson for String {
  fn to_json(&self) -> JsonValue {
    JsonValue::String(self.clone())
  }
}

pub fn dt(date: &str) -> Result<DateTime<Utc>, WHError> {
  let res = DateTime::parse_from_rfc3339(format!("{date}T00:00:00Z").as_str())?.into();

  Ok(res)
}

pub(crate) fn first_day_current_month(date: DateTime<Utc>) -> DateTime<Utc> {
  let date = NaiveDate::from_ymd_opt(date.year(), date.month(), 1)
    .unwrap()
    .and_hms_opt(0, 0, 0)
    .unwrap();
  DateTime::<Utc>::from_utc(date, Utc)
}

pub(crate) fn first_day_next_month(date: DateTime<Utc>) -> DateTime<Utc> {
  let d = date.naive_local();
  let (year, month) = if d.month() == Month::December.number_from_month() {
    (d.year() + 1, Month::January.number_from_month())
  } else {
    (d.year(), d.month() + 1)
  };

  let date = NaiveDate::from_ymd_opt(year, month, 1).unwrap().and_hms_opt(0, 0, 0).unwrap();
  DateTime::<Utc>::from_utc(date, Utc)
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct NumberForGoods {
  qty: Qty,
  cost: Option<Cost>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum Mode {
  Auto,
  Manual,
}

impl ToJson for Mode {
  fn to_json(&self) -> JsonValue {
    match self {
      Mode::Auto => JsonValue::String("auto".to_string()),
      Mode::Manual => JsonValue::String("manual".to_string()),
    }
  }
}

pub(crate) trait KeyValueStore {
  fn key(&self, s: &String) -> Result<Vec<u8>, WHError>;
  fn store_date_type_batch_id(&self) -> Vec<u8>;
  fn date_type_store_batch_id(&self) -> Vec<u8>;
  fn value(&self) -> Result<String, WHError>;
}

pub enum ReturnType {
  Good(AgregationStoreGoods),
  Store(AggregationStore),
  Empty,
}

#[derive(Debug, PartialEq)]
pub struct Report {
  pub from_date: DateTime<Utc>,
  pub till_date: DateTime<Utc>,
  pub items: (AggregationStore, Vec<AgregationStoreGoods>),
}

impl ToJson for Report {
  fn to_json(&self) -> JsonValue {
    let mut arr = JsonValue::new_array();

    for agr in self.items.1.iter() {
      arr.push(agr.to_json()).unwrap();
    }

    object! {
      from_date: time_to_naive_string(self.from_date),
      till_date: time_to_naive_string(self.till_date),
      items: vec![self.items.0.to_json(), arr]
    }
  }
}

pub(crate) fn time_to_naive_string(time: DateTime<Utc>) -> String {
  let mut res = time.to_string();
  let _ = res.split_off(10);
  res
}

pub fn receive_data(
  app: &(impl GetWarehouse + Services),
  wid: &str,
  time: DateTime<Utc>,
  data: JsonValue,
  ctx: &Vec<String>,
  before: JsonValue,
) -> Result<JsonValue, WHError> {
  // TODO if structure of input Json is invalid, should return it without changes and save it to memories anyway
  // If my data was corrupted, should rewrite it and do the operations
  // TODO tests with invalid structure of incoming JsonValue
  log::debug!("BEFOR: {:?}", before.dump());
  log::debug!("AFTER: {:?}", data.dump());

  let old_data = data.clone();
  let mut new_data = data.clone();
  let mut new_before = before.clone();

  let before = match json_to_ops(app, wid, &mut new_before, time.clone(), ctx) {
    Ok(res) => res,
    Err(e) => {
      println!("_WHERROR_ BEFORE: {}", e.message());
      println!("{}", data.dump());
      return Ok(old_data);
    },
  };

  let mut after = match json_to_ops(app, wid, &mut new_data, time, ctx) {
    Ok(res) => res,
    Err(e) => {
      println!("_WHERROR_ AFTER: {}", e.message());
      println!("{}", data.dump());
      return Ok(old_data);
    },
  };

  log::debug!("OPS BEFOR: {before:?}");
  log::debug!("OPS AFTER: {after:?}");

  let mut before = before.into_iter();

  let mut ops: Vec<OpMutation> = Vec::new();

  while let Some(ref b) = before.next() {
    if let Some(a) = after.remove_entry(&b.0) {
      if a.1.store == b.1.store
        && a.1.goods == b.1.goods
        && a.1.batch == b.1.batch
        && a.1.date == b.1.date
        && a.1.store_into == b.1.store_into
      {
        ops.push(OpMutation::new_from_ops(Some(b.1.clone()), Some(a.1)));
      } else {
        ops.push(OpMutation::new_from_ops(Some(b.1.clone()), None));
        ops.push(OpMutation::new_from_ops(None, Some(a.1)));
      }
    } else {
      ops.push(OpMutation::new_from_ops(Some(b.1.clone()), None));
    }
  }

  let mut after = after.into_iter();

  while let Some(ref a) = after.next() {
    ops.push(OpMutation::new_from_ops(None, Some(a.1.clone())));
  }

  log::debug!("OPS: {:?}", ops);

  if ops.is_empty() {
    Ok(old_data)
  } else {
    app.warehouse().mutate(&ops)?;
    Ok(new_data)
  }
}

#[derive(PartialEq)]
enum OpType {
  Inventory,
  Receive,
  Dispatch,
  Transfer,
}

fn json_to_ops(
  app: &(impl GetWarehouse + Services),
  wid: &str,
  data: &mut JsonValue,
  time: DateTime<Utc>,
  ctx: &Vec<String>,
) -> Result<HashMap<String, Op>, WHError> {
  // log::debug!("json_to_ops {data:?}");

  let mut ops = HashMap::new();

  if !data.is_object() {
    return Ok(ops);
  }

  let ctx_str: Vec<&str> = ctx.iter().map(|s| s.as_str()).collect();

  log::debug!("ctx: {ctx_str:?}");

  let type_of_operation = match ctx_str[..] {
    ["warehouse", "receive"] => OpType::Receive,
    ["warehouse", "dispatch"] => OpType::Dispatch,
    ["warehouse", "transfer"] => OpType::Transfer,
    ["warehouse", "inventory"] => OpType::Inventory,
    ["production", "material", "produced"] => OpType::Receive,
    ["production", "material", "used"] => OpType::Dispatch,
    _ => return Ok(ops),
  };

  let params = object! {oid: wid, ctx: [], enrich: false };
  let document =
    match app.service("memories").get(Context::local(), data["document"].string(), params) {
      Ok(d) => d,
      Err(_) => return Ok(ops), // TODO handle IO error differently!!!!
    };

  log::debug!("DOCUMENT: {:?}", document.dump());

  let date = match document["date"].date_with_check() {
    Ok(d) => d,
    Err(_) => return Ok(ops),
  };

  let (store_from, store_into) = if type_of_operation == OpType::Transfer {
    let store_from = if data["storage_from"].string() == "" {
      match resolve_store(app, wid, &document, "from") {
        Ok(uuid) => uuid,
        Err(_) => return Ok(ops), // TODO handle errors better, allow to catch only 'not found'
      }
    } else {
      match resolve_store(app, wid, &data, "storage_from") {
        Ok(uuid) => uuid,
        Err(_) => return Ok(ops), // TODO handle errors better, allow to catch only 'not found'
      }
    };

    let store_into = if data["storage_into"].string() == "" {
      match resolve_store(app, wid, &document, "into") {
        Ok(uuid) => uuid,
        Err(_) => return Ok(ops), // TODO handle errors better, allow to catch only 'not found'
      }
    } else {
      match resolve_store(app, wid, &data, "storage_into") {
        Ok(uuid) => uuid,
        Err(_) => return Ok(ops), // TODO handle errors better, allow to catch only 'not found'
      }
    };

    (store_from, Some(store_into))
  } else if ctx.get(0) == Some(&"production".to_string()) {
    let store_from = match resolve_store(app, wid, &data, "storage_from") {
      Ok(uuid) => uuid,
      Err(_) => return Ok(ops), // TODO handle errors better, allow to catch only 'not found'
    };
    (store_from, None)
  } else {
    let store_from = match resolve_store(app, wid, &document, "storage") {
      Ok(uuid) => uuid,
      Err(_) => return Ok(ops), // TODO handle errors better, allow to catch only 'not found'
    };
    (store_from, None)
  };

  println!("store from: {store_from:?} into: {store_into:?}");

  let params = object! {oid: wid, ctx: vec!["goods"] };
  let item = match app.service("memories").get(Context::local(), data["goods"].string(), params) {
    Ok(d) => d,
    Err(_) => return Ok(ops), // TODO handle IO error differently!!!!
  };

  let goods = match item["_uuid"].uuid_or_none() {
    Some(uuid) => uuid,
    None => return Ok(ops),
  };

  // log::debug!("before op");

  let op = match type_of_operation {
    OpType::Inventory => {
      let qty = data["qty"]["number"].number_or_none();
      let cost = data["cost"]["number"].number_or_none();

      if qty.is_none() && cost.is_none() {
        return Ok(ops);
      } else {
        let (cost, mode) =
          if let Some(cost) = cost { (cost.into(), Mode::Manual) } else { (0.into(), Mode::Auto) };

        let qty = qty.unwrap_or_default();

        InternalOperation::Inventory(BalanceForGoods { qty, cost }, BalanceDelta::default(), mode)
      }
    },
    OpType::Receive => {
      let qty = data["qty"]["number"].number_or_none();
      let cost = data["cost"]["number"].number_or_none();

      if qty.is_none() && cost.is_none() {
        return Ok(ops);
      } else {
        InternalOperation::Receive(qty.unwrap_or_default(), cost.unwrap_or_default().into())
      }
    },
    OpType::Transfer | OpType::Dispatch => {
      let qty = data["qty"]["number"].number_or_none();
      let cost = data["cost"]["number"].number_or_none();

      if qty.is_none() && cost.is_none() {
        return Ok(ops);
      } else {
        let (cost, mode) =
          if let Some(cost) = cost { (cost.into(), Mode::Manual) } else { (0.into(), Mode::Auto) };
        InternalOperation::Issue(qty.unwrap_or_default(), cost, mode)
      }
    },
  };

  log::debug!("after op {op:?}");

  let tid = if let Some(tid) = data["_uuid"].uuid_or_none() {
    tid
  } else {
    let tid = Uuid::new_v4();
    data["_uuid"] = JsonValue::String(tid.to_string());
    tid
  };

  let batch = if type_of_operation == OpType::Receive {
    Batch { id: tid, date }
  } else if type_of_operation == OpType::Inventory {
    match &data["batch"] {
      JsonValue::Object(d) => Batch { id: d["_uuid"].uuid()?, date: d["date"].date_with_check()? },
      _ => Batch { id: UUID_NIL, date: dt("1970-01-01")? }, // TODO is it ok?
    }
  } else {
    match &data["batch"] {
      JsonValue::Object(d) => {
        // let params = object! {oid: oid["data"][0]["_id"].as_str(), ctx: vec!["warehouse", "receive", "document"] };
        // let doc_from = app.service("memories").get(d["_id"].string(), params)?;
        Batch { id: data["batch"]["_uuid"].uuid()?, date: data["batch"]["date"].date_with_check()? }
      },
      _ => Batch { id: UUID_NIL, date: dt("1970-01-01")? },
    }
  };

  data["batch"] = object! {
    "_uuid": batch.id.to_string(),
    "date": time_to_string(batch.date),
    "barcode": batch.to_barcode(),
  };

  // let mut dependant = vec![];
  // match &data["batches"] {
  //   JsonValue::Array(array) => {
  //     for batch in array {
  //       dependant.push(Batch { id: batch["id"].uuid()?, date: batch["date"].date_with_check()? });
  //     }
  //   },
  //   _ => (),
  // }

  let op = Op {
    id: tid,
    date,
    store: store_from,
    store_into,
    goods,
    batch,
    op,
    is_dependent: false, // data["is_dependent"].boolean(),
    dependant: vec![],
  };

  ops.insert(tid.to_string(), op);

  Ok(ops)
}

fn resolve_store(
  app: &impl Services,
  wid: &str,
  document: &JsonValue,
  name: &str,
) -> Result<Uuid, service::error::Error> {
  let store_id = document[name].string();

  log::debug!("store_id {name} {store_id:?}");

  let params = object! {oid: wid, ctx: vec!["warehouse", "storage"] };
  let storage = app.service("memories").get(Context::local(), store_id, params)?;
  log::debug!("storage {:?}", storage.dump());
  storage["_uuid"].uuid()
}
