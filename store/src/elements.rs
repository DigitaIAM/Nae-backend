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

use crate::aggregations::{AggregationStore, AgregationStoreGoods};
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

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
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

  if data["status"].string() == "deleted".to_string() {
    return Ok(old_data);
  }

  let before = match json_to_ops(app, wid, &new_before, ctx) {
    Ok(res) => res,
    Err(e) => {
      println!("_WHERROR_ BEFORE: {}", e.message());
      println!("{}", data.dump());
      return Ok(old_data);
    },
  };

  let mut after = match json_to_ops(app, wid, &new_data, ctx) {
    Ok(res) => res,
    Err(e) => {
      println!("_WHERROR_ AFTER: {}", e.message());
      println!("{}", data.dump());
      return Ok(old_data);
    },
  };

  log::debug!("OPS BEFOR: {before:#?}");
  log::debug!("OPS AFTER: {after:#?}");

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

  log::debug!("OPS: {:#?}", ops);

  if ops.is_empty() {
    Ok(old_data)
  } else {
    app.warehouse().mutate(&ops)?;
    Ok(new_data)
  }
}

#[derive(PartialEq, Clone)]
enum OpType {
  Inventory,
  Receive,
  Dispatch,
  Transfer,
}

fn json_to_ops(
  app: &(impl GetWarehouse + Services),
  wid: &str,
  data: &JsonValue,
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
    ["production", "produce"] => OpType::Receive,
    ["production", "material", "produced"] => OpType::Receive,
    ["production", "material", "used"] => OpType::Dispatch,
    _ => return Ok(ops),
  };

  let params = object! {oid: wid, ctx: [], enrich: false };
  let mut document = match ctx_str[..] {
    ["production", "produce"] => {
      match app
        .service("memories")
        .get(Context::local(), data["order"].string(), params.clone())
      {
        Ok(d) => d,
        Err(_) => return Ok(ops), // TODO handle IO error differently!!!!
      }
    },
    _ => {
      match app.service("memories").get(Context::local(), data["document"].string(), params) {
        Ok(d) => d,
        Err(_) => return Ok(ops), // TODO handle IO error differently!!!!
      }
    },
  };

  log::debug!("DOCUMENT: {:?}", document.dump());

  // let date = match document["date"].date_with_check() {
  //   Ok(d) => d,
  //   Err(_) => return Ok(ops),
  // };

  let date = match ctx_str[..] {
    ["production", "produce"] => match data["date"].date_with_check() {
      Ok(d) => d,
      Err(_) => return Ok(ops),
    },
    _ => match document["date"].date_with_check() {
      Ok(d) => d,
      Err(_) => return Ok(ops),
    },
  };

  let (store_from, store_into) =
    match storages(app, wid, &ctx, data, &document, type_of_operation.clone()) {
      Ok((from, into)) => (from, into),
      Err(_) => return Ok(ops),
    };

  println!("store from: {store_from:?} into: {store_into:?}");

  let goods = match goods(app, wid, data, &document, ctx_str.clone()) {
    Ok(g) => g,
    Err(_) => return Ok(ops),
  };

  let goods_uuid = match goods["_uuid"].uuid_or_none() {
    Some(uuid) => uuid,
    None => return Ok(ops),
  };

  log::debug!("before op");

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
      let qty = match ctx_str[..] {
        ["production", "produce"] => data["qty"].number_or_none(),
        _ => data["qty"]["number"].number_or_none(),
      };
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
    return Ok(ops);
  };

  let batch = if type_of_operation == OpType::Receive {
    if ctx.get(1) == Some(&"produce".to_string()) {
      match document["_uuid"].uuid_or_none() {
        Some(id) => Batch { id, date },
        None => {
          let did = Uuid::new_v4();
          document["_uuid"] = JsonValue::String(did.to_string());
          // patch order
          let _app = app.service("memories");
          let params = object! {oid: wid, ctx: vec!["production", "produce"] };
          let _doc = _app.patch(Context::local(), document["_id"].string(), document, params)?;
          log::debug!("__doc {:#?}", _doc.dump());

          // save_data(
          //   app,
          //   &_app.ws,
          //   &_app.top_folder,
          //   &folder,
          //   &_app.ctx,
          //   &document["_id"].to_string(),
          //   Some(did),
          //   time,
          //   data,
          // )?

          Batch { id: _doc["_uuid"].uuid()?, date }
        },
      }
    } else {
      Batch { id: tid, date }
    }
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
    goods: goods_uuid,
    batch,
    op,
    is_dependent: false, // data["is_dependent"].boolean(),
    dependant: vec![],
  };

  ops.insert(tid.to_string(), op);

  Ok(ops)
}

fn storages(
  app: &(impl GetWarehouse + Services),
  wid: &str,
  ctx: &Vec<String>,
  data: &JsonValue,
  document: &JsonValue,
  type_of_operation: OpType,
) -> Result<(Uuid, Option<Uuid>), WHError> {
  let params = object! {oid: wid, ctx: [], enrich: false };

  return if type_of_operation == OpType::Transfer {
    let store_from = if data["storage_from"].string() == "" {
      match resolve_store(app, wid, document, "from") {
        Ok(uuid) => uuid,
        Err(_) => return Err(WHError::new("no from store")), // TODO handle errors better, allow to catch only 'not found'
      }
    } else {
      match resolve_store(app, wid, &data, "storage_from") {
        Ok(uuid) => uuid,
        Err(_) => return Err(WHError::new("no from store")), // TODO handle errors better, allow to catch only 'not found'
      }
    };

    let store_into = if data["storage_into"].string() == "" {
      match resolve_store(app, wid, document, "into") {
        Ok(uuid) => uuid,
        Err(_) => return Err(WHError::new("no into store")), // TODO handle errors better, allow to catch only 'not found'
      }
    } else {
      match resolve_store(app, wid, &data, "storage_into") {
        Ok(uuid) => uuid,
        Err(_) => return Err(WHError::new("no into store")), // TODO handle errors better, allow to catch only 'not found'
      }
    };

    Ok((store_from, Some(store_into)))
  } else if ctx.get(0) == Some(&"production".to_string()) {
    let store_from = if ctx.get(1) == Some(&"material".to_string()) {
      match resolve_store(app, wid, &data, "storage_from") {
        Ok(uuid) => uuid,
        Err(_) => return Err(WHError::new("no from store")), // TODO handle errors better, allow to catch only 'not found'
      }
    } else if ctx.get(1) == Some(&"produce".to_string()) {
      //*********["production", "produce"]***********
      let area = match app.service("memories").get(
        Context::local(),
        document["area"].clone().string(),
        params,
      ) {
        Ok(d) => d,
        Err(_) => return Err(WHError::new("no area in production")), // TODO handle IO error differently!!!!
      };
      match resolve_store(app, wid, &area, "storage") {
        Ok(uuid) => uuid,
        Err(_) => return Err(WHError::new("no storage in production")), // TODO handle errors better, allow to catch only 'not found'
      }
    } else {
      return Err(WHError::new("unknown context in production"));
    };

    Ok((store_from, None))
  } else {
    let store_from = match resolve_store(app, wid, document, "storage") {
      Ok(uuid) => uuid,
      Err(_) => return Err(WHError::new("no from store")), // TODO handle errors better, allow to catch only 'not found'
    };
    Ok((store_from, None))
  };
}

fn goods(
  app: &(impl GetWarehouse + Services),
  wid: &str,
  data: &JsonValue,
  document: &JsonValue,
  ctx_str: Vec<&str>,
) -> Result<JsonValue, WHError> {
  let params = object! {oid: wid, ctx: [], enrich: false };
  let goods_params = object! {oid: wid, ctx: vec!["goods"] };

  match ctx_str[..] {
    ["production", "produce"] => {
      let product =
        match app
          .service("memories")
          .get(Context::local(), document["product"].string(), params)
        {
          Ok(d) => d,
          Err(e) => return Err(WHError::new(&e.to_string())), // TODO handle IO error differently!!!!
        };

      if let Some(goods) = product["goods"].string_or_none() {
        Ok(app.service("memories").get(Context::local(), goods, goods_params)?)
      } else {
        Err(WHError::new("No data for goods"))
        // return if let (Some(customer), Some(label)) =
        //   (data["customer"].string_or_none(), data["label"].string_or_none())
        // {
        //   let goods_name =
        //     format!("{} {} {customer} {label}", product["name"], product["part_number"])
        //       .replace(",", "")
        //       .replace(".", "")
        //       .replace("  ", " ");
        //
        //   let params = object! {
        //     oid: wid,
        //     ctx: ["goods"],
        //     limit: 1,
        //     skip: 0,
        //     filter: object!{name: goods_name.clone()},
        //   };
        //
        //   let find = app.service("memories").find(Context::local(), params)?;
        //
        //   match find["data"].len() {
        //     1 => {
        //       // log::debug!("_old_ {:?}", find["data"][0]);
        //       Ok(find["data"][0].clone())
        //     },
        //     0 => {
        //       let goods = object! {
        //         name: goods_name,
        //         uom: product["uom"].clone(),
        //       };
        //       let params = object! {oid: wid, ctx: ["goods"]};
        //       let res = app.service("memories").create(Context::local(), goods, params)?;
        //       // log::debug!("_new_ {res:?}");
        //       Ok(res)
        //     },
        //     _ => Err(WHError::new("More than one goods with same name")),
        //   }
        // } else {
        //   Err(WHError::new("No data for goods"))
        // };
      }
    },
    _ => Ok(
      app
        .service("memories")
        .get(Context::local(), data["goods"].string(), goods_params)?,
    ),
  }
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
