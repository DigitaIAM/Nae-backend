use super::*;

use chrono::Utc;
use json::JsonValue;
use rust_decimal::Decimal;
use service::error::Error;
use service::utils::json::{JsonMerge, JsonParams};
use service::{Context, Service};
use std::collections::HashMap;
use std::sync::Arc;
use store::balance::BalanceForGoods;
use store::elements::ToJson;
use store::GetWarehouse;
use uuid::Uuid;

use crate::services::{Data, Params};

use crate::commutator::Application;

use stock::find_items;

// warehouse: { receiving, Put-away, transfer,  }
// production: { manufacturing }

pub struct MemoriesInFiles {
  app: Application,
  name: Arc<String>,
}

impl MemoriesInFiles {
  pub fn new(app: Application, name: &str) -> Arc<dyn Service> {
    Arc::new(MemoriesInFiles { app, name: Arc::new(name.to_string()) })
  }
}

impl Service for MemoriesInFiles {
  fn path(&self) -> &str {
    &self.name
  }

  fn find(&self, _ctx: Context, params: Params) -> crate::services::Result {
    // println!("find account {:?}", ctx.account.read().unwrap());

    let wsid = crate::services::oid(&params)?;
    let ctx = self.ctx(&params);

    let limit = self.limit(&params);
    let skip = self.skip(&params);

    let filter = self.params(&params)["filter"].clone();

    let reverse = self.params(&params)["reverse"].as_bool().unwrap_or(false);

    // workaround
    if ctx == vec!["drugs"] {
      let ws = self.app.wss.get(&wsid);

      let search = self.params(&params)["search"].as_str().unwrap_or_default();

      let (total, result) = {
        let engine = self.app.search.read().unwrap();
        engine.search(search, limit, skip)
      };

      let list: Vec<JsonValue> = result
        .into_iter()
        // .skip(skip)
        // .take(limit)
        .map(|id| id.resolve_to_json_object(&ws))
        .collect();

      return Ok(json::object! {
        data: JsonValue::Array(list),
        total: total,
        "$skip": skip,
      });
    }

    // workaround
    if ctx == vec!["warehouse", "stock"] {
      if skip != 0 {
        let list = vec![];
        let total = list.len();

        return Ok(json::object! {
          data: JsonValue::Array(list),
          total: total,
          "$skip": skip,
        });
      }

      let ws = self.app.wss.get(&wsid);

      let warehouse = self.app.warehouse().database;

      let balances = warehouse
        .get_balance_for_all(Utc::now())
        .map_err(|e| Error::GeneralError(e.message()))?;
      log::debug!("balances: {balances:?}");

      return find_items(&ws, &balances, &filter, skip);
    }

    let ws = self.app.wss.get(&wsid);
    let memories = ws.memories(ctx.clone());
    let list = memories.list(Some(reverse))?;

    let search = &self.params(&params)["search"];
    let filters = &self.params(&params)["filter"];
    let (total, mut list): (isize, Vec<JsonValue>) = if let Some(search) = search.as_str() {
      let mut total = 0;
      let list: Vec<JsonValue> = list
        .into_iter()
        .map(|o| o.json().unwrap_or_else(|_| JsonValue::Null))
        .filter(|o| o.is_object())
        .filter(|o| show_deleted(&ctx) || o["status"].string() != "deleted".to_string())
        .filter(|o| {
          for (_name, v) in o.entries() {
            if let Some(str) = v.as_str() {
              if str.contains(search) {
                return true;
              }
            }
          }
          return false;
        })
        .map(|o| {
          total += 1;
          o
        })
        .skip(skip)
        .take(limit)
        .collect::<_>();

      if list.is_empty() {
        (total, list)
      } else {
        (-1, list)
      }
    } else if filters.is_object() {
      let mut total = 0;
      let list: Vec<JsonValue> = list
        .into_iter()
        .map(|o| o.json().unwrap_or_else(|_| JsonValue::Null))
        .filter(|o| o.is_object())
        .filter(|o| show_deleted(&ctx) || o["status"].string() != "deleted".to_string())
        .filter(|o| filters.entries().all(|(n, v)| &o[n] == v))
        .map(|o| {
          total += 1;
          o
        })
        .skip(skip)
        .take(limit)
        .map(|o| o.enrich(&ws))
        .collect::<_>();

      if list.is_empty() {
        (total, list)
      } else {
        (-1, list)
      }
    } else {
      (
        list.len() as isize,
        list
          .into_iter()
          .skip(skip)
          .take(limit)
          .map(|o| o.json())
          // there shouldn't be status filter because we want to show all objects in relevant menu section (but not in pop up list)
          // .filter(|o| o.as_ref().unwrap()["status"].string() != "deleted".to_string())
          .collect::<Result<_, _>>()?,
      )
    };

    fn show_deleted(ctx: &Vec<String>) -> bool {
      let ctx: Vec<&str> = ctx.iter().map(|s| s.as_str()).collect();

      match ctx[..] {
        ["warehouse", "receive"] => true,
        ["warehouse", "transfer"] => true,
        ["warehouse", "dispatch"] => true,
        _ => false,
      }
    }

    // workaround: count produced
    if &ctx == &vec!["production", "order"] {
      let produced = self
        .app
        .wss
        .get(&wsid)
        .memories(vec!["production".into(), "produce".into()])
        .list(None)?;
      for order in &mut list {
        let filters = vec![("order", &order["_id"])];

        let mut boxes = 0_u32;
        let sum: Decimal = produced
          .iter()
          .map(|o| o.json().unwrap_or_else(|_| JsonValue::Null))
          .filter(|o| o.is_object())
          .filter(|o| filters.clone().into_iter().all(|(n, v)| &o[n] == v))
          .filter(|o| o["status"].string() != "deleted".to_string())
          .map(|o| o["qty"].number())
          .map(|o| {
            boxes += 1;
            o
          })
          .sum();

        // TODO rolls - kg, caps - piece
        order["produced"] = json::object! { "piece": sum.to_json(), "box": boxes.to_string() };
      }
    }

    // workaround: goods balance
    if &ctx == &vec!["goods"] {
      let warehouse = self.app.warehouse().database;

      let today = Utc::now();

      // let list_of_goods = list.iter().map(|goods| goods["_uuid"].uuid_or_none()).filter(|id| id.is_some()).map(|id| id.unwrap()).collect();

      let mut list_of_goods: Vec<Uuid> = Vec::new();
      for goods in &list {
        if let Some(uuid) = goods["_uuid"].uuid_or_none() {
          list_of_goods.push(uuid);
        }
      }

      let balances: HashMap<Uuid, BalanceForGoods> = warehouse
        .get_balance(today, &list_of_goods)
        .map_err(|e| Error::GeneralError(e.message()))?;

      for goods in &mut list {
        if let Some(uuid) = goods["_uuid"].uuid_or_none() {
          if let Some(balance) = balances.get(&uuid) {
            goods["_balance"] = balance.to_json();
          }
        }
      }
    }

    Ok(json::object! {
      data: JsonValue::Array(list),
      total: total,
      "$skip": skip,
    })
  }

  fn get(&self, _ctx: Context, id: String, params: Params) -> crate::services::Result {
    let oid = crate::services::oid(&params)?;
    let ctx = self.ctx(&params);
    let do_enrich = self.enrich(&params);

    if id.len() < 10 {
      return Err(Error::GeneralError(format!("id `{id}` not valid")));
    }

    let ws = self.app.wss.get(&oid);

    if let Some(memories) = ws.memories(ctx.clone()).get(&id) {
      if do_enrich {
        Ok(memories.json()?.enrich(&ws))
      } else {
        memories.json()
      }
    } else {
      Err(Error::GeneralError(format!("id `{id}` not found at {ctx:?}")))
    }
  }

  fn create(&self, _ctx: Context, data: Data, params: Params) -> crate::services::Result {
    let oid = crate::services::oid(&params)?;
    let ctx = self.ctx(&params);

    let ws = self.app.wss.get(&oid);

    let data = ws.memories(ctx).create(&self.app, data)?;

    Ok(data.enrich(&ws))
  }

  fn update(
    &self,
    _ctx: Context,
    id: String,
    data: Data,
    params: Params,
  ) -> crate::services::Result {
    if !data.is_object() {
      Err(Error::GeneralError("only object allowed".into()))
    } else {
      let oid = crate::services::oid(&params)?;
      let ctx = self.ctx(&params);

      if id.len() < 10 {
        return Err(Error::GeneralError(format!("id `{id}` not valid")));
      }

      let ws = self.app.wss.get(&oid);
      let memories = ws.memories(ctx);

      let data = memories.update(&self.app, id, data)?;

      Ok(data.enrich(&ws))
    }
  }

  fn patch(&self, _ctx: Context, id: String, data: Data, params: Params) -> crate::services::Result {
    let oid = crate::services::oid(&params)?;
    let ctx = self.ctx(&params);

    if id.len() < 10 {
      return Err(Error::GeneralError(format!("id `{id}` not valid")));
    }

    let ws = self.app.wss.get(&oid);
    let memories = ws.memories(ctx);

    if !data.is_object() {
      Err(Error::GeneralError("only object allowed".into()))
    } else {
      let doc = memories
        .get(&id)
        .ok_or(Error::GeneralError(format!("id '{id}' not found").into()))?;
      let mut obj = doc.json()?;

      let mut patch = data.clone();
      patch.remove("_id"); // TODO check id?

      obj = obj.merge(&patch);

      // for (n, v) in data.entries() {
      //   if n != "_id" {
      //     obj[n] = v.clone();
      //   }
      // }

      let data = memories.update(&self.app, id, obj)?;

      Ok(data.enrich(&ws))
    }
  }

  fn remove(&self, _ctx: Context, _id: String, _params: Params) -> crate::services::Result {
    Err(Error::NotImplemented)
  }
}
