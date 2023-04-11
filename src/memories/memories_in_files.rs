use super::*;

use chrono::Utc;
use json::JsonValue;
use rust_decimal::Decimal;
use service::error::Error;
use service::utils::json::{JsonMerge, JsonParams};
use service::Service;
use std::collections::HashMap;
use std::sync::Arc;
use store::balance::BalanceForGoods;
use store::elements::ToJson;
use store::GetWarehouse;
use tantivy::HasLen;
use uuid::Uuid;

use crate::services::{Data, Params};
use crate::storage::Workspaces;

use crate::commutator::Application;

// warehouse: { receiving, Put-away, transfer,  }
// production: { manufacturing }

pub struct MemoriesInFiles {
  app: Application,
  name: Arc<String>,

  wss: Workspaces,
}

impl MemoriesInFiles {
  pub fn new(app: Application, name: &str, ws: Workspaces) -> Arc<dyn Service> {
    Arc::new(MemoriesInFiles { app, name: Arc::new(name.to_string()), wss: ws })
  }
}

impl Service for MemoriesInFiles {
  fn path(&self) -> &str {
    &self.name
  }

  fn find(&self, params: Params) -> crate::services::Result {
    let wsid = crate::services::oid(&params)?;
    let ctx = self.ctx(&params);

    let limit = self.limit(&params);
    let skip = self.skip(&params);

    let reverse = self.params(&params)["reverse"].as_bool().unwrap_or(false);

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

      let warehouse = self.app.warehouse().database;

      let balances = warehouse
        .get_balance_for_all(Utc::now())
        .map_err(|e| Error::GeneralError(e.message()))?;

      let org = self.wss.get(&wsid);

      let mut list = vec![];
      for (store, sb) in balances {
        for (goods, gb) in sb {
          for (batch, bb) in gb {
            // workaround until get_balance_for_all remove zero balances
            if bb.is_zero() {
              continue;
            }

            // TODO: add date into this id
            let bytes: Vec<u8> = store
              .as_bytes()
              .into_iter()
              .zip(goods.as_bytes().into_iter().zip(batch.id.as_bytes().into_iter()))
              .map(|(a, (b, c))| a ^ b ^ c)
              .collect();

            let id = Uuid::from_bytes(bytes.try_into().unwrap_or_default());

            list.push(json::object! {
              _id: id.to_json(),
              storage: store.resolve_to_json_object(&org),
              goods: goods.resolve_to_json_object(&org),
              batch: batch.to_json(),
              qty: json::object! { number: bb.qty.to_json() },
              cost: json::object! { number: bb.cost.to_json() },
            })
          }
        }
      }

      let total = list.len();

      return Ok(json::object! {
        data: JsonValue::Array(list),
        total: total,
        "$skip": skip,
      });
    }

    let ws = self.wss.get(&wsid);
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
          .collect::<Result<_, _>>()?,
      )
    };

    // workaround: count produced
    if &ctx == &vec!["production", "order"] {
      let produced = self
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
          .map(|o| o["qty"].number())
          .map(|o| {
            boxes += 1;
            o
          })
          .sum();

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

  fn get(&self, id: String, params: Params) -> crate::services::Result {
    let oid = crate::services::oid(&params)?;
    let ctx = self.ctx(&params);

    if id.len() < 10 {
      return Err(Error::GeneralError(format!("id `{id}` not valid")));
    }

    let ws = self.wss.get(&oid);

    if let Some(memories) = ws.memories(ctx.clone()).get(&id) {
      Ok(memories.json()?.enrich(&ws))
    } else {
      Err(Error::GeneralError(format!("id `{id}` not found at {ctx:?}")))
    }
  }

  fn create(&self, data: Data, params: Params) -> crate::services::Result {
    let oid = crate::services::oid(&params)?;
    let ctx = self.ctx(&params);

    let ws = self.wss.get(&oid);

    let data = ws.memories(ctx).create(&self.app, data)?;

    Ok(data.enrich(&ws))
  }

  fn update(&self, id: String, data: Data, params: Params) -> crate::services::Result {
    if !data.is_object() {
      Err(Error::GeneralError("only object allowed".into()))
    } else {
      let oid = crate::services::oid(&params)?;
      let ctx = self.ctx(&params);

      if id.len() < 10 {
        return Err(Error::GeneralError(format!("id `{id}` not valid")));
      }

      let ws = self.wss.get(&oid);
      let memories = ws.memories(ctx);

      let data = memories.update(&self.app, id, data)?;

      Ok(data.enrich(&ws))
    }
  }

  fn patch(&self, id: String, data: Data, params: Params) -> crate::services::Result {
    let oid = crate::services::oid(&params)?;
    let ctx = self.ctx(&params);

    if id.len() < 10 {
      return Err(Error::GeneralError(format!("id `{id}` not valid")));
    }

    let ws = self.wss.get(&oid);
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

  fn remove(&self, _id: String, _params: Params) -> crate::services::Result {
    Err(Error::NotImplemented)
  }
}
