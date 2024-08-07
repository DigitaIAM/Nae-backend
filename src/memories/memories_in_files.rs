use super::*;

use chrono::Utc;
use json::{object, JsonValue};
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

use crate::links::GetLinks;
use stock::find_items;
use store::qty::Qty;
use values::c;
use values::c::IntoDomain;

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

      return Ok(object! {
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

        return Ok(object! {
          data: JsonValue::Array(list),
          total: total,
          "$skip": skip,
        });
      }

      let ws = self.app.wss.get(&wsid);

      let warehouse = self.app.warehouse().database;

      // log::debug!("__filters= {filter}");

      let balances = warehouse
        .get_balance_for_all(Utc::now())
        .map_err(|e| Error::GeneralError(e.message()))?;
      // log::debug!("balances: {balances:?}");

      return find_items(&ws, &balances, &filter, skip);
    }

    let ws = self.app.wss.get(&wsid);
    let memories = ws.memories(ctx.clone());
    let list = memories.list(Some(reverse))?;

    fn show_deleted(ctx: &Vec<String>) -> bool {
      let ctx: Vec<&str> = ctx.iter().map(|s| s.as_str()).collect();

      match ctx[..] {
        ["warehouse", "receive"] => true,
        ["warehouse", "transfer"] => true,
        ["warehouse", "dispatch"] => true,
        ["production", "material", "used"] => true,
        ["production", "material", "produced"] => true,
        ["production", "produce"] => true,
        _ => false,
      }
    }

    let search = &self.params(&params)["search"];
    let filters = &self.params(&params)["filter"];
    let (total, mut list): (isize, Vec<JsonValue>) = if let Some(search) = search.as_str() {
      let search = search.to_lowercase();
      let mut total = 0;
      let list: Vec<JsonValue> = list
        .into_iter()
        .map(|o| o.json().unwrap_or_else(|_| JsonValue::Null))
        .filter(|o| o.is_object())
        .filter(|o| show_deleted(&ctx) || o[c::STATUS].string().as_str() != c::DELETED)
        .filter(|o| {
          for (_name, v) in o.entries() {
            if let Some(str) = v.as_str() {
              if str.to_lowercase().contains(&search) {
                return true;
              }
            }
          }
          false
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
        .filter(|o| show_deleted(&ctx) || o[c::STATUS].string().as_str() != c::DELETED)
        .filter(|o| {
          filters.entries().all(|(n, v)| {
            if n == "$starts-with" {
              v.entries().all(|(n, v)| {
                o[n].as_str().map(|s| s.starts_with(v.as_str().unwrap_or(""))).unwrap_or(false)
              })
            } else {
              &o[n] == v
            }
          })
        })
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
          // .filter(|o| o.as_ref().unwrap()[STATUS].string() != "deleted".to_string())
          .collect::<Result<_, _>>()?,
      )
    };

    fn get_records(
      app: &impl GetLinks,
      id: Uuid,
      ctx: &Vec<String>,
      ws: &Workspace,
      filters: &Vec<(&str, &JsonValue)>,
    ) -> Result<Vec<JsonValue>, Error> {
      let links = app.links().get_source_links_for_ctx(id, ctx)?;

      let result: Vec<JsonValue> = links
        .iter()
        .map(|uuid| uuid.resolve_to_json_object(ws))
        .filter(|o| o.is_object())
        .filter(|o| filters.clone().into_iter().all(|(n, v)| &o[n] == v))
        .filter(|o| o[c::STATUS].string().as_str() != c::DELETED)
        .collect();

      // println!("get_records: {result:?}");

      Ok(result)
    }

    // workaround: count produced
    if &ctx == &vec!["production", "order"] {
      for order in &mut list {
        let order_uuid = order[c::UUID].uuid()?;

        let area_id = order["area"][c::ID].string();
        let product_goods_id = order["product"]["goods"][c::ID].string();

        let order_customer = order["customer"].string();
        let order_label = order["label"].string();

        // println!("order {order}");

        let produced =
          self.app.links().get_source_links_for_ctx(order_uuid, &c::P_PRODUCE.domain())?;

        // let mut boxes = Decimal::ZERO;
        let mut sum_produced: Qty = produced
          .iter()
          .map(|uuid| uuid.resolve_to_json_object(&ws))
          .filter(|o| o.is_object())
          .filter(|o| o[c::STATUS].string().as_str() != c::DELETED)
          .map(|o| o["qty"].clone().try_into().unwrap_or_else(|_| Qty::zero()))
          .sum();

        let filters = vec![("document", &order[c::ID])];

        let mut sum_used_materials: HashMap<String, (JsonValue, Qty)> = HashMap::new();

        let materials_used =
          get_records(&self.app, order_uuid, &c::PM_USED.domain(), &ws, &filters)?;

        let mut sum_material_used = Qty::zero();

        for rec in materials_used {
          let qty: Qty = rec["qty"].clone().try_into().unwrap_or_default();
          let goods = &rec["goods"];

          // println!("goods_id {product_goods_id} vs {}", goods[c::ID]);

          if product_goods_id == goods[c::ID].string() {
            if let Some(batch_id) = rec["batch"]["id"].uuid_or_none() {
              let origin = ws
                .resolve_uuid(&batch_id)
                .and_then(|s| s.json().ok())
                // .map(|data| data.enrich(&ws))
                .unwrap_or_else(|| {
                  json::object! {
                    "_uuid": batch_id.to_string(),
                    "_status": "not_found",
                  }
                });

              // println!("origin {origin}");
              // println!("area {area_id} vs {}", origin["area"].string());

              if area_id == origin["area"].string() {
                if order_customer == origin["customer"].string() {
                  if order_label == origin["label"].string() {
                    sum_produced -= &qty;
                    continue;
                  }
                }
              }
            };
          }

          let sum = sum_used_materials
            .entry(goods["_id"].string())
            .or_insert((goods.clone(), Qty::zero()));

          sum.1 += &qty;

          sum_material_used += &qty;
        }

        let used: Vec<JsonValue> = sum_used_materials
          .into_iter()
          .map(|(_k, v)| {
            object! {
              "goods": v.0,
              "qty": enrich_own_qty(&ws, v.1),
            }
          })
          .collect();

        let mut sum_produced_materials: HashMap<String, (JsonValue, Qty)> = HashMap::new();

        let materials_produced =
          get_records(&self.app, order_uuid, &c::PM_PRODUCED.domain(), &ws, &filters)?;

        let mut sum_material_produced = Qty::zero();

        for rec in materials_produced {
          let qty: Qty = rec["qty"].clone().try_into().unwrap_or_default();

          let goods = &rec["goods"];

          let sum = sum_produced_materials
            .entry(goods["_id"].string())
            .or_insert((goods.clone(), Qty::zero()));

          sum.1 += &qty;

          sum_material_produced += &qty;
        }

        let produced: Vec<JsonValue> = sum_produced_materials
          .into_iter()
          .map(|(_k, v)| {
            object! {
              "goods": v.0,
              "qty": enrich_own_qty(&ws, v.1),
            }
          })
          .collect();

        let delta = sum_material_produced.lower() + sum_produced.lower() - sum_material_used.lower();

        order["produced"] = enrich_own_qty(&ws, sum_produced.clone());
        order["_material"] = object! {
          "used": used,
          "produced": produced,
          "sum": object! {
            "used": enrich_own_qty(&ws, sum_material_used),
            "produced": enrich_own_qty(&ws, sum_material_produced),
            "delta": enrich_own_qty(&ws, delta),
          }
        };
      }
    }

    // workaround: goods balance
    if &ctx == &vec!["goods"] {
      let warehouse = self.app.warehouse().database;

      let today = Utc::now();

      // let list_of_goods = list.iter().map(|goods| goods[_UUID].uuid_or_none()).filter(|id| id.is_some()).map(|id| id.unwrap()).collect();

      let mut list_of_goods: Vec<Uuid> = Vec::new();
      for goods in &list {
        if let Some(uuid) = goods[c::UUID].uuid_or_none() {
          list_of_goods.push(uuid);
        }
      }

      let balances: HashMap<Uuid, BalanceForGoods> = warehouse
        .get_balance(today, &list_of_goods)
        .map_err(|e| Error::GeneralError(e.message()))?;

      for goods in &mut list {
        if let Some(uuid) = goods[c::UUID].uuid_or_none() {
          if let Some(balance) = balances.get(&uuid) {
            goods["_balance"] = balance.to_json();
          }
        }
      }
    }

    Ok(object! {
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
    let memories = ws.memories(ctx.clone());

    if !data.is_object() {
      Err(Error::GeneralError("only object allowed".into()))
    } else {
      let doc = memories.get(&id).ok_or(Error::GeneralError(format!("id '{id}' not found")))?;
      let obj = doc.json()?;

      let mut patch = data;
      patch.remove(c::ID); // TODO check id?

      let obj = obj.merge(&patch);

      // for (n, v) in data.entries() {
      //   if n != _ID {
      //     obj[n] = v.clone();
      //   }
      // }

      let data = memories.update(&self.app, id, obj.clone())?;

      Ok(data.enrich(&ws))
    }
  }

  fn remove(&self, _ctx: Context, _id: String, _params: Params) -> crate::services::Result {
    Err(Error::NotImplemented)
  }
}
