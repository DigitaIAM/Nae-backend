use super::*;

use chrono::Utc;
use json::{object, JsonValue};
use rust_decimal::Decimal;
use service::error::Error;
use service::utils::json::{JsonMerge, JsonParams};
use service::{Context, Service, Services};
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
use store::error::WHError;
use store::qty::Qty;
use values::constants::{_ID, _STATUS, _UUID};

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

    let search = &self.params(&params)["search"];
    let filters = &self.params(&params)["filter"];
    let (total, mut list): (isize, Vec<JsonValue>) = if let Some(search) = search.as_str() {
      let search = search.to_lowercase();
      let mut total = 0;
      let list: Vec<JsonValue> = list
        .into_iter()
        .map(|o| o.json().unwrap_or_else(|_| JsonValue::Null))
        .filter(|o| o.is_object())
        .filter(|o| show_deleted(&ctx) || o[_STATUS].string() != *"deleted")
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
        .filter(|o| show_deleted(&ctx) || o[_STATUS].string() != *"deleted")
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

    fn show_deleted(ctx: &Vec<String>) -> bool {
      let ctx: Vec<&str> = ctx.iter().map(|s| s.as_str()).collect();

      match ctx[..] {
        ["warehouse", "receive"] => true,
        ["warehouse", "transfer"] => true,
        ["warehouse", "dispatch"] => true,
        _ => false,
      }
    }

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
        .filter(|o| o[_STATUS].string() != *"deleted")
        .collect();

      // println!("get_records: {result:?}");

      Ok(result)
    }

    // workaround: count produced
    if &ctx == &vec!["production", "order"] {
      for mut order in &mut list {
        let order_uuid = order[_UUID].uuid()?;

        let produced = self
          .app
          .links()
          .get_source_links_for_ctx(order_uuid, &vec!["production".into(), "produce".into()])?;

        let mut boxes = Decimal::ZERO;
        let sum_produced: Decimal = produced
          .iter()
          .map(|uuid| uuid.resolve_to_json_object(&ws))
          .filter(|o| o.is_object())
          .filter(|o| o[_STATUS].string() != *"deleted")
          .map(|o| {
            let qty: Qty = o["qty"].clone().try_into().unwrap_or_default();
            let mut pieces = Decimal::ZERO;
            qty.inner.iter().for_each(|q| {
              boxes += q.number;
              if let Some(number) = q.name.number() {
                pieces += number;
              }
            });
            pieces
          })
          .sum();

        // TODO rolls - kg, caps - piece
        order["produced"] = object! { "piece": sum_produced.to_json(), "box": boxes.to_string() };

        // workaround: ignore all areas except "экструдер"
        // let area = order["area"].string().resolve_to_json_object(&ws);
        //
        // if area["name"].string() != "экструдер".to_string() {
        //   continue;
        // }

        let filters = vec![("document", &order[_ID])];

        let mut sum_used_materials: HashMap<String, (JsonValue, Qty)> = HashMap::new();

        let materials_used = get_records(
          &self.app,
          order_uuid,
          &vec!["production".into(), "material".into(), "used".into()],
          &ws,
          &filters,
        )?;

        let mut sum_material_used = Qty::new(vec![]);

        for material_used in materials_used {
          let qty: Qty = material_used["qty"]
            .clone()
            .try_into()
            .map_err(|e: WHError| Error::GeneralError(e.message()))?;

          let goods = material_used["goods"].clone();

          let s = sum_used_materials
            .entry(goods["name"].string())
            .or_insert((goods, Qty::new(vec![])));

          s.1 = s.1.agregate(&qty);

          sum_material_used = sum_material_used.agregate(&qty);
        }

        // println!("__used__ {sum_used_materials:?}");

        let used: Vec<JsonValue> = sum_used_materials
          .into_iter()
          .map(|(_k, mut v)| {
            let mut q: JsonValue = (&v.1).into();
            for element in q.members_mut() {
              enrich_qty(&ws, element);
            }
            v.0["used"] = q;
            v.0
          })
          .collect();

        let mut sum_produced_materials: HashMap<String, (JsonValue, Qty)> = HashMap::new();

        let materials_produced = get_records(
          &self.app,
          order_uuid,
          &vec!["production".into(), "material".into(), "produced".into()],
          &ws,
          &filters,
        )?;

        let mut sum_material_produced = Qty::new(vec![]);

        println!("before_agreagte {:?}", materials_produced.clone());
        for material_produced in &materials_produced {
          let qty: Qty = material_produced["qty"]
            .clone()
            .try_into()
            .map_err(|e: WHError| Error::GeneralError(e.message()))?;

          let goods = material_produced["goods"].clone();

          let s = sum_produced_materials
            .entry(goods["name"].string())
            .or_insert((goods, Qty::new(vec![])));

          s.1 = s.1.agregate(&qty);

          sum_material_produced = sum_material_produced.agregate(&qty);
        }
        println!("after_agreagte {:?}", sum_produced_materials.clone());

        let produced: Vec<JsonValue> = sum_produced_materials
          .into_iter()
          .map(|(_k, mut v)| {
            let mut q: JsonValue = (&v.1).into();
            for element in q.members_mut() {
              enrich_qty(&ws, element);
            }
            v.0["produced"] = q;
            v.0
          })
          .collect();

        // let delta = sum_material_produced + sum_produced - sum_material_used;
        let delta = Decimal::ZERO;

        order["_material"] = object! {
          "used": used,
          "produced": produced,
        };

        let mut json_sum_material_used: JsonValue = (&sum_material_used).into();
        for element in json_sum_material_used.members_mut() {
          enrich_qty(&ws, element);
        }
        println!("json_sum_material_used {json_sum_material_used:?}");

        let mut json_sum_material_produced: JsonValue = (&sum_material_produced).into();
        for element in json_sum_material_produced.members_mut() {
          enrich_qty(&ws, element);
        }

        order["_material"]
          .insert(
            "sum",
            object! {
              "used": json_sum_material_used,
              "produced": json_sum_material_produced,
              "delta": delta.to_json()
            },
          )
          .map_err(|e| Error::GeneralError(e.to_string()))?;
      }
    }

    // workaround: goods balance
    if &ctx == &vec!["goods"] {
      let warehouse = self.app.warehouse().database;

      let today = Utc::now();

      // let list_of_goods = list.iter().map(|goods| goods[_UUID].uuid_or_none()).filter(|id| id.is_some()).map(|id| id.unwrap()).collect();

      let mut list_of_goods: Vec<Uuid> = Vec::new();
      for goods in &list {
        if let Some(uuid) = goods[_UUID].uuid_or_none() {
          list_of_goods.push(uuid);
        }
      }

      let balances: HashMap<Uuid, BalanceForGoods> = warehouse
        .get_balance(today, &list_of_goods)
        .map_err(|e| Error::GeneralError(e.message()))?;

      for goods in &mut list {
        if let Some(uuid) = goods[_UUID].uuid_or_none() {
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
      let mut obj = doc.json()?;

      let mut patch = data;
      patch.remove(_ID); // TODO check id?

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
