use crate::memories::Resolve;
use crate::storage::organizations::Workspace;
use json::JsonValue;
use service::error::Error;
use service::utils::json::JsonParams;
use std::collections::HashMap;
use store::balance::BalanceForGoods;
use store::elements::{Batch, Cost, Goods, Store, ToJson};
use uuid::Uuid;

pub(crate) fn find_items(
  ws: &Workspace,
  balances: &HashMap<Store, HashMap<Goods, HashMap<Batch, BalanceForGoods>>>,
  filter: &JsonValue,
  skip: usize,
) -> crate::services::Result {
  let items = if filter["category"].is_null() && filter["warehouse"].is_null() {
    find_wh_and_categories(balances, ws)
  } else if filter["category"].is_null() && filter["warehouse"].is_string() {
    find_categories(balances, &filter["warehouse"], ws)
  } else if filter["category"].is_string() && filter["warehouse"].is_null() {
    find_warehouses(balances, &filter["category"], ws)
  } else if filter["category"].is_string() && filter["warehouse"].is_string() {
    find_goods(balances, filter, ws)
  } else {
    return Err(Error::GeneralError("Wrong filter parameters".to_string()));
  };

  let total = items.len();

  log::debug!("fn_find_items: {items:?}");

  return Ok(json::object! {
      data: items,
      total: total,
      "$skip": skip,
  });
}

fn find_wh_and_categories(
  balances: &HashMap<Store, HashMap<Goods, HashMap<Batch, BalanceForGoods>>>,
  ws: &Workspace,
) -> Vec<JsonValue> {
  let mut storages = HashMap::new();
  let mut categories = HashMap::new();

  for (store, sb) in balances {
    for (goods, gb) in sb {
      for (_, bb) in gb {
        // workaround until get_balance_for_all remove zero balances
        if bb.is_zero() {
          continue;
        }
        let mut store_cost = storages.entry(store).or_insert(Cost::ZERO);
        store_cost += bb.cost;

        let _goods = goods.resolve_to_json_object(&ws);
        let category = _goods["category"].string();

        let mut category_cost = categories.entry(category).or_insert(Cost::ZERO);
        category_cost += bb.cost;
      }
    }
  }

  let mut items: Vec<JsonValue> = storages
    .keys()
    .map(|id| id.clone())
    .collect::<Vec<_>>()
    .into_iter()
    .map(|id| (id.resolve_to_json_object(&ws), id))
    .map(|(mut o, id)| {
      let cost = storages.remove(&id).unwrap_or_default();
      o["_cost"] = cost.to_json();
      o
    })
    .collect();

  items.sort_by(|a, b| {
    let a = a["name"].as_str().unwrap_or_default();
    let b = b["name"].as_str().unwrap_or_default();

    a.cmp(b)
  });

  let mut category_items: Vec<JsonValue> = categories
    .keys()
    .map(|id| id.clone())
    .collect::<Vec<_>>()
    .into_iter()
    .map(|id| (id.resolve_to_json_object(&ws), id))
    .map(|(mut o, id)| {
      let cost = categories.remove(&id).unwrap_or_default();
      o["_cost"] = cost.to_json();
      o
    })
    .collect();

  category_items.sort_by(|a, b| {
    let a = a["name"].as_str().unwrap_or_default();
    let b = b["name"].as_str().unwrap_or_default();

    a.cmp(b)
  });

  items.append(&mut category_items);

  items
}

fn find_categories(
  balances: &HashMap<Store, HashMap<Goods, HashMap<Batch, BalanceForGoods>>>,
  filter: &JsonValue,
  ws: &Workspace,
) -> Vec<JsonValue> {
  let warehouse_filter = filter.to_string();

  let mut categories = HashMap::new();
  for (store, sb) in balances {
    if store.to_string() != warehouse_filter {
      continue;
    }
    for (goods, gb) in sb {
      for (_, bb) in gb {
        // workaround until get_balance_for_all remove zero balances
        if bb.is_zero() {
          continue;
        }

        let _goods = goods.resolve_to_json_object(&ws);
        let category = _goods["category"].string();

        let mut cost = categories.entry(category).or_insert(Cost::ZERO);
        cost += bb.cost;
      }
    }
  }

  // order categories
  let mut items: Vec<JsonValue> = categories
    .keys()
    .map(|id| id.clone())
    .collect::<Vec<_>>()
    .into_iter()
    .map(|id| (id.resolve_to_json_object(&ws), id))
    .map(|(mut o, id)| {
      let cost = categories.remove(&id).unwrap_or_default();
      o["_cost"] = cost.to_json();
      o
    })
    .collect();

  items.sort_by(|a, b| {
    let a = a["name"].as_str().unwrap_or_default();
    let b = b["name"].as_str().unwrap_or_default();

    a.cmp(b)
  });

  items
}

fn find_warehouses(
  balances: &HashMap<Store, HashMap<Goods, HashMap<Batch, BalanceForGoods>>>,
  filter: &JsonValue,
  ws: &Workspace,
) -> Vec<JsonValue> {
  let mut storages = HashMap::new();

  let category_filter = filter.to_string();

  for (store, sb) in balances {
    for (goods, gb) in sb {
      for (_, bb) in gb {
        // workaround until get_balance_for_all remove zero balances
        if bb.is_zero() {
          continue;
        }
        let _goods = goods.resolve_to_json_object(&ws);
        let category = _goods["category"].string();

        if category != category_filter {
          continue;
        }

        let mut cost = storages.entry(store).or_insert(Cost::ZERO);
        cost += bb.cost;
      }
    }
  }

  let mut items: Vec<JsonValue> = storages
    .keys()
    .map(|id| id.clone())
    .collect::<Vec<_>>()
    .into_iter()
    .map(|id| (id.resolve_to_json_object(&ws), id))
    .map(|(mut o, id)| {
      let cost = storages.remove(&id).unwrap_or_default();
      o["_cost"] = cost.to_json();
      o
    })
    .collect();

  items.sort_by(|a, b| {
    let a = a["name"].as_str().unwrap_or_default();
    let b = b["name"].as_str().unwrap_or_default();

    a.cmp(b)
  });

  items
}

fn find_goods(
  balances: &HashMap<Store, HashMap<Goods, HashMap<Batch, BalanceForGoods>>>,
  filter: &JsonValue,
  ws: &Workspace,
) -> Vec<JsonValue> {
  let mut goods_list = Vec::new();

  let warehouse_filter = filter["warehouse"].to_string();
  let category_filter = filter["category"].to_string();

  for (store, sb) in balances {
    if store.to_string() != warehouse_filter {
      continue;
    }
    for (goods, gb) in sb {
      for (batch, bb) in gb {
        // workaround until get_balance_for_all remove zero balances
        if bb.is_zero() {
          continue;
        }
        let _goods = goods.resolve_to_json_object(&ws);
        let category = _goods["category"].string();

        if category != category_filter {
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

        let _goods = goods.resolve_to_json_object(&ws);

        let record = json::object! {
              _id: id.to_json(),
              storage: store.resolve_to_json_object(&ws),
              goods: _goods,
              batch: batch.to_json(),
              qty: json::object! { number: bb.qty.to_json() },
              cost: json::object! { number: bb.cost.to_json() },
        };
        goods_list.push(record);
      }
    }
  }
  goods_list
}
