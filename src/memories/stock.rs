use crate::memories::Resolve;
use crate::storage::organizations::Workspace;
use json::{object, JsonValue};
use rust_decimal::Decimal;
use service::error::Error;
use service::utils::json::JsonParams;
use std::collections::{HashMap, HashSet};
use std::hash::Hash;
use std::str::FromStr;
use std::sync::RwLock;
use store::balance::BalanceForGoods;
use store::elements::{Batch, Cost, Goods, Store, ToJson};
use uuid::Uuid;

pub(crate) fn find_items(
  ws: &Workspace,
  balances: &HashMap<Store, HashMap<Goods, HashMap<Batch, BalanceForGoods>>>,
  filter: &JsonValue,
  skip: usize,
) -> crate::services::Result {
  println!("find_items filter: {filter:?}");

  let items = if filter.is_empty() {
    println!("storages_and_categories");
    storages_and_categories(balances, ws)
  } else {
    println!("find_elements");
    find_elements(balances, filter, ws)
  };

  let total = items.len();

  log::debug!("fn_find_items: {items:?}");

  return Ok(json::object! {
      data: items,
      total: total,
      "$skip": skip,
  });
}

fn find_elements(
  balances: &HashMap<Store, HashMap<Goods, HashMap<Batch, BalanceForGoods>>>,
  filter: &JsonValue,
  ws: &Workspace,
) -> Vec<JsonValue> {
  let mut storages = HashMap::new();
  let mut categories = HashMap::new();
  let mut goods_map = HashMap::new();
  let mut batches_list = Vec::new();

  'store: for (store, sb) in balances {
    for (label, value) in filter.entries() {
      if label == "storage" {
        let uuid = value.uuid_or_none();
        if uuid != None && uuid.unwrap() != *store {
          continue 'store;
        }
      }
    }
    'goods: for (goods, gb) in sb {
      'batch: for (batch, bb) in gb {
        // workaround until get_balance_for_all remove zero balances
        if bb.is_zero() {
          continue 'batch;
        }

        let _goods = goods.resolve_to_json_object(&ws);
        let category = _goods["category"].string();

        println!("filter_entries: {filter:?}");
        'label: for (label, value) in filter.entries() {
          // println!("label: {label} value: {value:?}");
          if label == "storage" {
            if filter.len() == 1 {
              let mut category_cost = categories.entry(category.clone()).or_insert(Cost::ZERO);
              category_cost += bb.cost;
            }
          } else if label == "category" {
            let cat = value.string();
            if cat != "" {
              if cat != category {
                continue 'goods;
              } else {
                if filter.len() == 1 {
                  let mut store_cost = storages.entry(*store).or_insert(Cost::ZERO);
                  store_cost += bb.cost;
                }
              }
            }
          } else if label == "stock" {
            let requested_goods = value.uuid().unwrap(); // TODO Handle Error?
            if requested_goods != *goods {
              continue 'goods;
            }

            let mut b =
              create_goods_with_category(ws, store, goods, _goods.clone(), batch, bb, "batch");

            batches_list.push(b);
            continue 'label;
          }
        }
        let record =
            // create_goods_with_category(ws, store, goods, _goods.clone(), batch, bb, "stock");
            create_empty_goods(ws, store, goods, _goods.clone());

        // let _ = goods_map.entry(record["_id"].to_string()).or_insert(record);

        let mut g = goods_map.entry(record["_id"].to_string()).or_insert(record);
        g["qty"]["number"] =
          (Decimal::from_str(&g["qty"]["number"].string()).unwrap_or_default() + bb.qty).to_json();
        g["cost"]["number"] =
          (Decimal::from_str(&g["cost"]["number"].string()).unwrap_or_default() + bb.cost).to_json();
      }
    }
  }
  let mut items: Vec<JsonValue> = Vec::new();

  if !batches_list.is_empty() {
    // println!("_batches: {batches_list:?}");
    items.append(&mut batches_list);
    // println!("_items1: {items:?}");
    return items;
  }

  if !storages.is_empty() {
    let mut storages_items: Vec<JsonValue> = process_and_sort(ws, storages, "storage");
    items.append(&mut storages_items);
  }

  if !categories.is_empty() {
    let mut category_items: Vec<JsonValue> = process_and_sort(ws, categories, "category");
    items.append(&mut category_items);
  }

  if !goods_map.is_empty() {
    println!("goods_map: {goods_map:?}");
    let mut goods_items: Vec<JsonValue> = goods_map.into_iter().map(|(_, v)| v).collect();

    goods_items.sort_by(|a, b| {
      let a = a["name"].as_str().unwrap_or_default();
      let b = b["name"].as_str().unwrap_or_default();

      a.cmp(b)
    });

    items.append(&mut goods_items);
  }

  // println!("_items2: {items:?}");
  items
}

fn create_empty_goods(ws: &Workspace, store: &Store, goods: &Goods, _goods: JsonValue) -> JsonValue {
  // TODO: add date into this id
  let bytes: Vec<u8> = store
    .as_bytes()
    .into_iter()
    .zip(goods.as_bytes().into_iter())
    .map(|(a, (b))| a ^ b)
    .collect();

  let id = Uuid::from_bytes(bytes.try_into().unwrap_or_default());

  json::object! {
    _id: id.to_json(),
    storage: store.resolve_to_json_object(ws),
    goods: _goods,
    qty: json::object! { number: Decimal::ZERO.to_json() },
    cost: json::object! { number: Decimal::ZERO.to_json() },
    _category: "stock",
  }
}

fn create_goods_with_category(
  ws: &Workspace,
  store: &Store,
  goods: &Goods,
  _goods: JsonValue,
  batch: &Batch,
  bb: &BalanceForGoods,
  category: &str,
) -> JsonValue {
  // TODO: add date into this id
  let bytes: Vec<u8> = store
    .as_bytes()
    .into_iter()
    .zip(goods.as_bytes().into_iter().zip(batch.id.as_bytes().into_iter()))
    .map(|(a, (b, c))| a ^ b ^ c)
    .collect();

  let id = Uuid::from_bytes(bytes.try_into().unwrap_or_default());

  json::object! {
    _id: id.to_json(),
    storage: store.resolve_to_json_object(ws),
    goods: _goods,
    batch: batch.to_json(),
    qty: json::object! { number: bb.qty.to_json() },
    cost: json::object! { number: bb.cost.to_json() },
    _category: category,
  }
}

fn storages_and_categories(
  balances: &HashMap<Store, HashMap<Goods, HashMap<Batch, BalanceForGoods>>>,
  ws: &Workspace,
) -> Vec<JsonValue> {
  let mut storages = HashMap::new();
  let mut categories = HashMap::new();

  let mut cache = Cache::new(ws);

  for (store, sb) in balances {
    for (goods, gb) in sb {
      for (_, bb) in gb {
        // workaround until get_balance_for_all remove zero balances
        if bb.is_zero() {
          continue;
        }

        let storage = cache.resolve_uuid(store);
        if let Some(id) = storage["location"].as_str() {
          let mut stack = HashSet::new();
          let mut current = id.to_owned();
          let top = loop {
            if stack.insert(current.clone()) {
              let storage = cache.resolve_str(id);
              if let Some(id) = storage["location"].as_str() {
                current = id.to_owned();
              } else {
                break current;
              }
            } else {
              // recursion detected, break
              continue;
            }
          };

          let storage = cache.resolve_str(top.as_str());
          match storage["_uuid"].uuid() {
            Ok(id) => {
              let mut store_cost = storages.entry(id).or_insert(Cost::ZERO);
              store_cost += bb.cost;
            },
            Err(_) => {
              // error detected, ignore for now
            },
          }

          continue;
        }

        let mut store_cost = storages.entry(*store).or_insert(Cost::ZERO);
        store_cost += bb.cost;

        let _goods = goods.resolve_to_json_object(&ws);
        let category = _goods["category"].string();

        let mut category_cost = categories.entry(category).or_insert(Cost::ZERO);
        category_cost += bb.cost;
      }
    }
  }

  let mut goods_items = process_and_sort(ws, storages, "storage");
  let mut category_items = process_and_sort(ws, categories, "category");

  [category_items, goods_items].concat()
}

fn process_and_sort<K, V>(ws: &Workspace, mut map: HashMap<K, V>, cat: &str) -> Vec<JsonValue>
where
  K: Resolve + Hash + Eq + PartialEq + Clone,
  V: ToJson + Default,
{
  let mut items: Vec<JsonValue> = map
    .keys()
    .map(|id| id.clone())
    .collect::<Vec<_>>()
    .into_iter()
    .map(|id| (id.resolve_to_json_object(&ws), id))
    .map(|(mut o, id)| {
      let cost = map.remove(&id).unwrap_or_default();
      o["_cost"] = cost.to_json();
      o["_category"] = cat.into();
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

struct Cache<'a> {
  ws: &'a Workspace,
  map: RwLock<HashMap<String, JsonValue>>,
}

impl<'a> Cache<'a> {
  fn new(ws: &'a Workspace) -> Self {
    Cache { ws, map: RwLock::new(HashMap::new()) }
  }

  fn resolve_uuid(&self, id: &Uuid) -> JsonValue {
    let mut cache = self.map.write().unwrap();

    cache
      .entry(id.to_string())
      .or_insert_with(|| id.resolve_to_json_object(self.ws))
      .clone()
  }

  fn resolve_str(&self, id: &str) -> JsonValue {
    let mut cache = self.map.write().unwrap();

    cache
      .entry(id.to_string())
      .or_insert_with(|| id.resolve_to_json_object(self.ws))
      .clone()
  }
}
