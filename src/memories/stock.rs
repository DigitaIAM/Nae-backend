use crate::memories::Resolve;
use crate::storage::organizations::Workspace;
use json::JsonValue;
use service::utils::json::JsonParams;
use std::collections::{HashMap, HashSet};
use std::hash::Hash;
use std::sync::RwLock;
use store::balance::{BalanceForGoods, Cost};
use store::batch::Batch;
use store::elements::{Goods, Store, ToJson};
use uuid::Uuid;

pub(crate) fn find_items(
  ws: &Workspace,
  balances: &HashMap<Store, HashMap<Goods, HashMap<Batch, BalanceForGoods>>>,
  filters: &JsonValue,
  skip: usize,
) -> crate::services::Result {
  println!("find_items filter: {filters:?}");

  let items = process(balances, filters, ws);
  let total = items.len();

  log::debug!("fn_find_items: {items:?}");

  Ok(json::object! {
      data: items,
      total: total,
      "$skip": skip,
  })
}

fn process(
  balances: &HashMap<Store, HashMap<Goods, HashMap<Batch, BalanceForGoods>>>,
  filters: &JsonValue,
  ws: &Workspace,
) -> Vec<JsonValue> {
  let mut storages_aggregation = HashMap::new();
  let mut categories_aggregation = HashMap::new();
  let mut goods_aggregation = HashMap::new();
  let mut batches_aggregation = HashMap::new();

  let cache = Cache::new(ws);

  let store_filter = filters["storage"].uuid_or_none();
  let cat_filter = filters["category"].uuid_or_none();
  let goods_filter = filters["goods"].uuid_or_none();

  for (store, sb) in balances {
    for (goods, gb) in sb {
      for (batch, bb) in gb {
        // workaround until get_balance_for_all remove zero balances
        if bb.is_zero() {
          continue;
        }

        // filtering

        // storage
        let (store_top, before, store_found) = top_and_before(&cache, *store, store_filter);
        // println!("store {store:?} store_top {store_top:?} before {before:?} found {store_found}");
        if store_filter.is_some() && !store_found {
          continue;
        }
        let store_uuid = before.unwrap_or(store_top);

        // category
        let goods_obj = goods.resolve_to_json_object(ws);
        let category_id = goods_obj["category"].string();
        let category_obj = category_id.resolve_to_json_object(ws);
        if let Some("") = filters["category"].as_str() {
          if !category_id.is_empty() {
            continue;
          }
        } else if let Some(filter) = cat_filter {
          if let Some(uuid) = category_obj["_uuid"].uuid_or_none() {
            if uuid != filter {
              continue;
            }
          } else {
            continue;
          }
        }

        // goods
        if let Some(filter) = goods_filter {
          if *goods != filter {
            continue;
          }
        }

        // aggregate
        let cost = storages_aggregation.entry(store_uuid).or_insert(Cost::ZERO);
        *cost += bb.cost;

        let cost = categories_aggregation.entry(category_id).or_insert(Cost::ZERO);
        *cost += bb.cost;

        let balance = goods_aggregation.entry(*goods).or_insert(BalanceForGoods::default());
        balance.qty += bb.qty;
        balance.cost += bb.cost;

        if goods_filter.is_some() {
          let balance = batches_aggregation
            .entry((*store, *goods, batch.clone()))
            .or_insert(BalanceForGoods::default());
          balance.qty += bb.qty;
          balance.cost += bb.cost;
        }
      }
    }
  }

  // workaround: remove filtering storage at aggregation
  if let Some(store) = store_filter.as_ref() {
    storages_aggregation.remove(store);
  }

  let storages_items = process_and_sort(ws, storages_aggregation, "storage", "_cost");
  let categories_items = process_and_sort(ws, categories_aggregation, "category", "_cost");
  let goods_items = process_and_sort(ws, goods_aggregation, "goods", "_balance");
  let batch_items = process_and_sort(ws, batches_aggregation, "batch", "_balance");

  if goods_filter.is_some() {
    println!("return - batches");
    batch_items // stores?
  } else if store_filter.is_none() || categories_items.len() > 1 {
    if storages_items.len() > 1 {
      if categories_items.len() > 1 {
        println!("return - storages + categories | categories = {categories_items:?}");
        [storages_items, categories_items].concat()
      } else {
        println!("return - storages + goods");
        [storages_items, goods_items].concat()
      }
    } else if categories_items.len() > 1 {
      println!("return - categories");
      [categories_items].concat()
    } else {
      println!("return - goods");
      [goods_items].concat()
    }
  } else {
    println!("return - storages + goods");
    [storages_items, goods_items].concat()
  }
}

fn top_and_before(cache: &Cache, store: Store, filter: Option<Uuid>) -> (Uuid, Option<Uuid>, bool) {
  if filter.is_some() && Some(store) == filter {
    return (store, None, true);
  }

  let mut prev_uuid = Some(store);

  let storage = cache.resolve_uuid(store);
  if let Some(id) = storage["location"].as_str() {
    let mut current_id = id.to_owned();

    let mut stack = HashSet::new();
    let top = loop {
      if stack.insert(current_id.clone()) {
        let current_storage = cache.resolve_str(current_id.as_str());

        // TODO review it, bad unwrap_or code
        let uuid = current_storage["_uuid"].uuid_or_none().unwrap_or(store);
        if filter.is_some() && Some(uuid) == filter {
          return (uuid, prev_uuid, true);
        }
        prev_uuid = Some(uuid);

        if let Some(id) = current_storage["location"].as_str() {
          // check next id
          current_id = id.to_owned();
        } else {
          break uuid;
        }
      } else {
        // recursion detected, break
        break store; // TODO review it, bad code
      }
    };

    (top, None, false)
  } else {
    (store, None, false)
  }
}

fn process_and_sort<K, V>(
  ws: &Workspace,
  mut map: HashMap<K, V>,
  cat: &str,
  name: &str,
) -> Vec<JsonValue>
where
  K: Resolve + Hash + Eq + PartialEq + Clone,
  V: ToJson + Default,
{
  let mut items: Vec<JsonValue> = map
    .keys().cloned()
    .collect::<Vec<_>>()
    .into_iter()
    .map(|id| (id.resolve_to_json_object(ws), id))
    .map(|(mut o, id)| {
      let cost = map.remove(&id).unwrap_or_default();
      o[name] = cost.to_json();
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

impl Resolve for (Store, Goods, Batch) {
  fn resolve_to_json_object(&self, ws: &Workspace) -> JsonValue {
    let bytes: Vec<u8> = self
      .0
      .as_bytes()
      .iter()
      .zip(self.1.as_bytes().iter().zip(self.2.id.as_bytes().iter()))
      .map(|(a, (b, c))| a ^ b ^ c)
      .collect();

    let id = Uuid::from_bytes(bytes.try_into().unwrap_or_default());

    json::object! {
      _id: id.to_json(),
      storage: self.0.resolve_to_json_object(ws),
      goods: self.1.resolve_to_json_object(ws),
      batch: self.2.to_json(),
      _category: "batch",
    }
  }
}

struct Cache<'a> {
  ws: &'a Workspace,
  map: RwLock<HashMap<String, JsonValue>>,
}

impl<'a> Cache<'a> {
  fn new(ws: &'a Workspace) -> Self {
    Cache { ws, map: RwLock::new(HashMap::new()) }
  }

  fn resolve_uuid(&self, id: Uuid) -> JsonValue {
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
