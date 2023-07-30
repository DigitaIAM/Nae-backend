use crate::GetWarehouse;
use csv::{ReaderBuilder, StringRecord, Trim};
use json::{object, JsonValue};
use rust_decimal::Decimal;
// use serde_json::json;
use crate::elements::ToJson;
use service::error::Error;
use service::{Context, Services};
use values::constants::_UUID;

const COUNTERPARTY: [&str; 1] = ["counterparty"];
const STORAGE: [&str; 2] = ["warehouse", "storage"];
const RECEIVE_DOCUMENT: [&str; 3] = ["warehouse", "receive", "document"];
const INVENTORY_DOCUMENT: [&str; 3] = ["warehouse", "inventory", "document"];
const TRANSFER_DOCUMENT: [&str; 3] = ["warehouse", "transfer", "document"];
const DISPATCH_DOCUMENT: [&str; 3] = ["warehouse", "dispatch", "document"];
const UOM: [&str; 1] = ["uom"];
const GOODS: [&str; 1] = ["goods"];
const CATEGORY: [&str; 2] = ["goods", "category"];
const CURRENCY: [&str; 1] = ["currency"];

pub fn report(
  app: &(impl GetWarehouse + Services),
  _company: &str,
  storage: &str,
  from_date: &str,
  till_date: &str,
) {
  println!("CSV_REPORT");
  // let oid = ID::from(company);
  let oid = "yjmgJUmDo_kn9uxVi8s9Mj9mgGRJISxRt63wT46NyTQ";
  let ctx = vec!["report"];

  let storage =
    json(app, object! { name: storage }, STORAGE.to_vec(), &|| object! { name: storage }).unwrap();

  println!("STORAGE: {:?}", storage[_UUID]);

  let params: JsonValue = object! {ctx: ctx, oid: oid, storage: storage[_UUID].clone(), dates: {"from": from_date, "till": till_date}};

  let result = app.service("inventory").find(Context::local(), params).unwrap();

  println!("report: {:#?}", result);
}

pub fn receive_csv_to_json(
  app: &(impl GetWarehouse + Services),
  path: &str,
  ctx: Vec<&str>,
  _doc_from: Option<&str>,
) -> Result<(), Error> {
  let mut reader = ReaderBuilder::new().delimiter(b',').trim(Trim::All).from_path(path).unwrap();

  for record in reader.records() {
    process_record(app, &ctx, record.unwrap())?;
    // println!("data: {_res:?}");
  }

  Ok(())
}

pub fn memories_find(
  app: &(impl GetWarehouse + Services),
  filter: JsonValue,
  ctx: Vec<&str>,
) -> Result<Vec<JsonValue>, Error> {
  let oid = "yjmgJUmDo_kn9uxVi8s9Mj9mgGRJISxRt63wT46NyTQ";
  let params = object! {oid: oid, ctx: ctx, filter: filter, "$limit": 100};
  let result = app.service("memories").find(Context::local(), params)?;

  Ok(result["data"].members().cloned().collect())
}

pub fn memories_create(
  app: &(impl GetWarehouse + Services),
  data: JsonValue,
  ctx: Vec<&str>,
) -> Result<JsonValue, Error> {
  let oid = "yjmgJUmDo_kn9uxVi8s9Mj9mgGRJISxRt63wT46NyTQ";
  let params = object! {oid: oid, ctx: ctx };
  let result = app.service("memories").create(Context::local(), data, params)?;

  // println!("create_result: {result:?}");
  Ok(result)
}

fn json(
  app: &(impl GetWarehouse + Services),
  filter: JsonValue,
  ctx: Vec<&str>,
  item: &dyn Fn() -> JsonValue,
) -> Result<JsonValue, Error> {
  if let Ok(items) = memories_find(app, filter, ctx.clone()) {
    match items.len() {
      0 => memories_create(app, item(), ctx),
      1 => Ok(items[0].clone()),
      _ => unreachable!("two or more same items in db"),
    }
  } else {
    memories_create(app, item(), ctx)
  }
}

pub fn process_record(
  app: &(impl GetWarehouse + Services),
  ctx: &Vec<&str>,
  record: StringRecord,
) -> Result<(), Error> {
  let category_name = match &record[1] {
    "расходные материалы" => "расходные материалы",
    "производственное сырьё" => "производственное сырьё",
    "стройматериалы" => "стройматериалы",
    "инструменты" => "инструменты",
    _ => return Ok(()),
  };

  println!("start process_record {record:?}");

  let date = &record[7];
  let date = format!("{}-{}-{}", &date[6..=9], &date[3..=4], &date[0..=1]);

  let number = match &record[0] {
    "" => "-1",
    n => n,
  };
  // if number.is_empty() {
  //   continue;
  // }

  let from_ctx = if ctx.get(1) == Some(&"transfer") || ctx.get(1) == Some(&"dispatch") {
    STORAGE.to_vec()
  } else {
    COUNTERPARTY.to_vec()
  };
  let from_name = &record[8].replace(['\\', '\"'], "").replace(",,", ",");
  let from = if from_name.is_empty() {
    JsonValue::String("".to_string())
  } else {
    json(
      app,
      object! { name: from_name.to_string() },
      from_ctx,
      &|| object! { name: from_name.to_string() },
    )?
  };

  let into_ctx = if ctx.get(1) == Some(&"transfer")
    || ctx.get(1) == Some(&"receive")
    || ctx.get(1) == Some(&"inventory")
  {
    STORAGE.to_vec()
  } else {
    COUNTERPARTY.to_vec()
  };
  let into_name = &record[9].replace(['\\', '\"'], "").replace(",,", ",");
  let into_name = match into_name.as_str() {
    "Гагарина 36" => "снабжение Бегбудиев Носир",
    "Склад" => "склад",
    "Материалы в пути" => "склад",
    "Администрация" => return Ok(()),
    "Кухня" => return Ok(()),
    "Топливо в баках" => return Ok(()),
    str => str,
  };
  let into = if into_name.is_empty() {
    JsonValue::String("".to_string())
  } else {
    json(app, object! { name: into_name }, into_ctx, &|| object! { name: into_name })?
  };

  let (doc_ctx, ignore_cost) = if ctx.get(1) == Some(&"receive") {
    (RECEIVE_DOCUMENT.to_vec(), false)
  } else if ctx.get(1) == Some(&"inventory") {
    (INVENTORY_DOCUMENT.to_vec(), true)
  } else if ctx.get(1) == Some(&"transfer") {
    (TRANSFER_DOCUMENT.to_vec(), true)
  } else {
    (DISPATCH_DOCUMENT.to_vec(), false)
  };

  let document = if &doc_ctx == &TRANSFER_DOCUMENT.to_vec() {
    json(
      app,
      object! {number: number, from: from["_id"].clone(), into: into["_id"].clone(), date: date.clone()},
      doc_ctx.clone(),
      &|| {
        object! {
          date: date.clone(),
          from: from["_id"].clone(),
          into: into["_id"].clone(),
          number: number,
        }
      },
    )?
  } else if &doc_ctx == &DISPATCH_DOCUMENT.to_vec() {
    json(
      app,
      object! {number: number, storage: from["_id"].clone(), counterparty: into["_id"].clone(), date: date.clone()},
      doc_ctx.clone(),
      &|| {
        object! {
          date: date.clone(),
          storage: from["_id"].clone(),
          counterparty: into["_id"].clone(),
          number: number,
        }
      },
    )?
  } else {
    json(
      app,
      object! {number: number, counterparty: from["_id"].clone(), storage: into["_id"].clone(), date: date.clone()},
      doc_ctx.clone(),
      &|| {
        object! {
          date: date.clone(),
          counterparty: from["_id"].clone(),
          storage: into["_id"].clone(),
          number: number,
        }
      },
    )?
  };

  let unit = match &record[4] {
    "пач." => "Пачк.",
    "пар." => "Пар",
    str => str,
  };
  let uom = json(app, object! {name: unit}, UOM.to_vec(), &|| object! { name: unit })?;

  let goods_name = record[2].replace(['\\', '\"'], "");
  let vendor_code = &record[3];

  let goods_category =
    json(app, object! { name: category_name.clone() }, CATEGORY.to_vec(), &|| {
      object! { name: category_name.clone() }
    })?;

  let item = json(app, object! { name: goods_name.clone() }, GOODS.to_vec(), &|| {
    object! {
        name: goods_name.clone(),
        vendor_code: vendor_code,
        category: goods_category["_id"].clone(),
        uom: uom["_id"].clone(),
    }
  })?;

  // cells at the warehouse
  let cell_from: Option<JsonValue> = if ctx.get(1) == Some(&"transfer") {
    match &record[10] {
      "" => None,
      _ => Some(json(
        app,
        object! { name: &record[10] },
        STORAGE.to_vec(),
        &|| object! { name: &record[10] },
      )?),
    }
  } else {
    None
  };

  let cell_into: Option<JsonValue> = if ctx.get(1) == Some(&"transfer") {
    match &record[11] {
      "" => None,
      _ => Some(json(
        app,
        object! { name: &record[11] },
        STORAGE.to_vec(),
        &|| object! { name: &record[11] },
      )?),
    }
  } else {
    None
  };

  let float_qty_str = &record[5].replace(',', ".");

  let qty = float_qty_str.parse::<Decimal>().unwrap();

  let cost = &record[6].parse::<Decimal>().unwrap_or_default();

  let currency = json(app, object! {name: "uzd"}, CURRENCY.to_vec(), &|| {
    object! {name: "uzd"}
  })?;

  let _comment = match &record[11] {
    "" => None,
    s => Some(s),
  };

  let comment = record[11].trim();

  let mut data = object! {
    document: document["_id"].clone(),
    goods: item["_id"].clone(),
    qty: object! { number: qty.to_json(), uom: uom["_id"].clone() },
  };

  if let Some(cell_from) = cell_from {
    data["storage_from"] = cell_from["_id"].clone();
  }

  if let Some(cell_into) = cell_into {
    data["storage_into"] = cell_into["_id"].clone();
  }

  if !ignore_cost {
    if ctx.get(1) == Some(&"receive") {
      data["cost"] = object! { number: cost.to_json(), currency: currency["_id"].clone() };
    } else {
      data["sell_cost"] = object! { number: cost.to_json(), currency: currency["_id"].clone() };
    }
  }

  if !comment.is_empty() {
    data["comment"] = comment.into();
  }

  let _res = memories_create(app, data, ctx.clone())?;

  // println!("_res: {_res:#?}");

  Ok(())
}
