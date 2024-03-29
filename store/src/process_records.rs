use crate::elements::{dt, ToJson};
use crate::GetWarehouse;
use csv::{ReaderBuilder, StringRecord, Trim};
use json::{object, JsonValue};
use rust_decimal::Decimal;
use service::error::Error;
use service::utils::json::JsonParams;
use service::{Context, Services};
use values::c;

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

const OID: &str = "yjmgJUmDo_kn9uxVi8s9Mj9mgGRJISxRt63wT46NyTQ";

pub fn report(
  app: &(impl GetWarehouse + Services),
  _company: &str,
  storage: &str,
  from_date: &str,
  till_date: &str,
) {
  log::debug!("CSV_REPORT");

  let ctx = vec!["report"];

  let storage =
    json(app, object! { name: storage }, STORAGE.to_vec(), &|| object! { name: storage }).unwrap();

  log::debug!("STORAGE: {:?}", storage[c::UUID]);

  let params: JsonValue = object! {ctx: ctx, oid: OID, storage: storage[c::UUID].clone(), dates: {"from": from_date, "till": till_date}};

  let result = app.service("inventory").find(Context::local(), params).unwrap();

  log::debug!("report: {:#?}", result);
}

pub fn receive_csv_to_json_for_warehouse(
  app: &(impl GetWarehouse + Services),
  path: &str,
  ctx: Vec<&str>,
  _doc_from: Option<&str>,
) -> Result<(), Error> {
  let mut reader = ReaderBuilder::new().delimiter(b',').trim(Trim::All).from_path(path).unwrap();

  let mut count = 0;

  for record in reader.records() {
    process_warehouse_record(app, &ctx, record.unwrap())?;
    count += 1;
  }

  println!("count: {count}");

  Ok(())
}

pub fn receive_csv_to_json_for_production(
  app: &(impl GetWarehouse + Services),
  path: &str,
) -> Result<(), Error> {
  let mut reader = ReaderBuilder::new().delimiter(b',').trim(Trim::All).from_path(path).unwrap();

  let mut count = 0;

  for record in reader.records() {
    process_production_record(app, record.unwrap())?;
    count += 1;
  }

  println!("count: {count}");

  Ok(())
}

pub fn memories_find(
  app: &(impl GetWarehouse + Services),
  filter: JsonValue,
  ctx: Vec<&str>,
) -> Result<Vec<JsonValue>, Error> {
  let params = object! {oid: OID, ctx: ctx, filter: filter, "$limit": 100};
  let result = app.service("memories").find(Context::local(), params)?;

  Ok(result["data"].members().cloned().collect())
}

pub fn memories_create(
  app: &(impl GetWarehouse + Services),
  data: JsonValue,
  ctx: Vec<&str>,
) -> Result<JsonValue, Error> {
  let params = object! {oid: OID, ctx: ctx };
  let result = app.service("memories").create(Context::local(), data, params)?;

  // log::debug!("create_result: {result:?}");
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

pub fn process_warehouse_record(
  app: &(impl GetWarehouse + Services),
  ctx: &Vec<&str>,
  record: StringRecord,
) -> Result<(), Error> {
  let category_name = match &record[1] {
    "расходные материалы" => "расходные материалы",
    "производственное сырьё" => "производственное сырьё",
    "стройматериалы" => "стройматериалы",
    "инструменты" => "инструменты",
    "производство" => "производство",
    _ => return Ok(()),
  };

  log::debug!("start process_record {record:?}");

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

  let goods_category = json(app, object! { name: category_name }, CATEGORY.to_vec(), &|| {
    object! { name: category_name }
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

  // let _comment = match &record[11] {
  //   "" => None,
  //   s => Some(s),
  // };
  //
  // let comment = record[11].trim();

  let mut data = object! {
    document: document["_id"].clone(),
    goods: item["_id"].clone(),
    qty: object! { number: qty.to_json(), uom: uom["_uuid"].clone() },
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

  // if !comment.is_empty() {
  //   data["comment"] = comment.into();
  // }

  let _res = memories_create(app, data, ctx.clone())?;

  // log::debug!("_res: {_res:#?}");

  Ok(())
}

// production areas
const SMALL_CARDBOARD_LABELS: &str = "production/area/2023-02-13T05:44:28.742Z"; // "малые картонные этикетки"
const BIG_CARDBOARD_LABELS: &str = "production/area/2023-02-13T05:44:41.272Z"; // "большие картонные этикетки"
const HEAT_SHRINK_LABELS: &str = "production/area/2023-11-10T11:18:57.746Z"; // "термоусадочная этикетка"
const CAP_THERMOFORMING: &str = "production/area/2023-02-08T05:58:21.725Z"; // "крышка термоформовка"
const CUPS_THERMOFORMING: &str = "production/area/2023-02-08T05:58:15.598Z"; // "стакан термоформовка"

// products
const C95_230: &str = "product/2023-02-13T05:52:46.192Z"; // "стакан полипропилен C95-230"
const A95_420: &str = "product/2023-02-07T06:48:41.284Z"; // "стакан полипропилен A95-420"
const B95_270: &str = "product/2023-02-13T05:53:30.198Z"; // "стакан полипропилен B95-270"
const CA_35: &str = "product/2023-07-21T12:35:58.665Z"; // "крышка CA-35"
const CA_50: &str = "product/2023-02-22T05:45:19.667Z"; // "крышка CA-50"

pub fn process_production_record(
  app: &(impl GetWarehouse + Services),
  record: StringRecord,
) -> Result<(), Error> {
  let (area_id, product_id) = match &record[0] {
    "этикеровка" => match &record[4] {
      "C95-230" => (SMALL_CARDBOARD_LABELS, C95_230),
      "A95-420" => (BIG_CARDBOARD_LABELS, A95_420),
      "B95-270" => (HEAT_SHRINK_LABELS, B95_270),
      _ => return Ok(()),
    },
    "крышка термоформовка" => match &record[4] {
      "CA-35" => (CAP_THERMOFORMING, CA_35),
      "CA-50" => (CAP_THERMOFORMING, CA_50),
      _ => return Ok(()),
    },
    "стакан термоформовка" => match &record[4] {
      "C95-230" => (CUPS_THERMOFORMING, C95_230),
      "A95-420" => (CUPS_THERMOFORMING, A95_420),
      "B95-270" => (CUPS_THERMOFORMING, B95_270),
      _ => return Ok(()),
    },
    _ => return Ok(()),
  };

  log::debug!("start process_production_record {record:?}");

  let order_date = dt(&record[2]).unwrap();

  let boxes_qty = record[7].parse::<usize>().unwrap();
  let qty_in_box = record[8].parse::<usize>().unwrap();
  let planned = boxes_qty * qty_in_box;

  let order_id = if record[1].starts_with("production/") {
    record[1].to_string()
  } else {
    let new_order = memories_create(
      app,
      object! { date: order_date.to_json(), area: area_id.to_string().to_json(), product: product_id.to_string().to_json(), planned: planned },
      vec!["production", "order"],
    )?;
    new_order[c::ID].string()
  };

  let piece_uom = String::from("1f93df2e-c423-45cf-8123-de02e0a0064e");
  let box_uom = String::from("76db8665-68bf-4088-857a-cce650bac352");

  let qty = object! {
    "number": 1,
    "uom": object! {
      "number": qty_in_box,
      "uom": piece_uom,
      "in": box_uom,
    }
  };

  for _ in 0..boxes_qty {
    let mut data = object! {
      document: order_id.to_json(),
      date: order_date.to_json(),
      qty: qty.clone(),
    };

    if &record[5] != "" {
      data["customer"] = JsonValue::String(record[5].to_string());
    }

    if &record[6] != "" {
      data["label"] = JsonValue::String(record[6].to_string());
    }

    let _res = memories_create(app, data, vec!["production", "produce"])?;
    // log::debug!("_res: {_res:#?}");
  }

  Ok(())
}
