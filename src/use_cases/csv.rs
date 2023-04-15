use csv::{ReaderBuilder, Trim};
use json::{object, JsonValue};
use rust_decimal::Decimal;

use store::elements::ToJson;

use crate::commutator::Application;
use service::error::Error;
use service::Services;

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

struct Quantity {
  number: Decimal,
  uom: JsonValue,
}

struct Cost {
  number: Decimal,
  currency: String,
}

pub(crate) fn report(
  app: &Application,
  company: &str,
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

  println!("STORAGE: {:?}", storage["_uuid"]);

  let result = app.service("inventory").find(object!{ctx: ctx, oid: oid, storage: storage["_uuid"].clone(), dates: {"from": from_date, "till": till_date}}).unwrap();

  println!("report: {:#?}", result);
}

fn memories_find(
  app: &Application,
  filter: JsonValue,
  ctx: Vec<&str>,
) -> Result<Vec<JsonValue>, Error> {
  let oid = "yjmgJUmDo_kn9uxVi8s9Mj9mgGRJISxRt63wT46NyTQ";
  let result = app.service("memories").find(object! {oid: oid, ctx: ctx, filter: filter})?;

  Ok(result["data"].members().map(|o| o.clone()).collect())
}

fn memories_create(app: &Application, data: JsonValue, ctx: Vec<&str>) -> Result<JsonValue, Error> {
  let oid = "yjmgJUmDo_kn9uxVi8s9Mj9mgGRJISxRt63wT46NyTQ";
  let result = app.service("memories").create(data, object! {oid: oid, ctx: ctx })?;

  // println!("create_result: {result:?}");
  Ok(result)
}

fn json(
  app: &Application,
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

pub(crate) fn receive_csv_to_json(
  app: &Application,
  path: &str,
  ctx: Vec<&str>,
  _doc_from: Option<&str>,
) -> Result<(), Error> {
  let mut reader = ReaderBuilder::new().delimiter(b',').trim(Trim::All).from_path(path).unwrap();

  for record in reader.records() {
    let record = record.unwrap();

    let date = &record[5];
    let date = format!("{}-{}-{}", &date[6..=9], &date[3..=4], &date[0..=1]);

    let number = &record[0];
    if number.is_empty() {
      continue;
    }

    let from_ctx = if ctx.get(1) == Some(&"transfer") || &record[6] == "склад" {
      STORAGE.to_vec()
    } else {
      COUNTERPARTY.to_vec()
    };
    let from_name = &record[6].replace("\\", "").replace("\"", "");
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

    let into_ctx = if ctx.get(1) == Some(&"transfer") || &record[7] == "склад" {
      STORAGE.to_vec()
    } else {
      COUNTERPARTY.to_vec()
    };
    let into_name = match &record[7] {
      "Гагарина 36" => "Снабжение Бегбудиев Носир",
      "Склад" => "Снабжение Бегбудиев Носир",
      "Материалы в пути" => "Снабжение Бегбудиев Носир",
      "Администрация" => continue,
      "Кухня" => continue,
      "Топливо в баках" => continue,
      str => str,
    };
    let into = if into_name.is_empty() {
      JsonValue::String("".to_string())
    } else {
      json(app, object! { name: into_name }, into_ctx, &|| object! { name: into_name })?
    };

    let doc_ctx = if ctx.get(1) == Some(&"receive") {
      RECEIVE_DOCUMENT.to_vec()
    } else if ctx.get(1) == Some(&"inventory") {
      INVENTORY_DOCUMENT.to_vec()
    } else if ctx.get(1) == Some(&"transfer") {
      TRANSFER_DOCUMENT.to_vec()
    } else {
      DISPATCH_DOCUMENT.to_vec()
    };

    let document =
      json(app, object! {number: number, date: date.clone()}, doc_ctx.clone(), &|| {
        if &doc_ctx == &TRANSFER_DOCUMENT.to_vec() {
          object! {
            date: date.clone(),
            from: from["_id"].clone(),
            into: into["_id"].clone(),
            number: number,
          }
        } else if &doc_ctx == &DISPATCH_DOCUMENT.to_vec() {
          object! {
            date: date.clone(),
            storage: from["_id"].clone(),
            counterparty: into["_id"].clone(),
            number: number,
          }
        } else {
          object! {
            date: date.clone(),
            counterparty: from["_id"].clone(),
            storage: into["_id"].clone(),
            number: number,
          }
        }
      })?;

    let unit = match &record[3] {
      "пач." => "Пачк.",
      "пар." => "Пар",
      str => str,
    };
    let uom = json(app, object! {name: unit}, UOM.to_vec(), &|| object! { name: unit })?;

    let goods_name = record[1].replace("\\", "").replace("\"", "");
    let vendor_code = &record[2];

    let category_name = &record[8];
    let goods_category =
      json(app, object! { name: category_name.clone() }, CATEGORY.to_vec(), &|| {
        object! { name: category_name.clone() }
      })?;

    let item = json(app, object! { name: goods_name.clone() }, GOODS.to_vec(), &|| {
      object! {
          name: goods_name.clone(),
          vendor_code: vendor_code,
          category: goods_category.clone(),
          uom: uom["_id"].clone(),
      }
    })?;

    // cells at the warehouse
    let cell_from: Option<JsonValue> = if ctx.get(1) == Some(&"transfer") {
      match &record[8] {
        "" => None,
        _ => Some(json(
          app,
          object! { name: &record[8] },
          STORAGE.to_vec(),
          &|| object! { name: &record[8] },
        )?),
      }
    } else {
      None
    };

    let cell_into: Option<JsonValue> = if ctx.get(1) == Some(&"transfer") {
      match &record[9] {
        "" => None,
        _ => Some(json(
          app,
          object! { name: &record[9] },
          STORAGE.to_vec(),
          &|| object! { name: &record[9] },
        )?),
      }
    } else {
      None
    };

    let price: Decimal = 0.into();

    let float_number = &record[4].replace(",", ".");

    let number = float_number.parse::<Decimal>().unwrap();

    let currency = json(app, object! {name: "uzd"}, CURRENCY.to_vec(), &|| {
      object! {name: "uzd"}
    })?;

    let data = object! {
        document: document["_id"].clone(),
        goods: item["_id"].clone(),
        storage_from: cell_from,
        storage_into: cell_into,
        qty: object! { number: number.to_json(), uom: uom["_id"].clone() },
        price: price.to_json(),
        cost: object! { number: Decimal::default().to_json(), currency: currency["_id"].clone() },
    };

    let _res = memories_create(app, data, ctx.clone())?;

    // println!("data: {res:?}");
  }

  Ok(())
}
