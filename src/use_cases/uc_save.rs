use crate::commutator::Application;
use chrono::Utc;
use csv::{ReaderBuilder, Trim, Writer};
use json::{object, JsonValue};
use rust_decimal::Decimal;
use service::utils::json::JsonParams;
use service::{Context, Services};
use std::collections::HashMap;
use std::fs::OpenOptions;
use std::io::{Error, ErrorKind};
use std::str::FromStr;
use store::error::WHError;
use store::process_records::memories_find;
use values::constants::{_DOCUMENT, _ID, _STATUS};
use values::ID;

pub fn save_roll(app: &Application) -> Result<(), Error> {
  let mut count = 0;

  let oid = ID::from_base64("yjmgJUmDo_kn9uxVi8s9Mj9mgGRJISxRt63wT46NyTQ")
    .map_err(|e| Error::new(ErrorKind::NotFound, e.to_string()))?;
  let ws = app.wss.get(&oid);

  let ctx = ["production".to_string(), "produce".to_string()].to_vec();
  let produce_docs = ws.memories(ctx).list(Some(false))?;

  let params = object! {oid: ws.id.to_string().as_str(), ctx: [], enrich: false };

  for doc in produce_docs {
    let ctx = doc.mem.ctx.clone();

    let ctx_str: Vec<&str> = ctx.iter().map(|s| s.as_str()).collect();

    match ctx_str[..] {
      ["production", "produce"] => {
        // let doc_id = doc.id;
        println!("{:?} {:?}", doc.id, doc.json()?);

        let produce = match app.service("memories").get(
          Context::local(),
          doc.json()?[_ID].string(),
          params.clone(),
        ) {
          Ok(d) => d,
          Err(_) => {
            return Err(Error::new(ErrorKind::InvalidData, "can't find a produce operation"))
          }, // TODO handle IO error differently!!!!
        };

        // println!("_produce {produce:?}");

        let order = match app.service("memories").get(
          Context::local(),
          doc.json()?[_DOCUMENT].string(),
          params.clone(),
        ) {
          Ok(d) => d,
          Err(_) => return Err(Error::new(ErrorKind::InvalidData, "can't find an order")), // TODO handle IO error differently!!!!
        };

        // println!("_order {order:?}");

        let product = match app.service("memories").get(
          Context::local(),
          order["product"].string(),
          params.clone(),
        ) {
          Ok(p) => p,
          Err(_) => return Err(Error::new(ErrorKind::InvalidData, "can't find a product")), // TODO handle IO error differently!!!!
        };

        // println!("_product {product:?}");

        if product["name"].as_str() == Some("Рулон полипропилен") {
          let material = order["material"].string().replace(',', ".");
          let file = OpenOptions::new()
            .write(true)
            .create(true)
            .append(true)
            .open("production_roll.csv")
            .unwrap();
          let mut wtr = Writer::from_writer(file);

          wtr
            .write_record([
              order["date"].string(),
              produce["date"].string(),
              order["thickness"].string(),
              material,
              produce["qty"].string(),
              produce[_ID].string(),
            ])
            .unwrap();

          count += 1;
        }
      },
      _ => continue,
    }
  }

  println!("count {count}");

  Ok(())
}

pub enum Product {
  CUPS,
  CAPS,
}

pub fn save_half_stuff_products(app: &Application, product_type: Product) -> Result<(), Error> {
  let mut count = 0;

  let oid = ID::from_base64("yjmgJUmDo_kn9uxVi8s9Mj9mgGRJISxRt63wT46NyTQ")
    .map_err(|e| Error::new(ErrorKind::NotFound, e.to_string()))?;
  let ws = app.wss.get(&oid);

  let ctx = ["production".to_string(), "produce".to_string()].to_vec();
  let produce_docs = ws.memories(ctx).list(Some(false))?;

  let params = object! {oid: ws.id.to_string().as_str(), ctx: [], enrich: false };

  let mut records: HashMap<(String, String), Vec<String>> = HashMap::new();

  let (area_name, file_name) = match product_type {
    Product::CUPS => ("стакан термоформовка", "cups"),
    Product::CAPS => ("крышка термоформовка", "caps"),
  };

  for doc in produce_docs {
    let ctx = doc.mem.ctx.clone();

    let ctx_str: Vec<&str> = ctx.iter().map(|s| s.as_str()).collect();

    match ctx_str[..] {
      ["production", "produce"] => {
        // let doc_id = doc.id;
        println!("{:?} {:?}", doc.id, doc.json()?);

        let produce = match app.service("memories").get(
          Context::local(),
          doc.json()?[_ID].string(),
          params.clone(),
        ) {
          Ok(d) => d,
          Err(_) => {
            return Err(Error::new(ErrorKind::InvalidData, "can't find a produced operation"))
          }, // TODO handle IO error differently!!!!
        };

        println!("_produce {produce:?}");

        if produce[_STATUS] == "deleted" {
          continue;
        }

        let order = match app.service("memories").get(
          Context::local(),
          doc.json()?[_DOCUMENT].string(),
          params.clone(),
        ) {
          Ok(d) => d,
          Err(_) => return Err(Error::new(ErrorKind::InvalidData, "can't find an order")), // TODO handle IO error differently!!!!
        };

        println!("_order {order:?}");

        let area =
          match app
            .service("memories")
            .get(Context::local(), order["area"].string(), params.clone())
          {
            Ok(p) => p,
            Err(_) => return Err(Error::new(ErrorKind::InvalidData, "can't find an area")), // TODO handle IO error differently!!!!
          };

        if area["name"].as_str() == Some(area_name) {
          // println!("_area {area:?}");

          let product = match app.service("memories").get(
            Context::local(),
            order["product"].string(),
            params.clone(),
          ) {
            Ok(p) => p,
            Err(_) => return Err(Error::new(ErrorKind::InvalidData, "can't find a product")), // TODO handle IO error differently!!!!
          };

          // println!("_product {product:?}");

          let id = order[_ID].string();
          let number = produce["qty"]["uom"]["number"].string();

          let mut record = records.entry((id.clone(), number.clone())).or_insert(vec![
            id.clone(),
            order["date"].string(),
            produce["date"].string(),
            product["part_number"].string(),
            "0".to_string(),
            number.clone(),
            "0".to_string(),
          ]);

          let boxes = usize::from_str(record[4].as_str()).unwrap() + 1;
          let sum = Decimal::try_from(record[6].as_str()).unwrap()
            + Decimal::try_from(number.as_str()).unwrap();

          record[4] = boxes.to_string();
          record[6] = sum.to_string();

          count += 1;
        }
      },
      _ => continue,
    }
  }

  let mut time = Utc::now().to_string();
  time.truncate(19);

  let file = OpenOptions::new()
    .write(true)
    .create(true)
    .append(true)
    .open(format!("half_stuff_{file_name}_{time}.csv"))
    .unwrap();
  let mut wtr = Writer::from_writer(file);

  for record in records.into_iter() {
    wtr.write_record(record.1).unwrap();
  }

  println!("count {count}");

  Ok(())
}

pub fn save_cups_and_caps(app: &Application) -> Result<(), Error> {
  let mut count = 0;

  let oid = ID::from_base64("yjmgJUmDo_kn9uxVi8s9Mj9mgGRJISxRt63wT46NyTQ")
    .map_err(|e| Error::new(ErrorKind::NotFound, e.to_string()))?;
  let ws = app.wss.get(&oid);

  let ctx = ["production".to_string(), "produce".to_string()].to_vec();
  let produce_docs = ws.memories(ctx).list(Some(false))?;

  let params = object! {oid: ws.id.to_string().as_str(), ctx: [], enrich: false };

  let mut records: HashMap<(String, String, String, String), Vec<String>> = HashMap::new();

  for doc in produce_docs {
    let ctx = doc.mem.ctx.clone();

    let ctx_str: Vec<&str> = ctx.iter().map(|s| s.as_str()).collect();

    match ctx_str[..] {
      ["production", "produce"] => {
        // let doc_id = doc.id;
        println!("{:?} {:?}", doc.id, doc.json()?);

        let produce = match app.service("memories").get(
          Context::local(),
          doc.json()?[_ID].string(),
          params.clone(),
        ) {
          Ok(d) => d,
          Err(_) => {
            return Err(Error::new(ErrorKind::InvalidData, "can't find a produced operation"))
          }, // TODO handle IO error differently!!!!
        };

        if produce[_STATUS] == "deleted" {
          continue;
        }

        let order = match app.service("memories").get(
          Context::local(),
          doc.json()?[_DOCUMENT].string(),
          params.clone(),
        ) {
          Ok(d) => d,
          Err(_) => return Err(Error::new(ErrorKind::InvalidData, "can't find an order")), // TODO handle IO error differently!!!!
        };

        let area =
          match app
            .service("memories")
            .get(Context::local(), order["area"].string(), params.clone())
          {
            Ok(p) => p,
            Err(_) => return Err(Error::new(ErrorKind::InvalidData, "can't find an area")), // TODO handle IO error differently!!!!
          };

        if area["name"].as_str() == Some("термоусадочная этикетка")
          || area["name"].as_str() == Some("большие картонные этикетки")
          || area["name"].as_str() == Some("малые картонные этикетки")
          || area["name"].as_str() == Some("офсетная печать")
          || area["name"].as_str() == Some("крышка термоформовка")
        {
          println!("_produce {produce:?}");
          println!("_order {order:?}");
          // println!("_area {area:?}");

          let product = match app.service("memories").get(
            Context::local(),
            order["product"].string(),
            params.clone(),
          ) {
            Ok(p) => p,
            Err(_) => return Err(Error::new(ErrorKind::InvalidData, "can't find a product")), // TODO handle IO error differently!!!!
          };

          println!("_product {product:?}");

          let id = order[_ID].string();
          let number = produce["qty"]["uom"]["number"].string();
          let customer = produce["customer"].string();
          let label = produce["label"].string();

          let mut record = records
            .entry((id.clone(), number.clone(), customer.clone(), label.clone()))
            .or_insert(vec![
              id.clone(),
              order["date"].string(),
              produce["date"].string(),
              product["part_number"].string(),
              customer.clone(),
              label.clone(),
              "0".to_string(),
              number.clone(),
              "0".to_string(),
            ]);

          let boxes = usize::from_str(record[6].as_str()).unwrap() + 1;
          let sum = Decimal::try_from(record[8].as_str()).unwrap()
            + Decimal::try_from(number.as_str()).unwrap();

          record[6] = boxes.to_string();
          record[8] = sum.to_string();

          count += 1;
        }
      },
      _ => continue,
    }
  }

  let mut time = Utc::now().naive_local().to_string();
  time.truncate(19);

  let file = OpenOptions::new()
    .write(true)
    .create(true)
    .append(true)
    .open(format!("production_cups_caps_{time}.csv"))
    .unwrap();
  let mut wtr = Writer::from_writer(file);

  for record in records.into_iter() {
    wtr.write_record(record.1).unwrap();
  }

  println!("count {count}");

  Ok(())
}

pub fn save_produced(app: &Application) -> Result<(), Error> {
  let mut count = 0;

  let oid = ID::from_base64("yjmgJUmDo_kn9uxVi8s9Mj9mgGRJISxRt63wT46NyTQ")
    .map_err(|e| Error::new(ErrorKind::NotFound, e.to_string()))?;
  let ws = app.wss.get(&oid);

  let ctx = ["production".to_string(), "material".to_string(), "produced".to_string()].to_vec();
  let produced_docs = ws.memories(ctx).list(None)?;

  let params = object! {oid: ws.id.to_string().as_str(), ctx: [], enrich: false };

  for doc in produced_docs {
    let ctx = doc.mem.ctx.clone();

    let ctx_str: Vec<&str> = ctx.iter().map(|s| s.as_str()).collect();

    match ctx_str[..] {
      ["production", "material", "produced"] => {
        // let doc_id = doc.id;
        println!("{:?} {:?}", doc.id, doc.json()?);

        let produced = match app.service("memories").get(
          Context::local(),
          doc.json()?[_ID].string(),
          params.clone(),
        ) {
          Ok(d) => d,
          Err(_) => {
            return Err(Error::new(ErrorKind::InvalidData, "can't find a produce operation"))
          }, // TODO handle IO error differently!!!!
        };

        println!("_produced {produced:?}");

        let order = match app.service("memories").get(
          Context::local(),
          doc.json()?[_DOCUMENT].string(),
          params.clone(),
        ) {
          Ok(d) => d,
          Err(_) => return Err(Error::new(ErrorKind::InvalidData, "can't find an order")), // TODO handle IO error differently!!!!
        };

        println!("_order {order:?}");

        let area =
          match app
            .service("memories")
            .get(Context::local(), order["area"].string(), params.clone())
          {
            Ok(p) => p,
            Err(_) => return Err(Error::new(ErrorKind::InvalidData, "can't find an area")), // TODO handle IO error differently!!!!
          };

        let product = match app.service("memories").get(
          Context::local(),
          order["product"].string(),
          params.clone(),
        ) {
          Ok(p) => p,
          Err(_) => return Err(Error::new(ErrorKind::InvalidData, "can't find a product")), // TODO handle IO error differently!!!!
        };

        let goods = match app.service("memories").get(
          Context::local(),
          produced["goods"].string(),
          params.clone(),
        ) {
          Ok(p) => p,
          Err(_) => return Err(Error::new(ErrorKind::InvalidData, "can't find a goods")), // TODO handle IO error differently!!!!
        };

        println!("_goods {goods:?}");

        let mut uom = produced["qty"]["uom"].clone();

        let mut qty_str = String::new();

        qty_str = format!("{} {} ", qty_str, produced["qty"]["number"].string());

        while uom.is_object() {
          if !uom["in"].is_null() {
            let in_name =
              match app
                .service("memories")
                .get(Context::local(), uom["in"].string(), params.clone())
              {
                Ok(i) => i["name"].string(),
                Err(_) => return Err(Error::new(ErrorKind::InvalidData, "can't find in_uom")), // TODO handle IO error differently!!!!
              };
            qty_str = format!("{} {} ", qty_str, in_name);
          }

          qty_str = format!("{} {} ", qty_str, uom["number"].string());

          uom = uom["uom"].clone();
        }

        let uom_name =
          match app.service("memories").get(Context::local(), uom.string(), params.clone()) {
            Ok(u) => u["name"].string(),
            Err(_) => return Err(Error::new(ErrorKind::InvalidData, "can't find uom")), // TODO handle IO error differently!!!!
          };
        qty_str = format!("{} {} ", qty_str, uom_name);

        // println!("qty_str {qty_str}");

        let file = OpenOptions::new()
          .write(true)
          .create(true)
          .append(true)
          .open("production_produced.csv")
          .unwrap();
        let mut wtr = Writer::from_writer(file);

        wtr
          .write_record([
            order["date"].string(),
            area["name"].string(),
            format!(
              "{} {} {} ",
              product["name"].string(),
              product["part_number"].string(),
              order["thickness"].string()
            ),
            goods["name"].string(),
            produced["qty"]["number"].string(),
            qty_str,
            produced[_ID].string(),
          ])
          .unwrap();

        count += 1;
      },
      _ => continue,
    }
  }

  println!("count {count}");

  Ok(())
}

pub fn save_transfer_from_file(app: &Application) -> Result<(), Error> {
  let mut count = 0;

  let mut reader = ReaderBuilder::new()
    .delimiter(b',')
    .trim(Trim::All)
    .from_path("./import/transfer_extra.csv")
    .unwrap();

  let doc_ctx = ["warehouse", "transfer", "document"].to_vec();
  let storage_ctx = ["warehouse", "storage"].to_vec();
  let op_ctx = ["warehouse", "transfer"].to_vec();

  for record in reader.records() {
    let record = record.unwrap();
    let number = match &record[0] {
      "" => "-1",
      n => n,
    };

    let date = &record[7];
    let date = format!("{}-{}-{}", &date[6..=9], &date[3..=4], &date[0..=1]);

    let from_name = &record[8].replace(['\\', '\"'], "").replace(",,", ",");

    let from = if let Ok(items) =
      memories_find(app, object! { name: from_name.to_string() }, storage_ctx.clone())
    {
      match items.len() {
        0 => Err(WHError::new("not found")),
        1 => Ok(items[0].clone()),
        _ => Err(WHError::new("too many docs found")),
      }
    } else {
      Err(WHError::new("not found"))
    }
    .unwrap();

    let into_name = &record[9].replace(['\\', '\"'], "").replace(",,", ",");
    let into = if let Ok(items) =
      memories_find(app, object! { name: into_name.to_string() }, storage_ctx.clone())
    {
      match items.len() {
        0 => Err(WHError::new("not found")),
        1 => Ok(items[0].clone()),
        _ => Err(WHError::new("too many docs found")),
      }
    } else {
      Err(WHError::new("not found"))
    }
    .unwrap();

    let filter =
      object! {number: number, from: from[_ID].clone(), into: into[_ID].clone(), date: date.clone()};

    let doc = if let Ok(items) = memories_find(app, filter, doc_ctx.clone()) {
      match items.len() {
        0 => Err(WHError::new("not found")),
        1 => Ok(items[0].clone()),
        _ => Err(WHError::new("too many docs found")),
      }
    } else {
      Err(WHError::new("not found"))
    }
    .unwrap();

    let operations = memories_find(app, object! { document: doc[_ID].string() }, op_ctx.clone())?;

    // println!("_OPERS {operations:?}");

    let file = OpenOptions::new()
      .write(true)
      .create(true)
      .append(true)
      .open("transfer_ops.csv")
      .unwrap();
    let mut wtr = Writer::from_writer(file);

    for op in operations {
      let goods_name = op["goods"]["name"].string();
      let uom = op["qty"]["uom"]["name"].string();
      let qty = op["qty"]["number"].string();

      if goods_name != record[2].to_string().replace('\"', "")
        || uom != record[4]
        || qty != record[5]
      {
        continue;
      }

      wtr
        .write_record([
          doc["number"].string(),
          doc["date"].string(),
          goods_name,
          qty,
          uom,
          from["name"].string(),
          into["name"].string(),
        ])
        .unwrap();

      count += 1;
    }
  }

  println!("count {count}");

  Ok(())
}

pub fn save_transfer_for_goods(app: &Application) -> Result<(), Error> {
  let mut count = 0;

  let oid = ID::from_base64("yjmgJUmDo_kn9uxVi8s9Mj9mgGRJISxRt63wT46NyTQ")
    .map_err(|e| Error::new(ErrorKind::NotFound, e.to_string()))?;
  let ws = app.wss.get(&oid);

  let params = object! {oid: ws.id.to_string().as_str(), ctx: [], enrich: false };

  let goods_filter = object! {name: "Гофрокоробка 50*57*37.5"};

  let goods = if let Ok(goods) = memories_find(app, goods_filter, ["goods"].to_vec()) {
    match goods.len() {
      0 => Err(WHError::new("goods not found")),
      1 => Ok(goods[0].clone()),
      _ => Err(WHError::new("too many goods found")),
    }
  } else {
    Err(WHError::new("goods not found"))
  }
  .unwrap();

  println!("_goods: {:?}", goods);

  let filter = object! {goods: goods[_ID].string()};

  let transfer_ops =
    if let Ok(items) = memories_find(app, filter, ["warehouse", "receive"].to_vec()) {
      match items.len() {
        0 => Err(WHError::new("not found")),
        1 => Ok(items),
        _ => Ok(items),
      }
    } else {
      Err(WHError::new("not found"))
    }
    .unwrap();

  println!("_transfers: {:?}", transfer_ops.len());

  for transfer in transfer_ops {
    let document = match app.service("memories").get(
      Context::local(),
      transfer[_DOCUMENT].string(),
      params.clone(),
    ) {
      Ok(p) => p,
      Err(_) => JsonValue::Null,
    };

    let from = match app.service("memories").get(
      Context::local(),
      document["counterparty"].string(),
      params.clone(),
    ) {
      Ok(p) => p,
      Err(_) => return Err(Error::new(ErrorKind::InvalidData, "can't find from storage")),
    };

    let into = match app.service("memories").get(
      Context::local(),
      document["storage"].string(),
      params.clone(),
    ) {
      Ok(p) => p,
      Err(_) => return Err(Error::new(ErrorKind::InvalidData, "can't find into storage")),
    };

    let storage_from = match app.service("memories").get(
      Context::local(),
      transfer["storage_from"].string(),
      params.clone(),
    ) {
      Ok(p) => p,
      Err(_) => JsonValue::Null,
    };

    let storage_into = match app.service("memories").get(
      Context::local(),
      transfer["storage_into"].string(),
      params.clone(),
    ) {
      Ok(p) => p,
      Err(_) => JsonValue::Null,
    };

    let file = OpenOptions::new()
      .write(true)
      .create(true)
      .append(true)
      .open("receive_boxes.csv")
      .unwrap();
    let mut wtr = Writer::from_writer(file);

    wtr
      .write_record([
        document["date"].string(),
        goods["name"].string(),
        from["name"].string(),
        into["name"].string(),
        transfer["qty"]["number"].string(),
        storage_from["name"].string(),
        storage_into["name"].string(),
      ])
      .unwrap();

    count += 1;
  }

  println!("count {count}");

  Ok(())
}
