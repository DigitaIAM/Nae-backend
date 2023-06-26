use crate::commutator::Application;
use crate::storage;
use actix_web::App;
use csv::{ReaderBuilder, Trim, Writer};
use json::object;
use service::utils::json::JsonParams;
use service::{Context, Services};
use std::fs::{File, OpenOptions};
use std::io::{BufRead, BufReader, Error, ErrorKind};
use store::error::WHError;
use store::process_records::memories_find;
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
          doc.json()?["_id"].string(),
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
          doc.json()?["order"].string(),
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
          let material = order["material"].string().replace(",", ".");
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

pub fn save_half_stuff_cups(app: &Application) -> Result<(), Error> {
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
          doc.json()?["_id"].string(),
          params.clone(),
        ) {
          Ok(d) => d,
          Err(_) => {
            return Err(Error::new(ErrorKind::InvalidData, "can't find a produced operation"))
          }, // TODO handle IO error differently!!!!
        };

        println!("_produce {produce:?}");

        let order = match app.service("memories").get(
          Context::local(),
          doc.json()?["order"].string(),
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

        if area["name"].as_str() == Some("стакан термоформовка") {
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

          let file = OpenOptions::new()
            .write(true)
            .create(true)
            .append(true)
            .open("production_half_stuff_cups.csv")
            .unwrap();
          let mut wtr = Writer::from_writer(file);

          wtr
            .write_record([
              order["date"].string(),
              produce["date"].string(),
              product["part_number"].string(),
              produce["qty"].string(),
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
          doc.json()?["_id"].string(),
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
          doc.json()?["document"].string(),
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

pub fn save_transfer(app: &Application) -> Result<(), Error> {
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

    let from_name = &record[8].replace("\\", "").replace("\"", "").replace(",,", ",");

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

    let into_name = &record[9].replace("\\", "").replace("\"", "").replace(",,", ",");
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

    let filter = object! {number: number, from: from["_id"].clone(), into: into["_id"].clone(), date: date.clone()};

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

    let operations = memories_find(app, object! { document: doc["_id"].string() }, op_ctx.clone())?;

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

      if goods_name != record[2].to_string().replace("\"", "")
        || uom != record[4].to_string()
        || qty != record[5].to_string()
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
