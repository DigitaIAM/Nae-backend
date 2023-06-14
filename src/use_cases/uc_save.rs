use crate::commutator::Application;
use crate::storage;
use actix_web::App;
use csv::Writer;
use json::object;
use service::utils::json::JsonParams;
use service::{Context, Services};
use std::fs::{File, OpenOptions};
use std::io::{BufRead, BufReader, Error, ErrorKind};

pub fn save_roll(app: &Application) -> Result<(), Error> {
  let mut count = 0;

  for ws in app.wss.list()? {
    let params = object! {oid: ws.id.to_string().as_str(), ctx: [], enrich: false };

    for doc in ws.clone().into_iter() {
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
  }

  println!("count {count}");

  Ok(())
}

pub fn save_half_stuff_cups(app: &Application) -> Result<(), Error> {
  let mut count = 0;

  for ws in app.wss.list()? {
    let params = object! {oid: ws.id.to_string().as_str(), ctx: [], enrich: false };

    for doc in ws.clone().into_iter() {
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

          let area = match app.service("memories").get(
            Context::local(),
            order["area"].string(),
            params.clone(),
          ) {
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
  }

  println!("count {count}");

  Ok(())
}

pub fn save_produced(app: &Application) -> Result<(), Error> {
  let mut count = 0;

  for ws in app.wss.list()? {
    let params = object! {oid: ws.id.to_string().as_str(), ctx: [], enrich: false };

    for doc in ws.clone().into_iter() {
      let ctx = doc.mem.ctx.clone();

      let ctx_str: Vec<&str> = ctx.iter().map(|s| s.as_str()).collect();

      match ctx_str[..] {
        ["production", "material", "used"] => {
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

          let area = match app.service("memories").get(
            Context::local(),
            order["area"].string(),
            params.clone(),
          ) {
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
              let in_name = match app.service("memories").get(
                Context::local(),
                uom["in"].string(),
                params.clone(),
              ) {
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
            .open("production_used.csv")
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
  }

  println!("count {count}");

  Ok(())
}
