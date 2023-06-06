use crate::commutator::Application;
use crate::storage;
use csv::Writer;
use json::object;
use service::utils::json::JsonParams;
use service::{Context, Services};
use std::fs::{File, OpenOptions};
use std::io::{BufRead, BufReader, Error, ErrorKind};

pub fn save(app: &Application) -> Result<(), Error> {
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
            write_to_file([
              order["date"].string(),
              produce["date"].string(),
              order["thickness"].string(),
              produce["qty"].string(),
            ])
          }
        },
        _ => continue,
      }
    }
  }

  println!("count {count}");

  Ok(())
}

fn write_to_file(record: [String; 4]) {
  let file = OpenOptions::new()
    .write(true)
    .create(true)
    .append(true)
    .open("production_roll.csv")
    .unwrap();
  let mut wtr = Writer::from_writer(file);

  wtr.write_record(record).unwrap();
}
