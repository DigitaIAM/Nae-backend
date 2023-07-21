use crate::commutator::Application;
use json::object;
use service::utils::json::JsonParams;
use service::{Context, Services};
use std::fs::File;
use std::io::{BufRead, BufReader, Error, ErrorKind};
use store::error::WHError;
use store::process_records::memories_find;
use values::ID;

pub fn delete_produce(app: &Application) -> Result<(), Error> {
  let mut count = 0;

  let oid = ID::from_base64("yjmgJUmDo_kn9uxVi8s9Mj9mgGRJISxRt63wT46NyTQ")
    .map_err(|e| Error::new(ErrorKind::NotFound, e.to_string()))?;
  let ws = app.wss.get(&oid);

  let ctx = ["production".to_string(), "produce".to_string()].to_vec();
  let produce_docs = ws.memories(ctx).list(Some(false))?;

  println!("produce_docs {:?}", produce_docs.len());

  for doc in produce_docs {
    let ctx = doc.mem.ctx;

    let ctx_str: Vec<&str> = ctx.iter().map(|s| s.as_str()).collect();

    match ctx_str[..] {
      ["production", "produce"] => {
        let doc_id = doc.id;
        // println!("doc_id {}", doc_id);

        let filepath = "production_delete.txt";

        let file = File::open(filepath)?;
        let reader = BufReader::new(file);

        for line in reader.lines() {
          let line = line?;
          // println!("doc_id {}\nline {}", doc_id, line);
          if doc_id == line {
            let params = object! {oid: ws.id.to_string(), ctx: [], enrich: false };
            let mut document = match app.service("memories").get(
              Context::local(),
              format!("production/produce/{}", doc_id),
              params,
            ) {
              Ok(d) => d,
              Err(e) => return Err(Error::from(e)), // TODO handle IO error differently!!!!
            };
            document["status"] = "deleted".into();

            let params = object! {oid: ws.id.to_string(), ctx: vec!["production", "produce"] };
            let _doc = app.service("memories").patch(
              Context::local(),
              format!("production/produce/{}", doc_id),
              document,
              params,
            )?;
            log::debug!("__doc {:#?}", _doc.dump());

            count += 1;
          }
        }
      },
      _ => continue,
    }
  }
  println!("count {count}");

  Ok(())
}

pub fn delete_transfers_for_one_goods(app: &Application) -> Result<(), Error> {
  let mut count = 0;

  let oid = ID::from_base64("yjmgJUmDo_kn9uxVi8s9Mj9mgGRJISxRt63wT46NyTQ")
    .map_err(|e| Error::new(ErrorKind::NotFound, e.to_string()))?;
  let ws = app.wss.get(&oid);

  let ctx = ["warehouse".to_string(), "transfer".to_string()].to_vec();
  let transfer_ops = ws.memories(ctx).list(Some(false))?;

  println!("transfer_ops {:?}", transfer_ops.len());

  let goods_id = if let Ok(goods) =
    memories_find(app, object! { name: "Скотч односторонний бесцветный" }, ["goods"].to_vec())
  {
    match goods.len() {
      0 => Err(WHError::new("not found")),
      1 => Ok(goods[0]["_id"].string()),
      _ => Err(WHError::new("too many docs found")),
    }
  } else {
    Err(WHError::new("not found"))
  }
  .unwrap();

  println!("_goods {goods_id:?}");

  for op in transfer_ops {
    let ctx = op.mem.clone().ctx;

    let ctx_str: Vec<&str> = ctx.iter().map(|s| s.as_str()).collect();

    match ctx_str[..] {
      ["warehouse", "transfer"] => {
        let op_id = op.id.clone();

        if op.json()?["goods"].string() == goods_id {
          let params = object! {oid: ws.id.to_string(), ctx: [], enrich: false };
          let mut operation = match app.service("memories").get(
            Context::local(),
            format!("warehouse/transfer/{}", op_id),
            params,
          ) {
            Ok(d) => d,
            Err(e) => return Err(Error::from(e)), // TODO handle IO error differently!!!!
          };
          operation["status"] = "".into();

          let params = object! {oid: ws.id.to_string(), ctx: vec!["warehouse", "transfer"] };
          let _op = app.service("memories").patch(
            Context::local(),
            format!("warehouse/transfer/{}", op_id),
            operation,
            params,
          )?;
          log::debug!("__op {:#?}", _op.dump());

          count += 1;
        }
      },
      _ => continue,
    }
  }
  println!("count {count}");

  Ok(())
}
