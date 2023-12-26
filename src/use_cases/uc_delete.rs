use crate::commutator::Application;
use json::{object, JsonValue};
use service::utils::json::JsonParams;
use service::{Context, Services};
use std::fs::File;
use std::io::{BufRead, BufReader, Error, ErrorKind};
use store::error::WHError;
use store::process_records::memories_find;
use values::constants::{_DOCUMENT, _STATUS};
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
            document[_STATUS] = "deleted".into();

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

pub fn delete_ops_for_goods(
  app: &Application,
  ctx: Vec<String>,
  storage_name: Option<&str>,
  goods_name: &str,
) -> Result<(), Error> {
  let mut count = 0;

  let oid = ID::from_base64("yjmgJUmDo_kn9uxVi8s9Mj9mgGRJISxRt63wT46NyTQ")
    .map_err(|e| Error::new(ErrorKind::NotFound, e.to_string()))?;
  let ws = app.wss.get(&oid);

  let ops = ws.memories(ctx.clone()).list(Some(false))?;

  println!("ops for deleting: {:?}", ops.len());

  let storage_id = if let Some(storage_name) = storage_name {
    find_object_field(
      app,
      object! { name: storage_name.to_string() },
      ["warehouse", "storage"].to_vec(),
      "_id",
    )
    .unwrap()
  } else {
    "".to_string()
  };

  println!("storage_id {storage_id}");

  let goods_id =
    find_object_field(app, object! { name: goods_name.to_string() }, ["goods"].to_vec(), "_id")
      .unwrap();

  println!("_goods {goods_id:?}");

  let ctx_str: Vec<&str> = ctx.iter().map(|s| s.as_str()).collect();

  // warehouse/transfer
  let op_id_str = ctx_str.join("/");

  for op in ops {
    let op_ctx = op.mem.clone().ctx;

    let op_ctx_str: Vec<&str> = op_ctx.iter().map(|s| s.as_str()).collect();

    let op_id = op.id.clone();

    let op = op.json()?;

    println!("_operationn {op:?}");

    match op_ctx_str {
      context => {
        if context != ctx_str { continue; }

        if !storage_id.is_empty() {
          let mut doc_ctx = ctx_str.clone();
          doc_ctx.push("document");

          let document = if let Ok(docs) = memories_find(
            app,
            object! { _id: op[_DOCUMENT].string() },
            doc_ctx,
          ) {
            match docs.len() {
              0 => Err(WHError::new("zero found")),
              1 => Ok(docs[0].clone()),
              _ => Err(WHError::new("too many docs found")),
            }
          } else {
            Err(WHError::new("not found"))
          }
          .unwrap();

          println!("_doc {document:?}");

          if document["storage"]["_id"].string() != storage_id
            && document["from"]["_id"].string() != storage_id
            && document["into"]["_id"].string() != storage_id
          {
            continue;
          }
        }

        if op["goods"].string() == goods_id {
          let params = object! {oid: ws.id.to_string(), ctx: [], enrich: false };
          let mut operation = match app.service("memories").get(
            Context::local(),
            format!("{}/{}", op_id_str, op_id),
            params,
          ) {
            Ok(d) => d,
            Err(e) => return Err(Error::from(e)), // TODO handle IO error differently!!!!
          };
          operation[_STATUS] = "deleted".into();

          let params = object! {oid: ws.id.to_string(), ctx: ctx_str.clone() };
          let _op = app.service("memories").patch(
            Context::local(),
            format!("{}/{}", op_id_str, op_id),
            operation,
            params,
          )?;
          log::debug!("__op {:#?}", _op.dump());

          count += 1;
        }
      },
    }
  }
  println!("count {count}");

  Ok(())
}

fn find_object_field(
  app: &Application,
  filter: JsonValue,
  ctx: Vec<&str>,
  field_name: &str,
) -> Option<String> {
  if let Ok(items) = memories_find(app, filter, ctx) {
    match items.len() {
      0 => None,
      1 => Some(items[0][field_name].string()),
      _ => None,
    }
  } else {
    None
  }
}
