use crate::commutator::Application;
use crate::storage::organizations::Workspace;
use json::{object, JsonValue};
use service::utils::json::JsonParams;
use service::{Context, Services};
use std::fs::File;
use std::io::{BufReader, Error, ErrorKind};
use store::elements::ToJson;
use store::error::WHError;
use store::process_records::memories_find;
use values::c;
use values::ID;

pub fn replace_storage_at_material_produced_and_used(app: &Application) -> Result<(), Error> {
  let mut count = 0;

  let oid = ID::from_base64("yjmgJUmDo_kn9uxVi8s9Mj9mgGRJISxRt63wT46NyTQ")
    .map_err(|e| Error::new(ErrorKind::NotFound, e.to_string()))?;
  let ws = app.wss.get(&oid);

  let ctx = ["production".into(), "material".into(), "produced".into()].to_vec();
  let docs = ws.memories(ctx.clone()).list(Some(false))?;

  println!("docs material produced {:?}", docs.len());

  for doc in docs {
    // delete "storage_from": "production/area/2023-02-07T15:07:18.051Z",
    // change "storage_into": "warehouse/storage/2023-04-20T06:48:50.633Z",

    let mut after = doc.json().unwrap();

    after["storage"] = after["storage_into"].clone();

    after["storage_from"] = JsonValue::Null;
    after["storage_into"] = JsonValue::Null;

    patch(app, &ws, after, ctx.clone())?;
    count += 1;
  }
  println!("count {count}");

  let ctx = ["production".into(), "material".into(), "used".into()].to_vec();
  let docs = ws.memories(ctx.clone()).list(Some(false))?;

  println!("docs material used {:?}", docs.len());

  for doc in docs {
    // change "storage_from": "warehouse/storage/2023-04-20T06:53:15.222Z",

    let mut after = doc.json().unwrap();

    after["storage"] = after["storage_from"].clone();

    after["storage_from"] = JsonValue::Null;
    after["storage_into"] = JsonValue::Null;

    patch(app, &ws, after, ctx.clone())?;
    count += 1;
  }
  println!("count {count}");

  Ok(())
}

pub fn replace_goods(app: &Application, old_name: &str, new_name: &str) -> Result<(), Error> {
  let mut count = 0;

  let oid = ID::from_base64("yjmgJUmDo_kn9uxVi8s9Mj9mgGRJISxRt63wT46NyTQ")
    .map_err(|e| Error::new(ErrorKind::NotFound, e.to_string()))?;
  let ws = app.wss.get(&oid);

  let goods_ctx = vec!["goods"].to_vec();

  let old = resolve(app, &goods_ctx, old_name).unwrap();

  // println!("_old {old:?}");

  let new = resolve(app, &goods_ctx, new_name).unwrap();

  // println!("_new {new:?}");

  for doc in ws.clone().into_iter() {
    let ctx = doc.mem.ctx.clone();

    let ctx_str: Vec<&str> = ctx.iter().map(|s| s.as_str()).collect();

    let mut after = doc.json().unwrap();

    match ctx_str[..] {
      ["goods"] => continue,
      _ => {
        if after[c::ID].string() == old {
          println!("_ids {} vs {}", after[c::ID].string(), old);

          after[c::ID] = new.to_json();

          patch(app, &ws, after, ctx)?;
          count += 1;
        } else if !after["goods"].is_null() && after["goods"].string() == old {
          println!("_goods {} vs {}", after["goods"].string(), old);
          after["goods"] = new.to_json();

          patch(app, &ws, after, ctx)?;
          count += 1;
        }
      },
    }
  }

  println!("count {count}");

  Ok(())
}

fn patch(app: &Application, ws: &Workspace, item: JsonValue, ctx: Vec<String>) -> Result<(), Error> {
  let params = object! {oid: ws.id.to_string(), ctx: ctx };
  let _rec = app
    .service("memories")
    .patch(Context::local(), item[c::ID].string(), item, params)?;
  log::debug!("__rec {:#?}", _rec.dump());

  Ok(())
}

fn resolve(app: &Application, ctx: &Vec<&str>, name: &str) -> Result<String, WHError> {
  if let Ok(items) = memories_find(app, object! { name: name.to_string() }, ctx.clone()) {
    match items.len() {
      0 => Err(WHError::new("not found")),
      1 => Ok(items[0][c::ID].string()),
      _ => Err(WHError::new("too many goods found")),
    }
  } else {
    Err(WHError::new("not found"))
  }
}
