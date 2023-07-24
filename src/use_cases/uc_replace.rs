use crate::commutator::Application;
use crate::storage::organizations::Workspace;
use crate::{storage, text_search};
use json::{object, JsonValue};
use service::utils::json::JsonParams;
use service::{Context, Services};
use std::io::{Error, ErrorKind};
use store::elements::ToJson;
use store::error::WHError;
use store::process_records::memories_find;
use values::ID;

pub fn replace_goods(app: &Application, old_name: &str, new_name: &str) -> Result<(), Error> {
  let mut count = 0;

  let oid = ID::from_base64("yjmgJUmDo_kn9uxVi8s9Mj9mgGRJISxRt63wT46NyTQ")
    .map_err(|e| Error::new(ErrorKind::NotFound, e.to_string()))?;
  let ws = app.wss.get(&oid);

  let goods_ctx = vec!["goods"].to_vec();

  let old = if let Ok(items) =
    memories_find(app, object! { name: old_name.to_string() }, goods_ctx.clone())
  {
    match items.len() {
      0 => Err(WHError::new("not found")),
      1 => Ok(items[0]["_id"].string()),
      _ => Err(WHError::new("too many goods found")),
    }
  } else {
    Err(WHError::new("not found"))
  }
  .unwrap();

  // println!("_old {old:?}");

  let new = if let Ok(items) = memories_find(app, object! { name: new_name.to_string() }, goods_ctx)
  {
    match items.len() {
      0 => Err(WHError::new("not found")),
      1 => Ok(items[0]["_id"].string()),
      _ => Err(WHError::new("too many goods found")),
    }
  } else {
    Err(WHError::new("not found"))
  }
  .unwrap();

  // println!("_new {new:?}");

  for doc in ws.clone().into_iter() {
    let ctx = doc.mem.ctx.clone();

    let ctx_str: Vec<&str> = ctx.iter().map(|s| s.as_str()).collect();

    let mut after = doc.json().unwrap();

    match ctx_str[..] {
      ["goods"] => continue,
      _ => {
        if after["_id"].string() == old {
          println!("_ids {} vs {}", after["_id"].string(), old);

          after["_id"] = new.to_json();

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

    fn patch(
      app: &Application,
      ws: &Workspace,
      item: JsonValue,
      ctx: Vec<String>,
    ) -> Result<(), Error> {
      let params = object! {oid: ws.id.to_string(), ctx: ctx };
      let _rec =
        app
          .service("memories")
          .patch(Context::local(), item["_id"].string(), item, params)?;
      log::debug!("__rec {:#?}", _rec.dump());

      Ok(())
    }
  }

  println!("count {count}");

  Ok(())
}
