use crate::commutator::Application;
use crate::storage::organizations::Workspace;
use csv::{ReaderBuilder, Trim};
use json::{object, JsonValue};
use rust_decimal::Decimal;
use service::utils::json::JsonParams;
use service::{Context, Services};
use std::io::{Error, ErrorKind};
use std::str::FromStr;
use std::string::ToString;
use store::elements::ToJson;
use store::error::WHError;
use store::process_records::{memories_create, memories_find};
use values::constants::{_ID, _UUID};
use values::ID;

fn patch(app: &Application, ws: &Workspace, item: JsonValue, ctx: Vec<String>) -> Result<(), Error> {
  let params = object! {oid: ws.id.to_string(), ctx: ctx };
  let _rec = app
    .service("memories")
    .patch(Context::local(), item[_ID].string(), item, params)?;
  log::debug!("__rec {:#?}", _rec.dump());

  Ok(())
}

pub fn replace_goods(app: &Application, old_name: &str, new_name: &str) -> Result<(), Error> {
  let mut count = 0;

  let oid = ID::from_base64("yjmgJUmDo_kn9uxVi8s9Mj9mgGRJISxRt63wT46NyTQ")
    .map_err(|e| Error::new(ErrorKind::NotFound, e.to_string()))?;
  let ws = app.wss.get(&oid);

  let goods_ctx = vec!["goods"];

  let old = if let Ok(items) =
    memories_find(app, object! { name: old_name.to_string() }, goods_ctx.clone())
  {
    match items.len() {
      0 => Err(WHError::new("not found")),
      1 => Ok(items[0][_ID].string()),
      _ => Err(WHError::new("too many goods found")),
    }
  } else {
    Err(WHError::new("not found"))
  }
  .unwrap();

  // println!("_old {old:?}");

  let new = if let Ok(items) =
    memories_find(app, object! { name: new_name.to_string() }, goods_ctx.clone())
  {
    match items.len() {
      0 => Err(WHError::new("not found")),
      1 => Ok(items[0][_ID].string()),
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
        if after[_ID].string() == old {
          println!("_ids {} vs {}", after[_ID].string(), old);

          after[_ID] = new.to_json();

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

pub fn replace_uom_and_goods(app: &Application, path: &str) -> Result<(), Error> {
  let mut reader = ReaderBuilder::new().delimiter(b',').trim(Trim::All).from_path(path).unwrap();

  let mut count = 0;

  let goods_ctx = vec!["goods"];

  let oid = ID::from_base64("yjmgJUmDo_kn9uxVi8s9Mj9mgGRJISxRt63wT46NyTQ")
    .map_err(|e| Error::new(ErrorKind::NotFound, e.to_string()))?;
  let ws = app.wss.get(&oid);

  for record in reader.records().skip(1) {
    let record = record.unwrap();
    let old_goods_name = record[0].to_string();
    let new_uom_name = record[1].to_string();

    let mut old_goods = if let Ok(items) =
      memories_find(app, object! { name: old_goods_name.to_string() }, goods_ctx.clone())
    {
      match items.len() {
        0 => Err(WHError::new("not found")),
        1 => Ok(items[0].clone()),
        _ => Err(WHError::new("too many goods found")),
      }
    } else {
      Err(WHError::new("not found"))
    }
    .unwrap();

    let new_uom_id = if let Ok(items) =
      memories_find(app, object! { name: new_uom_name.to_string() }, vec!["uom"])
    {
      match items.len() {
        0 => Err(WHError::new("not found")),
        1 => Ok(items[0][_UUID].string()),
        _ => Err(WHError::new("too many goods found")),
      }
    } else {
      Err(WHError::new("not found"))
    }
    .unwrap();

    // let capacity = usize::from_str(&record[2].to_string())?;
    let capacity = Decimal::from_str(&record[2].to_string()).unwrap();

    let new_goods_name = record[3].to_string();

    let old_uom = if old_goods["uom"].is_object() {
      old_goods["uom"][_ID].string()
    } else {
      old_goods["uom"].string()
    };

    let old_category = if old_goods["category"].is_object() {
      old_goods["category"][_ID].string()
    } else {
      old_goods["category"].string()
    };

    let new_goods_id = if let Ok(items) =
      memories_find(app, object! { name: new_goods_name.clone() }, goods_ctx.clone())
    {
      match items.len() {
        0 => Ok(
          memories_create(
            app,
            object! {
              name: new_goods_name,
              uom: old_uom,
              category: old_category,
            },
            goods_ctx.clone(),
          )?[_ID]
            .string(),
        ),
        1 => Ok(items[0][_ID].string()),
        _ => Err(WHError::new("too many goods found")),
      }
    } else {
      Err(WHError::new("not found"))
    }
    .unwrap();

    for doc in ws.clone().into_iter() {
      let ctx = doc.mem.ctx.clone();

      let ctx_str: Vec<&str> = ctx.iter().map(|s| s.as_str()).collect();

      let mut after = doc.json().unwrap();

      match ctx_str[..] {
        ["goods"] => continue,
        _ => {
          if !after["goods"].is_null() && after["goods"].string() == old_goods[_ID].string() {
            // println!("_goods {} vs {}", after["goods"].string(), old_goods[_ID].string());
            after["goods"] = new_goods_id.to_json();

            // from: "qty":{"number":"6400","uom":"1f93df2e-c423-45cf-8123-de02e0a0064e"}
            // to: "qty":{"number":10,"uom":{"number":"640","uom":"1f93df2e-c423-45cf-8123-de02e0a0064e","in":"3c887c88-964c-4ce2-b1f0-c7f1709e233a"}}
            if after["qty"].is_null() {
              continue;
            }

            println!("after: {}", after.clone());

            let mut inner_qty = after["qty"].clone();
            let old_uom = after["qty"]["uom"].clone();

            let pieces = inner_qty["number"].number_or_none().unwrap(); // TODO why .string() doesn't work there?

            println!("inner_qty: {}", inner_qty.clone());
            println!("pieces: {}", pieces);

            // TODO need to be only pieces for used material
            let remainder = pieces % capacity;
            let new_qty = pieces / capacity;

            if new_qty < Decimal::ONE {
              // do not change qty
              patch(app, &ws, after, ctx)?;
              count += 1;
              continue;
            }

            let inner_qty = object! {
             "number": capacity.to_json(),
              "in": new_uom_id.to_json(),
            };

            if remainder != Decimal::ZERO {
              // make an array of qty

              // println!("new_qty {} vs. remainder {}", new_qty, remainder);

              after["qty"] = JsonValue::Array(vec![
                object! {
                  number: (new_qty.trunc()).to_json(),
                  uom: inner_qty.clone(),
                },
                object! {
                  number: remainder.to_json(),
                  uom: old_uom.clone(),
                },
              ]);
            } else {
              // normal case
              after["qty"] = object! {number: new_qty.to_json(), uom: inner_qty.clone()};
            }
            // println!("after[\"qty\"] {:?}", after["qty"]);

            patch(app, &ws, after, ctx)?;
            count += 1;
          }
        },
      }
    }
    // TODO will need it after testing:
    // old_goods[_STATUS] = JsonValue::String("deleted".to_string());
    // patch(app, &ws, old_goods, vec!["goods".to_string()])?;
  }

  println!("count: {count}");

  Ok(())
}
