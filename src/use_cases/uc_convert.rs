use crate::commutator::Application;
use chrono::Utc;
use csv::{ReaderBuilder, Trim, Writer};
use json::object;
use service::utils::json::JsonParams;
use service::{Context, Services};
use std::fs::OpenOptions;
use std::io::Error;
use store::process_records::memories_find;
use values::constants::{_ID, _UUID};

pub fn convert_csv(app: &Application) -> Result<(), Error> {
  // range of loop hardcoded
  let mut old_date = String::from("");
  let mut global_count = 0;

  for i in 0..12 {
    convert_csv_inner(
      app,
      &format!("./data for 1C/upload/labeling/old_{i}.csv"),
      &mut old_date,
      &mut global_count,
    )
    .unwrap();
  }

  println!("global count: {global_count}");

  Ok(())
}

pub fn convert_csv_inner(
  app: &Application,
  path: &str,
  old_date: &mut String,
  global_count: &mut usize,
) -> Result<(), Error> {
  let str_id = "yjmgJUmDo_kn9uxVi8s9Mj9mgGRJISxRt63wT46NyTQ";
  // let oid = ID::from_base64(str_id).map_err(|e| Error::new(ErrorKind::NotFound, e.to_string()))?;
  // let ws = app.wss.get(&oid);

  let mut time = Utc::now().to_string();
  time.truncate(16);

  let new_file = OpenOptions::new()
    .write(true)
    .create(true)
    .append(true)
    .open(format!("./data for 1C/{time}.csv"))
    .unwrap();

  let mut wtr = Writer::from_writer(new_file);

  let mut reader = ReaderBuilder::new().delimiter(b',').trim(Trim::All).from_path(path).unwrap();

  let mut count = 0;

  let mut headers: Vec<(String, String, String)> = Vec::new();

  for header in reader.headers() {
    header.clone().iter().skip(2).for_each(|h| {
      let mut uuid = String::new();
      let mut uom = String::new();
      let header_name = h.to_lowercase();

      // println!("header_name {header_name}");

      let filter = object! {name: h};
      if let Ok(g) = memories_find(app, filter, vec!["goods"]) {
        if header_name.starts_with("клей") {
          uom = String::from("кг");
        } else if header_name.starts_with("стакан")
          || header_name.starts_with("полуфабрикат")
          || header_name.starts_with("этикетка")
        {
          uom = String::from("шт");
        }

        g.into_iter().for_each(|o| {
          let object_name = o["name"].string().to_lowercase();

          if object_name == header_name {
            uuid = o[_UUID].string();

            let params = object! {oid: str_id, ctx: ["uom"], enrich: false };
            let uom_id = o["uom"][_ID].string();

            if let Ok(u) = app.service("memories").get(Context::local(), uom_id, params) {
              uom = u["name"].string();
            }
          }
        });
      }

      if uuid == "".to_string() {
        uuid = if header_name.starts_with("клей") {
          "fc7072bc-838a-4c97-9ccc-76949403a54c".to_string()
        } else {
          "нет".to_string()
        };
      }

      headers.push((h.to_string(), uom, uuid));
    });
  }

  for record in reader.records() {
    let record = record.unwrap();

    let date = &record[1];

    if date != *old_date {
      *global_count += 1;
    }

    for (i, r) in record.into_iter().skip(2).enumerate() {
      if r == "" {
        continue;
      }

      let h = headers[i].clone();

      // hardcoded for labels area
      let is_product =
        h.0.to_lowercase().starts_with("стакан") && !h.0.to_lowercase().starts_with("стакан midas");

      if date == *old_date && is_product {
        *global_count += 1;
      }

      // hardcoded for concrete table
      let str_count = format!("{global_count}Э");

      if is_product {
        wtr.write_record([&str_count, date, &h.0, "", r, &h.1, &h.2]).unwrap();
      } else {
        wtr.write_record([&str_count, date, "", &h.0, r, &h.1, &h.2]).unwrap();
      }
    }

    *old_date = date.to_string();

    count += 1;
  }

  println!("local count: {count}");

  Ok(())
}
