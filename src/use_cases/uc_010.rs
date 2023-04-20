use crate::commutator::Application;
use json::{object, JsonValue};
use service::Services;
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::time::Instant;

const DRUGS: [&str; 1] = ["drugs"];

pub(crate) fn import(app: &Application) {
  // println!("IMPORT");
  let items = load();
  let ctx = DRUGS.to_vec();

  for item in items {
    let start = Instant::now();
    crate::use_cases::csv::memories_create(app, item, ctx.clone());
    let duration = start.elapsed();
    println!("elapsed {:?}", duration);
  }

  app.search.write().unwrap().commit();
}

pub(crate) fn report(app: &Application) {
  let oid = "yjmgJUmDo_kn9uxVi8s9Mj9mgGRJISxRt63wT46NyTQ";
  let ctx = DRUGS.to_vec();
  println!("CTX\t{ctx:?}");
  let result = app
    .service("memories")
    .find(object! {oid: oid, ctx: ctx, search: "ПОЯС"})
    .unwrap();
  println!("report:\n{}\nend of report", result.dump());

  let json_to_string = result.dump();
}

fn sort_json_value(json: &mut JsonValue) -> JsonValue {
  match json {
    JsonValue::Object(obj) => {
      let mut sorted = object! {};
      for (key, mut value) in obj.iter() {
        let value = &mut value.clone();
        sorted[key] = sort_json_value(value);
      }
      sorted
    }
    JsonValue::Array(arr) => {
      let mut sorted = arr.clone();
      sorted.sort_by(|a, b| sort_json_value(&mut a).dump().cmp(&sort_json_value(&mut b).dump()));
      json::JsonValue::Array(sorted)
    }
    _ => json.clone(),
  }
}

fn load() -> Vec<JsonValue> {
  let text_file = "./import/utf8_dbo.GOOD.Table.sql";
  let file = File::open(text_file).unwrap();

  // let mut search_id = 0;

  BufReader::new(file)
    .lines()
    .map(|l| l.unwrap())
    .filter(|l| l.starts_with("INSERT"))
    .map(|l| l[398..].to_string())
    .map(|l| {
      let mut name = l.split("N'").nth(1).unwrap().to_string();
      let mut manufacturer = l.split("N'").nth(2).unwrap().to_string();

      name.truncate((name.len() as isize - 3).max(0) as usize);
      manufacturer.truncate((manufacturer.len() as isize - 3).max(0) as usize);

      // println!("AFTER NAME: {name}");
      // println!("AFTER MANU: {manufacturer}");

      json::object! {
        name: name,
        manufacturer: manufacturer,
      }
    })
    .collect()
}
