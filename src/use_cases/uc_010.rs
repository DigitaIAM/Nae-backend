use crate::commutator::Application;
use std::fs::File;
use std::io::{BufRead, BufReader};
use json::{JsonValue, object};
use service::Services;
use std::time::Instant;


use crate::storage::memories::memories_create;
// use crate::text_search::process_text_search;
#[allow(unused)]
const DRUGS: [&str; 1] = ["drugs"];
#[allow(unused)]
pub(crate) fn import(app: &Application) {
  // println!("IMPORT");
  let items = load();
  let ctx = DRUGS.to_vec();

  for item in items {
    let start = Instant::now();
    memories_create(app, item, ctx.clone());
    let duration = start.elapsed();
    println!("Time elapsed in memories_create() is: {:?}", duration);
  }
}
#[allow(unused)]
pub(crate) fn report(app: &Application) {
  let oid = "yjmgJUmDo_kn9uxVi8s9Mj9mgGRJISxRt63wT46NyTQ";
  let ctx = DRUGS.to_vec();
  println!("CTX\t{ctx:?}");
  let result = app.service("memories").find(object! {oid: oid, ctx: ctx, search: "ПЛАКВЕНИЛ"}).unwrap();
  println!("report:\n{}\nend of report", result.dump());

  use std::backtrace;
  use backtrace::Backtrace;
  println!("Custom backtrace: {}", Backtrace::force_capture());
}

fn load() -> Vec<JsonValue> {
  let text_file = "utf8_dbo.GOOD.Table.sql";
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
      }).collect()
}
