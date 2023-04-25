use crate::commutator::Application;
use json::{object, JsonValue};
use service::Services;
use std::fs::File;
use std::io::{BufRead, BufReader, self, Write};
use std::time::Instant;

const DRUGS: [&str; 1] = ["drugs"];

pub(crate) fn import(app: &Application) {
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

  loop {
    print!("Текст для поиска: ");
    io::stdout().flush().unwrap();
    let mut search = String::new();
    io::stdin().read_line(&mut search).unwrap();
    let search = search.trim();
    if search == "exit" {
      break;
    }
    let result = app
      .service("memories")
      .find(object! {oid: oid, ctx: ctx.clone(), search: search})
      .unwrap();
    let json_to_string = result.dump();
    print_only_name(&json_to_string);
  }
}

fn print_only_name(json: &str) {
  let mut vektor: Vec<&str> = json.split('{').collect();
  vektor.remove(0);
  let mut namevek: Vec<&str> = Vec::new();
  for i in 1..vektor.len() {
    let name = vektor[i].split("manufacturer").nth(0).unwrap();
    let end = name.len() - 2;
    namevek.push(&name[7..end]);
  }
  let mut index = 1;
  for i in namevek.clone() {
    println!("\t{index:>2}) {}", i);
    index += 1
  }
}

fn load() -> Vec<JsonValue> {
  let text_file = "./import/utf8_dbo.GOOD.Table.sql";
  let file = File::open(text_file).unwrap();

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

      json::object! {
        name: name,
        manufacturer: manufacturer,
      }
    })
    .collect()
}
