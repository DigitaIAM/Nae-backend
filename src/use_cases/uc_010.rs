use crate::commutator::Application;
use json::{object, JsonValue};
use service::Services;
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::time::Instant;

const DRUGS: [&str; 1] = ["drugs"];
const SEARCH: &str = "ТОНУС";

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
    .find(object! {oid: oid, ctx: ctx, search: SEARCH})
    .unwrap();
  println!("report:\n{}\nend of report", result.dump());

  let json_to_string = result.dump();
  println!("\tJSON SORTED: \n{}", sort_json_value(&json_to_string));
}

fn sort_json_value(json: &str) -> JsonValue {
  let mut vektor: Vec<&str> = json.split('{').collect();
  vektor.remove(0);
  vektor.sort();
  let vektor_2 = compare_strings(vektor.clone());
  print_only_name(vektor.clone(), vektor_2.clone());
  // print_only_name();

  JsonValue::String(vektor.join("{"))
}

fn print_only_name(alphabet: Vec<&str>, leven: Vec<&str>) {
  println!("Поисковый запрос: {SEARCH}");
  let mut namevek: Vec<&str> = Vec::new();
  for i in 1..alphabet.len() {
    let name = alphabet[i].split("manufacturer").nth(0).unwrap();
    namevek.push(&name);
  }
  for i in namevek {
    println!("\tALPHABET: {}", i)
  }
  let mut namevek: Vec<&str> = Vec::new();
  for i in 1..leven.len() {
    let name = leven[i].split("manufacturer").nth(0).unwrap();
    namevek.push(&name);
  } 
  for i in namevek {
    println!("\tLEVENSHTEIN: {}", i)
  }
}

use levenshtein::levenshtein;
fn compare_strings(vektor: Vec<&str>) -> Vec<&str> {
  let mut result: Vec<(usize, &str)> = Vec::new();
  let search = SEARCH;
  
  for i in 0..vektor.len() {
    let mut distance = levenshtein(search, vektor[i]);
    result.push((distance, vektor[i]));
  }
  result.sort();
  let mut result_2: Vec<&str> = Vec::new();
  for i in 0..result.len() {
    result_2.push(result[i].1);
  }
  result_2
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
