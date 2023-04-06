use crate::commutator::Application;
use std::fs::{File, self};
use std::io::{self, Write};
use std::io::{BufRead, BufReader};
use json::JsonValue;
use memories_create;

const WAREHOUSE_RECEIVE: [&str; 2] = ["warehouse", "receive"];

pub(crate) fn import(app: &Application) {
  let j_val = load();
  let ctx = WAREHOUSE_RECEIVE.to_vec();
  memories_create(app, j_val, ctx);
  unimplemented!()
}

pub(crate) fn report(app: &Application) {
  unimplemented!()
}

fn load() -> JsonValue {
  let text_file = "utf8_dbo.GOOD.Table.sql";
  let file = File::open(text_file).unwrap();

  let mut search_id = 0;

  let mut jvalue = JsonValue::new_array();

  BufReader::new(file)
      .lines()
      .map(|l| l.unwrap())
      .filter(|l| l.starts_with("INSERT"))
      .map(|l| l[398..].to_string())
      .map(|l| {
          let name = l.split("N'").nth(1).unwrap();
          let manufacturer = l.split("N'").nth(1).unwrap();
          let both = format!(
              "{}; {}",
              name[0..name.len() - 3].to_owned(),
              manufacturer[0..manufacturer.len() - 3].to_owned()
          );
          jvalue.push(both)
      })
      .for_each(|empty| {
          search_id += 1;
      });

  jvalue
}
