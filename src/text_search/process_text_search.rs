use serde::{Deserialize, Serialize};
use json::JsonValue;
use crate::commutator::Application;

#[derive(Debug, Deserialize, Serialize, PartialEq)]
struct JsonValueObject {
  name: String,
  manufacturer: String,
  // id: String,
  // uuid: String,
}

pub fn process_text_search(app: &Application,  ctx: &Vec<String>, before: &JsonValue, data: &JsonValue) {
  if ctx[0] == "drugs" {
    let before_str = format!("{}", before);
    let data_str = format!("{}", data);
    let bfr: JsonValueObject = serde_json::from_str(&before_str).unwrap();
    let dta: JsonValueObject = serde_json::from_str(&data_str).unwrap();

    if dta.name.is_empty() && !bfr.name.is_empty() {
      todo!() // remove bfr.name
    }
    if !dta.name.is_empty() && bfr.name.is_empty() {
      todo!() // add dta.name
    }
    if dta.name != bfr.name {
      todo!() // replace bfr.name with dta.name
    }

    todo!()
  }
}