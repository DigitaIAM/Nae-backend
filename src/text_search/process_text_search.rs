use serde::{Deserialize, Serialize};
use json::JsonValue;

#[derive(Debug, Deserialize, Serialize, PartialEq)]
struct JsonValueObject {
  name: String,
  manufacturer: String,
  // id: String,
  // uuid: String,
}

pub fn process_text_search(/* app: &Application,  */ctx: Vec<&str>, /* before: &JsonValue, */ data: &JsonValue) {
  if ctx[0] == "drugs" {
    // let before_str = format!("{}", before);
    let data_str = format!("{}", data);
    // let bfr: JsonValueObject = serde_json::from_str(&before_str).unwrap();
    let dta: JsonValueObject = serde_json::from_str(&data_str).unwrap();

    println!("DATA.NAME = {}", dta.name);
    // assert_eq!(before, data);

    todo!()
  }
}