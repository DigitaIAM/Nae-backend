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
  if ctx == &vec!["drugs"] {
    let before_name = before["name"].as_str();
    let after_name = data["name"].as_str();

    todo!()
  }
}