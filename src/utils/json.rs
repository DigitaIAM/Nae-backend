use json::JsonValue;

pub trait JsonParams {
  fn string(&self) -> String;

  fn string_or_none(&self) -> Option<String>;
}

impl JsonParams for JsonValue {
  fn string(&self) -> String {
    self.as_str().unwrap_or("").to_string()
  }

  fn string_or_none(&self) -> Option<String> {
    self.as_str().map(|s| s.to_string())
  }
}
