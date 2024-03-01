pub const STATUS: &str = "_status";
pub const DELETED: &str = "deleted";

pub const ID: &str = "_id";
pub const UUID: &str = "_uuid";
pub const DOCUMENT: &str = "document";

pub const P_PRODUCE: [&str; 2] = ["production", "produce"];

pub const PM_USED: [&str; 3] = ["production", "material", "used"];
pub const PM_PRODUCED: [&str; 3] = ["production", "material", "produced"];

pub trait IntoDomain {
  fn domain(&self) -> Vec<String>;
}

impl IntoDomain for [&str; 2] {
  fn domain(&self) -> Vec<String> {
    self.map(|s| s.to_string()).to_vec()
  }
}

impl IntoDomain for [&str; 3] {
  fn domain(&self) -> Vec<String> {
    self.map(|s| s.to_string()).to_vec()
  }
}
