mod memories_in_files;

pub use memories_in_files::MemoriesInFiles;

pub trait Resolve {
  fn resolve_to_json_object(
    &self,
    org: &crate::storage::organizations::Workspace,
  ) -> json::JsonValue;
}

impl Resolve for uuid::Uuid {
  fn resolve_to_json_object(
    &self,
    org: &crate::storage::organizations::Workspace,
  ) -> json::JsonValue {
    org.resolve(self).and_then(|s| s.json().ok()).unwrap_or_else(|| {
      json::object! {
        "_uuid": self.to_string(),
        "name": self.to_string(),
      }
    })
  }
}
