mod memories_in_files;

use crate::storage::memories::Document;
use crate::storage::organizations::Workspace;
use json::JsonValue;
pub use memories_in_files::MemoriesInFiles;

pub trait Enrich {
  fn enrich(&self, ws: &crate::storage::organizations::Workspace) -> JsonValue;
}

impl Enrich for JsonValue {
  fn enrich(&self, ws: &Workspace) -> JsonValue {
    let mut data = self.clone();

    // workaround for 'qty: {number: 5, uom: {number: 10, uom: uom/2023-04-07T07:56:50.249Z, in: uom/2023-04-07T07:57:02.154Z}}'
    let mut processing = &mut data["qty"];
    while processing.is_object() {
      if let Some(uom) = processing["in"].as_str() {
        processing["in"] = id_into_object(ws, uom);
      }

      let node = &processing["uom"];
      if let Some(uom) = node.as_str() {
        processing["uom"] = id_into_object(ws, uom);
        break;
      } else if node.is_object() {
        processing = &mut processing["uom"];
      } else {
        break;
      }
    }

    // workaround for uom
    if let Some(uom) = data["uom"].as_str() {
      data["uom"] = id_into_object(ws, uom);
    }

    data
  }
}

pub trait Resolve {
  fn resolve_to_json_object(
    &self,
    org: &crate::storage::organizations::Workspace,
  ) -> json::JsonValue;
}

impl Resolve for uuid::Uuid {
  fn resolve_to_json_object(
    &self,
    ws: &crate::storage::organizations::Workspace,
  ) -> json::JsonValue {
    ws.resolve_uuid(self)
      .and_then(|s| s.json().ok())
      .and_then(|mut data| Some(data.enrich(ws)))
      .unwrap_or_else(|| {
        json::object! {
          "_uuid": self.to_string(),
          "_status": "not_found",
        }
      })
  }
}

fn id_into_object(ws: &crate::storage::organizations::Workspace, id: &str) -> JsonValue {
  ws.resolve_id(id).and_then(|s| s.json().ok()).unwrap_or_else(|| {
    json::object! {
      "_id": id,
      "_status": "not_found",
    }
  })
}
