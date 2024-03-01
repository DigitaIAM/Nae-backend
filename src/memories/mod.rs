mod memories_in_files;
pub(crate) mod stock;

use crate::storage::organizations::Workspace;
use json::JsonValue;
pub use memories_in_files::MemoriesInFiles;
use uuid::Uuid;

pub trait Enrich {
  fn enrich(&self, ws: &Workspace) -> JsonValue;
}

impl Enrich for JsonValue {
  fn enrich(&self, ws: &Workspace) -> JsonValue {
    let mut data = self.clone();

    // workaround for uom
    if let Some(uom) = data["uom"].as_str() {
      data["uom"] = uom.resolve_to_json_object(ws);
    }

    // workaround for product
    if let Some(product) = data["product"].as_str() {
      data["product"] = product.resolve_to_json_object(ws);
    }

    // workaround for operator
    if let Some(operator) = data["operator"].as_str() {
      data["operator"] = operator.resolve_to_json_object(ws);
    }

    // workaround for area
    if let Some(area) = data["area"].as_str() {
      data["area"] = area.resolve_to_json_object(ws);
    }

    // workaround for goods
    if let Some(goods) = data["goods"].as_str() {
      data["goods"] = goods.resolve_to_json_object(ws);
    }

    // workaround for from and into
    if let Some(from) = data["from"].as_str() {
      data["from"] = from.resolve_to_json_object(ws);
    }

    if let Some(into) = data["into"].as_str() {
      data["into"] = into.resolve_to_json_object(ws);
    }

    // workaround for counterparty and storage
    if let Some(counterparty) = data["counterparty"].as_str() {
      data["counterparty"] = counterparty.resolve_to_json_object(ws);
    }

    if let Some(storage) = data["storage"].as_str() {
      data["storage"] = storage.resolve_to_json_object(ws);
    }

    // workaround for storage_from and storage_into
    if let Some(storage_from) = data["storage_from"].as_str() {
      data["storage_from"] = storage_from.resolve_to_json_object(ws);
    }

    if let Some(storage_into) = data["storage_into"].as_str() {
      data["storage_into"] = storage_into.resolve_to_json_object(ws);
    }

    // workaround for 'qty: {number: 5, uom: {number: 10, uom: uom/2023-04-07T07:56:50.249Z, in: uom/2023-04-07T07:57:02.154Z}}'
    if data["qty"].is_object() && !data["qty"]["store"].is_null() {
      data["qty"] = data["qty"]["store"].clone();
    }

    if data["qty"].is_array() && !data["qty"].is_empty() {
      for element in data["qty"].members_mut() {
        enrich_qty(ws, element);
      }
    } else if data["qty"].is_object() {
      enrich_qty(ws, &mut data["qty"]);
    }

    // log::debug!("enrich_qty {:?}", data["qty"]);
    data
  }
}

pub fn enrich_own_qty(ws: &Workspace, mut element: JsonValue) -> JsonValue {
  let mut processing = &mut element;
  while processing.is_object() {
    if let Some(uom) = processing["in"].as_str() {
      processing["in"] = uom.resolve_to_json_object(ws);
    }

    let node = &processing["uom"];
    if let Some(uom) = node.as_str() {
      processing["uom"] = uom.resolve_to_json_object(ws);
      break;
    } else if node.is_object() {
      processing = &mut processing["uom"];
    } else {
      break;
    }
  }
  element
}

pub fn enrich_qty(ws: &Workspace, element: &mut JsonValue) {
  let mut processing = element;
  while processing.is_object() {
    if let Some(uom) = processing["in"].as_str() {
      processing["in"] = uom.resolve_to_json_object(ws);
    }

    let node = &processing["uom"];
    if let Some(uom) = node.as_str() {
      processing["uom"] = uom.resolve_to_json_object(ws);
      break;
    } else if node.is_object() {
      processing = &mut processing["uom"];
    } else {
      break;
    }
  }
}

pub trait Resolve {
  fn resolve_to_json_object(&self, ws: &Workspace) -> JsonValue;
}

impl Resolve for uuid::Uuid {
  fn resolve_to_json_object(&self, ws: &Workspace) -> JsonValue {
    ws.resolve_uuid(self)
      .and_then(|s| s.json().ok())
      .map(|data| data.enrich(ws))
      .unwrap_or_else(|| {
        json::object! {
          "_uuid": self.to_string(),
          "_status": "not_found",
        }
      })
  }
}

impl Resolve for String {
  fn resolve_to_json_object(&self, ws: &Workspace) -> JsonValue {
    self.as_str().resolve_to_json_object(ws)
  }
}

impl Resolve for &String {
  fn resolve_to_json_object(&self, ws: &Workspace) -> JsonValue {
    self.as_str().resolve_to_json_object(ws)
  }
}

impl Resolve for &str {
  fn resolve_to_json_object(&self, ws: &Workspace) -> JsonValue {
    // try to resolve by UUID
    match Uuid::parse_str(self) {
      Ok(uuid) => {
        return uuid.resolve_to_json_object(ws);
      },
      Err(_) => {},
    }

    if let Some(doc) = ws.resolve_id(self) {
      match doc.json() {
        Ok(o) => o.enrich(ws),
        Err(e) => json::object! {
          "_id": self.to_string(),
          "_err": e.to_string(),
        },
      }
    } else {
      json::object! {
        "_id": self.to_string(),
        "_status": "not_found",
      }
    }
  }
}
