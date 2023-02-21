use actix_web::error::ParseError::Status;
use dbase::FieldConversionError;
use json::object::Object;
use json::JsonValue;
use std::collections::{BTreeMap, HashMap};
use std::convert::Infallible;
use std::io::Write;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, RwLock};
use std::time::SystemTime;
use tantivy::HasLen;
use uuid::Uuid;

use crate::animo::error::DBError;
use crate::services::{Data, Params, Service};
use errors::Error;
use crate::storage::SOrganizations;
use utils::json::{JsonMerge, JsonParams};
use crate::ws::error_general;
use crate::{
  auth, commutator::Application, services::Services, animo::memory::{Memory, Transformation, TransformationKey, Value, ID},
};
// warehouse: { receiving, Put-away, transfer,  }
// production: { manufacturing }

pub struct MemoriesInFiles {
  app: Application,
  name: Arc<String>,

  orgs: SOrganizations,
}

impl MemoriesInFiles {
  pub(crate) fn new(app: Application, name: &str, orgs: SOrganizations) -> Arc<dyn Service> {
    Arc::new(MemoriesInFiles { app, name: Arc::new(name.to_string()), orgs })
  }
}

impl Service for MemoriesInFiles {
  fn path(&self) -> &str {
    &self.name
  }

  fn find(&self, params: Params) -> crate::services::Result {
    let oid = self.oid(&params)?;
    let ctx = self.ctx(&params);

    let limit = self.limit(&params);
    let skip = self.skip(&params);

    let memories = self.orgs.get(&oid).memories(ctx);
    let list = memories.list()?;

    let filters = &self.params(&params)["filter"];
    let (total, list): (isize, Vec<JsonValue>) = if filters.is_object() {
      let mut total = 0;
      let list: Vec<JsonValue> = list
        .into_iter()
        .map(|o| o.json().unwrap_or_else(|_| JsonValue::Null))
        .filter(|o| o.is_object())
        .filter(|o| {
          for (name, value) in filters.entries() {
            if &o[name] != value {
              return false;
            }
          }
          return true;
        })
        .map(|o| {
          total += 1;
          o
        })
        .skip(skip)
        .take(limit)
        .collect::<_>();

      if list.is_empty() {
        (total, list)
      } else {
        (-1, list)
      }
    } else {
      (
        list.len() as isize,
        list
          .into_iter()
          .skip(skip)
          .take(limit)
          .map(|o| o.json())
          .collect::<Result<_, _>>()?,
      )
    };

    Ok(json::object! {
      data: JsonValue::Array(list),
      total: total,
      "$skip": skip,
    })
  }

  fn get(&self, id: String, params: Params) -> crate::services::Result {
    let oid = self.oid(&params)?;
    let ctx = self.ctx(&params);

    if id.len() < 10 {
      return Err(Error::GeneralError(format!("id `{id}` not valid")));
    }

    let memories = self.orgs.get(&oid).memories(ctx).get(&id);
    memories.json()
  }

  fn create(&self, data: Data, params: Params) -> crate::services::Result {
    let oid = self.oid(&params)?;
    let ctx = self.ctx(&params);

    let memories = self.orgs.get(&oid).memories(ctx);

    memories.create(&self.app, chrono::Utc::now(), data)
  }

  fn update(&self, id: String, data: Data, params: Params) -> crate::services::Result {
    if !data.is_object() {
      Err(Error::GeneralError("only object allowed".into()))
    } else {
      let oid = self.oid(&params)?;
      let ctx = self.ctx(&params);

      if id.len() < 10 {
        return Err(Error::GeneralError(format!("id `{id}` not valid")));
      }

      let memories = self.orgs.get(&oid).memories(ctx);

      memories.update(&self.app, id, data)
    }
  }

  fn patch(&self, id: String, data: Data, params: Params) -> crate::services::Result {
    let oid = self.oid(&params)?;
    let ctx = self.ctx(&params);

    if id.len() < 10 {
      return Err(Error::GeneralError(format!("id `{id}` not valid")));
    }

    let memories = self.orgs.get(&oid).memories(ctx);

    if !data.is_object() {
      Err(Error::GeneralError("only object allowed".into()))
    } else {
      let doc = memories.get(&id);
      let mut obj = doc.json()?;

      let mut patch = data.clone();
      patch.remove("_id"); // TODO check id?

      obj.merge(&patch);

      // for (n, v) in data.entries() {
      //   if n != "_id" {
      //     obj[n] = v.clone();
      //   }
      // }

      memories.update(&self.app, id, obj)
    }
  }

  fn remove(&self, id: String, params: Params) -> crate::services::Result {
    Err(Error::NotImplemented)
  }
}
