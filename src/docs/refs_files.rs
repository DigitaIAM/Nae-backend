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
use crate::services::{Data, Error, Params, Service};
use crate::storage::SOrganizations;
use crate::ws::error_general;
use crate::{auth, Application, Memory, Services, Transformation, TransformationKey, Value, ID};

// warehouse: { receiving, Put-away, transfer,  }
// production: { manufacturing }

pub struct ReferencesFiles {
  app: Application,
  name: Arc<String>,

  orgs: SOrganizations,
}

impl ReferencesFiles {
  pub(crate) fn new(app: Application, name: &str, orgs: SOrganizations) -> Arc<dyn Service> {
    Arc::new(ReferencesFiles { app, name: Arc::new(name.to_string()), orgs })
  }
}

impl Service for ReferencesFiles {
  fn path(&self) -> &str {
    &self.name
  }

  fn find(&self, params: Params) -> crate::services::Result {
    let oid = self.oid(&params)?;
    let ctx = self.ref_type(&params);

    let limit = self.limit(&params);
    let skip = self.skip(&params);

    let refs = self.orgs.get(&oid).refs(ctx);
    let list = refs.list()?;

    let total = list.len();
    let list = list
      .into_iter()
      .skip(skip)
      .take(limit)
      .map(|o| o.json())
      .collect::<Result<_, _>>()?;

    Ok(json::object! {
      data: JsonValue::Array(list),
      total: total,
      "$skip": skip,
    })
  }

  fn get(&self, id: String, params: Params) -> crate::services::Result {
    let oid = self.oid(&params)?;
    let ctx = self.ref_type(&params);

    let refs = self.orgs.get(&oid).refs(ctx).get(&id);
    refs.json()
  }

  fn create(&self, data: Data, params: Params) -> crate::services::Result {
    let oid = self.oid(&params)?;
    let ctx = self.ref_type(&params);

    let refs = self.orgs.get(&oid).refs(ctx);

    refs.create(chrono::Utc::now(), data)
  }

  fn update(&self, id: String, data: Data, params: Params) -> crate::services::Result {
    if !data.is_object() {
      Err(Error::GeneralError("only object allowed".into()))
    } else {
      let oid = self.oid(&params)?;
      let ctx = self.ref_type(&params);

      let refs = self.orgs.get(&oid).refs(ctx);

      refs.get(&id).update(data)
    }
  }

  fn patch(&self, id: String, data: Data, params: Params) -> crate::services::Result {
    let oid = self.oid(&params)?;
    let ctx = self.ref_type(&params);

    let refs = self.orgs.get(&oid).refs(ctx);

    if !data.is_object() {
      Err(Error::GeneralError("only object allowed".into()))
    } else {
      let r = refs.get(&id);
      let mut obj = r.json()?;
      for (n, v) in data.entries() {
        if n != "_id" {
          obj[n] = v.clone();
        }
      }

      r.update(data)
    }
  }

  fn remove(&self, id: String, params: Params) -> crate::services::Result {
    Err(Error::NotImplemented)
  }
}
