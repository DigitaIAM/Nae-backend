use actix_web::error::ParseError::Status;
use chrono::{DateTime, Datelike, ParseResult, SecondsFormat, Utc};
use dbase::FieldConversionError;
use json::object::Object;
use json::JsonValue;
use std::collections::{BTreeMap, HashMap};
use std::convert::Infallible;
use std::fmt::{Display, Formatter};
use std::io::Write;
use std::path::Component;
use std::str::FromStr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, RwLock};
use std::time::SystemTime;
use tantivy::HasLen;
use uuid::Uuid;
use walkdir::WalkDir;

use crate::animo::error::DBError;
use crate::services::{string_to_id, Data, Params, Service};
use errors::Error;
use crate::storage::SEvent;
use utils::time::string_to_time;
use crate::ws::error_general;
use crate::{
  auth, commutator::Application, storage::SOrganizations, services::Services, animo::memory::{Memory, Transformation, TransformationKey, Value, ID},
};
pub struct Events {
  app: Application,
  path: Arc<String>,

  orgs: SOrganizations,
}

impl Events {
  pub(crate) fn new(app: Application, path: &str, orgs: SOrganizations) -> Arc<dyn Service> {
    Arc::new(Events { app, path: Arc::new(path.to_string()), orgs })
  }
}

impl Service for Events {
  fn path(&self) -> &str {
    &self.path
  }

  fn find(&self, params: Params) -> crate::services::Result {
    let oid = self.oid(&params)?;
    // let cid = self.cid(&params)?;

    let date = Utc::now(); // self.date(&params)?;

    let limit = self.limit(&params);
    let skip = self.skip(&params);

    let mut list: Vec<SEvent> = vec![];

    let cams = self.orgs.get(&oid).cameras();
    for cam in cams {
      let events = cam.events_month(date);
      list.extend(events);
    }
    let total = list.len();

    // order by timestamp
    list.sort_by(|a, b| b.id.cmp(&a.id));

    let list = list.into_iter().skip(skip).take(limit).map(|o| o.json()).collect();

    Ok(json::object! {
      data: JsonValue::Array(list),
      total: total,
      "$skip": skip
    })
  }

  fn get(&self, id: String, params: Params) -> crate::services::Result {
    let oid = self.oid(&params)?;
    let cid = self.cid(&params)?;

    let ts = string_to_time(id.clone())?;

    self.orgs.get(&oid).camera(&cid).event(&id, &ts).load()
  }

  fn create(&self, data: Data, params: Params) -> crate::services::Result {
    let event = &data["event"];
    if event["major"].as_usize().unwrap_or(0) != 5 {
      return Err(Error::GeneralError("major is not equal to 5".into()));
    }
    if event["minor"].as_usize().unwrap_or(0) != 75 {
      return Err(Error::GeneralError("minor is not equal to 75".into()));
    }

    let time = event["time"].as_str().unwrap_or("").trim().to_string();
    let time = string_to_time(time)?;

    let oid = self.oid(&data)?;
    let cid = self.cid(&data)?;

    let id = time.to_rfc3339_opts(SecondsFormat::Millis, true);

    let mut obj = data.clone();
    obj["_id"] = JsonValue::String(id.clone());

    self.orgs.get(&oid).camera(&cid).event(&id, &time).create()?.save(obj.dump())?;

    Ok(obj)
  }

  fn update(&self, id: String, data: Data, params: Params) -> crate::services::Result {
    Err(Error::NotImplemented)
  }

  fn patch(&self, id: String, data: Data, params: Params) -> crate::services::Result {
    Err(Error::NotImplemented)
  }

  fn remove(&self, id: String, params: Params) -> crate::services::Result {
    Err(Error::NotImplemented)
  }
}
