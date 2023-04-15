use chrono::{SecondsFormat, Utc};

use json::JsonValue;

use std::sync::Arc;

use crate::services::{Data, Params};
use crate::storage::SEvent;

use crate::{commutator::Application, storage::Workspaces};
use service::error::Error;
use service::utils::time::string_to_time;
use service::Service;
pub struct Events {
  app: Application,
  path: Arc<String>,

  ws: Workspaces,
}

impl Events {
  pub(crate) fn new(app: Application, path: &str, ws: Workspaces) -> Arc<dyn Service> {
    Arc::new(Events { app, path: Arc::new(path.to_string()), ws })
  }
}

impl Service for Events {
  fn path(&self) -> &str {
    &self.path
  }

  fn find(&self, params: Params) -> crate::services::Result {
    let oid = crate::services::oid(&params)?;
    // let cid = self.cid(&params)?;

    let date = Utc::now(); // self.date(&params)?;

    let limit = self.limit(&params);
    let skip = self.skip(&params);

    let mut list: Vec<SEvent> = vec![];

    let cams = self.ws.get(&oid).cameras();
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
    let oid = crate::services::oid(&params)?;
    let cid = crate::services::cid(&params)?;

    let ts = string_to_time(id.clone())?;

    self.ws.get(&oid).camera(&cid).event(&id, &ts).load()
  }

  fn create(&self, data: Data, _params: Params) -> crate::services::Result {
    let event = &data["event"];
    if event["major"].as_usize().unwrap_or(0) != 5 {
      return Err(Error::GeneralError("major is not equal to 5".into()));
    }
    if event["minor"].as_usize().unwrap_or(0) != 75 {
      return Err(Error::GeneralError("minor is not equal to 75".into()));
    }

    let time = event["time"].as_str().unwrap_or("").trim().to_string();
    let time = string_to_time(time)?;

    let oid = crate::services::oid(&data)?;
    let cid = crate::services::cid(&data)?;

    let id = time.to_rfc3339_opts(SecondsFormat::Millis, true);

    let mut obj = data.clone();
    obj["_id"] = JsonValue::String(id.clone());

    self.ws.get(&oid).camera(&cid).event(&id, &time).create()?.save(obj.dump())?;

    Ok(obj)
  }

  fn update(&self, _id: String, _data: Data, _params: Params) -> crate::services::Result {
    Err(Error::NotImplemented)
  }

  fn patch(&self, _id: String, _data: Data, _params: Params) -> crate::services::Result {
    Err(Error::NotImplemented)
  }

  fn remove(&self, _id: String, _params: Params) -> crate::services::Result {
    Err(Error::NotImplemented)
  }
}
