use actix_web::error::ParseError::Status;
use chrono::{Date, DateTime, Datelike, NaiveDate, ParseResult, SecondsFormat, TimeZone, Utc};
use dbase::FieldConversionError;
use json::object::Object;
use json::JsonValue;
use serde_json::json;
use std::collections::{BTreeMap, HashMap};
use std::convert::Infallible;
use std::io::Write;
use std::ops::Sub;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, RwLock};
use std::time::SystemTime;
use tantivy::HasLen;
use uuid::Uuid;

use crate::animo::error::DBError;
use crate::hr::storage::SEvent;
use crate::services::JsonData;
use crate::services::{Data, Error, Params, Service};
use crate::utils::json::JsonParams;
use crate::warehouse::turnover::Organization;
use crate::ws::error_general;
use crate::{
  auth, Application, Memory, SOrganizations, Services, Transformation, TransformationKey, Value, ID,
};

pub(crate) struct AttendanceReport {
  app: Application,
  name: String,

  orgs: SOrganizations,
}

impl AttendanceReport {
  pub(crate) fn new(app: Application, orgs: SOrganizations) -> Arc<dyn Service> {
    Arc::new(AttendanceReport { app, name: "attendance-report".to_string(), orgs })
  }

  fn events(
    &self,
    oid: ID,
    cid: &str,
    date: Date<Utc>,
    event_type: &str,
    events: &mut Vec<(String, JsonValue)>,
  ) {
    let cid = ID::from_base64(cid.as_bytes()).unwrap();
    let mut evs: Vec<(String, JsonValue)> = self
      .orgs
      .get(&oid)
      .camera(&cid)
      .events_on_date(date)
      .iter()
      .map(|e| (e.id.clone(), e.load().unwrap_or(JsonValue::Null)))
      .filter(|(_, e)| e.is_object())
      .collect();

    evs
      .iter_mut()
      .for_each(|(_, e)| e["event"]["event_type"] = JsonValue::String(event_type.to_string()));

    events.extend(evs);
  }
}

#[derive(Debug, Clone)]
struct State {
  intervals: Vec<Interval>,
}

impl Default for State {
  fn default() -> Self {
    State { intervals: vec![] }
  }
}

#[derive(Debug, Clone)]
struct Interval {
  from: Option<DateTime<Utc>>,
  last_from: Option<DateTime<Utc>>,
  till: Option<DateTime<Utc>>,
  last_till: Option<DateTime<Utc>>,
}

impl Interval {
  fn to_json(&self) -> JsonValue {
    json::object! {
      from: self.from.map(|d| d.to_rfc3339_opts(SecondsFormat::Millis, true).into()).unwrap_or(JsonValue::Null),
      last_from: self.last_from.map(|d| d.to_rfc3339_opts(SecondsFormat::Millis, true).into()).unwrap_or(JsonValue::Null),
      till: self.till.map(|d| d.to_rfc3339_opts(SecondsFormat::Millis, true).into()).unwrap_or(JsonValue::Null),
      last_till: self.last_till.map(|d| d.to_rfc3339_opts(SecondsFormat::Millis, true).into()).unwrap_or(JsonValue::Null),
    }
  }
}

impl Service for AttendanceReport {
  fn path(&self) -> &str {
    &self.name
  }

  fn find(&self, params: Params) -> crate::services::Result {
    let limit = self.limit(&params);
    let skip = self.skip(&params);

    let oid = self.oid(&params)?;

    let people = self.orgs.get(&oid).people();

    println!("people {}", people.len());

    let division = self.params(&params)["division"].string();

    let people: Vec<(ID, JsonValue)> = people
      .iter()
      .map(|p| (p.id, p.load().unwrap()))
      .filter(|(_, p)| p.is_object())
      .filter(|(_, p)| p["division"].string() == division)
      .collect();

    // mapping from short id to long one
    let mut mapping = HashMap::with_capacity(people.len());
    let mut statuses = HashMap::new();

    people.iter().for_each(|(id, _)| {
      mapping.insert(id.to_clear(), id);
    });
    println!("mapping {}", mapping.len());

    if mapping.len() == 0 {
      let list = vec![];
      return Ok(json::object! {
        data: JsonValue::Array(list),
        total: 0,
        "$skip": 0,
      });
    }

    // expected events (short id, ?)
    // let expected_events = HashMap::new();

    // list of expected events ?

    // data
    let date = self.date(&params)?; // Utc.ymd(2022, 11, 11);

    let mut events: Vec<(String, JsonValue)> = Vec::with_capacity(100_000);

    self.events(oid, "GmFPOZhz7UY-YblCkVoymVXNeVjAh3cjIbLcLBI_lrQ", date, "in", &mut events);
    self.events(oid, "RZqvsRCm7BhJFywq-kgLegBdE1X-eIpKpNoBmEa64rQ", date, "out", &mut events);

    events.sort_by(|(a, _), (b, _)| a.cmp(b));

    println!("events {}", events.len());

    // person > state

    for (event_id, event) in events {
      let event = &event["event"];
      let event_type = event["event_type"].string();

      let short_id = event["employeeNoString"].string();

      if let Some(&pid) = mapping.get(&short_id) {
        let time = event["time"].string();
        let dt: DateTime<Utc> = match DateTime::parse_from_rfc3339(&time) {
          Ok(dt) => dt.into(),
          Err(_) => continue,
        };

        let status = statuses.entry(pid.clone()).or_insert(State::default());
        if let Some(current) = status.intervals.last_mut() {
          match event_type.as_str() {
            "in" => {
              if let Some(last) = current.last_from {
                if dt.sub(last) > chrono::Duration::minutes(1) {
                  status.intervals.push(Interval {
                    from: Some(dt),
                    last_from: Some(dt),
                    till: None,
                    last_till: None,
                  });
                } else {
                  current.last_from = Some(dt)
                }
              } else {
                status.intervals.push(Interval {
                  from: Some(dt),
                  last_from: Some(dt),
                  till: None,
                  last_till: None,
                });
              }
            },
            "out" => {
              if current.till.is_none() {
                current.till = Some(dt);
                current.last_till = Some(dt);
              } else {
                if let Some(last_till) = current.last_till {
                  // println!("last till {dt} - {last_till} = {}", dt.sub(last_till));
                  if dt.sub(last_till) > chrono::Duration::minutes(1) {
                    status.intervals.push(Interval {
                      from: None,
                      last_from: None,
                      till: Some(dt),
                      last_till: Some(dt),
                    });
                  } else {
                    current.last_till = Some(dt);
                  }
                } else {
                  // can't be
                  todo!()
                }
              }
            },
            _ => continue, // must not happen
          }
        } else {
          match event_type.as_str() {
            "in" => {
              status.intervals.push(Interval {
                from: Some(dt),
                last_from: Some(dt),
                till: None,
                last_till: None,
              });
            },
            "out" => {
              status.intervals.push(Interval {
                from: None,
                last_from: None,
                till: Some(dt),
                last_till: Some(dt),
              });
            },
            _ => continue, // must not happen
          }
        }
      }
    }

    println!("statuses {}", statuses.len());

    let mut list = vec![];

    for (pid, state) in statuses.into_iter() {
      let intervals = state.intervals.into_iter().map(|o| o.to_json()).collect::<Vec<_>>();

      let person = self.orgs.get(&oid).person(&pid).load()?;

      let id = format!("{}_{}_{}", oid.to_base64(), pid.to_base64(), date.to_string());

      list.push(json::object! {
        _id: id,
        person: person,
        intervals: JsonValue::Array(intervals)
      });
    }

    // let list = list.into_iter().skip(skip).take(total).map(|o| o.json()).collect();
    let total = list.len();

    Ok(json::object! {
      data: JsonValue::Array(list),
      total: total,
      "$skip": 0, // skip,
    })
  }

  fn get(&self, id: String, params: Params) -> crate::services::Result {
    Err(Error::NotImplemented)
  }

  fn create(&self, data: Data, params: Params) -> crate::services::Result {
    Err(Error::NotImplemented)
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