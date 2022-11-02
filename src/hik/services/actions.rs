use actix::{Actor, Addr};
use actix_web::error::ParseError::Status;
use chrono::{DateTime, Datelike, ParseResult, SecondsFormat, Utc};
use dbase::FieldConversionError;
use json::object::Object;
use json::JsonValue;
use reqwest::Client;
use std::collections::{BTreeMap, HashMap};
use std::convert::Infallible;
use std::fmt::{Display, Formatter};
use std::io::Write;
use std::path::Component;
use std::str::FromStr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, RwLock};
use std::time::{Duration, SystemTime};
use tantivy::HasLen;
use uuid::Uuid;
use walkdir::WalkDir;

use crate::animo::error::DBError;
use crate::hik::actions::list_devices::{DeviceMgmt, GetRequest, HttpClient};
use crate::hik::actions::task::{CommandMeta, Stage};
use crate::hik::{ConfigCamera, StatusCamera};
use crate::services::{string_to_id, Data, Error, Params, Service};
use crate::utils::json::JsonParams;
use crate::utils::time::now_in_seconds;
use crate::ws::error_general;
use crate::{
  auth, Application, Memory, SOrganizations, Services, Transformation, TransformationKey, Value, ID,
};

pub struct Actions {
  app: Application,
  path: Arc<String>,

  orgs: SOrganizations,

  actor: Addr<HttpClient>,
  tasks: Arc<RwLock<BTreeMap<ID, CommandMeta>>>,
}

impl Actions {
  pub(crate) fn new(app: Application, path: &str, orgs: SOrganizations) -> Arc<dyn Service> {
    // let mut commands = BTreeMap::new();
    // commands.insert("list_devices", ListDevicesCommand {});

    // let commands = Arc::new(commands);
    let tasks = Arc::new(RwLock::new(BTreeMap::new()));

    let actor = HttpClient::new(app.clone()).start();

    Arc::new(Actions { app, path: Arc::new(path.to_string()), orgs, actor, tasks })
  }

  fn cleanup(&self) {
    let one_minute_before = now_in_seconds() - 60;
    let ten_minutes_before = now_in_seconds() - 60 * 10;
    let one_hour_before = now_in_seconds() - 60 * 60;

    let mut tasks = self.tasks.write().unwrap();
    tasks.retain(|_, v| match v.state {
      Stage::Created(ts) => ts >= one_hour_before,
      Stage::Requested(ts) => ts >= ten_minutes_before,
      Stage::Completed(ts) => ts >= one_minute_before,
    });
  }
}

fn string_to_time<S: AsRef<str>>(data: S) -> Result<DateTime<Utc>, Error> {
  DateTime::parse_from_rfc3339(data.as_ref())
    .map(|ts| ts.into())
    .map_err(|e| Error::GeneralError(e.to_string()))
}

impl Service for Actions {
  fn path(&self) -> &str {
    &self.path
  }

  fn find(&self, params: Params) -> crate::services::Result {
    let limit = self.limit(&params);
    let skip = self.skip(&params);

    let tasks = self.tasks.read().unwrap();

    let total = tasks.len();
    let list = tasks.iter().skip(skip).take(total).map(|(id, meta)| meta.to_json()).collect();

    Ok(json::object! {
      data: JsonValue::Array(list),
      total: total,
      "$skip": skip
    })
  }

  fn get(&self, id: String, params: Params) -> crate::services::Result {
    let id = string_to_id(id)?;

    let tasks = self.tasks.read().unwrap();
    match tasks.get(&id) {
      None => Err(Error::NotFound(id.to_base64())),
      Some(task) => Ok(task.to_json()),
    }
  }

  fn create(&self, data: Data, params: Params) -> crate::services::Result {
    self.cleanup();

    let command = data["command"].as_str().unwrap_or("").trim().to_string();
    let params = data["params"].clone();

    match command.as_str() {
      "list_devices" => {
        let id = ID::random();
        let protocol = params["protocol"].string();
        let ip = params["ip"].string();
        let port = params["port"].string_or_none();
        let username = params["username"].string();
        let password = params["password"].string();

        let mgmt = DeviceMgmt { protocol, ip, port, username, password };
        let request = mgmt.list_devices(id).map_err(Error::CameraError)?;

        let meta = CommandMeta::new(id, command, params);

        let answer = meta.to_json();
        {
          let mut tasks = self.tasks.write().unwrap();
          tasks.insert(id, meta);
        }

        self.actor.do_send(request);

        Ok(answer)
      },
      "create_user" => {
        let id = ID::random();
        let mut sub = vec![];

        let oid = self.oid(&params)?;
        let cid = self.cid(&params)?;
        let pid = self.pid(&params)?;

        // let mut cameras = self.app.storage.as_ref().unwrap().get(&oid).camera_configs();

        let camera = self.app.storage.as_ref().unwrap().get(&oid).camera(&cid).config()?;

        let person = self
          .app
          .service("people")
          .get(pid.to_base64(), json::object! { "oid": oid.to_base64() })?;

        // for camera in cameras {
        // let cid = camera.id;

        let dev_index = &camera.dev_index;
        let name = person["name"].string();

        let mgmt = DeviceMgmt::new(&camera);
        let request = mgmt.create_user(id, dev_index, pid, name).map_err(Error::CameraError)?;

        let aid = ID::random();
        sub.push(aid.clone());

        let params = JsonValue::Null;
        // TODO fix it
        // json::object! {
        //   "oid": oid.to_base64(),
        //   "cid", cid,
        //   "pid": pid.to_base64()
        // };

        let meta = CommandMeta::sub(id, aid, command.clone(), params);
        let answer = meta.to_json();
        {
          let mut tasks = self.tasks.write().unwrap();
          tasks.insert(id, meta);
        }

        self.actor.do_send(request);
        // }

        Ok(answer)
      },
      "register_picture" => {
        let id = ID::random();
        let mut sub = vec![];

        let oid = self.oid(&params)?;
        let pid = self.pid(&params)?;

        let mut cameras = self.app.storage.as_ref().unwrap().get(&oid).camera_configs();

        let person = self
          .app
          .service("people")
          .get(pid.to_base64(), json::object! { "oid": oid.to_base64() })?;

        for camera in cameras {
          let dev_index = camera.dev_index.clone();
          let name = person["name"].string();

          let picture_path = self.orgs.get(&oid).person(&pid).picture().path();

          let mgmt = DeviceMgmt::new(&camera);
          let request = mgmt
            .register_picture(id, dev_index, pid, picture_path)
            .map_err(Error::CameraError)?;

          let aid = ID::random();
          sub.push(aid.clone());

          let params = JsonValue::Null;
          // TODO fix it
          // json::object! {
          //   "oid": oid.to_base64(),
          //   "cid", cid,
          //   "pid": pid.to_base64()
          // };

          let meta = CommandMeta::sub(id, aid, command.clone(), params);
          {
            let mut tasks = self.tasks.write().unwrap();
            tasks.insert(id, meta);
          }

          self.actor.do_send(request);

          // break;
        }

        let meta = CommandMeta::master(id, sub, command, params);
        let answer = meta.to_json();
        {
          let mut tasks = self.tasks.write().unwrap();
          tasks.insert(id, meta);
        }

        Ok(answer)
      },
      _ => Err(Error::NotImplemented),
    }
  }

  fn update(&self, id: String, data: Data, params: Params) -> crate::services::Result {
    Err(Error::NotImplemented)
  }

  fn patch(&self, id: String, data: Data, params: Params) -> crate::services::Result {
    if !data.is_object() {
      Err(Error::GeneralError("only object allowed".into()))
    } else {
      let id = crate::services::string_to_id(id)?;

      println!("patch {:?}", data.dump());
      let mut objs = self.tasks.write().unwrap();
      if let Some(task) = objs.get_mut(&id) {
        // mutation block
        let data = {
          for (n, v) in data.entries() {
            match n {
              "state" => match Stage::from_json(v) {
                Ok(stage) => task.state = stage,
                Err(_) => {},
              },
              "data" => task.result = Some(Ok(v.clone())),
              "error" => {
                task.result =
                  Some(Err(Error::GeneralError(v.as_str().unwrap_or("").trim().to_string())))
              },
              _ => {}, // ignore
            }
          }
          task.to_json()
        };

        Ok(data)
      } else {
        Err(crate::services::Error::NotFound(id.to_base64()))
      }
    }
  }

  fn remove(&self, id: String, params: Params) -> crate::services::Result {
    Err(Error::NotImplemented)
  }
}