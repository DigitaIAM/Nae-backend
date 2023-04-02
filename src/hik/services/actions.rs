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
use crate::services::{string_to_id, Data, Params};
use crate::ws::error_general;
use crate::{
  animo::memory::{Memory, Transformation, TransformationKey, Value, ID},
  auth,
  commutator::Application,
  storage::Workspaces,
};
use service::utils::{json::JsonParams, time::now_in_seconds};
use service::{Service, Services};

pub struct Actions {
  app: Application,
  path: Arc<String>,

  orgs: Workspaces,

  actor: Addr<HttpClient>,
  tasks: Arc<RwLock<BTreeMap<ID, CommandMeta>>>,
}

impl Actions {
  pub(crate) fn new(app: Application, path: &str, orgs: Workspaces) -> Arc<dyn Service> {
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
      None => Err(service::error::Error::NotFound(id.to_base64())),
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
        let request = mgmt
          .list_devices(id)
          .map_err(|e| service::error::Error::CameraError(e.to_string()))?;

        let meta = CommandMeta::new(id, command, params);

        let answer = meta.to_json();
        {
          let mut tasks = self.tasks.write().unwrap();
          tasks.insert(id, meta);
        }

        self.actor.do_send(request);

        Ok(answer)
      },
      "hikvision-create_user" => {
        let id = ID::random();

        let oid = crate::services::oid(&params)?;
        let cid = crate::services::cid(&params)?;
        let pid = crate::services::pid(&params)?;

        // let mut cameras = self.app.storage.as_ref().unwrap().get(&oid).camera_configs();

        let camera = self.app.storage.as_ref().unwrap().get(&oid).camera(&cid).config()?;

        let person = self
          .app
          .service("people")
          .get(pid.to_base64(), json::object! { "oid": oid.to_base64() })?;

        let dev_index = &camera.dev_index;
        let name = person["name"].string();
        let gender = person["gender"].string_or_none().unwrap_or("male".into());

        let mgmt = DeviceMgmt::new(&camera);
        let request = mgmt
          .create_user(id, dev_index, pid, name, gender)
          .map_err(|e| service::error::Error::CameraError(e.to_string()))?;

        let meta = CommandMeta::new(id, command.clone(), params);
        let answer = meta.to_json();
        {
          let mut tasks = self.tasks.write().unwrap();
          tasks.insert(id, meta);
        }

        self.actor.do_send(request);

        Ok(answer)
      },
      "hikvision-register_image" => {
        let id = ID::random();

        let oid = crate::services::oid(&params)?;
        let cid = crate::services::cid(&params)?;
        let pid = crate::services::pid(&params)?;

        // let mut cameras = self.app.storage.as_ref().unwrap().get(&oid).camera_configs();
        let camera = self.app.storage.as_ref().unwrap().get(&oid).camera(&cid).config()?;

        let person = self
          .app
          .service("people")
          .get(pid.to_base64(), json::object! { "oid": oid.to_base64() })?;

        let dev_index = camera.dev_index.clone();
        let name = person["name"].string();

        let picture_path = self.orgs.get(&oid).person(&pid).picture().path();

        let mgmt = DeviceMgmt::new(&camera);
        let request = mgmt
          .register_picture(id, dev_index, pid, picture_path)
          .map_err(|e| service::error::Error::CameraError(e.to_string()))?;

        let meta = CommandMeta::new(id, command.clone(), params);
        let answer = meta.to_json();
        {
          let mut tasks = self.tasks.write().unwrap();
          tasks.insert(id, meta);
        }

        self.actor.do_send(request);

        Ok(answer)
      },
      _ => Err(service::error::Error::NotImplemented),
    }
  }

  fn update(&self, id: String, data: Data, params: Params) -> crate::services::Result {
    Err(service::error::Error::NotImplemented)
  }

  fn patch(&self, id: String, data: Data, params: Params) -> crate::services::Result {
    if !data.is_object() {
      Err(service::error::Error::GeneralError("only object allowed".into()))
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
              "errors" => {
                task.result = Some(Err(service::error::Error::GeneralError(
                  v.as_str().unwrap_or("").trim().to_string(),
                )))
              },
              _ => {}, // ignore
            }
          }
          task.to_json()
        };

        Ok(data)
      } else {
        Err(service::error::Error::NotFound(id.to_base64()))
      }
    }
  }

  fn remove(&self, id: String, params: Params) -> crate::services::Result {
    Err(service::error::Error::NotImplemented)
  }
}
