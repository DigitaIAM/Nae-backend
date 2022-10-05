use actix_web::error::ParseError::Status;
use dbase::FieldConversionError;
use json::object::Object;
use json::JsonValue;
use serde_json::ser::State;
use std::collections::{BTreeMap, HashMap};
use std::convert::Infallible;
use std::io::Write;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::time::SystemTime;
use tantivy::HasLen;
use uuid::Uuid;

use crate::animo::error::DBError;
use crate::hik::camera::States;
use crate::hik::error::Error;
use crate::hik::{camera, Camera, ConfigCamera, StatusCamera};
use crate::services::{Data, Params, Service};
use crate::ws::error_general;
use crate::{auth, Application, Memory, Services, Transformation, TransformationKey, Value, ID};

pub const PATH: &str = "./data/services/cameras/";

pub struct Cameras {
  app: Application,
  path: Arc<String>,

  objs: Arc<RwLock<BTreeMap<ID, Arc<Mutex<crate::hik::ConfigCamera>>>>>,
}

impl Cameras {
  pub(crate) fn new(app: Application, path: &str) -> Arc<dyn Service> {
    // make sure folder exist
    std::fs::create_dir_all(PATH).unwrap();

    let mut data = BTreeMap::new();
    // load data
    for entry in std::fs::read_dir(PATH).unwrap() {
      let entry = entry.unwrap();
      let path = entry.path();
      if path.is_dir() {
        let contents = std::fs::read_to_string(entry.path().join("data.json")).unwrap();

        let mut config: crate::hik::ConfigCamera = serde_json::from_str(contents.as_str()).unwrap();

        // reset state and status
        let was_on = config.state.is_on();
        config.status = StatusCamera::disconnect();
        if was_on {
          config.state.force(States::Enabling);
        } else {
          config.state.force(States::Disabled);
        }

        let id = config.id;
        let config = Arc::new(Mutex::new(config));

        data.entry(id).or_insert(config.clone());

        ConfigCamera::connect(config, app.clone(), PATH.to_string());
      }
    }

    Arc::new(Cameras { app, path: Arc::new(path.to_string()), objs: Arc::new(RwLock::new(data)) })
  }

  fn save(&self, config: &ConfigCamera) -> Result<(), Error> {
    let folder = format!("{PATH}{}/", config.id.to_base64());
    std::fs::create_dir_all(folder.clone())
      .map_err(|e| Error::IOError(format!("can't create folder {}: {}", folder, e)))?;

    let path = format!("{folder}/data.json");

    let mut file = std::fs::OpenOptions::new()
      .create(true)
      .write(true)
      .open(path.clone())
      .map_err(|e| Error::IOError(format!("fail to write file: {}", e)))?;

    let data =
      serde_json::to_string(config).map_err(|e| Error::IOError(format!("fail to generate json")))?;

    file
      .write_all(data.as_bytes())
      .map_err(|e| Error::IOError(format!("fail to write file: {}", e)))
  }
}

fn now_in_seconds() -> u64 {
  SystemTime::now()
    .duration_since(std::time::UNIX_EPOCH)
    .expect("system time is likely incorrect")
    .as_secs()
}

impl Service for Cameras {
  fn path(&self) -> &str {
    &self.path
  }

  fn find(&self, params: Params) -> crate::services::Result {
    let limit = self.limit(&params);
    let skip = self.skip(&params);

    let objs = self.objs.read().unwrap();
    let total = objs.len();

    let mut list = Vec::with_capacity(limit);
    for (_, obj) in objs.iter().skip(skip).take(limit) {
      let data = obj.lock().unwrap().to_json();
      list.push(data);
    }

    Ok(json::object! {
      data: JsonValue::Array(list),
      total: total,
      "$skip": skip,
    })
  }

  fn get(&self, id: String, params: Params) -> crate::services::Result {
    let id = crate::services::string_to_id(id)?;

    let objs = self.objs.read().unwrap();
    match objs.get(&id) {
      None => Err(crate::services::Error::NotFound(id.to_base64())),
      Some(config) => Ok(config.lock().unwrap().to_json()),
    }
  }

  fn create(&self, data: Data, params: Params) -> crate::services::Result {
    let name = data["name"].as_str().unwrap_or("").trim().to_string();
    let dev_index = data["devIndex"].as_str().unwrap_or("").trim().to_string();
    let protocol = data["protocol"].as_str().unwrap_or("").trim().to_string();
    let ip = data["ip"].as_str().unwrap_or("").trim().to_string();
    let port = data["port"].as_str().unwrap_or("").trim().to_string();
    let username = data["username"].as_str().unwrap_or("").trim().to_string();
    let password = data["password"].as_str().unwrap_or("").trim().to_string();

    let enabled = data["enabled"].as_bool().unwrap_or(false);

    let port = match port.parse::<u16>() {
      Ok(n) => Some(n),
      Err(_) => None,
    };

    let config = crate::hik::ConfigCamera {
      id: ID::random(),
      name,
      dev_index,
      protocol,
      ip,
      port,
      username,
      password,

      status: crate::hik::StatusCamera::default(),
      state: crate::hik::camera::State::default(),
      jh: None,
    };

    self.save(&config).map_err(|e| crate::services::Error::IOError(e.to_string()))?;

    let id = config.id;
    let json = config.to_json();

    let config = Arc::new(Mutex::new(config));
    {
      let mut objs = self.objs.write().unwrap();
      objs.entry(id.clone()).or_insert(config.clone());
    }

    ConfigCamera::connect(config, self.app.clone(), PATH.to_string());

    Ok(json)
  }

  fn update(&self, id: String, data: Data, params: Params) -> crate::services::Result {
    self.patch(id, data, params)
  }

  fn patch(&self, id: String, data: Data, params: Params) -> crate::services::Result {
    let id = crate::services::string_to_id(id)?;

    println!("patch {:?}", data.dump());
    let mut objs = self.objs.write().unwrap();
    if let Some(config) = objs.get_mut(&id) {
      // mutation block
      let (was_on, data) = {
        let mut config = config.lock().unwrap();
        let was_on = config.state.is_on();
        if data.is_object() {
          for (n, v) in data.entries() {
            match n {
              "name" => config.name = v.as_str().unwrap_or("").trim().to_string(),
              "devIndex" => config.dev_index = v.as_str().unwrap_or("").trim().to_string(),
              "protocol" => config.protocol = v.as_str().unwrap_or("").trim().to_string(),
              "ip" => config.ip = v.as_str().unwrap_or("").trim().to_string(),
              "port" => {
                config.port = match v.as_str().unwrap_or("").trim().parse::<u16>() {
                  Ok(n) => Some(n),
                  Err(_) => None,
                }
              },
              "username" => config.username = v.as_str().unwrap_or("").trim().to_string(),
              "password" => {
                let password = v.as_str().unwrap_or("").trim().to_string();
                if !password.is_empty() {
                  config.password = password;
                }
              },
              "enabled" => {
                if v.as_bool().unwrap_or(false) {
                  config.state.enabling();
                } else {
                  config.state.disabling();
                }
              },
              "status" => {
                // TODO change status only on internal patches
                match StatusCamera::from_json(v) {
                  Some(status) => {
                    if status.ts() > config.status.ts() {
                      config.status = status
                    }
                  },
                  None => {},
                }
              },
              _ => {}, // ignore
            }
          }
        }
        (was_on, config.to_json())
      };

      println!("was_on {was_on}");

      // connect if required
      if was_on {
        // TODO wait for jh and set it to None
      } else {
        ConfigCamera::connect(config.clone(), self.app.clone(), PATH.to_string());
      }

      Ok(data)
    } else {
      Err(crate::services::Error::NotFound(id.to_base64()))
    }
  }

  fn remove(&self, id: String, params: Params) -> crate::services::Result {
    Err(crate::services::Error::NotImplemented)
  }
}
