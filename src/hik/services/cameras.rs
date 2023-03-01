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
use crate::services::{string_to_id, Data, Params};
use service::{Service, Services};
use crate::storage::{SCamera, SOrganizations};
use crate::warehouse::turnover::Organization;
use crate::ws::error_general;
use crate::{
  auth, commutator::Application, animo::memory::{Memory, Transformation, TransformationKey, Value, ID},
};
type ORG = crate::animo::memory::ID;
type CAM = crate::animo::memory::ID;

pub struct Cameras {
  app: Application,
  path: Arc<String>,

  orgs: SOrganizations,

  // organization id > camera id
  mapping: Arc<RwLock<BTreeMap<ID, Vec<ID>>>>, // TODO switch to ordered hash set
  objs: Arc<RwLock<BTreeMap<ID, (SCamera, Arc<Mutex<crate::hik::ConfigCamera>>)>>>,
}

impl Cameras {
  pub(crate) fn new(app: Application, path: &str, orgs: SOrganizations) -> Arc<dyn Service> {
    let mut mapping = BTreeMap::new();
    let mut objs = BTreeMap::new();

    let list = match orgs.list() {
      Ok(list) => list,
      Err(e) => {
        println!("Error on loading organizations: {e}");
        vec![]
      },
    };

    for org in list {
      for cam in org.cameras() {
        println!("loading camera {cam:?}");
        let contents = cam.data().unwrap();

        let mut config: crate::hik::ConfigCamera = match serde_json::from_str(contents.as_str()) {
          Ok(o) => o,
          Err(e) => {
            println!("Error on loading camera {cam:?} {e}");
            continue;
          },
        };

        // reset state and status
        let was_on = config.state.is_on();
        config.status = StatusCamera::disconnect();
        if was_on {
          config.state.force(States::Enabling);
        } else {
          config.state.force(States::Disabled);
        }

        let oid = config.oid;
        let id = config.id;
        mapping.entry(oid).or_insert(Vec::new()).push(id);

        let config = Arc::new(Mutex::new(config));
        objs.entry(id).or_insert((cam.clone(), config.clone()));

        ConfigCamera::connect(config, app.clone(), cam);
      }
    }

    Arc::new(Cameras {
      app,
      path: Arc::new(path.to_string()),
      orgs,
      mapping: Arc::new(RwLock::new(mapping)),
      objs: Arc::new(RwLock::new(objs)),
    })
  }

  fn save(&self, config: &crate::hik::ConfigCamera) -> crate::services::Result {
    // let data = config.data().map_err(|e| crate::services::Error::IOError(e.to_string()))?;
    // cam.save(data)?;

    let cam = self.orgs.get(&config.oid).camera(&config.id).create()?;
    let data = config.data().map_err(|e| errors::Error::IOError(e.to_string()))?;
    cam.save(data)?;
    Ok(JsonValue::Null)
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
    let oid = crate::services::oid(&params)?;

    let limit = self.limit(&params);
    let skip = self.skip(&params);

    let ids = {
      let mapping = self.mapping.read().unwrap();
      mapping.get(&oid).map(|v| v.clone()).unwrap_or_default()
    };

    let objs = self.objs.read().unwrap();

    let mut list = Vec::with_capacity(limit);
    for id in ids.iter().skip(skip).take(limit) {
      let data = objs.get(id).map(|(_, v)| v.lock().unwrap().to_json()).unwrap_or(json::object! {
        "_id": id.to_base64()
      });
      list.push(data);
    }

    Ok(json::object! {
      data: JsonValue::Array(list),
      total: ids.len(),
      "$skip": skip,
    })
  }

  fn get(&self, id: String, params: Params) -> crate::services::Result {
    let id = crate::services::string_to_id(id)?;

    let objs = self.objs.read().unwrap();
    match objs.get(&id) {
      None => Err(errors::Error::NotFound(id.to_base64())),
      Some((_, config)) => Ok(config.lock().unwrap().to_json()),
    }
  }

  fn create(&self, data: Data, params: Params) -> crate::services::Result {
    let oid = data["oid"].as_str().unwrap_or("").trim().to_string();
    let oid = string_to_id(oid)?;

    let name = data["name"].as_str().unwrap_or("").trim().to_string();
    let event_type = data["eventType"].as_str().unwrap_or("").trim().to_string();
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

    let id = ID::random();

    let cam = self.orgs.get(&oid).camera(&id).create()?;

    let config = crate::hik::ConfigCamera {
      id,
      oid,
      name,
      event_type,
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

    self.save(&config)?;

    let json = config.to_json();

    let config = Arc::new(Mutex::new(config));
    {
      let mut objs = self.objs.write().unwrap();
      objs.entry(id).or_insert((cam.clone(), config.clone()));
    }
    {
      let mut mapping = self.mapping.write().unwrap();
      mapping.entry(oid).or_insert(Vec::new()).push(id);
    }

    ConfigCamera::connect(config, self.app.clone(), cam);

    Ok(json)
  }

  fn update(&self, id: String, data: Data, params: Params) -> crate::services::Result {
    self.patch(id, data, params)
  }

  fn patch(&self, id: String, data: Data, params: Params) -> crate::services::Result {
    let id = crate::services::string_to_id(id)?;

    println!("patch {:?}", data.dump());
    let mut objs = self.objs.write().unwrap();
    if let Some((scam, config)) = objs.get_mut(&id) {
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

        self.save(&config)?;

        (was_on, config.to_json())
      };

      println!("was_on {was_on}");

      // connect if required
      if was_on {
        // TODO wait for jh and set it to None
      } else {
        ConfigCamera::connect(config.clone(), self.app.clone(), scam.clone());
      }

      Ok(data)
    } else {
      Err(errors::Error::NotFound(id.to_base64()))
    }
  }

  fn remove(&self, id: String, params: Params) -> crate::services::Result {
    Err(errors::Error::NotImplemented)
  }
}
