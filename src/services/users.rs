use crate::animo::error::DBError;
use crate::services::{string_to_id, Data, Error, Mutation, Params, Service};
use crate::ws::error_general;
use crate::{auth, Application, Memory, Services, Transformation, TransformationKey, Value, ID};
use json::object::Object;
use json::JsonValue;
use std::collections::BTreeMap;
use std::io::Write;
use std::sync::{Arc, RwLock};

pub const PATH: &str = "./data/services/users/";

pub(crate) struct Users {
  app: Application,
  path: Arc<String>,
  folder: String,

  objs: Arc<RwLock<BTreeMap<ID, JsonValue>>>,
}

impl Users {
  pub(crate) fn new(app: Application, path: &str) -> Arc<dyn Service> {
    // make sure folder exist
    std::fs::create_dir_all(PATH).unwrap();

    let mut data = BTreeMap::new();
    // load data
    for entry in std::fs::read_dir(PATH).unwrap() {
      let entry = entry.unwrap();
      let path = entry.path();
      if path.is_file() {
        if entry.file_name().to_string_lossy().ends_with(".json") {
          let contents = std::fs::read_to_string(path).unwrap();

          let obj = json::parse(contents.as_str()).unwrap();

          let id = obj["_id"].as_str().unwrap_or("").to_string();
          let id = string_to_id(id).unwrap();

          data.entry(id).or_insert(obj);
        }
      }
    }

    Arc::new(Users {
      app,
      path: Arc::new(path.to_string()),
      folder: PATH.to_string(),
      objs: Arc::new(RwLock::new(data)),
    })
  }

  fn save(&self, id: &ID, obj: &JsonValue) -> Result<(), Error> {
    let path = format!("{}/{}.json", self.folder, id.to_base64());

    let mut file = std::fs::OpenOptions::new()
      .create(true)
      .write(true)
      .truncate(true)
      .open(path.clone())
      .map_err(|e| Error::IOError(format!("fail to write file: {}", e)))?;

    let data = obj.dump();
    // .map_err(|e| Error::IOError(format!("fail to generate json")))?;

    file
      .write_all(data.as_bytes())
      .map_err(|e| Error::IOError(format!("fail to write file: {}", e)))?;

    let mut objs = self.objs.write().unwrap();
    objs.insert(id.clone(), obj.clone());
    Ok(())
  }
}

impl Service for Users {
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
      list.push(obj.clone());
    }

    Ok(json::object! {
      data: JsonValue::Array(list),
      total: total,
      "$skip": skip,
    })
  }

  fn get(&self, id: String, params: Params) -> crate::services::Result {
    let id = crate::services::string_to_id(id)?;

    let names = ["label", "email", "avatar"];
    let keys = names.iter().map(|name| TransformationKey::simple(id, name)).collect();
    match self.app.db.query(keys) {
      Ok(records) => {
        let mut obj = Object::with_capacity(names.len() + 1);

        names
          .iter()
          .zip(records.iter())
          .filter(|(n, v)| v.into != Value::Nothing)
          .for_each(|(n, v)| obj.insert(n, v.into.to_json()));

        if obj.len() == 0 {
          Err(Error::NotFound(id.to_base64()))
        } else {
          obj.insert("_id", id.to_base64().into());
          Ok(JsonValue::Object(obj))
        }
      },
      Err(msg) => Err(Error::IOError(msg.to_string())),
    }
  }

  fn create(&self, data: Data, params: Params) -> crate::services::Result {
    let id = ID::random();
    let mut obj = data.clone();

    obj["_id"] = JsonValue::String(id.to_base64());

    match self.save(&id, &obj) {
      Ok(_) => {},
      Err(e) => return Err(Error::IOError(e.to_string())),
    }

    let email = data["email"].as_str().unwrap_or("").to_string();
    let password = data["password"].as_str().unwrap_or("").to_string();

    let signup = crate::auth::SignUpRequest { email: email.clone(), password };

    match auth::signup_procedure(&self.app, signup) {
      Ok((account, token)) => Ok(json::object! {
        _id: account.to_base64(),
        accessToken: token,
        email: email,
      }),
      Err(msg) => Err(Error::IOError(msg)),
    }
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