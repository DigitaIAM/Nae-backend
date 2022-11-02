use crate::animo::error::DBError;
use crate::hik::{ConfigCamera, StatusCamera};
use crate::services::{Error, JsonData, Params};
use crate::warehouse::turnover::Organization;
use crate::ID;
use chrono::SecondsFormat::Millis;
use chrono::{DateTime, Datelike, SecondsFormat, Utc};
use json::JsonValue;
use serde_json::json;
use std::io::Write;
use std::path::{Path, PathBuf};

type TOTAL = usize;

// pub(crate) trait Storage {
//   fn list(&self) -> Result<Vec<String>, Error>;
//   fn load(&self, id: String) -> Result<JsonValue, Error>;
//   fn save(&self, id: String, data: JsonValue) -> Result<JsonValue, Error>;
// }

#[derive(Debug, Clone)]
pub(crate) struct SOrganizations {
  folder: PathBuf,
}

impl SOrganizations {
  pub(crate) fn new<S: Into<String>>(folder: S) -> Self {
    let folder = folder.into();
    std::fs::create_dir_all(&folder).map_err(|e| panic!("can't create folder {}: {}", folder, e));

    SOrganizations { folder: folder.into() }
  }

  pub(crate) fn create(&self, id: ID) -> Result<SOrganization, Error> {
    let mut folder = self.folder.clone();
    folder.push(id.to_base64());

    let mut path = folder.clone();
    path.push("organization.json");

    std::fs::create_dir_all(&folder).map_err(|e| {
      Error::IOError(format!("can't create folder {}: {}", folder.to_string_lossy(), e))
    })?;

    Ok(SOrganization { id, folder, path })
  }

  pub(crate) fn get(&self, id: &ID) -> SOrganization {
    let mut folder = self.folder.clone();
    folder.push(id.to_base64());

    let mut path = folder.clone();
    path.push("organization.json");

    SOrganization { id: id.clone(), folder, path }
  }

  pub(crate) fn list(&self) -> Result<Vec<SOrganization>, Error> {
    let mut result = Vec::new();

    for entry in std::fs::read_dir(&self.folder).unwrap() {
      let entry = entry.unwrap();
      let folder = entry.path();
      if folder.is_dir() {
        let mut path = folder.clone();
        path.push("organization.json");
        // TODO check existence of json

        let id_name = entry.file_name().to_string_lossy().to_string();
        match ID::from_base64(id_name.as_bytes()) {
          Ok(id) => result.push(SOrganization { id, folder, path }),
          Err(_) => {}, // ignore?
        }
      }
    }

    Ok(result)
  }
}

pub(crate) struct SOrganization {
  id: ID,

  folder: PathBuf,
  path: PathBuf,
}

impl SOrganization {
  pub(crate) fn json(&self) -> JsonValue {
    json(self.id.to_base64(), &self.path)
  }

  pub(crate) fn load(&self) -> crate::services::Result {
    load(&self.path)
  }

  pub(crate) fn save(&self, obj: String) -> Result<(), Error> {
    save(&self.path, obj)
  }

  pub(crate) fn delete(&self) -> crate::services::Result {
    Err(Error::NotImplemented)
  }

  pub(crate) fn department(&self, id: ID) -> SDepartment {
    let mut path = self.folder.clone();
    path.push("departments");
    path.push(format!("{}.json", id.to_base64()));

    SDepartment { id: id.clone(), oid: self.id.clone(), path }
  }

  pub(crate) fn departments(&self) -> Result<Vec<SDepartment>, Error> {
    let mut result = Vec::new();

    let mut folder = self.path.clone();
    folder.push("departments");

    let entries = match std::fs::read_dir(&folder) {
      Ok(entries) => entries,
      Err(e) => {
        println!("fail to read folder {}: {e}", folder.to_string_lossy());
        return Ok(vec![]);
      },
    };

    for entry in entries {
      let entry = entry.unwrap();
      let path = entry.path();
      if path.is_file() {
        let name = entry.file_name().to_string_lossy().to_string();
        if let Some(id) = name.strip_suffix(".json") {
          match ID::from_base64(id.as_bytes()) {
            Ok(id) => result.push(SDepartment { id, oid: self.id.clone(), path }),
            Err(_) => {}, // ignore?
          }
        }
      }
    }

    Ok(result)
  }

  pub(crate) fn shift(&self, id: ID) -> SShift {
    let mut path = self.folder.clone();
    path.push("departments");
    path.push(format!("{}.json", id.to_base64()));

    SShift { id: id.clone(), oid: self.id.clone(), path }
  }

  pub(crate) fn shifts(&self) -> Vec<SShift> {
    let mut result = Vec::new();

    let mut folder = self.path.clone();
    folder.push("shifts");

    let entries = match std::fs::read_dir(&folder) {
      Ok(entries) => entries,
      Err(e) => {
        println!("fail to read folder {}: {e}", folder.to_string_lossy());
        return vec![];
      },
    };

    for entry in entries {
      let entry = entry.unwrap();
      let path = entry.path();
      if path.is_file() {
        let name = entry.file_name().to_string_lossy().to_string();
        if let Some(id) = name.strip_suffix(".json") {
          match ID::from_base64(id.as_bytes()) {
            Ok(id) => result.push(SShift { id, oid: self.id.clone(), path }),
            Err(_) => {}, // ignore?
          }
        }
      }
    }

    result
  }

  pub(crate) fn location(&self, id: ID) -> SLocation {
    let mut path = self.folder.clone();
    path.push("locations");
    path.push(format!("{}.json", id.to_base64()));

    SLocation { id: id.clone(), oid: self.id.clone(), path }
  }

  pub(crate) fn locations(&self) -> Vec<SLocation> {
    let mut result = Vec::new();

    let mut folder = self.path.clone();
    folder.push("locations");

    let entries = match std::fs::read_dir(&folder) {
      Ok(entries) => entries,
      Err(e) => {
        println!("fail to read folder {}: {e}", folder.to_string_lossy());
        return vec![];
      },
    };

    for entry in entries {
      let entry = entry.unwrap();
      let path = entry.path();
      if path.is_file() {
        let name = entry.file_name().to_string_lossy().to_string();
        if let Some(id) = name.strip_suffix(".json") {
          match ID::from_base64(id.as_bytes()) {
            Ok(id) => result.push(SLocation { id, oid: self.id.clone(), path }),
            Err(_) => {}, // ignore?
          }
        }
      }
    }

    result
  }

  pub(crate) fn camera(&self, id: &ID) -> SCamera {
    let mut folder = self.folder.clone();
    folder.push("cameras");
    folder.push(id.to_base64());

    let mut path = folder.clone();
    path.push("camera.json");

    SCamera { id: id.clone(), oid: self.id.clone(), folder, path }
  }

  pub(crate) fn cameras(&self) -> Vec<SCamera> {
    let mut result = Vec::new();

    let mut folder = self.folder.clone();
    folder.push("cameras");

    let entries = match std::fs::read_dir(&folder) {
      Ok(entries) => entries,
      Err(e) => {
        println!("fail to read folder {}: {e}", folder.to_string_lossy());
        return vec![];
      },
    };

    for entry in entries {
      let entry = entry.unwrap();
      let folder = entry.path();
      if folder.is_dir() {
        let mut path = folder.clone();
        path.push("camera.json");
        // TODO check existence of json

        let id_name = entry.file_name().to_string_lossy().to_string();
        match ID::from_base64(id_name.as_bytes()) {
          Ok(id) => result.push(SCamera { id, oid: self.id.clone(), folder, path }),
          Err(_) => {}, // ignore?
        }
      }
    }

    result
  }

  pub(crate) fn camera_configs(&self) -> Vec<crate::hik::ConfigCamera> {
    let mut cameras = vec![];
    for cam in self.cameras() {
      let contents = cam.data().unwrap();

      let config: crate::hik::ConfigCamera = match serde_json::from_str(contents.as_str()) {
        Ok(o) => o,
        Err(e) => {
          println!("Error on loading camera {cam:?} {e}");
          continue;
        },
      };

      cameras.push(config);
    }

    cameras
  }

  pub(crate) fn person(&self, id: &ID) -> SPerson {
    let mut folder = self.folder.clone();
    folder.push("people");
    folder.push(id.to_base64());

    let mut path = folder.clone();
    path.push("person.json");

    SPerson { id: id.clone(), oid: self.id.clone(), folder, path }
  }

  pub(crate) fn people(&self) -> Vec<SPerson> {
    let mut result = Vec::new();

    let mut folder = self.folder.clone();
    folder.push("people");

    let entries = match std::fs::read_dir(&folder) {
      Ok(entries) => entries,
      Err(e) => {
        println!("fail to read folder {}: {e}", folder.to_string_lossy());
        return vec![];
      },
    };

    for entry in entries {
      let entry = entry.unwrap();
      let folder = entry.path();
      if folder.is_dir() {
        let id_name = entry.file_name().to_string_lossy().to_string();

        let mut path = folder.clone();
        path.push("person.json");

        match ID::from_base64(id_name.as_bytes()) {
          Ok(id) => result.push(SPerson { id, oid: self.id.clone(), folder, path }),
          Err(_) => {}, // ignore?
        }
      }
    }

    result
  }
}

pub(crate) struct SDepartment {
  id: ID,
  oid: ID,

  path: PathBuf,
}

impl SDepartment {
  pub(crate) fn json(&self) -> JsonValue {
    json(self.id.to_base64(), &self.path)
  }

  pub(crate) fn load(&self) -> crate::services::Result {
    load(&self.path)
  }

  pub(crate) fn create(self) -> Result<Self, Error> {
    // TODO: check that do not exist
    Ok(self)
  }

  pub(crate) fn save(&self, data: String) -> Result<(), Error> {
    save(&self.path, data)
  }

  pub(crate) fn delete(&self) -> Result<JsonValue, Error> {
    Err(Error::NotImplemented)
  }
}

pub(crate) struct SShift {
  id: ID,
  oid: ID,

  path: PathBuf,
}

impl SShift {
  pub(crate) fn json(&self) -> JsonValue {
    json(self.id.to_base64(), &self.path)
  }

  pub(crate) fn load(&self) -> crate::services::Result {
    load(&self.path)
  }

  pub(crate) fn save(&self, data: String) -> Result<(), Error> {
    save(&self.path, data)
  }

  pub(crate) fn delete(&self) -> Result<JsonValue, Error> {
    Err(Error::NotImplemented)
  }
}

pub(crate) struct SLocation {
  id: ID,
  oid: ID,

  path: PathBuf,
}

#[derive(Debug, Clone)]
pub struct SCamera {
  id: ID,
  oid: ID,

  folder: PathBuf,
  path: PathBuf,
}

impl SCamera {
  pub(crate) fn create(self) -> Result<Self, Error> {
    // TODO check that do not exist
    Ok(self)
  }

  pub(crate) fn config(&self) -> Result<crate::hik::ConfigCamera, Error> {
    let contents = self.data()?;

    serde_json::from_str(contents.as_str()).map_err(|e| Error::IOError(e.to_string()))
  }

  // pub(crate) fn load(&self) -> crate::services::Result {
  //   load(&self.path)
  // }

  pub(crate) fn data(&self) -> Result<String, Error> {
    data(&self.path)
  }

  pub(crate) fn save(&self, data: String) -> Result<(), Error> {
    save(&self.path, data)
  }

  pub(crate) fn save_binary(
    &self,
    ts: DateTime<Utc>,
    prefix: &str,
    suffix: &str,
    mut buf: &[u8],
  ) -> Result<PathBuf, Error> {
    let mut folder = self.folder.clone();
    folder.push(format!("{:0>4}/{:0>2}/", ts.year(), ts.month()));

    // create folder
    std::fs::create_dir_all(&folder).map_err(|e| {
      Error::IOError(format!("can't create folder {}: {}", folder.to_string_lossy(), e))
    })?;

    // store image
    let mut path = folder.clone();
    path.push(format!("{prefix}{}{suffix}", ts.to_rfc3339_opts(Millis, true)));

    let mut count = 9001;
    loop {
      count -= 1;
      if count <= 0 {
        return Err(Error::IOError(format!("fail to open file: {}", path.to_string_lossy())));
      }
      let mut file = match std::fs::OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(true)
        .open(path.clone())
      {
        Ok(file) => file,
        Err(_) => {
          path = folder.clone();
          path.push(format!("{prefix}{}_{count}{suffix}", ts.to_rfc3339_opts(Millis, true)));
          continue;
        },
      };

      file
        .write_all(buf)
        .map_err(|e| Error::IOError(format!("fail to write file: {}", e)))?;

      break Ok(path);
    }
  }

  pub(crate) fn events(&self, ts: DateTime<Utc>, skip: usize, limit: usize) -> Vec<SEvent> {
    let mut result = Vec::new();

    let mut folder = self.folder.clone();
    folder.push(format!("{:0>4}/{:0>2}/", ts.year(), ts.month()));

    let entries = match std::fs::read_dir(&folder) {
      Ok(entries) => entries,
      Err(e) => {
        println!("fail to read folder {}: {e}", folder.to_string_lossy());
        return vec![];
      },
    };

    for entry in entries {
      let entry = entry.unwrap();
      let path = entry.path();
      if path.is_file() {
        let name = entry.file_name().to_string_lossy().to_string();
        if let Some(id) = name.strip_suffix("_event.json") {
          result.push(SEvent { id: id.to_string(), oid: self.oid, cid: self.id.clone(), path });
        }
      }
    }

    result
  }

  pub(crate) fn event(&self, id: &String, ts: &DateTime<Utc>) -> SEvent {
    let mut folder = self.folder.clone();
    folder.push(format!("{:0>4}/{:0>2}/", ts.year(), ts.month()));

    let mut path = folder.clone();
    path.push(format!("{id}_event.json"));

    SEvent { id: id.clone(), oid: self.oid.clone(), cid: self.id.clone(), path }
  }
}

pub(crate) struct SEvent {
  id: String,
  oid: ID,
  cid: ID,

  path: PathBuf,
}

impl SEvent {
  pub(crate) fn create(self) -> Result<Self, Error> {
    // TODO check that do not exist
    Ok(self)
  }

  pub(crate) fn save(&self, data: String) -> Result<(), Error> {
    save(&self.path, data)
  }

  pub(crate) fn load(&self) -> crate::services::Result {
    load(&self.path)
  }

  pub(crate) fn json(&self) -> JsonValue {
    json(self.id.clone(), &self.path)
  }
}

pub(crate) struct SPerson {
  id: ID,
  oid: ID,

  folder: PathBuf,
  path: PathBuf,
}

impl SPerson {
  pub(crate) fn create(self) -> Result<Self, Error> {
    // TODO check that do not exist
    Ok(self)
  }

  pub(crate) fn save(&self, data: String) -> Result<(), Error> {
    save(&self.path, data)
  }

  pub(crate) fn load(&self) -> crate::services::Result {
    load(&self.path)
  }

  pub(crate) fn json(&self) -> JsonValue {
    json(self.id.to_base64(), &self.path)
  }

  pub(crate) fn picture(&self) -> SPicture {
    let mut path = self.folder.clone();
    path.push("picture.jpg");

    SPicture { path }
  }
}

pub(crate) struct SPicture {
  path: PathBuf,
}

impl SPicture {
  pub(crate) fn path(&self) -> PathBuf {
    self.path.clone()
  }
}

fn data(path: &PathBuf) -> Result<String, Error> {
  std::fs::read_to_string(path).map_err(|e| Error::IOError(e.to_string()))
}

fn load(path: &PathBuf) -> crate::services::Result {
  data(path)?.json()
}

fn json(id: String, path: &PathBuf) -> JsonValue {
  match load(path) {
    Ok(v) => v,
    Err(e) => json::object! {
      "_id": id,
      "_err": e.to_string()
    },
  }
}

fn save(path: &PathBuf, data: String) -> Result<(), Error> {
  let folder = match path.parent() {
    None => return Err(Error::IOError(format!("can't get parent for {}", path.to_string_lossy()))),
    Some(f) => f,
  };
  std::fs::create_dir_all(folder).map_err(|e| {
    Error::IOError(format!("can't create folder {}: {}", folder.to_string_lossy(), e))
  })?;

  let mut file = std::fs::OpenOptions::new()
    .create(true)
    .write(true)
    .truncate(true)
    .open(path)
    .map_err(|e| Error::IOError(format!("fail to open for write file: {}", e)))?;

  file
    .write_all(data.as_bytes())
    .map_err(|e| Error::IOError(format!("fail to write file: {}", e)))
}
