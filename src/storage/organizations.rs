use crate::animo::memory::ID;
use service::error::Error;
use crate::storage::memories::SMemories;
use crate::storage::old_references::{SDepartment, SLocation, SPerson, SShift};
use crate::storage::{json, load, save, SCamera};
use json::JsonValue;
use std::path::{Path, PathBuf};

#[derive(Debug, Clone)]
pub struct SOrganizations {
  folder: PathBuf,
}

impl SOrganizations {
  pub fn new<S: AsRef<Path>>(folder: S) -> Self
  where
    PathBuf: std::convert::From<S>,
  {
    std::fs::create_dir_all(&folder).map_err(|e| panic!("can't create folder: {}", e)); // folder

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

  pub(crate) fn memories(&self, ctx: Vec<String>) -> SMemories {
    let mut folder = self.folder.clone();
    folder.push("memories");
    ctx.iter().for_each(|name| folder.push(name.as_str()));

    // workaround because of first request fail with none existing folder
    // TODO remove it from here
    std::fs::create_dir_all(&folder);

    SMemories { oid: self.id.clone(), ctx, folder }
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
