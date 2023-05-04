use json::JsonValue;
use service::error::Error;
use std::path::PathBuf;

use crate::storage::{json, load, save};
use values::ID;

pub(crate) struct SDepartment {
  pub(crate) id: ID,
  pub(crate) oid: ID,

  pub(crate) path: PathBuf,
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
  pub(crate) id: ID,
  pub(crate) oid: ID,

  pub(crate) path: PathBuf,
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
  pub(crate) id: ID,
  pub(crate) oid: ID,

  pub(crate) path: PathBuf,
}

pub(crate) struct SPerson {
  pub(crate) id: ID,
  pub(crate) oid: ID,

  pub(crate) folder: PathBuf,
  pub(crate) path: PathBuf,
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
