mod cameras;
pub mod memories;
mod old_references;
pub mod organizations;

use crate::services::JsonData;
pub(crate) use cameras::{SCamera, SEvent};
use json::JsonValue;
pub use organizations::Workspaces;
use service::error::Error;
use std::io::Write;
use std::path::PathBuf;

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

pub(crate) fn save(path: &PathBuf, data: String) -> Result<(), Error> {
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

pub(crate) fn remove_dir(path: &PathBuf) -> Result<(), Error> {
  let folder = match path.parent() {
    None => return Err(Error::IOError(format!("can't get parent for {}", path.to_string_lossy()))),
    Some(f) => f,
  };

  std::fs::remove_dir_all(folder)
    .map_err(|e| Error::IOError(format!("can't remove folder {}: {}", folder.to_string_lossy(), e)))
}
