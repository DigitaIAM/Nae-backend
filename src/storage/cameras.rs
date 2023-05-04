use chrono::SecondsFormat::Millis;
use chrono::{DateTime, Datelike, SecondsFormat, Utc};
use json::JsonValue;
use std::io::Write;
use std::path::PathBuf;

use crate::storage::{data, json, load, save};
use service::error::Error;
use values::ID;

#[derive(Debug, Clone)]
pub struct SCamera {
  pub(crate) id: ID,
  pub(crate) oid: ID,

  pub(crate) folder: PathBuf,
  pub(crate) path: PathBuf,
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
    buf: &[u8],
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

  pub(crate) fn events_month(&self, ts: DateTime<Utc>) -> Vec<SEvent> {
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

    result.sort_by(|a, b| b.id.cmp(&a.id));

    result
  }

  pub(crate) fn events_on_date(&self, ts: DateTime<Utc>) -> Vec<SEvent> {
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

    let date = ts.to_rfc3339_opts(SecondsFormat::Millis, true);
    let date: &str = &date.as_str()[..10];

    for entry in entries {
      let entry = entry.unwrap();
      let path = entry.path();
      if path.is_file() {
        let name = entry.file_name().to_string_lossy().to_string();
        if let Some(id) = name.strip_suffix("_event.json") {
          if id.starts_with(date) {
            result.push(SEvent { id: id.to_string(), oid: self.oid, cid: self.id.clone(), path });
          }
        }
      }
    }

    result.sort_by(|a, b| a.id.cmp(&b.id));

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
  pub(crate) id: String,
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
