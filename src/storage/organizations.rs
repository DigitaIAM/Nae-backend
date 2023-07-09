use json::JsonValue;
use std::fs;
use std::path::{Path, PathBuf};
use uuid::Uuid;
use walkdir::{DirEntry, WalkDir};

use crate::storage::memories::{Document, Memories};
use crate::storage::{json, load, save};
use service::error::Error;
use values::ID;

#[derive(Debug, Clone)]
pub struct Workspaces {
  folder: PathBuf,
}

impl Workspaces {
  pub fn new<S: AsRef<Path>>(folder: S) -> Self
  where
    PathBuf: From<S>,
  {
    fs::create_dir_all(&folder)
      .map_err(|e| panic!("can't create folder: {}", e))
      .unwrap(); // folder

    Workspaces { folder: folder.into() }
  }

  pub(crate) fn create(&self, id: ID) -> Result<Workspace, Error> {
    let mut folder = self.folder.clone();
    folder.push(id.to_base64());

    let mut path = folder.clone();
    path.push("organization.json");

    std::fs::create_dir_all(&folder).map_err(|e| {
      Error::IOError(format!("can't create folder {}: {}", folder.to_string_lossy(), e))
    })?;

    Ok(Workspace { id, folder, path })
  }

  pub(crate) fn get(&self, id: &ID) -> Workspace {
    let mut folder = self.folder.clone();
    folder.push(id.to_base64());

    let mut path = folder.clone();
    path.push("organization.json");

    Workspace { id: *id, folder, path }
  }

  pub fn list(&self) -> Result<Vec<Workspace>, Error> {
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
          Ok(id) => result.push(Workspace { id, folder, path }),
          Err(_) => {}, // ignore?
        }
      }
    }

    Ok(result)
  }
}

#[derive(Clone)]
pub struct Workspace {
  pub id: ID,

  folder: PathBuf,
  path: PathBuf,
}

impl Workspace {
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

  pub(crate) fn memories(&self, ctx: Vec<String>) -> Memories {
    let mut top_folder = self.folder.clone();
    top_folder.push("memories");

    let mut folder = top_folder.clone();
    ctx.iter().for_each(|name| folder.push(name.as_str()));

    // workaround because of first request fail with none existing folder
    // TODO remove it from here
    let _ = fs::create_dir_all(&folder);

    Memories { ws: self.clone(), ctx, top_folder, folder }
  }

  pub(crate) fn resolve_uuid(&self, id: &Uuid) -> Option<Document> {
    // println!("resolve_uuid {id}");
    let mut top_folder = self.folder.clone();
    top_folder.push("memories");

    let id = id.to_string();

    let mut path = top_folder.clone();
    path.push("uuid");
    path.push(&id[0..4]);
    path.push(id);

    // println!("path {path:?}");

    let path = match fs::read_link(path) {
      Ok(r) => r,
      Err(_) => return None,
    };

    // println!("read link {path:?}");

    let mut id = path.to_string_lossy().to_string();
    while &id.as_str()[0..3] == "../" {
      id = id[3..].to_string();
    }

    // println!("id {id}");

    let mut path = top_folder.clone();
    path.push(format!("{}/latest.json", id));

    // let ctx = vec![];
    let mut ctx: Vec<_> = id.split('/').map(|s| s.to_string()).collect();
    ctx.pop();

    // println!("path {path:?} ctx {ctx:?}");

    Some(Document { mem: self.memories(ctx), id, path })
  }

  pub(crate) fn resolve_id(&self, id: &str) -> Option<Document> {
    // println!("resolve_id {id}");
    if id.is_empty() {
      return None;
    }

    let mut top_folder = self.folder.clone();
    top_folder.push("memories");

    let mut ctx: Vec<_> = id.split('/').map(|s| s.to_string()).collect();
    let ctx_id = ctx.pop().unwrap_or_default();

    let mut path = top_folder.clone();
    ctx.iter().for_each(|name| path.push(name));

    if ctx_id.is_empty() {
      return None;
    }

    let mut path = match crate::storage::memories::build_folder_path(&ctx_id, &path) {
      Some(f) => f,
      None => return None,
    };
    path.push("latest.json");

    // println!("id {id:?}");
    // println!("path {path:?}");

    Some(Document { mem: self.memories(ctx), id: id.to_string(), path })
  }
}

pub struct Documents {
  ws: Workspace,
  it: Box<dyn Iterator<Item = DirEntry>>,
}

impl IntoIterator for Workspace {
  type Item = Document;
  type IntoIter = Documents;

  fn into_iter(self) -> Self::IntoIter {
    let mut top_folder = self.folder.clone();
    top_folder.push("memories");

    let it = WalkDir::new(top_folder.clone())
      .follow_links(false)
      .into_iter()
      .filter_map(|e| e.ok())
      .filter(|e| {
        let f_name = e.file_name().to_string_lossy();
        f_name == "latest.json"
      });

    Documents { ws: self, it: Box::new(it) }
  }
}

impl Iterator for Documents {
  type Item = Document;

  fn next(&mut self) -> Option<Self::Item> {
    if let Some(e) = self.it.next() {
      let mut top_folder = self.ws.folder.canonicalize().unwrap();
      top_folder.push("memories");
      println!("top_ {top_folder:?}");

      let path = e.into_path().canonicalize().unwrap();
      println!("path {path:?}");

      let record = path.parent().unwrap();

      let name = record.file_name().unwrap().to_string_lossy().to_string();
      let id = name.replace(".json", "");

      let context = record.parent().unwrap().parent().unwrap().parent().unwrap();

      let ctx: Vec<String> = context
        .strip_prefix(&top_folder)
        .unwrap()
        .to_string_lossy()
        .to_string()
        .split('/')
        .map(|s| s.to_string())
        .collect();
      let ctx_folder = context.into();

      println!("ctx {ctx:?}");

      let did = id;

      let mem =
        Memories { ws: self.ws.clone(), top_folder: top_folder.clone(), ctx, folder: ctx_folder };

      Some(Document { mem, id: did, path })
    } else {
      None
    }
  }
}
