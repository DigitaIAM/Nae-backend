use crate::memories::Resolve;
use crate::storage::organizations::Workspace;
use blake2::digest::Output;
use blake2::{digest::consts::U16, Blake2b, Digest};
use db::PrefixIterator;
use json::JsonValue;
use rocksdb::{BoundColumnFamily, ColumnFamilyDescriptor, Options, DB};
use service::error::Error;
use service::utils::json::JsonParams;
use std::path::Path;
use std::sync::Arc;
use uuid::Uuid;
use values::c;

const CF_TARGET_CTX_SOURCE: &str = "target_ctx_source";

type Blake2b80 = Blake2b<U16>;

#[derive(Clone)]
pub struct LinksIndex {
  pub database: Arc<DB>,
}

impl LinksIndex {
  pub fn cf_name() -> &'static str {
    CF_TARGET_CTX_SOURCE
  }

  fn cf(&self) -> Result<Arc<BoundColumnFamily>, Error> {
    if let Some(cf) = self.database.cf_handle(LinksIndex::cf_name()) {
      Ok(cf)
    } else {
      Err(Error::NotFound("column family not found".into()))
    }
  }

  pub fn open<P: AsRef<Path>>(path: P) -> Result<Self, Error> {
    std::fs::create_dir_all(&path).map_err(|e| Error::GeneralError(e.to_string()))?;

    let mut opts = Options::default();

    let mut cfs = Vec::new();
    let cf = ColumnFamilyDescriptor::new(CF_TARGET_CTX_SOURCE, opts.clone());
    cfs.push(cf);

    opts.create_if_missing(true);
    opts.create_missing_column_families(true);

    let tmp_db =
      DB::open_cf_descriptors(&opts, &path, cfs).map_err(|e| Error::GeneralError(e.to_string()))?;
    let inner_db = Arc::new(tmp_db);

    Ok(LinksIndex { database: inner_db })
  }

  pub fn save_links(
    &self,
    ws: &Workspace,
    ctx: &Vec<String>,
    after: &JsonValue,
    before: &JsonValue,
  ) -> Result<(), Error> {
    let ctx: String = ctx.join("/");

    if before.is_object() {
      if after.is_object() {
        self.update(ws, &ctx, &before, after)?;
      } else {
        self.delete(ws, &ctx, before)?;
      }
    } else {
      if after.is_object() {
        self.create(ws, &ctx, after)?;
      } else {
        // nothing to do
      }
    }

    Ok(())
  }

  fn source_and_target(&self, ws: &Workspace, data: &JsonValue) -> Option<(Uuid, Uuid)> {
    let doc_id = match data[c::DOCUMENT].string_or_none() {
      None => return None,
      Some(id) => id,
    };

    let target = match Uuid::try_parse(&doc_id) {
      Ok(uuid) => uuid,
      Err(_) => {
        let document = doc_id.resolve_to_json_object(ws);

        match document[c::UUID].uuid_or_none() {
          Some(uuid) => uuid,
          None => return None, // return Err(ErrorKind::Incomplete),
        }
      },
    };

    let source = match data[c::UUID].uuid_or_none() {
      Some(uuid) => uuid,
      None => return None, // return Err(Error::NotFound("after uuid not found".into())),
    };

    Some((source, target))
  }

  fn create(&self, ws: &Workspace, ctx: &String, after: &JsonValue) -> Result<(), Error> {
    if let Some((source, target)) = self.source_and_target(ws, after) {
      self
        .database
        .put_cf(&self.cf()?, self.to_bytes(target, ctx, source), "")
        .map_err(|e| Error::GeneralError(e.to_string()))
    } else {
      Ok(())
    }
  }

  fn update(
    &self,
    ws: &Workspace,
    ctx: &String,
    before: &JsonValue,
    after: &JsonValue,
  ) -> Result<(), Error> {
    if before[c::DOCUMENT].string() == after[c::DOCUMENT].string() {
      // do nothing
    } else {
      self.delete(ws, ctx, before)?;
      self.create(ws, ctx, after)?;
    }

    Ok(())
  }

  fn delete(&self, ws: &Workspace, ctx: &String, before: &JsonValue) -> Result<(), Error> {
    if let Some((source, target)) = self.source_and_target(ws, before) {
      self
        .database
        .delete_cf(&self.cf()?, self.to_bytes(target, ctx, source))
        .map_err(|e| Error::GeneralError(e.to_string()))
    } else {
      Ok(())
    }
  }

  fn to_bytes(&self, target: Uuid, ctx: &String, source: Uuid) -> Vec<u8> {
    let ctx = LinksIndex::ctx_to_hash(ctx);

    target
      .as_bytes()
      .iter()
      .chain(ctx.as_slice())
      .chain(source.as_bytes().iter())
      .copied()
      .collect()
  }

  fn prefix_to_bytes(&self, target: Uuid, ctx: &String) -> Vec<u8> {
    let ctx = LinksIndex::ctx_to_hash(ctx);

    target.as_bytes().iter().chain(ctx.as_slice()).copied().collect()
  }

  fn ctx_to_hash(ctx: &String) -> Output<Blake2b80> {
    let mut hasher = Blake2b80::new();
    hasher.update(ctx);
    hasher.finalize()
  }

  pub fn get_source_links_for_ctx(
    &self,
    target: Uuid,
    ctx: &Vec<String>,
  ) -> Result<Vec<Uuid>, Error> {
    let ctx: String = ctx.join("/");

    let prefix = self.prefix_to_bytes(target, &ctx);

    let mut result: Vec<Uuid> = Vec::new();

    for (k, _) in self
      .database
      .prefix(&self.cf()?, prefix)
      .map_err(|e| Error::GeneralError(e.to_string()))?
    {
      let source_uuid = Uuid::from_slice(&k[32..=47])?;

      result.push(source_uuid);
    }
    Ok(result)
  }

  pub fn get_source_links_without_ctx(&self, target: Uuid) -> Result<Vec<Uuid>, Error> {
    let prefix: Vec<u8> = target.as_bytes().iter().copied().collect();

    let mut result: Vec<Uuid> = Vec::new();

    for (k, _) in self
      .database
      .prefix(&self.cf()?, prefix)
      .map_err(|e| Error::GeneralError(e.to_string()))?
    {
      let source_uuid = Uuid::from_slice(&k[32..=47])?;

      result.push(source_uuid);
    }
    Ok(result)
  }
}
