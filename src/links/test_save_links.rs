use crate::animo::db::AnimoDB;
use crate::animo::memory::Memory;
use crate::animo::{Animo, Topology};
use crate::commutator::Application;
use crate::links::GetLinks;
use crate::memories::MemoriesInFiles;
use crate::settings::{Database, JWTConfig, Settings};
use crate::storage::Workspaces;
use json::{object, JsonValue};
use service::Services;
use std::path::PathBuf;
use std::str::FromStr;
use std::sync::Arc;
use store::elements::ToJson;
use tempfile::{tempdir, TempDir};
use uuid::Uuid;
use values::ID;

#[actix_web::test]
async fn links_cud() {
  let (tmp_dir, settings, db) = init();

  let wss = Workspaces::new(tmp_dir.path().join("companies"));
  let oid = ID::from_base64("yjmgJUmDo_kn9uxVi8s9Mj9mgGRJISxRt63wT46NyTQ").unwrap();
  let ws = wss.create(oid).unwrap();

  let (mut app, _) = Application::new(Arc::new(settings), Arc::new(db), wss).await.unwrap();

  let ctx: Vec<String> = vec!["production".into(), "produce".into()];

  // // 7 failed, 5 success attempts on prefix_iterator_cf
  // // 12 of 12 attempts success on iterator_cf_opt
  // let d0 = Uuid::new_v4();
  // let d1 = Uuid::new_v4();
  // let d2 = Uuid::new_v4();

  // 12 of 12 attempts failed on prefix_iterator_cf
  // 12 of 12 attempts success on iterator_cf_opt
  let d0 = Uuid::from_str("00000000-0000-0000-0000-000000000000").unwrap();
  let d1 = Uuid::from_str("00000000-0000-0000-0000-000000000001").unwrap();
  let d2 = Uuid::from_str("00000000-0000-0000-0000-000000000002").unwrap();
  let d3 = Uuid::from_str("00000000-0000-0000-0000-000000000003").unwrap();

  // create
  let v1 = object! {
      "_uuid": d0.to_json(),
      "document": d1.to_json(),
  };

  app.links().save_links(&ws, &ctx, &v1, &JsonValue::Null).unwrap();

  let uuids = app.links().get_source_links(d1, &ctx).unwrap();

  // println!("uuids1 {uuids:?}");

  assert_eq!(uuids.len(), 1);
  assert_eq!(uuids[0], d0);

  // update
  let v2 = object! {
      "_uuid": d0.to_json(),
      "document": d2.to_json(),
  };

  app.links().save_links(&ws, &ctx, &v2, &v1).unwrap();

  let v3 = object! {
      "_uuid": d0.to_json(),
      "document": d3.to_json(),
  };
  app.links().save_links(&ws, &ctx, &v3, &JsonValue::Null).unwrap();

  let uuids = app.links().get_source_links(d1, &ctx).unwrap();

  // println!("uuids2 {uuids:?}");

  assert_eq!(uuids.len(), 0);

  let uuids = app.links().get_source_links(d2, &ctx).unwrap();

  // println!("uuids3 {uuids:?}");

  assert_eq!(uuids.len(), 1);
  assert_eq!(uuids[0], d0);

  // delete
  app.links().save_links(&ws, &ctx, &JsonValue::Null, &v2).unwrap();

  let uuids = app.links().get_source_links(d1, &ctx).unwrap();
  assert_eq!(uuids.len(), 0);

  let uuids = app.links().get_source_links(d2, &ctx).unwrap();
  assert_eq!(uuids.len(), 0);
}

#[cfg(test)]
fn init() -> (TempDir, Settings, AnimoDB) {
  std::env::set_var("RUST_LOG", "actix_web=debug,nae_backend=debug");
  let _ = env_logger::builder().is_test(true).try_init();

  let tmp_dir = tempdir().unwrap();
  let tmp_path = tmp_dir.path().to_str().unwrap();

  let settings = Settings::test(tmp_path.into());

  let mut db: AnimoDB = Memory::init(tmp_path.into()).unwrap();

  (tmp_dir, settings, db)
}
