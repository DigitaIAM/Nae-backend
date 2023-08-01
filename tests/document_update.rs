mod test_init;

use crate::test_init::{document_create, document_update, goods, init, receive, store};
use chrono::Utc;
use json::object;
use nae_backend::commutator::Application;
use nae_backend::memories::MemoriesInFiles;
use nae_backend::storage::Workspaces;
use service::utils::json::JsonParams;
use service::Services;
use std::str::FromStr;
use std::sync::Arc;
use store::batch::Batch;
use store::elements::dt;
use store::GetWarehouse;

#[actix_web::test]
async fn update_document() {
  std::env::set_var("RUST_LOG", "debug,tantivy=off");
  env_logger::init();

  let (tmp_dir, settings, db) = init();

  let wss = Workspaces::new(tmp_dir.path().join("companies"));

  let (mut app, _) = Application::new(Arc::new(settings), Arc::new(db), wss).await.unwrap();

  app.register(MemoriesInFiles::new(app.clone(), "memories"));
  app.register(nae_backend::inventory::service::Inventory::new(app.clone()));

  let s1 = store(&app, "s1");
  let s2 = store(&app, "s2");
  let g1 = goods(&app, "g1");

  // create document
  let mut doc = object! {
    date: "2023-01-01",
    counterparty: s1.to_string(),
    storage: s2.to_string(),
    number: "1",
  };

  let d1 = document_create(&app, doc.clone(), vec!["warehouse", "receive", "document"]);

  // create receive operation
  let receive_obj = object! {
    document: d1["_id"].to_string(),
    goods: g1.to_string(),
    qty: object! {number: "3.0"},
    cost: object! {number: "0.3"},
  };

  let r1 = document_create(&app, receive_obj, vec!["warehouse", "receive"]);
  // log::debug!("receive_data: {:#?}", r1.dump());

  let r1_batch = Batch { id: r1["_uuid"].uuid().unwrap(), date: dt("2023-01-01").unwrap() };

  // change document
  doc["date"] = "2023-05-25".into();

  let d2 =
    document_update(&app, d1["_uuid"].string(), doc, vec!["warehouse", "receive", "document"]);

  // let balances = app.warehouse().database.get_balance_for_all(Utc::now()).unwrap();
  // log::debug!("balances: {balances:#?}");

  // report for old batch
  let report1 = app
    .warehouse()
    .database
    .get_report_for_goods(s2, g1, &r1_batch, dt("2022-01-01").unwrap(), dt("2023-12-31").unwrap())
    .unwrap();

  // println!("report1: {report1:#?}");

  assert_eq!(report1.len(), 2);

  let changed_batch = Batch { id: r1["_uuid"].uuid().unwrap(), date: dt("2023-05-25").unwrap() };

  // report for changed batch
  let report2 = app
    .warehouse()
    .database
    .get_report_for_goods(
      s2,
      g1,
      &changed_batch,
      dt("2022-01-01").unwrap(),
      dt("2023-12-31").unwrap(),
    )
    .unwrap();

  // println!("report2: {report2:#?}");

  assert_eq!(report2.len(), 3);
}