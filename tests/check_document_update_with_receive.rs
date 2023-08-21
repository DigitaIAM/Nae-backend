mod test_init;

use crate::test_init::{goods, init, receive, store, DocumentCreation};
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
async fn check_document_update_with_receive() {
  let (tmp_dir, settings, db) = init();

  let wss = Workspaces::new(tmp_dir.path().join("companies"));

  let (mut app, _) = Application::new(Arc::new(settings), Arc::new(db), wss).await.unwrap();

  app.register(MemoriesInFiles::new(app.clone(), "memories"));
  app.register(nae_backend::inventory::service::Inventory::new(app.clone()));

  let receive_op = vec!["warehouse", "receive"];
  let receive_doc = vec!["warehouse", "receive", "document"];

  let s1 = store(&app, "s1");
  let s2 = store(&app, "s2");
  let g1 = goods(&app, "g1");

  // create document
  let mut receiveDoc = object! {
    date: "2023-01-01",
    counterparty: s1.to_string(),
    storage: s2.to_string(),
    number: "1",
  };

  let d1 = receive_doc.create(&app, receiveDoc.clone());

  // create receive operation
  let receiveOp = object! {
    document: d1["_id"].to_string(),
    goods: g1.to_string(),
    qty: object! {number: "3.0"},
    cost: object! {number: "0.3"},
  };

  let r1 = receive_op.create(&app, receiveOp);
  // log::debug!("receive_data: {:#?}", r1.dump());

  let r1_batch = Batch { id: r1["_uuid"].uuid().unwrap(), date: dt("2023-01-01").unwrap() };

  app.warehouse().database.ordered_topologies[0].debug().unwrap();

  // change document
  receiveDoc["date"] = "2023-05-25".into();

  let d2 = receive_doc.update(&app, d1["_id"].string(), receiveDoc);

  app.warehouse().database.ordered_topologies[0].debug().unwrap();

  // let balances = app.warehouse().database.get_balance_for_all(Utc::now()).unwrap();
  // log::debug!("balances: {balances:#?}");

  // report for old batch
  let report1 = app
    .warehouse()
    .database
    .get_report_for_goods(s2, g1, &r1_batch, dt("2022-01-01").unwrap(), dt("2023-12-31").unwrap())
    .unwrap();

  // log::debug!("report1: {report1:#?}");

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

  // log::debug!("report2: {report2:#?}");

  assert_eq!(report2.len(), 3);
}
