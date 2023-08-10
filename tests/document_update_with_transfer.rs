mod test_init;

use crate::test_init::{goods, init, receive, store};
use chrono::Utc;
use json::object;
use json::JsonValue;
use nae_backend::commutator::Application;
use nae_backend::memories::MemoriesInFiles;
use nae_backend::storage::Workspaces;
use rust_decimal::Decimal;
use service::utils::json::JsonParams;
use service::Services;
use std::str::FromStr;
use std::sync::Arc;
use store::batch::Batch;
use store::elements::dt;
use store::elements::ToJson;
use store::GetWarehouse;
use tantivy::HasLen;
use test_init::DocumentCreation;
use values::constants::_UUID;
use values::ID;

#[actix_web::test]
async fn update_document_with_transfer() {
  std::env::set_var("RUST_LOG", "debug,tantivy=off");
  env_logger::init();

  let (tmp_dir, settings, db) = init();

  let wss = Workspaces::new(tmp_dir.path().join("companies"));

  let (mut app, _) = Application::new(Arc::new(settings), Arc::new(db), wss).await.unwrap();

  app.register(MemoriesInFiles::new(app.clone(), "memories"));
  app.register(nae_backend::inventory::service::Inventory::new(app.clone()));

  let ctx_receive = vec!["warehouse", "receive"];
  let ctx_transfer = vec!["warehouse", "transfer"];
  let ctx_receive_doc = vec!["warehouse", "receive", "document"];
  let ctx_transfer_doc = vec!["warehouse", "transfer", "document"];

  let s1 = store(&app, "s1");
  let s2 = store(&app, "s2");
  let s3 = store(&app, "s3");
  let g1 = goods(&app, "g1");

  // create receive document
  let receive_doc = object! {
    date: "2023-01-02",
    counterparty: s1.to_string(),
    storage: s2.to_string(),
    number: "1",
  };

  let d1 = ctx_receive_doc.create(&app, receive_doc.clone());

  // create receive operation
  let receive_obj = object! {
    document: d1["_id"].to_string(),
    goods: g1.to_string(),
    qty: object! {number: "3.0"},
    cost: object! {number: "0.3"},
  };

  let r1 = ctx_receive.create(&app, receive_obj);
  // log::debug!("receive_data: {:#?}", r1.dump());

  let r1_batch = Batch { id: r1["_uuid"].uuid().unwrap(), date: dt("2023-01-02").unwrap() };

  // create transfer document
  let mut transfer_doc = object! {
    date: "2023-01-03",
    from: s2.to_string(),
    into: s3.to_string(),
    number: "1",
  };

  let d2 = ctx_transfer_doc.create(&app, transfer_doc.clone());

  // create transfer operation
  let transfer_obj = object! {
    document: d2["_id"].to_string(),
    goods: g1.to_string(),
    qty: object! {number: "3.0"},
    cost: object! {number: "0.3"},
  };

  let t1 = ctx_transfer.create(&app, transfer_obj);

  app.warehouse().database.ordered_topologies[0].debug().unwrap();

  // change document
  transfer_doc["date"] = "2022-12-15".into();

  let d3 = ctx_transfer_doc.update(&app, d2["_id"].string(), transfer_doc);

  app.warehouse().database.ordered_topologies[0].debug().unwrap();

  // get stock
  let wsid = ID::from("yjmgJUmDo_kn9uxVi8s9Mj9mgGRJISxRt63wT46NyTQ");

  let ws = app.wss.get(&wsid);

  let balances = app.warehouse().database.get_balance_for_all(Utc::now()).unwrap();

  // println!("balances: {balances:#?}");
  //
  // println!("s1: {s1:#?}");
  // println!("s2: {s2:#?}");
  // println!("s3: {s3:#?}");

  assert_eq!(balances.get(&s1), None);

  assert_eq!(balances[&s2][&g1][&r1_batch].qty, Decimal::from(3));

  assert_eq!(balances[&s2][&g1][&Batch::no()].qty, Decimal::from(-3));

  assert_eq!(balances[&s3][&g1][&Batch::no()].qty, Decimal::from(3));
}
