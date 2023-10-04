mod test_init;

use crate::test_init::{goods, init, receive, store, uom, DocumentCreation};
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
use store::qty::{Number, Qty};
use store::GetWarehouse;
use tantivy::HasLen;
use uuid::Uuid;
use values::constants::_UUID;
use values::ID;

#[actix_web::test]
async fn check_document_update_with_transfer() {
  let (tmp_dir, settings, db) = init();

  let wss = Workspaces::new(tmp_dir.path().join("companies"));

  let (mut app, _) = Application::new(Arc::new(settings), Arc::new(db), wss).await.unwrap();

  app.register(MemoriesInFiles::new(app.clone(), "memories"));
  app.register(nae_backend::inventory::service::Inventory::new(app.clone()));

  let receive_op = vec!["warehouse", "receive"];
  let transfer_op = vec!["warehouse", "transfer"];
  let receive_doc = vec!["warehouse", "receive", "document"];
  let transfer_doc = vec!["warehouse", "transfer", "document"];

  let s1 = store(&app, "s1");
  let s2 = store(&app, "s2");
  let s3 = store(&app, "s3");
  let g1 = goods(&app, "g1");

  let uom0 = uom(&app, "uom0");
  let uom1 = uom(&app, "uom1");

  // create receive document
  let receiveDoc = object! {
    date: "2023-01-02",
    counterparty: s1.to_string(),
    storage: s2.to_string(),
    number: "1",
  };

  log::debug!("CREATE RECEIVE HEAD 2023-01-02 S2");
  let d1 = receive_doc.create(&app, receiveDoc.clone());

  // create receive operation
  let qty0 = Qty::new(vec![Number::new(
    Decimal::from(1),
    uom0,
    Some(Box::new(Number::new(Decimal::from(3), uom1, None))),
  )]);

  let qty0_json: JsonValue = (&qty0).into();

  let receiveOp = object! {
    document: d1["_id"].to_string(),
    goods: g1.to_string(),
    // qty: object! {number: "3.0"},
    qty: qty0_json.clone(),
    cost: object! {number: "0.3"},
  };

  log::debug!("CREATE RECEIVE OPERATION 2023-01-02 S2");
  let r1 = receive_op.create(&app, receiveOp);
  // log::debug!("receive_data: {:#?}", r1.dump());

  let r1_batch = Batch { id: r1["_uuid"].uuid().unwrap(), date: dt("2023-01-02").unwrap() };

  // create transfer document
  let mut transferDoc = object! {
    date: "2023-01-03",
    from: s2.to_string(),
    into: s3.to_string(),
    number: "1",
  };

  log::debug!("CREATE TRANSFER HEAD 2023-01-03 S2 > S3");
  let d2 = transfer_doc.create(&app, transferDoc.clone());

  // create transfer operation
  let transferOp = object! {
    document: d2["_id"].to_string(),
    goods: g1.to_string(),
    // qty: object! {number: "3.0"},
    qty: qty0_json,
    cost: object! {number: "0.3"},
  };

  log::debug!("CREATE TRANSFER OPERATION 2023-01-03 S2 > S3");
  let t1 = transfer_op.create(&app, transferOp);

  app.warehouse().database.ordered_topologies[0].debug().unwrap();

  // change document
  transferDoc["date"] = "2022-12-15".into();

  log::debug!("UPDATE TRANSFER HEAD 2023-01-03 > 2022-12-15 S2 > S3");
  let d3 = transfer_doc.update(&app, d2["_id"].string(), transferDoc);

  app.warehouse().database.ordered_topologies[0].debug().unwrap();

  // get stock
  let wsid = ID::from("yjmgJUmDo_kn9uxVi8s9Mj9mgGRJISxRt63wT46NyTQ");

  let ws = app.wss.get(&wsid);

  let balances = app.warehouse().database.get_balance_for_all(Utc::now()).unwrap();

  log::debug!("s1: {s1:#?}");
  log::debug!("s2: {s2:#?}");
  log::debug!("s3: {s3:#?}");

  log::debug!("balances: {balances:#?}");

  assert_eq!(balances.get(&s1), None);

  assert_eq!(balances[&s2][&g1][&r1_batch].qty, qty0.clone());

  let qty1 = Qty::new(vec![Number::new(
    Decimal::from(-1),
    uom0,
    Some(Box::new(Number::new(Decimal::from(3), uom1, None))),
  )]);
  assert_eq!(balances[&s2][&g1][&Batch::no()].qty, qty1);

  assert_eq!(balances[&s3][&g1][&Batch::no()].qty, qty0);
}
