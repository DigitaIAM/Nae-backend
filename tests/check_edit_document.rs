mod test_init;

use chrono::{DateTime, Utc};
use json::{array, object};
use serde_json::from_str;
use std::io;
use std::str::FromStr;
use std::sync::Arc;
use test_init::init;
use uuid::Uuid;

use crate::test_init::{
  create_record, document_create, document_update, goods, receive, store, transfer,
};
use nae_backend::commutator::Application;
use nae_backend::memories::MemoriesInFiles;
use nae_backend::storage::Workspaces;
use service::utils::json::JsonParams;
use service::{Context, Services};
use store::balance::{BalanceForGoods, Cost};
use store::batch::Batch;
use store::elements::{dt, Goods, Mode, Qty, Store};
use store::operations::{InternalOperation, OpMutation};
use store::process_records::process_record;
use store::GetWarehouse;

#[actix_web::test]
async fn check_transfer_receive_transfer() {
  std::env::set_var("RUST_LOG", "debug,tantivy=off");
  env_logger::init();

  let (tmp_dir, settings, db) = init();

  let wss = Workspaces::new(tmp_dir.path().join("companies"));

  let (mut app, _) = Application::new(Arc::new(settings), Arc::new(db), wss).await.unwrap();

  app.register(MemoriesInFiles::new(app.clone(), "memories"));
  app.register(nae_backend::inventory::service::Inventory::new(app.clone()));

  let s1 = store(&app, "s1");
  let s2 = store(&app, "s2");
  let s3 = store(&app, "s3");
  let g1 = goods(&app, "g1");

  let mut doc = object! {
    date: "2023-01-20",
    counterparty: s1.to_string(),
    storage: s2.to_string(),
    number: "1",
  };

  let d1 = document_create(&app, doc.clone(), vec!["warehouse", "receive", "document"]);
  // log::debug!("d1: {:#?}", d1.dump());

  let receive_doc = object! {
    document: d1["_id"].to_string(),
    goods: g1.to_string(),
    qty: object! {number: "3.0"},
    cost: object! {number: "0.3"},
  };

  let receive = document_create(&app, receive_doc, vec!["warehouse", "receive"]);

  // log::debug!("receive_data: {:#?}", receive.dump());

  doc["storage"] = s3.to_string().into();

  let d2 =
    document_update(&app, d1["_uuid"].string(), doc, vec!["warehouse", "receive", "document"]);
  // log::debug!("d2: {:#?}", d2.dump());

  let balances = app.warehouse().database.get_balance_for_all(Utc::now()).unwrap();
  log::debug!("balances: {balances:#?}");

  log::debug!("s1: {s1:#?}");
  log::debug!("s2: {s2:#?}");
  log::debug!("s3: {s3:#?}");

  let s3_bs = balances.get(&s3).unwrap();
  assert_eq!(s3_bs.len(), 1);
}
