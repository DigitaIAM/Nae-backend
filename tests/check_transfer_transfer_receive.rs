mod test_init;

use chrono::{DateTime, Utc};
use json::{array, object};
use serde_json::from_str;
use std::io;
use std::str::FromStr;
use std::sync::Arc;
use test_init::init;
use uuid::Uuid;

use crate::test_init::{create_record, goods, receive, store, transfer};
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
async fn check_transfer_transfer_receive() {
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

  log::debug!("transfer 03.02 s1 > s2 1");
  transfer(&app, "2023-02-03", s1, s2, g1, 1.into());

  log::debug!("transfer 03.02 s2 > s3 1");
  transfer(&app, "2023-02-03", s2, s3, g1, 1.into());

  log::debug!("receive 02.02 s1 1");
  let r1 = receive(&app, "2023-02-02", s1, g1, 1.into(), "15".try_into().unwrap());
  let r1_batch = Batch { id: r1, date: dt("2023-02-02").unwrap() };

  // s1 b0 0 0
  // s1 r1 0 0
  // s2 b0 0 0
  // s2 r1 0 0
  // s3 b0 0 0
  // s3 r1 1 15 (03.02)

  let balances = app.warehouse().database.get_balance_for_all(Utc::now()).unwrap();
  log::debug!("balances: {balances:#?}");

  // s1: 8823d2fa-485d-426b-9d06-ef67191965ba
  // s2: 13b3227e-f032-491f-a9ca-188628b0abf5
  // s3: 070cd0cd-4cd8-4dbb-bc04-e798ed31ebde

  log::debug!("s1: {s1:#?}");
  log::debug!("s2: {s2:#?}");
  log::debug!("s3: {s3:#?}");

  assert_eq!(balances.len(), 1);

  // s1
  let s1_bs = balances.get(&s1);
  assert_eq!(s1_bs, None);

  // s2
  let s2_bs = balances.get(&s2);
  assert_eq!(s2_bs, None);

  // s3
  let s3_bs = balances.get(&s3).unwrap();
  assert_eq!(s3_bs.len(), 1);

  let s3_g1_bs = s3_bs.get(&g1).unwrap();
  assert_eq!(s3_g1_bs.len(), 1);

  assert_eq!(s3_g1_bs.get(&Batch::no()).clone(), None);

  assert_eq!(
    s3_g1_bs.get(&r1_batch).unwrap().clone(),
    BalanceForGoods { qty: 1.into(), cost: 15.into() }
  );
}
