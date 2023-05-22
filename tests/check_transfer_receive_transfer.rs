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
async fn check_transfer_receive_transfer() {
  std::env::set_var("RUST_LOG", "debug,tantivy=off");
  env_logger::init();

  let (tmp_dir, settings, db) = init();

  let wss = Workspaces::new(tmp_dir.path().join("companies"));

  let (mut app, _) = Application::new(Arc::new(settings), Arc::new(db), wss)
    .await
    .map_err(|e| io::Error::new(io::ErrorKind::Unsupported, e))
    .unwrap();

  app.register(MemoriesInFiles::new(app.clone(), "memories"));
  app.register(nae_backend::inventory::service::Inventory::new(app.clone()));

  let s1 = store(&app, "s1");
  let s2 = store(&app, "s2");
  let s3 = store(&app, "s3");
  let g1 = goods(&app, "g1");

  log::debug!("transfer 26.01 s1 > s2 2");
  transfer(&app, "2023-01-26", s1, s2, g1, 2.into());

  log::debug!("receive 20.01 s1 2");
  let r1 = receive(&app, "2023-01-20", s1, g1, 2.into(), "2000".try_into().unwrap());
  let r1_batch = Batch { id: r1, date: dt("2023-01-20").unwrap() };

  log::debug!("transfer 23.01 s1 > s3 1");
  transfer(&app, "2023-01-23", s1, s3, g1, 1.into());

  // s1 r0 -1 0
  // s2 r0 1 0
  // s2 r1 1 1000
  // s3 r1 1 1000

  let balances = app.warehouse().database.get_balance_for_all(Utc::now()).unwrap();
  log::debug!("balances: {balances:#?}");

  assert_eq!(balances.len(), 3);

  // s1
  let s1_bs = balances.get(&s1).unwrap();
  assert_eq!(s1_bs.len(), 1);

  let s1_g1_bs = s1_bs.get(&g1).unwrap();
  assert_eq!(s1_g1_bs.len(), 1);

  // s2
  let s2_bs = balances.get(&s2).unwrap();
  assert_eq!(s2_bs.len(), 2);

  let s2_g1_bs = s2_bs.get(&g1).unwrap();
  assert_eq!(s2_g1_bs.len(), 2);

  assert_eq!(
    s2_g1_bs.get(&Batch::no()).unwrap().clone(),
    BalanceForGoods { qty: 1.into(), cost: "0".try_into().unwrap() }
  );

  assert_eq!(
    s2_g1_bs.get(&r1_batch).unwrap().clone(),
    BalanceForGoods { qty: 1.into(), cost: "1000".try_into().unwrap() }
  );

  // s3
  let s3_bs = balances.get(&s3).unwrap();
  assert_eq!(s3_bs.len(), 1);

  let s3_g1_bs = s3_bs.get(&g1).unwrap();
  assert_eq!(s3_g1_bs.len(), 1);

  assert_eq!(
    s3_g1_bs.get(&r1_batch).unwrap().clone(),
    BalanceForGoods { qty: 1.into(), cost: "1000".try_into().unwrap() }
  );
}
