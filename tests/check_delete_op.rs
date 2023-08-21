mod test_init;

use chrono::{DateTime, Utc};
use json::{array, object};
use serde_json::from_str;
use std::io;
use std::str::FromStr;
use std::sync::Arc;
use uuid::Uuid;

use crate::test_init::{create_record, delete, goods, init, receive, store, transfer};
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
async fn check_delete_op() {
  let (tmp_dir, settings, db) = init();

  let wss = Workspaces::new(tmp_dir.path().join("companies"));

  let (mut app, _) = Application::new(Arc::new(settings), Arc::new(db), wss).await.unwrap();

  app.register(MemoriesInFiles::new(app.clone(), "memories"));
  app.register(nae_backend::inventory::service::Inventory::new(app.clone()));

  let s1 = store(&app, "s1");
  let s2 = store(&app, "s2");
  let s3 = store(&app, "s3");
  let g1 = goods(&app, "g1");

  log::debug!("transfer 20.01 s1 > s2 11");
  let t1 = transfer(&app, "2023-01-20", s1, s2, g1, 11.into());

  log::debug!("transfer 21.01 s2 > s3 11");
  let t2 = transfer(&app, "2023-01-21", s2, s3, g1, 11.into());

  delete(
    &app,
    "2023-01-20",
    s1,
    Some(s2),
    g1,
    t1,
    Batch::no(),
    InternalOperation::Issue(11.into(), Cost::ZERO, Mode::Auto),
  );

  let balances = app.warehouse().database.get_balance_for_all(Utc::now()).unwrap();
  log::debug!("balances: {balances:#?}");

  log::debug!("s1: {s1:#?}");
  log::debug!("s2: {s2:#?}");
  log::debug!("s3: {s3:#?}");

  assert_eq!(balances.len(), 2);

  // s2
  let s2_bs = balances.get(&s2).unwrap();
  assert_eq!(s2_bs.len(), 1);

  let s2_g1_bs = s2_bs.get(&g1).unwrap();
  assert_eq!(s2_g1_bs.len(), 1);

  assert_eq!(
    s2_g1_bs.get(&Batch::no()).unwrap().clone(),
    BalanceForGoods { qty: (-11).into(), cost: "0".try_into().unwrap() }
  );

  // s3
  let s3_bs = balances.get(&s3).unwrap();
  assert_eq!(s3_bs.len(), 1);

  let s3_g1_bs = s3_bs.get(&g1).unwrap();
  assert_eq!(s3_g1_bs.len(), 1);

  assert_eq!(
    s3_g1_bs.get(&Batch::no()).unwrap().clone(),
    BalanceForGoods { qty: 11.into(), cost: "0".try_into().unwrap() }
  );
}
