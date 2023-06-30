mod test_init;

use chrono::{DateTime, Utc};
use json::{array, object};
use serde_json::from_str;
use std::io;
use std::str::FromStr;
use std::sync::Arc;
use test_init::init;
use uuid::Uuid;

use crate::test_init::{create_record, delete, goods, receive, store, transfer, update};
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
async fn check_zero_batch_deleted() {
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

  log::debug!("receive 06.01.23 s1 14000");
  let r1 = receive(&app, "2023-01-06", s1, g1, 14000.into(), "276566780.0".try_into().unwrap());
  let r1_batch = Batch { id: r1, date: dt("2023-01-06").unwrap() };

  log::debug!("transfer 07.01 s1 > s2 14007.6");
  let t1 = transfer(&app, "2023-01-07", s1, s2, g1, "14007.6".try_into().unwrap());

  log::debug!("update receive 06.01.23 s1 14000 > 14007.6");
  update(
    &app,
    "2023-01-06",
    s1,
    None,
    g1,
    r1,
    r1_batch.clone(),
    InternalOperation::Receive(14000.into(), "276566780.0".try_into().unwrap()),
    InternalOperation::Receive("14007.6".try_into().unwrap(), "276566780.0".try_into().unwrap()),
  );

  let balances = app.warehouse().database.get_balance_for_all(Utc::now()).unwrap();
  log::debug!("balances: {balances:#?}");

  log::debug!("s1: {s1:#?}");
  log::debug!("s2: {s2:#?}");

  // assert_eq!(balances.len(), 1);

  // s2
  let s2_bs = balances.get(&s2).unwrap();
  assert_eq!(s2_bs.len(), 1);

  let s2_g1_bs = s2_bs.get(&g1).unwrap();
  assert_eq!(s2_g1_bs.len(), 1);

  assert_eq!(
    s2_g1_bs.get(&r1_batch).unwrap().clone(),
    BalanceForGoods { qty: "14007.6".try_into().unwrap(), cost: "276566780.0".try_into().unwrap() }
  );
}
