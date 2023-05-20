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

const WID: &str = "yjmgJUmDo_kn9uxVi8s9Mj9mgGRJISxRt63wT46NyTQ";

#[actix_web::test]
async fn check_two_backdated_receive() {
  std::env::set_var("RUST_LOG", "debug,tantivy=off");
  env_logger::init();

  let (tmp_dir, settings, db) = init();

  let (mut app, _) = Application::new(Arc::new(settings), Arc::new(db))
    .await
    .map_err(|e| io::Error::new(io::ErrorKind::Unsupported, e))
    .unwrap();

  let storage = Workspaces::new(tmp_dir.path().join("companies"));
  app.storage = Some(storage.clone());

  app.register(MemoriesInFiles::new(app.clone(), "memories", storage.clone()));
  app.register(nae_backend::inventory::service::Inventory::new(app.clone()));

  let s1 = store(&app, "s1");
  let s2 = store(&app, "s2");
  let g1 = goods(&app, "g1");

  log::debug!("transfer 26.01 s1 > s2 25");
  transfer(&app, "2023-01-26", s1, s2, g1, 25.into());

  // log::debug!("transfer 27.01 s1 > s2 75");
  // transfer(&app, "2023-01-27", s1, s2, g1, 75.into());

  log::debug!("receive 20.01 s1 25");
  let r1 = receive(&app, "2023-01-20", s1, g1, 25.into(), "25".try_into().unwrap());
  let r1_batch = Batch { id: r1, date: dt("2023-01-20").unwrap() };

  // s1 b0 0 0
  // s2 b0 25 25
  // s2 r1 25 25

  let balances = app.warehouse().database.get_balance_for_all(Utc::now()).unwrap();
  log::debug!("balances: {balances:#?}");

  assert_eq!(balances.len(), 1);

  let s1_bs = balances.get(&s1);
  assert_eq!(s1_bs, None);

  if let Some(s1_bs) = s1_bs {
    let s1_g1_bs = s1_bs.get(&g1).unwrap();
    assert_eq!(s1_g1_bs.len(), 0);

    assert_eq!(
      s1_g1_bs.get(&Batch::no()).unwrap().clone(),
      BalanceForGoods { qty: (0).into(), cost: "0".try_into().unwrap() }
    );
  }

  let s2_bs = balances.get(&s2).unwrap();
  assert_eq!(s2_bs.len(), 1);

  let s2_g1_bs = s2_bs.get(&g1).unwrap();
  assert_eq!(s2_g1_bs.len(), 1);

  assert_eq!(
    s2_g1_bs.get(&r1_batch).unwrap().clone(),
    BalanceForGoods { qty: 25.into(), cost: "25".try_into().unwrap() }
  );

  assert_eq!(s2_g1_bs.get(&Batch::no()), None);
}
