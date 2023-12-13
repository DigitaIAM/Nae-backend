mod test_init;

use chrono::{DateTime, Utc};
use json::{array, object};
use serde_json::from_str;
use std::io;
use std::str::FromStr;
use std::sync::Arc;
use uuid::Uuid;

use crate::test_init::{create_record, goods, init, receive, store, transfer};
use nae_backend::commutator::Application;
use nae_backend::memories::MemoriesInFiles;
use nae_backend::storage::Workspaces;
use rust_decimal::Decimal;
use service::utils::json::JsonParams;
use service::{Context, Services};
use store::balance::{BalanceForGoods, Cost};
use store::batch::Batch;
use store::elements::{dt, Goods, Mode, Store};
use store::operations::{InternalOperation, OpMutation};
use store::process_records::process_warehouse_record;
use store::qty::{Number, Qty};
use store::GetWarehouse;

#[actix_web::test]
async fn check_recursion() {
  let (tmp_dir, settings, db) = init();

  let wss = Workspaces::new(tmp_dir.path().join("companies"));

  let (mut app, _) = Application::new(Arc::new(settings), Arc::new(db), wss).await.unwrap();

  app.register(MemoriesInFiles::new(app.clone(), "memories"));
  app.register(nae_backend::inventory::service::Inventory::new(app.clone()));

  let s1 = Uuid::from_str("00000000-0000-0000-0000-000000000001").unwrap();
  let s2 = Uuid::from_str("00000000-0000-0000-0000-000000000002").unwrap();
  let g1 = goods(&app, "g1");

  let uom0 = Uuid::new_v4();
  let uom1 = Uuid::new_v4();

  log::debug!("transfer 26.01 s1 > s2 11");
  let qty0 = Qty::new(vec![Number::new(
    Decimal::from(1),
    uom0,
    Some(Box::new(Number::new(Decimal::from(11), uom1, None))),
  )]);

  transfer(&app, "2023-01-26", s1, s2, g1, qty0.clone());

  log::debug!("transfer 26.01 s2 > s1 11");
  transfer(&app, "2023-01-26", s2, s1, g1, qty0.clone());

  app.warehouse().database.ordered_topologies[0].debug().unwrap();
  app.warehouse().database.checkpoint_topologies[0].debug().unwrap();

  log::debug!("receive 20.01 s1 11");
  let r1 = receive(&app, "2023-01-20", s1, g1, qty0.clone(), Decimal::try_from(1).unwrap().into());
  let r1_batch = Batch { id: r1, date: dt("2023-01-20").unwrap() };

  app.warehouse().database.ordered_topologies[0].debug().unwrap();
  app.warehouse().database.checkpoint_topologies[0].debug().unwrap();

  let balances = app.warehouse().database.get_balance_for_all(Utc::now()).unwrap();
  log::debug!("balances: {balances:#?}");

  assert_eq!(balances.len(), 1);

  let s1_bs = balances.get(&s1).unwrap();
  assert_eq!(s1_bs.len(), 1);

  let s1_g1_bs = s1_bs.get(&g1).unwrap();
  assert_eq!(s1_g1_bs.len(), 1);

  assert_eq!(
    s1_g1_bs.get(&r1_batch).unwrap().clone(),
    BalanceForGoods { qty: qty0, cost: Decimal::try_from(1).unwrap().into() }
  );
}
