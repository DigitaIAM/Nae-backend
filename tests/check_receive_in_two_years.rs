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
use store::process_records::process_record;
use store::qty::{Number, Qty};
use store::GetWarehouse;

#[actix_web::test]
async fn check_receive_in_two_years() {
  let (tmp_dir, settings, db) = init();

  let wss = Workspaces::new(tmp_dir.path().join("companies"));

  let (mut app, _) = Application::new(Arc::new(settings), Arc::new(db), wss).await.unwrap();

  app.register(MemoriesInFiles::new(app.clone(), "memories"));
  app.register(nae_backend::inventory::service::Inventory::new(app.clone()));

  let s1 = store(&app, "s1");
  let g1 = goods(&app, "g1");

  let uom0 = Uuid::new_v4();
  let uom1 = Uuid::new_v4();

  log::debug!("receive 19.12.22 s1 2");
  let qty0 = Qty::new(vec![Number::new(
    Decimal::from(2),
    uom0,
    Some(Box::new(Number::new(Decimal::from(10), uom1, None))),
  )]);

  let r1 = receive(&app, "2022-12-19", s1, g1, qty0.clone(), "0.2".try_into().unwrap());
  let r1_batch = Batch { id: r1, date: dt("2022-12-19").unwrap() };

  log::debug!("receive 20.01.23 s1 1");
  let qty1 = Qty::new(vec![Number::new(
    Decimal::from(1),
    uom0,
    Some(Box::new(Number::new(Decimal::from(10), uom1, None))),
  )]);
  let r2 = receive(&app, "2023-01-20", s1, g1, qty1.clone(), "0.1".try_into().unwrap());
  let r2_batch = Batch { id: r2, date: dt("2023-01-20").unwrap() };

  let balances = app.warehouse().database.get_balance_for_all(Utc::now()).unwrap();
  log::debug!("balances: {balances:#?}");

  assert_eq!(balances.len(), 1);

  // s1
  let s1_bs = balances.get(&s1).unwrap();
  assert_eq!(s1_bs.len(), 1);

  let s1_g1_bs = s1_bs.get(&g1).unwrap();
  assert_eq!(s1_g1_bs.len(), 2);

  assert_eq!(
    s1_g1_bs.get(&r1_batch).unwrap().clone(),
    BalanceForGoods { qty: qty0, cost: "0.2".try_into().unwrap() }
  );

  assert_eq!(
    s1_g1_bs.get(&r2_batch).unwrap().clone(),
    BalanceForGoods { qty: qty1, cost: "0.1".try_into().unwrap() }
  );
}
