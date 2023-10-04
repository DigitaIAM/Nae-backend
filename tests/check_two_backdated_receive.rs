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
async fn check_two_backdated_receive() {
  let (tmp_dir, settings, db) = init();

  let wss = Workspaces::new(tmp_dir.path().join("companies"));

  let (mut app, _) = Application::new(Arc::new(settings), Arc::new(db), wss).await.unwrap();

  app.register(MemoriesInFiles::new(app.clone(), "memories"));
  app.register(nae_backend::inventory::service::Inventory::new(app.clone()));

  let s1 = store(&app, "s1");
  let s2 = store(&app, "s2");
  let g1 = goods(&app, "g1");

  let uom0 = Uuid::new_v4();
  let uom1 = Uuid::new_v4();

  log::debug!("transfer 26.01 s1 > s2 25");
  // let qty0 = Qty::new(vec![Number::new(Decimal::from(25), uom1, None)]);
  let qty0 = Qty::new(vec![
    Number::new(Decimal::from(2), uom0, Some(Box::new(Number::new(Decimal::from(10), uom1, None)))),
    Number::new(Decimal::from(5), uom1, None),
  ]);

  transfer(&app, "2023-01-26", s1, s2, g1, qty0);

  log::debug!("transfer 27.01 s1 > s2 75");
  // let qty1 = Qty::new(vec![Number::new(Decimal::from(75), uom1, None)]);
  let qty1 = Qty::new(vec![
    Number::new(Decimal::from(7), uom0, Some(Box::new(Number::new(Decimal::from(10), uom1, None)))),
    Number::new(Decimal::from(5), uom1, None),
  ]);
  transfer(&app, "2023-01-27", s1, s2, g1, qty1);

  // log::debug!("receive 20.01 s1 60");
  // let qty2 = Qty::new(vec![Number::new(
  //   Decimal::from(6),
  //   uom0,
  //   Some(Box::new(Number::new(Decimal::from(10), uom1, None))),
  // )]);
  //
  // let r1 = receive(&app, "2023-01-20", s1, g1, qty2, "60".try_into().unwrap());
  // let r1_batch = Batch { id: r1, date: dt("2023-01-20").unwrap() };

  // log::debug!("receive 22.01 s1 40");
  // let qty3 = Qty::new(vec![Number::new(
  //   Decimal::from(4),
  //   uom0,
  //   Some(Box::new(Number::new(Decimal::from(10), uom1, None))),
  // )]);
  //
  // let r2 = receive(&app, "2023-01-22", s1, g1, qty3, "40".try_into().unwrap());
  // let r2_batch = Batch { id: r2, date: dt("2023-01-22").unwrap() };

  // s2 r1 60 60
  // s2 r2 40 40

  let balances = app.warehouse().database.get_balance_for_all(Utc::now()).unwrap();
  log::debug!("s1: {s1:#?}");
  log::debug!("s2: {s2:#?}");
  log::debug!("balances: {balances:#?}");

  assert_eq!(balances.len(), 1);

  let s2_bs = balances.get(&s2).unwrap();
  assert_eq!(s2_bs.len(), 1);

  let s2_g1_bs = s2_bs.get(&g1).unwrap();
  assert_eq!(s2_g1_bs.len(), 2);

  // let qty4 = Qty::new(vec![Number::new(Decimal::from(60), uom1, None)]);
  // assert_eq!(
  //   s2_g1_bs.get(&r1_batch).unwrap().clone(),
  //   BalanceForGoods { qty: qty4, cost: "60".try_into().unwrap() }
  // );

  // let qty5 = Qty::new(vec![Number::new(Decimal::from(40), uom1, None)]);
  // assert_eq!(
  //   s2_g1_bs.get(&r2_batch).unwrap().clone(),
  //   BalanceForGoods { qty: qty5, cost: "40".try_into().unwrap() }
  // );
}
