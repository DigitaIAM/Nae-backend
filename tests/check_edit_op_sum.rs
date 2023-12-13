mod test_init;

use chrono::{DateTime, Utc};
use json::{array, object};
use serde_json::from_str;
use std::io;
use std::str::FromStr;
use std::sync::Arc;
use uuid::Uuid;

use crate::test_init::{create_record, delete, goods, init, receive, store, transfer, update};
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
async fn check_edit_op_sum() {
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

  let qty0 = Qty::new(vec![Number::new(
    Decimal::from(1),
    uom0,
    Some(Box::new(Number::new(Decimal::from(14000), uom1, None))),
  )]);

  log::debug!("receive 06.01.23 s1 14000");
  let r1 = receive(&app, "2023-01-06", s1, g1, qty0.clone(), "276566780.0".try_into().unwrap());
  let r1_batch = Batch { id: r1, date: dt("2023-01-06").unwrap() };

  let qty1 = Qty::new(vec![Number::new(
    Decimal::from(1),
    uom0,
    Some(Box::new(Number::new(Decimal::try_from(14007.6).unwrap(), uom1, None))),
  )]);

  log::debug!("transfer 07.01 s1 > s2 14007.6");
  let t1 = transfer(&app, "2023-01-07", s1, s2, g1, qty1.clone());

  log::debug!("update receive 06.01.23 s1 14000 > 14007.6");
  update(
    &app,
    "2023-01-06",
    s1,
    None,
    g1,
    r1,
    r1_batch.clone(),
    InternalOperation::Receive(qty0, "276566780.0".try_into().unwrap()),
    InternalOperation::Receive(qty1.clone(), "276566780.0".try_into().unwrap()),
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
    BalanceForGoods { qty: qty1, cost: "276566780.0".try_into().unwrap() }
  );
}
