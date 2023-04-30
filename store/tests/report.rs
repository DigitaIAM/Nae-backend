use core::str::FromStr;
use rust_decimal::Decimal; // TODO change to another Decimal
use store::balance::{BalanceDelta, BalanceForGoods};
use store::elements::{
  dt, AggregationStore, AgregationStoreGoods, Batch, InternalOperation, Mode, OpMutation,
};
use store::error::WHError;
use store::wh_storage::WHStorage;
use tempfile::TempDir;
use uuid::Uuid;

const G1: Uuid = Uuid::from_u128(1);
const G2: Uuid = Uuid::from_u128(2);
const G3: Uuid = Uuid::from_u128(3);

#[test]
fn store_test_report() {
  let tmp_dir = TempDir::new().expect("Can't create tmp dir in test_report");

  let wh = WHStorage::open(&tmp_dir.path()).unwrap();
  let mut db = wh.database;

  let start_d = dt("2022-11-07").expect("test_report");
  let end_d = dt("2022-11-08").expect("test_report");
  let w1 = Uuid::new_v4();
  let party = Batch { id: Uuid::new_v4(), date: start_d };

  let ops = vec![
    OpMutation::new(
      Uuid::new_v4(),
      dt("2022-10-30").expect("test_report"),
      w1,
      None,
      G1,
      party.clone(),
      None,
      Some(InternalOperation::Receive(4.into(), 4000.into())),
    ),
    OpMutation::new(
      Uuid::new_v4(),
      dt("2022-11-03").expect("test_report"),
      w1,
      None,
      G3,
      party.clone(),
      None,
      Some(InternalOperation::Receive(2.into(), 6000.into())),
    ),
    OpMutation::new(
      Uuid::new_v4(),
      start_d,
      w1,
      None,
      G2,
      party.clone(),
      None,
      Some(InternalOperation::Receive(1.into(), 1000.into())),
    ),
    OpMutation::new(
      Uuid::new_v4(),
      start_d,
      w1,
      None,
      G2,
      party.clone(),
      None,
      Some(InternalOperation::Receive(1.into(), 1000.into())),
    ),
    OpMutation::new(
      Uuid::new_v4(),
      start_d,
      w1,
      None,
      G3,
      party.clone(),
      None,
      Some(InternalOperation::Issue(Decimal::from_str("0.5").unwrap(), 1500.into(), Mode::Manual)),
    ),
    OpMutation::new(
      Uuid::new_v4(),
      start_d,
      w1,
      None,
      G3,
      party.clone(),
      None,
      Some(InternalOperation::Issue(Decimal::from_str("0.5").unwrap(), 1500.into(), Mode::Manual)),
    ),
  ];

  db.record_ops(&ops).expect("test_report");

  let agr_store = AggregationStore {
    store: Some(w1),
    open_balance: 10000.into(),
    receive: 2000.into(),
    issue: (-3000).into(),
    close_balance: 9000.into(),
  };

  let ex_items = vec![
    AgregationStoreGoods {
      store: Some(w1),
      goods: Some(G1),
      batch: Some(party.clone()),
      open_balance: BalanceForGoods { qty: 4.into(), cost: 4000.into() },
      receive: BalanceDelta::default(),
      issue: BalanceDelta::default(),
      close_balance: BalanceForGoods { qty: 4.into(), cost: 4000.into() },
    },
    AgregationStoreGoods {
      store: Some(w1),
      goods: Some(G2),
      batch: Some(party.clone()),
      open_balance: BalanceForGoods::default(),
      receive: BalanceDelta { qty: 2.into(), cost: 2000.into() },
      issue: BalanceDelta::default(),
      close_balance: BalanceForGoods { qty: 2.into(), cost: 2000.into() },
    },
    AgregationStoreGoods {
      store: Some(w1),
      goods: Some(G3),
      batch: Some(party.clone()),
      open_balance: BalanceForGoods { qty: 2.into(), cost: 6000.into() },
      receive: BalanceDelta::default(),
      issue: BalanceDelta { qty: (-1).into(), cost: (-3000).into() },
      close_balance: BalanceForGoods { qty: 1.into(), cost: 3000.into() },
    },
  ];

  let report = db.get_report(w1, start_d, end_d).unwrap();

  assert_eq!(report.items.0, agr_store);
  assert_eq!(report.items.1, ex_items);

  tmp_dir.close().expect("Can't remove tmp dir in test_report");
}
