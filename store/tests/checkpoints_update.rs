use rust_decimal::Decimal;
use store::aggregations::AggregationStoreGoods;
use store::balance::{Balance, BalanceDelta, BalanceForGoods};
use store::batch::Batch;
use store::elements::dt;
use store::operations::{InternalOperation, OpMutation};
use store::qty::{Number, Qty};
use store::wh_storage::WHStorage;
use tempfile::TempDir;
use uuid::Uuid;

const G1: Uuid = Uuid::from_u128(1);
const G2: Uuid = Uuid::from_u128(2);

#[test]
fn store_test_checkpoints_update() {
  let tmp_dir = TempDir::new().unwrap();

  let wh = WHStorage::open(&tmp_dir.path()).unwrap();
  let mut db = wh.database;

  let start_d = dt("2023-08-10").unwrap();
  let w1 = Uuid::new_v4();
  let w2 = Uuid::new_v4();

  let id1 = Uuid::from_u128(101);

  let uom0 = Uuid::new_v4();
  let uom1 = Uuid::new_v4();
  let inner = Some(Box::new(Number::new(Decimal::from(3), uom1, None)));

  let batch0 = Batch { id: Uuid::new_v4(), date: dt("2022-05-10").unwrap() };

  let op_0 = vec![OpMutation::new(
    id1,
    dt("2022-05-10").unwrap(),
    w1,
    None,
    G1,
    batch0.clone(),
    None,
    Some(InternalOperation::Receive(
      Qty::new(vec![Number::new(Decimal::from(3), uom0, inner.clone())]),
      10.into(),
    )),
  )];

  db.record_ops(&op_0).unwrap();

  // let old_check = Balance {
  //   date: dt("2023-07-01").unwrap(),
  //   store: w1,
  //   goods: G1,
  //   batch: batch0.clone(),
  //   number: BalanceForGoods {
  //     qty: Qty::new(vec![Number::new(Decimal::from(3), uom0, inner.clone())]),
  //     cost: 10.into(),
  //   },
  // };
  //
  // let mut old_checkpoints = db.get_checkpoints_for_one_storage_before_date(w1, start_d).unwrap();
  //
  // println!("OLD_CHECKPOINTS: {old_checkpoints:#?}");
  //
  // assert_eq!(old_check, old_checkpoints[0]);

  let batch1 = Batch { id: Uuid::new_v4(), date: dt("2023-07-05").unwrap() };

  let op_1 = vec![OpMutation::new(
    id1,
    dt("2023-07-05").unwrap(),
    w2,
    None,
    G2,
    batch1.clone(),
    None,
    Some(InternalOperation::Receive(
      Qty::new(vec![Number::new(Decimal::from(5), uom0, inner.clone())]),
      100.into(),
    )),
  )];

  db.record_ops(&op_1).unwrap();

  let w1_check = Balance {
    date: dt("2023-08-01").unwrap(),
    store: w1,
    goods: G1,
    batch: batch0.clone(),
    number: BalanceForGoods {
      qty: Qty::new(vec![Number::new(Decimal::from(3), uom0, inner.clone())]),
      cost: 10.into(),
    },
  };

  let w1_balance = db.get_checkpoints_for_one_storage_before_date(w1, start_d).unwrap();
  // println!("w1_balance: {w1_balance:#?}");
  assert_eq!(w1_check, w1_balance[0]);

  let w2_check = Balance {
    date: dt("2023-08-01").unwrap(),
    store: w2,
    goods: G2,
    batch: batch1.clone(),
    number: BalanceForGoods {
      qty: Qty::new(vec![Number::new(Decimal::from(5), uom0, inner.clone())]),
      cost: 100.into(),
    },
  };

  let w2_balance = db.get_checkpoints_for_one_storage_before_date(w2, start_d).unwrap();
  // println!("w2_balance: {w2_balance:#?}");
  assert_eq!(w2_check, w2_balance[0]);

  // assert_eq!(Some(new_check), new_checkpoints.next());

  // let res = db.get_report_for_storage(w1, start_d, end_d).unwrap();
  //
  // let agr = AgregationStoreGoods {
  //   store: Some(w1),
  //   goods: Some(G1),
  //   batch: Some(batch0.clone()),
  //   open_balance: BalanceForGoods {
  //     qty: Qty::new(vec![Number::new(Decimal::from(5), uom0, inner.clone())]),
  //     cost: 130.into(),
  //   },
  //   receive: BalanceDelta::default(),
  //   issue: BalanceDelta::default(),
  //   close_balance: BalanceForGoods {
  //     qty: Qty::new(vec![Number::new(Decimal::from(5), uom0, inner.clone())]),
  //     cost: 130.into(),
  //   },
  // };
  //
  // assert_eq!(res.items.1[0], agr);

  tmp_dir.close().unwrap();
}
