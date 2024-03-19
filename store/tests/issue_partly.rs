use rust_decimal::Decimal;
use store::aggregations::AggregationStoreGoodsBatch;
use store::balance::{BalanceDelta, BalanceForGoods};
use store::batch::Batch;
use store::elements::{dt, Mode};
use store::operations::{InternalOperation, OpMutation};
use store::qty::{Number, Qty};
use store::wh_storage::WHStorage;
use tempfile::TempDir;
use uuid::Uuid;

const G1: Uuid = Uuid::from_u128(1);

#[test]
fn store_test_issue_partly() {
  // std::env::set_var("RUST_LOG", "debug");
  // env_logger::init();

  let tmp_dir = TempDir::new().unwrap();

  let wh = WHStorage::open(&tmp_dir.path()).unwrap();
  let mut db = wh.database;

  // case # 1
  let start_d = dt("2022-10-10").unwrap();
  let end_d = dt("2022-10-11").unwrap();
  let w1 = Uuid::new_v4();

  let batch = Batch { id: Uuid::new_v4(), date: start_d };

  let id1 = Uuid::from_u128(101);
  let id2 = Uuid::from_u128(102);

  let uom0 = Uuid::new_v4();
  let uom1 = Uuid::new_v4();

  let inner = Some(Box::new(Number::new(Decimal::from(3), uom1, None)));

  let ops = vec![
    OpMutation::new(
      id1,
      start_d,
      w1,
      None,
      G1,
      batch.clone(),
      None,
      Some(InternalOperation::Receive(
        Qty::new(vec![Number::new(Decimal::from(2), uom0, inner.clone())]),
        9.into(),
      )),
    ),
    OpMutation::new(
      id2,
      start_d,
      w1,
      None,
      G1,
      Batch::no(),
      None,
      Some(InternalOperation::Issue(
        Qty::new(vec![Number::new(Decimal::from(1), uom0, inner.clone())]),
        0.into(),
        Mode::Auto,
      )),
    ),
  ];

  db.record_ops(&ops).unwrap();

  let res = db.report_for_store(w1, start_d, end_d).unwrap();
  println!("res0= {:#?}", res.items.1);

  let agr = AggregationStoreGoodsBatch {
    store: Some(w1),
    goods: Some(G1),
    batch: Some(batch.clone()),
    open_balance: BalanceForGoods::default(),
    receive: BalanceDelta {
      qty: Qty::new(vec![Number::new(Decimal::from(2), uom0, inner.clone())]),
      cost: 9.into(),
    },
    issue: BalanceDelta {
      qty: Qty::new(vec![Number::new(Decimal::from(-1), uom0, inner.clone())]),
      cost: Decimal::try_from(-4.5).unwrap().into(),
    },
    close_balance: BalanceForGoods {
      qty: Qty::new(vec![Number::new(Decimal::from(1), uom0, inner.clone())]),
      cost: Decimal::try_from(4.5).unwrap().into(),
    },
  };

  assert_eq!(agr, res.items.1[0]);

  // case # 2
  let start_d = dt("2022-11-10").unwrap();
  let end_d = dt("2022-11-11").unwrap();
  let w1 = Uuid::new_v4();

  let batch = Batch { id: Uuid::new_v4(), date: start_d };

  let id1 = Uuid::from_u128(201);
  let id2 = Uuid::from_u128(202);

  let uom0 = Uuid::new_v4();
  let uom1 = Uuid::new_v4();

  let inner = Some(Box::new(Number::new(Decimal::from(3), uom1, None)));

  let ops = vec![
    OpMutation::new(
      id1,
      start_d,
      w1,
      None,
      G1,
      batch.clone(),
      None,
      Some(InternalOperation::Receive(
        Qty::new(vec![Number::new(Decimal::from(1), uom0, inner.clone())]),
        9.into(),
      )),
    ),
    OpMutation::new(
      id2,
      start_d,
      w1,
      None,
      G1,
      Batch::no(),
      None,
      Some(InternalOperation::Issue(
        Qty::new(vec![Number::new(Decimal::from(1), uom1, None)]),
        0.into(),
        Mode::Auto,
      )),
    ),
  ];

  db.record_ops(&ops).unwrap();

  let res = db.report_for_store(w1, start_d, end_d).unwrap();
  // println!("res1= {:#?}", res.items.1);

  let agr = AggregationStoreGoodsBatch {
    store: Some(w1),
    goods: Some(G1),
    batch: Some(batch.clone()),
    open_balance: BalanceForGoods::default(),
    receive: BalanceDelta {
      qty: Qty::new(vec![Number::new(Decimal::from(1), uom0, inner)]),
      cost: 9.into(),
    },
    issue: BalanceDelta {
      qty: Qty::new(vec![Number::new(Decimal::from(-1), uom1, None)]),
      cost: (-3).into(),
    },
    close_balance: BalanceForGoods {
      qty: Qty::new(vec![Number::new(Decimal::from(2), uom1, None)]),
      cost: 6.into(),
    },
  };

  assert_eq!(agr, res.items.1[0]);

  tmp_dir.close().unwrap();
}
