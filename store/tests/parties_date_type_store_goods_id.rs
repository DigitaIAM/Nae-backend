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
fn store_test_parties_date_type_store_goods_id() {
  let tmp_dir = TempDir::new().expect("Can't create tmp dir in test_parties");

  let wh = WHStorage::open(&tmp_dir.path()).unwrap();
  let mut db = wh.database;

  let start_d = dt("2022-10-10").expect("test_parties");
  let end_d = dt("2022-10-11").expect("test_parties");
  let w1 = Uuid::new_v4();
  let doc1 = Batch { id: Uuid::new_v4(), date: dt("2022-10-08").expect("test_parties") };
  let doc2 = Batch { id: Uuid::new_v4(), date: start_d };

  let id1 = Uuid::from_u128(101);
  let id2 = Uuid::from_u128(102);
  let id3 = Uuid::from_u128(102);

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
      doc1.clone(),
      None,
      Some(InternalOperation::Receive(
        Qty::new(vec![Number::new(Decimal::from(3), uom0, inner.clone())]),
        3000.into(),
      )),
    ),
    OpMutation::new(
      id2,
      start_d,
      w1,
      None,
      G1,
      doc2.clone(),
      None,
      Some(InternalOperation::Receive(
        Qty::new(vec![Number::new(Decimal::from(4), uom0, inner.clone())]),
        2000.into(),
      )),
    ),
    OpMutation::new(
      id3,
      start_d,
      w1,
      None,
      G1,
      doc2.clone(),
      None,
      Some(InternalOperation::Issue(
        Qty::new(vec![Number::new(Decimal::from(1), uom0, inner.clone())]),
        500.into(),
        Mode::Manual,
      )),
    ),
  ];

  db.record_ops(&ops).expect("test_parties");

  let res = db.report_for_store(w1, start_d, end_d).unwrap();

  let agrs = vec![
    AggregationStoreGoodsBatch {
      store: Some(w1),
      goods: Some(G1),
      batch: Some(doc1.clone()),
      open_balance: BalanceForGoods::default(),
      receive: BalanceDelta {
        qty: Qty::new(vec![Number::new(Decimal::from(3), uom0, inner.clone())]),
        cost: 3000.into(),
      },
      issue: BalanceDelta::default(),
      close_balance: BalanceForGoods {
        qty: Qty::new(vec![Number::new(Decimal::from(3), uom0, inner.clone())]),
        cost: 3000.into(),
      },
    },
    AggregationStoreGoodsBatch {
      store: Some(w1),
      goods: Some(G1),
      batch: Some(doc2.clone()),
      open_balance: BalanceForGoods::default(),
      receive: BalanceDelta {
        qty: Qty::new(vec![Number::new(Decimal::from(4), uom0, inner.clone())]),
        cost: 2000.into(),
      },
      issue: BalanceDelta {
        qty: Qty::new(vec![Number::new(Decimal::from(-1), uom0, inner.clone())]),
        cost: (-500).into(),
      },
      close_balance: BalanceForGoods {
        qty: Qty::new(vec![Number::new(Decimal::from(3), uom0, inner)]),
        cost: 1500.into(),
      },
    },
  ];

  assert_eq!(res.items.1[0], agrs[0]);
  assert_eq!(res.items.1[1], agrs[1]);

  tmp_dir.close().expect("Can't close tmp dir in test_parties");
}
