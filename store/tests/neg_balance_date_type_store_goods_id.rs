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
fn store_test_neg_balance_date_type_store_goods_id() {
  let tmp_dir = TempDir::new().expect("Can't create tmp dir in test_get_neg_balance");

  let wh = WHStorage::open(&tmp_dir.path()).unwrap();
  let mut db = wh.database;

  let op_d = dt("2022-10-10").expect("test_get_neg_balance");
  let check_d = dt("2022-10-11").expect("test_get_neg_balance");
  let w1 = Uuid::new_v4();
  let party = Batch { id: Uuid::new_v4(), date: op_d };

  let id1 = Uuid::from_u128(101);

  let uom0 = Uuid::new_v4();
  let uom1 = Uuid::new_v4();
  let inner = Some(Box::new(Number::new(Decimal::from(3), uom1, None)));

  let ops = vec![OpMutation::new(
    id1,
    op_d,
    w1,
    None,
    G1,
    party.clone(),
    None,
    Some(InternalOperation::Issue(
      Qty::new(vec![Number::new(Decimal::from(2), uom0, inner.clone())]),
      2000.into(),
      Mode::Manual,
    )),
  )];

  db.record_ops(&ops).expect("test_get_neg_balance");

  let agr = AggregationStoreGoodsBatch {
    store: Some(w1),
    goods: Some(G1),
    batch: Some(party.clone()),
    open_balance: BalanceForGoods::default(),
    receive: BalanceDelta::default(),
    issue: BalanceDelta {
      qty: Qty::new(vec![Number::new(Decimal::from(-2), uom0, inner.clone())]),
      cost: (-2000).into(),
    },
    close_balance: BalanceForGoods {
      qty: Qty::new(vec![Number::new(Decimal::from(-2), uom0, inner)]),
      cost: (-2000).into(),
    },
  };

  let res = db.report_for_store(w1, op_d, check_d).unwrap();
  assert_eq!(res.items.1[0], agr);

  tmp_dir.close().expect("Can't close tmp dir in test_get_neg_balance");
}
