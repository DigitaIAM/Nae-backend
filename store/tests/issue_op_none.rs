use rust_decimal::Decimal;
use store::aggregations::AggregationStoreGoods;
use store::balance::{BalanceDelta, BalanceForGoods};
use store::batch::Batch;
use store::elements::dt;
use store::operations::{InternalOperation, OpMutation};
use store::qty::{Number, Qty};
use store::wh_storage::WHStorage;
use tempfile::TempDir;
use uuid::Uuid;

const G1: Uuid = Uuid::from_u128(1);

#[test]
fn store_test_issue_op_none() {
  let tmp_dir = TempDir::new().expect("Can't create tmp dir in test_issue_op_none");

  let wh = WHStorage::open(&tmp_dir.path()).unwrap();
  let mut db = wh.database;

  let start_d = dt("2022-10-10").unwrap();
  let end_d = dt("2022-10-11").unwrap();
  let w1 = Uuid::new_v4();

  let doc = Batch { id: Uuid::new_v4(), date: start_d };

  let id1 = Uuid::from_u128(101);
  let id3 = Uuid::from_u128(103);

  let uom = Uuid::new_v4();

  let ops = vec![
    OpMutation::new(
      id1,
      start_d,
      w1,
      None,
      G1,
      doc.clone(),
      None,
      Some(InternalOperation::Receive(
        Qty::new(vec![Number::new(Decimal::from(3), uom, None)]),
        10.into(),
      )),
    ),
    OpMutation::new(id3, start_d, w1, None, G1, doc.clone(), None, None),
  ];

  db.record_ops(&ops).unwrap();

  let res = db.get_report_for_storage(w1, start_d, end_d).unwrap();

  // println!("REPORT: {res:#?}");

  let agr = AggregationStoreGoods {
    store: Some(w1),
    goods: Some(G1),
    batch: Some(doc.clone()),
    open_balance: BalanceForGoods::default(),
    receive: BalanceDelta {
      qty: Qty::new(vec![Number::new(Decimal::from(3), uom, None)]),
      cost: 10.into(),
    },
    issue: BalanceDelta::default(),
    close_balance: BalanceForGoods {
      qty: Qty::new(vec![Number::new(Decimal::from(3), uom, None)]),
      cost: 10.into(),
    },
  };

  assert_eq!(agr, res.items.1[0]);

  tmp_dir.close().expect("Can't remove tmp dir in test_issue_op_none");
}
