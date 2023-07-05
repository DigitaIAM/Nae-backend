use store::balance::{BalanceDelta, BalanceForGoods};
use store::elements::{
  dt, AggregationStore, AgregationStoreGoods, Balance, Batch, InternalOperation, Mode, OpMutation,
};
use store::error::WHError;
use store::wh_storage::WHStorage;
use tempfile::TempDir;
use uuid::Uuid;

const G1: Uuid = Uuid::from_u128(1);

#[test]
fn store_test_receive_change_op() {
  let tmp_dir = TempDir::new().expect("Can't create tmp dir in test_receive_change_op");

  let wh = WHStorage::open(&tmp_dir.path()).unwrap();
  let mut db = wh.database;

  let start_d = dt("2022-10-10").expect("test_receive_change_op");
  let end_d = dt("2022-10-11").expect("test_receive_change_op");
  let w1 = Uuid::new_v4();

  let doc = Batch { id: Uuid::new_v4(), date: start_d };

  let id1 = Uuid::from_u128(101);

  let ops_old = vec![
    OpMutation::new(
      id1,
      dt("2022-08-25").expect("test_receive_change_op"),
      w1,
      None,
      G1,
      doc.clone(),
      None,
      Some(InternalOperation::Receive(3.into(), 10.into())),
    ),
    OpMutation::new(
      id1,
      dt("2022-09-20").expect("test_receive_change_op"),
      w1,
      None,
      G1,
      doc.clone(),
      None,
      Some(InternalOperation::Receive(1.into(), 30.into())),
    ),
  ];

  db.record_ops(&ops_old).expect("test_receive_change_op");

  let old_check = Balance {
    date: dt("2022-10-01").unwrap(),
    store: w1,
    goods: G1,
    batch: doc.clone(),
    number: BalanceForGoods { qty: 4.into(), cost: 40.into() },
  };

  let mut old_checkpoints =
    db.get_checkpoints_before_date(w1, start_d).expect("test_receive_change_op");

  // println!("OLD_CHECKPOINTS: {old_checkpoints:#?}");

  assert_eq!(old_check, old_checkpoints[0]);

  let ops_new = vec![OpMutation::new(
    id1,
    dt("2022-08-25").expect("test_receive_change_op"),
    w1,
    None,
    G1,
    doc.clone(),
    Some(InternalOperation::Receive(3.into(), 10.into())),
    Some(InternalOperation::Receive(4.into(), 100.into())),
  )];

  db.record_ops(&ops_new).expect("test_receive_change_op");

  let new_check = Balance {
    date: dt("2022-10-01").expect("test_receive_change_op"),
    store: w1,
    goods: G1,
    batch: doc.clone(),
    number: BalanceForGoods { qty: 5.into(), cost: 130.into() },
  };

  let mut new_checkpoints = db
    .get_checkpoints_before_date(w1, start_d)
    .expect("test_receive_change_op")
    .into_iter();

  assert_eq!(Some(new_check), new_checkpoints.next());

  let res = db.get_report(w1, start_d, end_d).unwrap();

  let agr = AgregationStoreGoods {
    store: Some(w1),
    goods: Some(G1),
    batch: Some(doc.clone()),
    open_balance: BalanceForGoods { qty: 5.into(), cost: 130.into() },
    receive: BalanceDelta::default(),
    issue: BalanceDelta { qty: 0.into(), cost: 0.into() },
    close_balance: BalanceForGoods { qty: 5.into(), cost: 130.into() },
  };

  assert_eq!(res.items.1[0], agr);

  tmp_dir.close().expect("Can't remove tmp dir in test_receive_change_op");
}
