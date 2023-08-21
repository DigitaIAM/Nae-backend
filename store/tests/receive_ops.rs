use store::balance::{Balance, BalanceForGoods};
use store::batch::Batch;
use store::elements::{dt, Mode};
use store::operations::{InternalOperation, OpMutation};
use store::wh_storage::WHStorage;
use tempfile::TempDir;
use uuid::Uuid;

const G1: Uuid = Uuid::from_u128(1);
const G2: Uuid = Uuid::from_u128(2);

#[test]
fn store_test_receive_ops() {
  let tmp_dir = TempDir::new().expect("Can't create tmp dir in test_get_wh_balance");

  let mut wh = WHStorage::open(&tmp_dir.path()).unwrap();
  let mut db = wh.database;

  let op_d = dt("2022-10-10").expect("test_receive_ops");
  let check_d = dt("2022-11-01").expect("test_receive_ops");
  let w1 = Uuid::new_v4();
  let party = Batch { id: Uuid::new_v4(), date: op_d };

  let id1 = Uuid::from_u128(101);
  let id2 = Uuid::from_u128(102);
  let id3 = Uuid::from_u128(103);
  let id4 = Uuid::from_u128(104);

  let ops = vec![
    OpMutation::receive_new(id1, op_d, w1, G1, party.clone(), 3.into(), 3000.into()),
    OpMutation::new(
      id2,
      op_d,
      w1,
      None,
      G1,
      party.clone(),
      None,
      Some(InternalOperation::Issue(1.into(), 1000.into(), Mode::Manual)),
    ),
    OpMutation::new(
      id3,
      op_d,
      w1,
      None,
      G2,
      party.clone(),
      None,
      Some(InternalOperation::Issue(2.into(), 2000.into(), Mode::Manual)),
    ),
    OpMutation::new(
      id4,
      op_d,
      w1,
      None,
      G2,
      party.clone(),
      None,
      Some(InternalOperation::Receive(2.into(), 2000.into())),
    ),
  ];

  db.record_ops(&ops).expect("test_receive_ops");

  let balance = vec![Balance {
    date: check_d,
    store: w1,
    goods: G1,
    batch: party,
    number: BalanceForGoods { qty: 2.into(), cost: 2000.into() },
  }];

  for checkpoint_topology in db.checkpoint_topologies.iter() {
    let res = checkpoint_topology
      .get_checkpoints_for_one_storage_before_date(w1, check_d)
      .unwrap();
    assert_eq!(&res, &balance);
  }

  tmp_dir.close().expect("Can't close tmp dir in test_receive_ops");
}
