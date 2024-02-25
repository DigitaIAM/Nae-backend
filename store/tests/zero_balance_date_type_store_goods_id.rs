use store::balance::{BalanceDelta, BalanceForGoods};
use store::elements::{dt, AgregationStoreGoods, Batch, InternalOperation, Mode, OpMutation};
use store::wh_storage::WHStorage;
use tempfile::TempDir;
use uuid::Uuid;

const G1: Uuid = Uuid::from_u128(1);

#[test]
fn store_test_zero_balance_date_type_store_goods_id() {
  let tmp_dir = TempDir::new().expect("Can't create tmp dir in test_get_zero_balance");

  let wh = WHStorage::open(&tmp_dir.path()).unwrap();
  let mut db = wh.database;

  let start_d = dt("2022-10-10").expect("test_get_zero_balance");
  let end_d = dt("2022-10-11").expect("test_get_zero_balance");
  let w1 = Uuid::new_v4();
  let party = Batch { id: Uuid::new_v4(), date: start_d };

  let id1 = Uuid::from_u128(101);
  let id2 = Uuid::from_u128(102);

  let ops = vec![
    OpMutation::receive_new(id1, start_d, w1, G1, party.clone(), 3.into(), 3000.into()),
    OpMutation::new(
      id2,
      start_d,
      w1,
      None,
      G1,
      party.clone(),
      None,
      Some(InternalOperation::Issue(3.into(), 3000.into(), Mode::Manual)),
    ),
  ];

  db.record_ops(&ops);

  let res = db.get_report(w1, start_d, end_d).unwrap();

  let agr = AgregationStoreGoods {
    store: Some(w1),
    goods: Some(G1),
    batch: Some(party.clone()),
    open_balance: BalanceForGoods::default(),
    receive: BalanceDelta { qty: 3.into(), cost: 3000.into() },
    issue: BalanceDelta { qty: (-3).into(), cost: (-3000).into() },
    close_balance: BalanceForGoods::default(),
  };

  assert_eq!(res.items.1[0], agr);

  tmp_dir.close().expect("Can't close tmp dir in test_get_zero_balance");
}
