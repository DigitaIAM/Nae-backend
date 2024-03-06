use rust_decimal::Decimal;
use store::batch::Batch;
use store::elements::dt;
use store::error::WHError;
use store::operations::{InternalOperation, OpMutation};
use store::qty::{Number, Qty};
use store::wh_storage::WHStorage;
use tempfile::TempDir;
use uuid::Uuid;

const G1: Uuid = Uuid::from_u128(1);

#[test]
fn store_test_get_wh_ops() -> Result<(), WHError> {
  let tmp_dir = TempDir::new().expect("Can't create tmp dir in test_get_wh_balance");

  let wh = WHStorage::open(&tmp_dir.path()).unwrap();
  let mut db = wh.database;

  let start_d = dt("2022-10-10")?;
  let end_d = dt("2022-10-11")?;
  let w1 = Uuid::new_v4();
  let party = Batch { id: Uuid::new_v4(), date: start_d };

  let id1 = Uuid::from_u128(101);
  let id2 = Uuid::from_u128(102);

  let uom = Uuid::new_v4();

  let ops = vec![
    OpMutation::new(
      id1,
      start_d,
      w1,
      None,
      G1,
      party.clone(),
      None,
      Some(InternalOperation::Receive(
        Qty::new(vec![Number::new(
          Decimal::from(2),
          uom,
          Some(Box::new(Number::new(Decimal::from(3), Uuid::new_v4(), None))),
        )]),
        2000.into(),
      )),
    ),
    OpMutation::new(
      id2,
      start_d,
      w1,
      None,
      G1,
      party.clone(),
      None,
      Some(InternalOperation::Receive(
        Qty::new(vec![Number::new(Decimal::from(1), uom, None)]),
        1000.into(),
      )),
    ),
  ];

  db.record_ops(&ops).unwrap();

  for ordered_topology in db.ordered_topologies[2..].iter() {
    let res = ordered_topology.ops_for_store(w1, start_d, end_d).unwrap();
    for i in 0..res.len() {
      assert_eq!(res[i], ops[i].to_op_after().unwrap());
    }
  }

  Ok(())
}
