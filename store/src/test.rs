use crate::elements::{dt, Batch, OpMutation, InternalOperation, Mode, Balance, AgregationStoreGoods, AgregationStore};
use uuid::Uuid;
use crate::check_date_store_batch::CheckDateStoreBatch;
use crate::wh_storage::WHStorage;
use crate::balance::{BalanceForGoods, BalanceDelta};
use crate::error::WHError;
use crate::date_type_store_batch_id::DateTypeStoreBatchId;
use crate::store_date_type_batch_id::StoreDateTypeBatchId;
use rocksdb::IteratorMode;
use tempfile::TempDir;
use rust_decimal::Decimal; // TODO change to another Decimal
use core::str::FromStr;

const G1: Uuid = Uuid::from_u128(1);
const G2: Uuid = Uuid::from_u128(2);
const G3: Uuid = Uuid::from_u128(3);

#[test]
fn store_test_key_to_data() {
  let date1 = dt("2022-12-15").unwrap();
  let storage1 = Uuid::from_u128(201);
  let goods1 = Uuid::from_u128(101);
  let batch = Batch { id: Uuid::from_u128(102), date: date1 };

  let key: Vec<u8> = []
      .iter()
      .chain((date1.timestamp() as u64).to_be_bytes().iter())
      .chain(storage1.as_bytes().iter())
      .chain(goods1.as_bytes().iter())
      .chain((batch.date.timestamp() as u64).to_be_bytes().iter())
      .chain(batch.id.as_bytes().iter())
      .map(|b| *b)
      .collect();

  let (d, s, g, b) = CheckDateStoreBatch::key_to_data(key).unwrap();

  // println!("{d:?}, {s:?}, {g:?}, {b:?}");

  assert_eq!(date1, d);
  assert_eq!(storage1, s);
  assert_eq!(goods1, g);
  assert_eq!(batch, b);
}

#[test]
fn store_test_greater_issue() {
  let tmp_dir = TempDir::new().expect("Can't create tmp dir in test_get_wh_balance");

  let mut wh = WHStorage::open(&tmp_dir.path()).unwrap();
  let mut db = wh.database;

  let op_d = dt("2022-10-10").expect("test_receive_ops");
  let check_d = dt("2022-11-01").expect("test_receive_ops");
  let w1 = Uuid::new_v4();

  let id1 = Uuid::from_u128(101);
  let id2 = Uuid::from_u128(102);

  let party = Batch { id: id1, date: op_d };

  let op_receive = vec![
    OpMutation::new(
      id1,
      op_d,
      w1,
      None,
      G1,
      party.clone(),
      None,
      Some(InternalOperation::Receive(1.into(), 10.into())),
    ),
  ];

  db.record_ops(&op_receive).unwrap();

  let op_issue = vec![
    OpMutation::new(
      id2,
      op_d,
      w1,
      None,
      G1,
      party.clone(),
      None,
      Some(InternalOperation::Issue(3.into(), 5.into(), Mode::Auto)),
    ),
  ];

  db.record_ops(&op_issue).unwrap();

  let balance = vec![Balance {
    date: check_d,
    store: w1,
    goods: G1,
    batch: party,
    number: BalanceForGoods { qty: (-2).into(), cost: (-20).into() },
  }];

  for checkpoint_topology in db.checkpoint_topologies.iter() {
    let res = checkpoint_topology.get_checkpoints_before_date(w1, check_d).unwrap();
    assert_eq!(&res, &balance);
  }

  tmp_dir.close().expect("Can't close tmp dir in test_receive_ops");
}

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
    let res = checkpoint_topology.get_checkpoints_before_date(w1, check_d).unwrap();
    assert_eq!(&res, &balance);
  }

  tmp_dir.close().expect("Can't close tmp dir in test_receive_ops");
}

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

  let ops = vec![OpMutation::new(
    id1,
    op_d,
    w1,
    None,
    G1,
    party.clone(),
    None,
    Some(InternalOperation::Issue(2.into(), 2000.into(), Mode::Manual)),
  )];

  db.record_ops(&ops).expect("test_get_neg_balance");

  let agr = AgregationStoreGoods {
    store: Some(w1),
    goods: Some(G1),
    batch: Some(party.clone()),
    open_balance: BalanceForGoods::default(),
    receive: BalanceDelta::default(),
    issue: BalanceDelta { qty: (-2).into(), cost: (-2000).into() },
    close_balance: BalanceForGoods { qty: (-2).into(), cost: (-2000).into() },
  };

  let res = db.get_report(w1, op_d, check_d).unwrap();
  assert_eq!(res.items.1[0], agr);

  tmp_dir.close().expect("Can't close tmp dir in test_get_neg_balance");
}

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

  let ops = vec![
    OpMutation::new(
      id1,
      start_d,
      w1,
      None,
      G1,
      party.clone(),
      None,
      Some(InternalOperation::Receive(2.into(), 2000.into())),
    ),
    OpMutation::new(
      id2,
      start_d,
      w1,
      None,
      G1,
      party.clone(),
      None,
      Some(InternalOperation::Receive(1.into(), 1000.into())),
    ),
  ];

  db.record_ops(&ops).unwrap();

  for ordered_topology in db.ordered_topologies.iter() {
    let res = ordered_topology.get_ops(w1, start_d, end_d).unwrap();
    for i in 0..res.len() {
      assert_eq!(res[i], ops[i].to_op());
    }
  }

  Ok(())
}

#[test]
fn store_test_get_aggregations_without_checkpoints() -> Result<(), WHError> {
  let tmp_dir = TempDir::new().expect("Can't create tmp dir in test_get_wh_balance");

  let wh = WHStorage::open(&tmp_dir.path()).unwrap();
  let mut db = wh.database;

  let op_d = dt("2022-10-10")?;
  let check_d = dt("2022-10-11")?;
  let w1 = Uuid::new_v4();
  let doc1 = Batch { id: Uuid::new_v4(), date: dt("2022-10-09")? };
  let doc2 = Batch { id: Uuid::new_v4(), date: op_d };

  let id1 = Uuid::from_u128(101);
  let id2 = Uuid::from_u128(102);
  let id3 = Uuid::from_u128(103);
  let id4 = Uuid::from_u128(104);

  let ops = vec![
    OpMutation::new(
      id1,
      op_d,
      w1,
      None,
      G1,
      doc1.clone(),
      None,
      Some(InternalOperation::Receive(3.into(), 3000.into())),
    ),
    OpMutation::new(
      id2,
      op_d,
      w1,
      None,
      G1,
      doc1.clone(),
      None,
      Some(InternalOperation::Issue(1.into(), 1000.into(), Mode::Manual)),
    ),
    OpMutation::new(
      id3,
      op_d,
      w1,
      None,
      G2,
      doc2.clone(),
      None,
      Some(InternalOperation::Issue(2.into(), 2000.into(), Mode::Manual)),
    ),
    OpMutation::new(
      id4,
      op_d,
      w1,
      None,
      G2,
      doc2.clone(),
      None,
      Some(InternalOperation::Receive(2.into(), 2000.into())),
    ),
  ];

  db.record_ops(&ops).unwrap();

  let agregations = vec![
    AgregationStoreGoods {
      store: Some(w1),
      goods: Some(G1),
      batch: Some(doc1.clone()),
      open_balance: BalanceForGoods::default(),
      receive: BalanceDelta { qty: 3.into(), cost: 3000.into() },
      issue: BalanceDelta { qty: (-1).into(), cost: (-1000).into() },
      close_balance: BalanceForGoods { qty: 2.into(), cost: 2000.into() },
    },
    AgregationStoreGoods {
      store: Some(w1),
      goods: Some(G2),
      batch: Some(doc2.clone()),
      open_balance: BalanceForGoods::default(),
      receive: BalanceDelta { qty: 2.into(), cost: 2000.into() },
      issue: BalanceDelta { qty: (-2).into(), cost: (-2000).into() },
      close_balance: BalanceForGoods::default(),
    },
  ];

  let res = db.get_report(w1, op_d, check_d)?;

  assert_eq!(agregations, res.items.1);

  tmp_dir.close().expect("Can't close tmp dir in store_test_get_wh_balance");

  Ok(())
}

#[test]
fn store_test_op_iter() {
  let tmp_dir = TempDir::new().expect("Can't create tmp dir in test_op_iter");

  let wh = WHStorage::open(&tmp_dir.path()).unwrap();
  let mut db = wh.database;

  let start_d = dt("2022-11-01").expect("test_op_iter");
  let w1 = Uuid::new_v4();
  let party = Batch { id: Uuid::new_v4(), date: start_d };

  let id1 = Uuid::from_u128(101);
  let id2 = Uuid::from_u128(102);
  let id3 = Uuid::from_u128(103);
  let id4 = Uuid::from_u128(104);

  let ops = vec![
    OpMutation::new(
      id3,
      start_d,
      w1,
      None,
      G2,
      party.clone(),
      None,
      Some(InternalOperation::Receive(1.into(), 1000.into())),
    ),
    OpMutation::new(
      id4,
      start_d,
      w1,
      None,
      G2,
      party.clone(),
      None,
      Some(InternalOperation::Receive(1.into(), 1000.into())),
    ),
    OpMutation::new(
      id1,
      start_d,
      w1,
      None,
      G3,
      party.clone(),
      None,
      Some(InternalOperation::Issue(Decimal::from_str("0.5").unwrap(), 1500.into(), Mode::Manual)),
    ),
    OpMutation::new(
      id2,
      start_d,
      w1,
      None,
      G3,
      party.clone(),
      None,
      Some(InternalOperation::Issue(Decimal::from_str("0.5").unwrap(), 1500.into(), Mode::Manual)),
    ),
  ];

  db.record_ops(&ops).unwrap();

  let cf1 = db.db.cf_handle(DateTypeStoreBatchId::cf_name()).unwrap();
  let cf2 = db.db.cf_handle(StoreDateTypeBatchId::cf_name()).unwrap();

  let iter1 = db.db.iterator_cf(&cf1, IteratorMode::Start);
  let iter2 = db.db.iterator_cf(&cf2, IteratorMode::Start);

  let mut res1: Vec<String> = Vec::new();
  let mut res2: Vec<String> = Vec::new();

  for item in iter1 {
    let (_, v) = item.unwrap();
    let str = String::from_utf8_lossy(&v).to_string();

    // println!("{str:?}");

    res1.push(str);
  }

  for item in iter2 {
    let (_, v) = item.unwrap();
    let str = String::from_utf8_lossy(&v).to_string();

    // println!("{str:?}");

    res2.push(str);
  }

  for i in 0..res1.len() {
    assert_eq!(res2[i], res1[i]);
  }

  tmp_dir.close().expect("Can't remove tmp dir in test_op_iter");
}

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

  let agr_store = AgregationStore {
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

  let ops = vec![
    OpMutation::new(
      id1,
      start_d,
      w1,
      None,
      G1,
      doc1.clone(),
      None,
      Some(InternalOperation::Receive(3.into(), 3000.into())),
    ),
    OpMutation::new(
      id2,
      start_d,
      w1,
      None,
      G1,
      doc2.clone(),
      None,
      Some(InternalOperation::Receive(4.into(), 2000.into())),
    ),
    OpMutation::new(
      id3,
      start_d,
      w1,
      None,
      G1,
      doc2.clone(),
      None,
      Some(InternalOperation::Issue(1.into(), 500.into(), Mode::Manual)),
    ),
  ];

  db.record_ops(&ops).expect("test_parties");

  let res = db.get_report(w1, start_d, end_d).unwrap();

  let agrs = vec![
    AgregationStoreGoods {
      store: Some(w1),
      goods: Some(G1),
      batch: Some(doc1.clone()),
      open_balance: BalanceForGoods::default(),
      receive: BalanceDelta { qty: 3.into(), cost: 3000.into() },
      issue: BalanceDelta::default(),
      close_balance: BalanceForGoods { qty: 3.into(), cost: 3000.into() },
    },
    AgregationStoreGoods {
      store: Some(w1),
      goods: Some(G1),
      batch: Some(doc2.clone()),
      open_balance: BalanceForGoods::default(),
      receive: BalanceDelta { qty: 4.into(), cost: 2000.into() },
      issue: BalanceDelta { qty: (-1).into(), cost: (-500).into() },
      close_balance: BalanceForGoods { qty: 3.into(), cost: 1500.into() },
    },
  ];

  assert_eq!(res.items.1[0], agrs[0]);
  assert_eq!(res.items.1[1], agrs[1]);

  tmp_dir.close().expect("Can't close tmp dir in test_parties");
}

#[test]
fn store_test_issue_cost_none() {
  let tmp_dir = TempDir::new().expect("Can't create tmp dir in test_issue_cost_none");

  let wh = WHStorage::open(&tmp_dir.path()).unwrap();
  let mut db = wh.database;

  let start_d = dt("2022-10-10").expect("test_issue_cost_none");
  let end_d = dt("2022-10-11").expect("test_issue_cost_none");
  let w1 = Uuid::new_v4();

  let doc = Batch { id: Uuid::new_v4(), date: start_d };

  let id1 = Uuid::from_u128(101);
  let id2 = Uuid::from_u128(102);

  let ops = vec![
    OpMutation::new(
      id1,
      start_d,
      w1,
      None,
      G1,
      doc.clone(),
      None,
      Some(InternalOperation::Receive(4.into(), 2000.into())),
    ),
    OpMutation::new(
      id2,
      start_d,
      w1,
      None,
      G1,
      doc.clone(),
      None,
      Some(InternalOperation::Issue(1.into(), 0.into(), Mode::Auto)),
    ),
  ];

  db.record_ops(&ops).expect("test_issue_cost_none");

  let res = db.get_report(w1, start_d, end_d).unwrap();

  let agr = AgregationStoreGoods {
    store: Some(w1),
    goods: Some(G1),
    batch: Some(doc.clone()),
    open_balance: BalanceForGoods::default(),
    receive: BalanceDelta { qty: 4.into(), cost: 2000.into() },
    issue: BalanceDelta { qty: (-1).into(), cost: (-500).into() },
    close_balance: BalanceForGoods { qty: 3.into(), cost: 1500.into() },
  };

  assert_eq!(agr, res.items.1[0]);

  tmp_dir.close().expect("Can't remove tmp dir in test_issue_cost_none");
}

#[test]
fn store_test_receive_cost_none() {
  let tmp_dir = TempDir::new().expect("Can't create tmp dir in test_receive_cost_none");

  let wh = WHStorage::open(&tmp_dir.path()).unwrap();
  let mut db = wh.database;

  let start_d = dt("2022-10-10").expect("test_receive_cost_none");
  let end_d = dt("2022-10-11").expect("test_receive_cost_none");
  let w1 = Uuid::new_v4();

  let doc = Batch { id: Uuid::new_v4(), date: start_d };

  let id1 = Uuid::from_u128(101);
  let id2 = Uuid::from_u128(102);

  let ops = vec![
    OpMutation::new(
      id1,
      start_d,
      w1,
      None,
      G1,
      doc.clone(),
      None,
      Some(InternalOperation::Receive(4.into(), 2000.into())),
    ),
    OpMutation::new(
      id2,
      start_d,
      w1,
      None,
      G1,
      doc.clone(),
      None,
      Some(InternalOperation::Receive(1.into(), 0.into())),
    ),
  ];

  db.record_ops(&ops).expect("test_receive_cost_none");

  let res = db.get_report(w1, start_d, end_d).unwrap();

  let agr = AgregationStoreGoods {
    store: Some(w1),
    goods: Some(G1),
    batch: Some(doc.clone()),
    open_balance: BalanceForGoods::default(),
    receive: BalanceDelta { qty: 5.into(), cost: 2000.into() },
    issue: BalanceDelta { qty: 0.into(), cost: 0.into() },
    close_balance: BalanceForGoods { qty: 5.into(), cost: 2000.into() },
  };

  assert_eq!(agr, res.items.1[0]);

  tmp_dir.close().expect("Can't remove tmp dir in test_receive_cost_none");
}

#[test]
fn store_test_issue_remainder() {
  let tmp_dir = TempDir::new().expect("Can't create tmp dir in test_issue_remainder");

  let wh = WHStorage::open(&tmp_dir.path()).unwrap();
  let mut db = wh.database;

  let start_d = dt("2022-10-10").expect("test_issue_remainder");
  let end_d = dt("2022-10-11").expect("test_issue_remainder");
  let w1 = Uuid::new_v4();

  let doc = Batch { id: Uuid::new_v4(), date: start_d };

  let id1 = Uuid::from_u128(101);
  let id2 = Uuid::from_u128(102);
  let id3 = Uuid::from_u128(103);

  let ops = vec![
    OpMutation::new(
      id1,
      start_d,
      w1,
      None,
      G1,
      doc.clone(),
      None,
      Some(InternalOperation::Receive(3.into(), 10.into())),
    ),
    OpMutation::new(
      id2,
      start_d,
      w1,
      None,
      G1,
      doc.clone(),
      None,
      Some(InternalOperation::Issue(1.into(), 0.into(), Mode::Auto)),
    ),
    OpMutation::new(
      id3,
      start_d,
      w1,
      None,
      G1,
      doc.clone(),
      None,
      Some(InternalOperation::Issue(2.into(), 0.into(), Mode::Auto)),
    ),
  ];

  db.record_ops(&ops).expect("test_issue_remainder");

  // let st = DateTypeStoreGoodsId();
  // let res = st.get_report(start_d, end_d, w1, &mut db).expect("test_issue_remainder");

  // println!("HELLO: {:#?}", res.items.1);

  let res = db.get_report(w1, start_d, end_d).unwrap();

  let agr = AgregationStoreGoods {
    store: Some(w1),
    goods: Some(G1),
    batch: Some(doc.clone()),
    open_balance: BalanceForGoods::default(),
    receive: BalanceDelta { qty: 3.into(), cost: 10.into() },
    issue: BalanceDelta { qty: (-3).into(), cost: (-10).into() },
    close_balance: BalanceForGoods { qty: 0.into(), cost: 0.into() },
  };

  assert_eq!(agr, res.items.1[0]);

  tmp_dir.close().expect("Can't remove tmp dir in test_issue_remainder");
}

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
  let id2 = Uuid::from_u128(102);
  let id3 = Uuid::from_u128(103);

  let ops = vec![
    OpMutation::new(
      id1,
      start_d,
      w1,
      None,
      G1,
      doc.clone(),
      None,
      Some(InternalOperation::Receive(3.into(), 10.into())),
    ),
    // КОРРЕКТНАЯ ОПЕРАЦИЯ С ДВУМЯ NONE?
    OpMutation::new(id3, start_d, w1, None, G1, doc.clone(), None, None),
  ];

  db.record_ops(&ops).unwrap();

  let res = db.get_report(w1, start_d, end_d).unwrap();

  // println!("REPORT: {res:#?}");

  let agr = AgregationStoreGoods {
    store: Some(w1),
    goods: Some(G1),
    batch: Some(doc.clone()),
    open_balance: BalanceForGoods::default(),
    receive: BalanceDelta { qty: 3.into(), cost: 10.into() },
    issue: BalanceDelta { qty: 0.into(), cost: 0.into() },
    close_balance: BalanceForGoods { qty: 3.into(), cost: 10.into() },
  };

  assert_eq!(agr, res.items.1[0]);

  tmp_dir.close().expect("Can't remove tmp dir in test_issue_op_none");
}

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