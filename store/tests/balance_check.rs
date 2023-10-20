use chrono::Utc;
use rust_decimal::Decimal;
use std::str::FromStr;
use store::balance::Cost;
use store::batch::Batch;
use store::elements::{dt, Mode};
use store::operations::{InternalOperation, OpMutation};
use store::qty::Uom::In;
use store::qty::{Number, Qty};
use store::wh_storage::WHStorage;
use tempfile::TempDir;
use uuid::Uuid;

#[test]
fn store_test_balance_check() {
  std::env::set_var("RUST_LOG", "debug,actix_web=debug,actix_server=debug");
  env_logger::init();

  let tmp_dir = TempDir::new().expect("Can't create tmp dir in test_receive_change_op");

  let wh = WHStorage::open(&tmp_dir.path()).unwrap();
  let mut db = wh.database;

  let materials_in_route = Uuid::from_str("9afd9f8c-42cd-44e6-9d8c-2ffebf1ba061").unwrap(); // материалы в пути
  let store = Uuid::from_str("404037f2-3db7-4dae-9884-6a79fd9cd94e").unwrap(); // склад
  let customs_store = Uuid::from_str("4b009d57-2a56-4e3f-8d37-d4d12fc44164").unwrap(); // таможенный склад

  let goods = Uuid::from_str("c74f7aab-bbdd-4832-8bd3-0291470e8964").unwrap(); // Socar

  let uom = Uuid::from_str("30816a3e-1340-482d-b144-f1dd72bd69c9").unwrap(); // kg

  let empty_batch = Batch {
    id: Uuid::from_str("00000000-0000-0000-0000-000000000000").unwrap(),
    date: dt("1970-01-01").unwrap(),
  };

  let op1 = vec![OpMutation {
    id: Uuid::from_str("65cdc181-6b6e-448b-9ccc-a3fc903fb850").unwrap(),
    date: dt("2023-01-17").unwrap(),
    store,
    transfer: Some(customs_store),
    goods,
    batch: empty_batch.clone(),
    before: None,
    after: Some((
      InternalOperation::Issue(
        Qty::new(vec![Number { number: Decimal::try_from(21250).unwrap(), name: In(uom, None) }]),
        Cost::from(Decimal::try_from("0").unwrap()),
        Mode::Auto,
      ),
      false,
    )),
  }];
  db.record_ops(&op1).unwrap();
  db.checkpoint_topologies[0].debug().unwrap();

  let op2 = vec![OpMutation {
    id: Uuid::from_str("a4b45b21-8254-420e-9a1f-883652cd8107").unwrap(),
    date: dt("2023-05-19").unwrap(),
    store: customs_store,
    transfer: Some(store),
    goods,
    batch: empty_batch.clone(),
    before: None,
    after: Some((
      InternalOperation::Issue(
        Qty::new(vec![Number { number: Decimal::try_from(21250).unwrap(), name: In(uom, None) }]),
        Cost::from(Decimal::try_from("0").unwrap()),
        Mode::Auto,
      ),
      false,
    )),
  }];
  db.record_ops(&op2).unwrap();
  db.checkpoint_topologies[0].debug().unwrap();

  let op3 = vec![OpMutation {
    id: Uuid::from_str("eee0d98e-729b-4f5f-80b0-7377c2527e1c").unwrap(),
    date: dt("2023-07-18").unwrap(),
    store: customs_store,
    transfer: Some(store),
    goods,
    batch: empty_batch.clone(),
    before: None,
    after: Some((
      InternalOperation::Issue(
        Qty::new(vec![Number { number: Decimal::try_from(8750).unwrap(), name: In(uom, None) }]),
        Cost::from(Decimal::try_from("0").unwrap()),
        Mode::Auto,
      ),
      false,
    )),
  }];
  db.record_ops(&op3).unwrap();
  db.checkpoint_topologies[0].debug().unwrap();

  let op4 = vec![OpMutation {
    id: Uuid::from_str("a6bd6502-8a91-407c-886b-df27ca02d527").unwrap(),
    date: dt("2023-01-06").unwrap(),
    store: materials_in_route,
    transfer: Some(store),
    goods,
    batch: empty_batch.clone(),
    before: None,
    after: Some((
      InternalOperation::Issue(
        Qty::new(vec![Number { number: Decimal::try_from(21250).unwrap(), name: In(uom, None) }]),
        Cost::from(Decimal::try_from("0").unwrap()),
        Mode::Auto,
      ),
      false,
    )),
  }];
  db.record_ops(&op4).unwrap();
  db.checkpoint_topologies[0].debug().unwrap();

  let op5 = vec![OpMutation {
    id: Uuid::from_str("724271ec-eaf4-43ce-a66b-d20998c8961e").unwrap(),
    date: dt("2023-05-31").unwrap(),
    store: materials_in_route,
    transfer: Some(customs_store),
    goods,
    batch: empty_batch.clone(),
    before: None,
    after: Some((
      InternalOperation::Issue(
        Qty::new(vec![Number { number: Decimal::try_from(21250).unwrap(), name: In(uom, None) }]),
        Cost::from(Decimal::try_from("0").unwrap()),
        Mode::Auto,
      ),
      false,
    )),
  }];
  db.record_ops(&op5).unwrap();
  db.checkpoint_topologies[0].debug().unwrap();

  let op6 = vec![OpMutation {
    id: Uuid::from_str("54f511c8-7b7b-4cf4-95a4-333f2207f4a2").unwrap(),
    date: dt("2022-12-29").unwrap(),
    store: materials_in_route,
    transfer: None,
    goods,
    batch: Batch {
      id: Uuid::from_str("54f511c8-7b7b-4cf4-95a4-333f2207f4a2").unwrap(),
      date: dt("2022-12-29").unwrap(),
    },
    before: None,
    after: Some((
      InternalOperation::Receive(
        Qty::new(vec![Number { number: Decimal::try_from(21250.0).unwrap(), name: In(uom, None) }]),
        Cost::from(Decimal::try_from("290245152.25").unwrap()),
      ),
      false,
    )),
  }];
  db.record_ops(&op6).unwrap();
  db.checkpoint_topologies[0].debug().unwrap();

  let balances = db.get_balance_for_all(Utc::now()).unwrap();
  println!("balances: {balances:#?}");

  // expect:
  // materials_in_route -21250
  // store 21250, 8750
  // customs_store 12500

  // fact:
  // materials_in_route -21250 (zero batch)
  // store 21250 (2022-12-29 batch), 8750 (zero batch)
  // customs_store -8750 (zero batch)

  // 9afd9f8c-42cd-44e6-9d8c-2ffebf1ba061 - материалы в пути
  // 404037f2-3db7-4dae-9884-6a79fd9cd94e - склад
  // 8e50d3b8-2323-47b1-8424-7d30263b1235 - сырьё
  // 4b009d57-2a56-4e3f-8d37-d4d12fc44164 - таможенный склад

  let customs_store = Uuid::try_from("4b009d57-2a56-4e3f-8d37-d4d12fc44164").unwrap();
  let customs_store_balance = balances.get(&customs_store).unwrap();
  assert_eq!(customs_store_balance.len(), 1);

  let goods = Uuid::from_str("c74f7aab-bbdd-4832-8bd3-0291470e8964").unwrap();
  let goods_balance = customs_store_balance.get(&goods).unwrap();
  assert_eq!(goods_balance.len(), 1);

  let empty_batch_balance = goods_balance.get(&Batch::no());
  assert_eq!(empty_batch_balance.is_none(), true);

  tmp_dir.close().unwrap();
}
