use chrono::Utc;
use rust_decimal::Decimal;
use std::str::FromStr;
use store::balance::{BalanceForGoods, Cost};
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
  let tmp_dir = TempDir::new().expect("Can't create tmp dir in test_receive_change_op");

  let wh = WHStorage::open(&tmp_dir.path()).unwrap();
  let mut db = wh.database;

  let materials_in_route = Uuid::from_str("9afd9f8c-42cd-44e6-9d8c-2ffebf1ba061").unwrap(); // материалы в пути
  let raw_materials = Uuid::from_str("8e50d3b8-2323-47b1-8424-7d30263b1235").unwrap(); // сырьё
  let store = Uuid::from_str("404037f2-3db7-4dae-9884-6a79fd9cd94e").unwrap(); // склад
  let customs_store = Uuid::from_str("4b009d57-2a56-4e3f-8d37-d4d12fc44164").unwrap(); // таможенный склад

  let goods = Uuid::from_str("c74f7aab-bbdd-4832-8bd3-0291470e8964").unwrap(); // Socar

  let uom = Uuid::from_str("30816a3e-1340-482d-b144-f1dd72bd69c9").unwrap(); // kg

  let empty_batch = Batch {
    id: Uuid::from_str("00000000-0000-0000-0000-000000000000").unwrap(),
    date: dt("1970-01-01").unwrap(),
  };

  let ops_old = vec![
    OpMutation {
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
    },
    // OpMutation {
    //   id: Uuid::from_str("c58b5475-66a6-4c40-8484-937bf904b3c7").unwrap(),
    //   date: dt("2023-02-04").unwrap(),
    //   store,
    //   transfer: Some(raw_materials),
    //   goods,
    //   batch: empty_batch.clone(),
    //   before: None,
    //   after: Some((
    //     InternalOperation::Issue(
    //       Qty::new(vec![Number {
    //         number: Decimal::try_from(1250).unwrap(),
    //         name: In(uom, None),
    //       }]),
    //       Cost::from(Decimal::try_from("0").unwrap()),
    //       Mode::Auto,
    //     ),
    //     false,
    //   )),
    // },
    // OpMutation {
    //   id: Uuid::from_str("de7d3129-0550-47ff-ad18-032a31459121").unwrap(),
    //   date: dt("2023-02-06").unwrap(),
    //   store,
    //   transfer: Some(raw_materials),
    //   goods,
    //   batch: empty_batch.clone(),
    //   before: None,
    //   after: Some((
    //     InternalOperation::Issue(
    //       Qty::new(vec![Number {
    //         number: Decimal::try_from(1250).unwrap(),
    //         name: In(uom, None),
    //       }]),
    //       Cost::from(Decimal::try_from("0").unwrap()),
    //       Mode::Auto,
    //     ),
    //     false,
    //   )),
    // },
    // OpMutation {
    //   id: Uuid::from_str("9fa0161e-6549-4a08-89a5-3cb67c045d15").unwrap(),
    //   date: dt("2023-02-08").unwrap(),
    //   store,
    //   transfer: Some(raw_materials),
    //   goods,
    //   batch: empty_batch.clone(),
    //   before: None,
    //   after: Some((
    //     InternalOperation::Issue(
    //       Qty::new(vec![Number {
    //         number: Decimal::try_from(1250).unwrap(),
    //         name: In(uom, None),
    //       }]),
    //       Cost::from(Decimal::try_from("0").unwrap()),
    //       Mode::Auto,
    //     ),
    //     false,
    //   )),
    // },
    // OpMutation {
    //   id: Uuid::from_str("e6a34624-d62e-45b6-a81a-88b258bb30fd").unwrap(),
    //   date: dt("2023-02-11").unwrap(),
    //   store,
    //   transfer: Some(raw_materials),
    //   goods,
    //   batch: empty_batch.clone(),
    //   before: None,
    //   after: Some((
    //     InternalOperation::Issue(
    //       Qty::new(vec![Number {
    //         number: Decimal::try_from(1250).unwrap(),
    //         name: In(uom, None),
    //       }]),
    //       Cost::from(Decimal::try_from("0").unwrap()),
    //       Mode::Auto,
    //     ),
    //     false,
    //   )),
    // },
    // OpMutation {
    //   id: Uuid::from_str("6fd99043-57cc-4eb6-8d9f-d3bdcfcd6d19").unwrap(),
    //   date: dt("2023-02-14").unwrap(),
    //   store,
    //   transfer: Some(raw_materials),
    //   goods,
    //   batch: empty_batch.clone(),
    //   before: None,
    //   after: Some((
    //     InternalOperation::Issue(
    //       Qty::new(vec![Number {
    //         number: Decimal::try_from(2500).unwrap(),
    //         name: In(uom, None),
    //       }]),
    //       Cost::from(Decimal::try_from("0").unwrap()),
    //       Mode::Auto,
    //     ),
    //     false,
    //   )),
    // },
    // OpMutation {
    //   id: Uuid::from_str("3f647f04-abbb-4cfc-a053-71d488b69b58").unwrap(),
    //   date: dt("2023-02-15").unwrap(),
    //   store,
    //   transfer: Some(raw_materials),
    //   goods,
    //   batch: empty_batch.clone(),
    //   before: None,
    //   after: Some((
    //     InternalOperation::Issue(
    //       Qty::new(vec![Number {
    //         number: Decimal::try_from(1250).unwrap(),
    //         name: In(uom, None),
    //       }]),
    //       Cost::from(Decimal::try_from("0").unwrap()),
    //       Mode::Auto,
    //     ),
    //     false,
    //   )),
    // },
    // OpMutation {
    //   id: Uuid::from_str("0597af24-f581-4637-8f87-7d11eaf31bc9").unwrap(),
    //   date: dt("2023-03-10").unwrap(),
    //   store,
    //   transfer: Some(raw_materials),
    //   goods,
    //   batch: empty_batch.clone(),
    //   before: None,
    //   after: Some((
    //     InternalOperation::Issue(
    //       Qty::new(vec![Number {
    //         number: Decimal::try_from(1250).unwrap(),
    //         name: In(uom, None),
    //       }]),
    //       Cost::from(Decimal::try_from("0").unwrap()),
    //       Mode::Auto,
    //     ),
    //     false,
    //   )),
    // },
    // OpMutation {
    //   id: Uuid::from_str("c9285ca0-bb7c-4653-bc4b-444f87b90a45").unwrap(),
    //   date: dt("2023-03-11").unwrap(),
    //   store,
    //   transfer: Some(raw_materials),
    //   goods,
    //   batch: empty_batch.clone(),
    //   before: None,
    //   after: Some((
    //     InternalOperation::Issue(
    //       Qty::new(vec![Number {
    //         number: Decimal::try_from(2500).unwrap(),
    //         name: In(uom, None),
    //       }]),
    //       Cost::from(Decimal::try_from("0").unwrap()),
    //       Mode::Auto,
    //     ),
    //     false,
    //   )),
    // },
    // OpMutation {
    //   id: Uuid::from_str("a8891abe-18d7-4579-b334-a98f33e05e70").unwrap(),
    //   date: dt("2023-03-13").unwrap(),
    //   store,
    //   transfer: Some(raw_materials),
    //   goods,
    //   batch: empty_batch.clone(),
    //   before: None,
    //   after: Some((
    //     InternalOperation::Issue(
    //       Qty::new(vec![Number {
    //         number: Decimal::try_from(1250).unwrap(),
    //         name: In(uom, None),
    //       }]),
    //       Cost::from(Decimal::try_from("0").unwrap()),
    //       Mode::Auto,
    //     ),
    //     false,
    //   )),
    // },
    // OpMutation {
    //   id: Uuid::from_str("a4ea8deb-9762-419f-873d-1b6726f82e33").unwrap(),
    //   date: dt("2023-05-03").unwrap(),
    //   store,
    //   transfer: Some(raw_materials),
    //   goods,
    //   batch: empty_batch.clone(),
    //   before: None,
    //   after: Some((
    //     InternalOperation::Issue(
    //       Qty::new(vec![Number {
    //         number: Decimal::try_from(1250).unwrap(),
    //         name: In(uom, None),
    //       }]),
    //       Cost::from(Decimal::try_from("0").unwrap()),
    //       Mode::Auto,
    //     ),
    //     false,
    //   )),
    // },
    // OpMutation {
    //   id: Uuid::from_str("118b47aa-1fd6-48ad-a867-e3ac93b68f06").unwrap(),
    //   date: dt("2023-05-06").unwrap(),
    //   store,
    //   transfer: Some(raw_materials),
    //   goods,
    //   batch: empty_batch.clone(),
    //   before: None,
    //   after: Some((
    //     InternalOperation::Issue(
    //       Qty::new(vec![Number {
    //         number: Decimal::try_from(2500).unwrap(),
    //         name: In(uom, None),
    //       }]),
    //       Cost::from(Decimal::try_from("0").unwrap()),
    //       Mode::Auto,
    //     ),
    //     false,
    //   )),
    // },
    // OpMutation {
    //   id: Uuid::from_str("abf23ce3-62be-4ba1-a7bf-0dcc1046838d").unwrap(),
    //   date: dt("2023-05-18").unwrap(),
    //   store,
    //   transfer: Some(raw_materials),
    //   goods,
    //   batch: empty_batch.clone(),
    //   before: None,
    //   after: Some((
    //     InternalOperation::Issue(
    //       Qty::new(vec![Number {
    //         number: Decimal::try_from(1250).unwrap(),
    //         name: In(uom, None),
    //       }]),
    //       Cost::from(Decimal::try_from("0").unwrap()),
    //       Mode::Auto,
    //     ),
    //     false,
    //   )),
    // },
    // OpMutation {
    //   id: Uuid::from_str("818c1f14-a7c1-40ef-af34-5e14dc0dee77").unwrap(),
    //   date: dt("2023-05-19").unwrap(),
    //   store,
    //   transfer: Some(raw_materials),
    //   goods,
    //   batch: empty_batch.clone(),
    //   before: None,
    //   after: Some((
    //     InternalOperation::Issue(
    //       Qty::new(vec![Number {
    //         number: Decimal::try_from(2500).unwrap(),
    //         name: In(uom, None),
    //       }]),
    //       Cost::from(Decimal::try_from("0").unwrap()),
    //       Mode::Auto,
    //     ),
    //     false,
    //   )),
    // },
    // OpMutation {
    //   id: Uuid::from_str("9112f1a1-122a-42b5-aafb-3bcb5deaaad0").unwrap(),
    //   date: dt("2023-07-18").unwrap(),
    //   store,
    //   transfer: Some(raw_materials),
    //   goods,
    //   batch: empty_batch.clone(),
    //   before: None,
    //   after: Some((
    //     InternalOperation::Issue(
    //       Qty::new(vec![Number {
    //         number: Decimal::try_from(5000).unwrap(),
    //         name: In(uom, None),
    //       }]),
    //       Cost::from(Decimal::try_from("0").unwrap()),
    //       Mode::Auto,
    //     ),
    //     false,
    //   )),
    // },
    // OpMutation {
    //   id: Uuid::from_str("557b7272-40e0-4d87-be2b-86c06a950769").unwrap(),
    //   date: dt("2023-07-21").unwrap(),
    //   store,
    //   transfer: Some(raw_materials),
    //   goods,
    //   batch: empty_batch.clone(),
    //   before: None,
    //   after: Some((
    //     InternalOperation::Issue(
    //       Qty::new(vec![Number {
    //         number: Decimal::try_from(3750).unwrap(),
    //         name: In(uom, None),
    //       }]),
    //       Cost::from(Decimal::try_from("0").unwrap()),
    //       Mode::Auto,
    //     ),
    //     false,
    //   )),
    // },
    // OpMutation {
    //   id: Uuid::from_str("02f87508-3938-43b2-86e8-8a4901c20944").unwrap(),
    //   date: dt("2023-02-01").unwrap(),
    //   store: customs_store,
    //   transfer: Some(store),
    //   goods,
    //   batch: empty_batch.clone(),
    //   before: None,
    //   after: Some((
    //     InternalOperation::Issue(
    //       Qty::new(vec![Number { number: Decimal::try_from(5000).unwrap(), name: In(uom, None) }]),
    //       Cost::from(Decimal::try_from("0").unwrap()),
    //       Mode::Auto,
    //     ),
    //     false,
    //   )),
    // },
    // OpMutation {
    //   id: Uuid::from_str("0be07ab4-a287-4f11-9b75-aed36a6496f6").unwrap(),
    //   date: dt("2023-02-14").unwrap(),
    //   store: customs_store,
    //   transfer: Some(store),
    //   goods,
    //   batch: empty_batch.clone(),
    //   before: None,
    //   after: Some((
    //     InternalOperation::Issue(
    //       Qty::new(vec![Number { number: Decimal::try_from(5000).unwrap(), name: In(uom, None) }]),
    //       Cost::from(Decimal::try_from("0").unwrap()),
    //       Mode::Auto,
    //     ),
    //     false,
    //   )),
    // },
    // OpMutation {
    //   id: Uuid::from_str("5f940ab5-b0ce-478d-8d31-699a8670e88a").unwrap(),
    //   date: dt("2023-02-17").unwrap(),
    //   store: customs_store,
    //   transfer: Some(store),
    //   goods,
    //   batch: empty_batch.clone(),
    //   before: None,
    //   after: Some((
    //     InternalOperation::Issue(
    //       Qty::new(vec![Number { number: Decimal::try_from(5000).unwrap(), name: In(uom, None) }]),
    //       Cost::from(Decimal::try_from("0").unwrap()),
    //       Mode::Auto,
    //     ),
    //     false,
    //   )),
    // },
    // OpMutation {
    //   id: Uuid::from_str("913f25ce-5e89-429e-a3a7-b3842e180274").unwrap(),
    //   date: dt("2023-05-02").unwrap(),
    //   store: customs_store,
    //   transfer: Some(store),
    //   goods,
    //   batch: empty_batch.clone(),
    //   before: None,
    //   after: Some((
    //     InternalOperation::Issue(
    //       Qty::new(vec![Number { number: Decimal::try_from(3750).unwrap(), name: In(uom, None) }]),
    //       Cost::from(Decimal::try_from("0").unwrap()),
    //       Mode::Auto,
    //     ),
    //     false,
    //   )),
    // },
    // OpMutation {
    //   id: Uuid::from_str("a4b45b21-8254-420e-9a1f-883652cd8107").unwrap(),
    //   date: dt("2023-05-19").unwrap(),
    //   store: customs_store,
    //   transfer: Some(store),
    //   goods,
    //   batch: empty_batch.clone(),
    //   before: None,
    //   after: Some((
    //     InternalOperation::Issue(
    //       Qty::new(vec![Number { number: Decimal::try_from(2500).unwrap(), name: In(uom, None) }]),
    //       Cost::from(Decimal::try_from("0").unwrap()),
    //       Mode::Auto,
    //     ),
    //     false,
    //   )),
    // },

    // ********* substitution op (sum of several ops above) *********
    OpMutation {
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
    },
    // ********* ^ substitution op is here ^ *********
    OpMutation {
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
    },
    OpMutation {
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
    },
    OpMutation {
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
    },
    OpMutation {
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
          Qty::new(vec![Number {
            number: Decimal::try_from(21250.0).unwrap(),
            name: In(uom, None),
          }]),
          Cost::from(Decimal::try_from("290245152.25").unwrap()),
        ),
        false,
      )),
    },
  ];

  db.record_ops(&ops_old).unwrap();

  let balances = db.get_balance_for_all(Utc::now()).unwrap();
  println!("balances: {balances:#?}");

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
