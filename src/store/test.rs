    use std::io;

  use crate::{
    animo::{
      db::AnimoDB,
      memory::{Memory, ID},
    },
    api,
    memories::MemoriesInFiles,
    services::{Error, Services},
    settings::{self, Settings},
    storage::SOrganizations,
    store::date_type_store_goods_id::DateTypeStoreGoodsId,
  };

  use super::{store_date_type_goods_id::StoreDateTypeGoodsId, *};
  use crate::warehouse::test_util::init;
  use actix_web::{http::header::ContentType, test, web, App};
  use futures::TryFutureExt;
  use json::object;
  use rocksdb::{ColumnFamilyDescriptor, Options};
  use tempfile::TempDir;
  use uuid::Uuid;

  #[actix_web::test]
  async fn store_test_app_move() {
    let (tmp_dir, settings, db) = init();

    let (mut application, events_receiver) = Application::new(Arc::new(settings), Arc::new(db))
      .await
      .map_err(|e| io::Error::new(io::ErrorKind::Unsupported, e))
      .unwrap();

    let storage = SOrganizations::new(tmp_dir.path().join("companies"));
    application.storage = Some(storage.clone());

    application.register(MemoriesInFiles::new(application.clone(), "docs", storage.clone()));

    let app = test::init_service(
      App::new()
        // .app_data(web::Data::new(db.clone()))
        .app_data(web::Data::new(application.clone()))
        // .wrap(middleware::Logger::default())
        .service(api::docs_create)
        .service(api::docs_update)
        // .service(api::memory_modify)
        // .service(api::memory_query)
        .default_service(web::route().to(api::not_implemented)),
    )
    .await;

    let goods1 = Uuid::from_u128(101);
    let goods2 = Uuid::from_u128(102);
    let storage1 = Uuid::from_u128(201);
    let storage2 = Uuid::from_u128(202);
    let oid = ID::from("99");

    //receive
    let data0: JsonValue = object! {
        _id: "",
        date: "2023-01-18",
        storage: storage1.to_string(),
        goods: [
            {
                goods: goods1.to_string(),
                uom: "",
                qty: 1,
                price: 10,
                cost: 10,
                _tid: ""
            },
            {
                goods: goods2.to_string(),
                uom: "",
                qty: 2,
                price: 8,
                cost: 16,
                _tid: ""
            }
        ]
    };

    let req = test::TestRequest::post()
      .uri(&format!("/api/docs?oid={}&ctx=warehouse,receive", oid.to_base64()))
      .set_payload(data0.dump())
      .insert_header(ContentType::json())
      // .param("oid", oid.to_base64())
      // .param("document", "warehouse")
      // .param("document", "receive")
      .to_request();

    let response = test::call_and_read_body(&app, req).await;

    let result0: serde_json::Value = serde_json::from_slice(&response).unwrap();

    assert_ne!("", result0["goods"][0]["_tid"].as_str().unwrap());
    assert_ne!("", result0["goods"][1]["_tid"].as_str().unwrap());

    // move
    // let id = result0["goods"][0]["_tid"].as_str().unwrap();

    // let data1: JsonValue = object! {
    //     _id: "",
    //     date: "2023-01-18",
    //     storage: storage1.to_string(),
    //     transfer: storage2.to_string(),
    //     goods: [
    //         {
    //             goods: goods1.to_string(),
    //             uom: "",
    //             qty: 2,
    //             price: 10,
    //             cost: 20,
    //             _tid: id,
    //         }
    //     ]
    // };

    // let req = test::TestRequest::post()
    //   .uri(&format!("/api/docs/{id}?oid={}&ctx=warehouse,receive", oid.to_base64()))
    //   .set_payload(data1.dump())
    //   .insert_header(ContentType::json())
    //   .to_request();

    // let response = test::call_and_read_body(&app, req).await;

    // let result: serde_json::Value = serde_json::from_slice(&response).unwrap();

    // let compare: serde_json::Value = serde_json::from_str(&data1.dump()).unwrap();

    // assert_eq!(compare, result);

    // let x = DateTypeStoreGoodsId { db:  };

    // let report = x
    //   .get_report(
    //     dt("2023-01-17").unwrap(),
    //     dt("2023-01-20").unwrap(),
    //     storage1,
    //     &mut application.warehouse.database,
    //   )
    //   .unwrap();

    // println!("REPORT: {report:#?}");
  }

  #[actix_web::test]
  async fn store_test_app_receive_issue_change() {
    let (tmp_dir, settings, db) = init();

    let (mut application, events_receiver) = Application::new(Arc::new(settings), Arc::new(db))
      .await
      .map_err(|e| io::Error::new(io::ErrorKind::Unsupported, e))
      .unwrap();

    let storage = SOrganizations::new(tmp_dir.path().join("companies"));
    application.storage = Some(storage.clone());

    application.register(MemoriesInFiles::new(application.clone(), "docs", storage.clone()));

    let app = test::init_service(
      App::new()
        // .app_data(web::Data::new(db.clone()))
        .app_data(web::Data::new(application.clone()))
        // .wrap(middleware::Logger::default())
        .service(api::docs_create)
        .service(api::docs_update)
        // .service(api::memory_modify)
        // .service(api::memory_query)
        .default_service(web::route().to(api::not_implemented)),
    )
    .await;

    let goods1 = Uuid::from_u128(201);
    let goods2 = Uuid::from_u128(101);
    let storage1 = Uuid::from_u128(202);
    let oid = ID::from("99");

    //receive
    let data0: JsonValue = object! {
        _id: "",
        date: "2023-01-18",
        storage: storage1.to_string(),
        goods: [
            {
                goods: goods1.to_string(),
                uom: "",
                qty: 1,
                price: 10,
                cost: 10,
                _tid: ""
            },
            {
                goods: goods2.to_string(),
                uom: "",
                qty: 2,
                price: 8,
                cost: 16,
                _tid: ""
            }
        ]
    };

    let req = test::TestRequest::post()
      .uri(&format!("/api/docs?oid={}&ctx=warehouse,receive", oid.to_base64()))
      .set_payload(data0.dump())
      .insert_header(ContentType::json())
      // .param("oid", oid.to_base64())
      // .param("document", "warehouse")
      // .param("document", "receive")
      .to_request();

    let response = test::call_and_read_body(&app, req).await;

    let result0: serde_json::Value = serde_json::from_slice(&response).unwrap();

    assert_ne!("", result0["goods"][0]["_tid"].as_str().unwrap());
    assert_ne!("", result0["goods"][1]["_tid"].as_str().unwrap());

    // issue
    let data1: JsonValue = object! {
        _id: "",
        date: "2023-01-18",
        storage: storage1.to_string(),
        goods: [
            {
                goods: goods2.to_string(),
                uom: "",
                qty: 1,
                // price: 0,
                // cost: 0,
                _tid: result0["goods"][1]["_tid"].as_str().unwrap(),
            },
        ]
    };

    let req = test::TestRequest::post()
      .uri(&format!("/api/docs?oid={}&ctx=warehouse,issue", oid.to_base64()))
      .set_payload(data1.dump())
      .insert_header(ContentType::json())
      // .param("oid", oid.to_base64())
      // .param("document", "warehouse")
      // .param("document", "receive")
      .to_request();

    let response = test::call_and_read_body(&app, req).await;

    let result1: serde_json::Value = serde_json::from_slice(&response).unwrap();

    // change
    let id = result0["goods"][0]["_tid"].as_str().unwrap();

    let data2: JsonValue = object! {
        _id: "",
        date: "2023-01-18",
        storage: storage1.to_string(),
        goods: [
            {
                goods: goods1.to_string(),
                uom: "",
                qty: 2,
                price: 10,
                cost: 20,
                _tid: id,
            }
        ]
    };

    let req = test::TestRequest::post()
      .uri(&format!("/api/docs/{id}?oid={}&ctx=warehouse,receive", oid.to_base64()))
      .set_payload(data2.dump())
      .insert_header(ContentType::json())
      .to_request();

    let response = test::call_and_read_body(&app, req).await;

    let result: serde_json::Value = serde_json::from_slice(&response).unwrap();

    let compare: serde_json::Value = serde_json::from_str(&data2.dump()).unwrap();

    assert_eq!(compare, result);

    // let x = DateTypeStoreGoodsId();

    // let report = x
    //   .get_report(
    //     dt("2023-01-17").unwrap(),
    //     dt("2023-01-20").unwrap(),
    //     storage1,
    //     &mut application.warehouse.database,
    //   )
    //   .unwrap();

    // println!("REPORT: {report:#?}");
  }

  #[actix_web::test]
  async fn store_test_receive_ops() {
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
        chrono::Utc::now(),
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
        chrono::Utc::now(),
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
        chrono::Utc::now(),
      ),
    ];

    db.record_ops(&ops).expect("test_receive_ops");

    let balance = Balance {
      date: check_d,
      store: w1,
      goods: G1,
      batch: party,
      number: BalanceForGoods { qty: 2.into(), cost: 2000.into() },
    };

    // for checkpoint_topology in db. ...
    todo!();
    // let b1 = db.find_checkpoint(&ops[0], CHECK_DATE_STORE_BATCH).expect("test_receive_ops");
    // assert_eq!(b1, Some(balance.clone()));

    // let b2 = db.find_checkpoint(&ops[0], CHECK_BATCH_STORE_DATE).expect("test_receive_ops");
    // assert_eq!(b2, Some(balance));

    // let b3 = db.find_checkpoint(&ops[2], CHECK_DATE_STORE_BATCH).expect("test_receive_ops");
    // assert_eq!(b3, None);

    // let b4 = db.find_checkpoint(&ops[2], CHECK_BATCH_STORE_DATE).expect("test_receive_ops");
    // assert_eq!(b4, None);

    tmp_dir.close().expect("Can't close tmp dir in test_receive_ops");
  }

  #[actix_web::test]
  async fn store_test_neg_balance_date_type_store_goods_id() {
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
      chrono::Utc::now(),
    )];

    db.record_ops(&ops).expect("test_get_neg_balance");

    let agr = AgregationStoreGoods {
      store: Some(w1),
      goods: Some(G1),
      batch: Some(party.clone()),
      open_balance: BalanceForGoods::default(),
      receive: BalanceDelta::default(),
      issue: BalanceDelta { qty: 2.into(), cost: 2000.into() },
      close_balance: BalanceForGoods { qty: (-2).into(), cost: (-2000).into() },
    };

    // let st = DateTypeStoreGoodsId();
    // let res = st.get_report(op_d, check_d, w1, &mut db).expect("test_get_neg_balance");

    // assert_eq!(res.items.1[0], agr);

    // tmp_dir.close().expect("Can't close tmp dir in test_get_neg_balance");
  }

  #[actix_web::test]
  async fn store_test_zero_balance_date_type_store_goods_id() {
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
        chrono::Utc::now(),
      ),
    ];

    db.record_ops(&ops);

    // let st = DateTypeStoreGoodsId();
    // let res = st.get_report(start_d, end_d, w1, &mut db).expect("test_get_zero_balance");

    // let agr = AgregationStoreGoods {
    //   store: Some(w1),
    //   goods: Some(G1),
    //   batch: Some(party.clone()),
    //   open_balance: BalanceForGoods::default(),
    //   receive: BalanceDelta { qty: 3.into(), cost: 3000.into() },
    //   issue: BalanceDelta { qty: 3.into(), cost: 3000.into() },
    //   close_balance: BalanceForGoods::default(),
    // };

    // assert_eq!(res.items.1[0], agr);

    tmp_dir.close().expect("Can't close tmp dir in test_get_zero_balance");
  }

//   #[actix_web::test]
//   async fn store_test_get_wh_ops_date_type_store_goods_id() {
//     get_wh_ops(DateTypeStoreGoodsId()).expect("test_get_wh_ops_date_type_store_goods_id");
//   }

//   #[actix_web::test]
//   async fn store_test_get_wh_ops_store_date_type_goods_id() {
//     get_wh_ops(StoreDateTypeGoodsId()).expect("test_get_wh_ops_store_date_type_goods_id");
//   }

  fn get_wh_ops(key: impl StoreTopology) -> Result<(), WHError> {
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
        chrono::Utc::now(),
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
        chrono::Utc::now(),
      ),
    ];

    db.record_ops(&ops).unwrap();

    let res = key.get_ops(start_d, end_d, w1, &db)?;

    for i in 0..ops.len() {
      assert_eq!(ops[i].to_op(), res[i]);
    }

    Ok(())
  }

//   #[actix_web::test]
//   async fn store_test_get_agregations_without_checkpoints_date_type_store_goods_id() {
//     get_agregations_without_checkpoints(DateTypeStoreGoodsId()).expect("test_get_agregations");
//   }

//   #[actix_web::test]
//   async fn store_test_get_agregations_without_checkpoints_store_date_type_goods_id() {
//     get_agregations_without_checkpoints(StoreDateTypeGoodsId()).expect("test_get_agregations");
//   }

  fn get_agregations_without_checkpoints(key: impl StoreTopology) -> Result<(), WHError> {
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
        chrono::Utc::now(),
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
        chrono::Utc::now(),
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
        chrono::Utc::now(),
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
        chrono::Utc::now(),
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
        issue: BalanceDelta { qty: 1.into(), cost: 1000.into() },
        close_balance: BalanceForGoods { qty: 2.into(), cost: 2000.into() },
      },
      AgregationStoreGoods {
        store: Some(w1),
        goods: Some(G2),
        batch: Some(doc2.clone()),
        open_balance: BalanceForGoods::default(),
        receive: BalanceDelta { qty: 2.into(), cost: 2000.into() },
        issue: BalanceDelta { qty: 2.into(), cost: 2000.into() },
        close_balance: BalanceForGoods::default(),
      },
    ];

    let res = key.get_report(op_d, check_d, w1, &mut db)?;
    let mut iter = res.items.1.into_iter();

    // println!("MANY BALANCES: {:#?}", res);

    for agr in agregations {
      assert_eq!(iter.next().expect("option in get_agregations"), agr);
    }
    assert_eq!(iter.next(), None);

    tmp_dir.close().expect("Can't close tmp dir in store_test_get_wh_balance");

    Ok(())
  }

  #[actix_web::test]
  async fn store_test_op_iter() {
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
        chrono::Utc::now(),
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
        chrono::Utc::now(),
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
        chrono::Utc::now(),
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
        chrono::Utc::now(),
      ),
    ];

    for op in &ops {
      db.put(&op.store_date_type_batch_id(), &op.value().expect("test_op_iter"))
        .expect("Can't put op in db in test_op_iter");
    }

    let iter = db.db.iterator(IteratorMode::Start);

    let mut res = Vec::new();

    for item in iter {
      let (_, v) = item.unwrap();
      let op = serde_json::from_slice(&v).unwrap();
      res.push(op);
    }

    for i in 0..ops.len() {
      assert_eq!(ops[i], res[i]);
    }

    tmp_dir.close().expect("Can't remove tmp dir in test_op_iter");
  }

  #[actix_web::test]
  async fn store_test_report() {
    let tmp_dir = TempDir::new().expect("Can't create tmp dir in test_report");

    // let key1 = DateTypeStoreGoodsId();
    // let key2 = StoreDateTypeGoodsId();

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
        chrono::Utc::now(),
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
        chrono::Utc::now(),
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
        chrono::Utc::now(),
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
        chrono::Utc::now(),
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
        chrono::Utc::now(),
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
        chrono::Utc::now(),
      ),
    ];

    db.record_ops(&ops).expect("test_report");

    let agr_store = AgregationStore {
      store: Some(w1),
      open_balance: 10000.into(),
      receive: 2000.into(),
      issue: 3000.into(),
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
        issue: BalanceDelta { qty: 1.into(), cost: 3000.into() },
        close_balance: BalanceForGoods { qty: 1.into(), cost: 3000.into() },
      },
    ];

    let mut ex_map: HashMap<Goods, AgregationStoreGoods> = HashMap::new();

    for agr in ex_items.clone() {
      ex_map.insert(agr.goods.unwrap(), agr);
    }

    // let report1 = key1.get_report(start_d, end_d, w1, &mut db).expect("test_report");
    // let report2 = key2.get_report(start_d, end_d, w1, &mut db).expect("test_report");

    // println!("ITEMS: {:#?}", report1.items);

    // assert_eq!(report1, report2);

    // assert_eq!(report1.items.0, agr_store);
    // assert_eq!(report1.items.1, ex_items);

    // for item in report2.items.1 {
    //   assert_eq!(&item, ex_map.get(&item.goods.unwrap()).unwrap());
    // }

    tmp_dir.close().expect("Can't remove tmp dir in test_report");
  }

  #[actix_web::test]
  async fn store_test_parties_date_type_store_goods_id() {
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
        chrono::Utc::now(),
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
        chrono::Utc::now(),
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
        chrono::Utc::now(),
      ),
    ];

    db.record_ops(&ops).expect("test_parties");

    // let st = DateTypeStoreGoodsId();
    // let res = st.get_report(start_d, end_d, w1, &mut db).expect("test_parties");

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
        issue: BalanceDelta { qty: 1.into(), cost: 500.into() },
        close_balance: BalanceForGoods { qty: 3.into(), cost: 1500.into() },
      },
    ];

    // assert_eq!(res.items.1[0], agrs[0]);
    // assert_eq!(res.items.1[1], agrs[1]);

    tmp_dir.close().expect("Can't close tmp dir in test_parties");
  }

  #[actix_web::test]
  async fn store_test_issue_cost_none() {
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
        chrono::Utc::now(),
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
        chrono::Utc::now(),
      ),
    ];

    db.record_ops(&ops).expect("test_issue_cost_none");

    // let st = DateTypeStoreGoodsId();
    // let res = st.get_report(start_d, end_d, w1, &mut db).expect("test_issue_cost_none");

    let agr = AgregationStoreGoods {
      store: Some(w1),
      goods: Some(G1),
      batch: Some(doc.clone()),
      open_balance: BalanceForGoods::default(),
      receive: BalanceDelta { qty: 4.into(), cost: 2000.into() },
      issue: BalanceDelta { qty: 1.into(), cost: 500.into() },
      close_balance: BalanceForGoods { qty: 3.into(), cost: 1500.into() },
    };

    // assert_eq!(agr, res.items.1[0]);

    tmp_dir.close().expect("Can't remove tmp dir in test_issue_cost_none");
  }

  #[actix_web::test]
  async fn store_test_receive_cost_none() {
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
        chrono::Utc::now(),
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
        chrono::Utc::now(),
      ),
    ];

    db.record_ops(&ops).expect("test_receive_cost_none");

    // let st = DateTypeStoreGoodsId();
    // let res = st.get_report(start_d, end_d, w1, &mut db).expect("test_receive_cost_none");

    let agr = AgregationStoreGoods {
      store: Some(w1),
      goods: Some(G1),
      batch: Some(doc.clone()),
      open_balance: BalanceForGoods::default(),
      receive: BalanceDelta { qty: 5.into(), cost: 2000.into() },
      issue: BalanceDelta { qty: 0.into(), cost: 0.into() },
      close_balance: BalanceForGoods { qty: 5.into(), cost: 2000.into() },
    };

    // assert_eq!(agr, res.items.1[0]);

    tmp_dir.close().expect("Can't remove tmp dir in test_receive_cost_none");
  }

  #[actix_web::test]
  async fn store_test_issue_remainder() {
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
        chrono::Utc::now(),
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
        chrono::Utc::now(),
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
        chrono::Utc::now(),
      ),
    ];

    db.record_ops(&ops).expect("test_issue_remainder");

    // let st = DateTypeStoreGoodsId();
    // let res = st.get_report(start_d, end_d, w1, &mut db).expect("test_issue_remainder");

    // println!("HELLO: {:#?}", res.items.1);

    let agr = AgregationStoreGoods {
      store: Some(w1),
      goods: Some(G1),
      batch: Some(doc.clone()),
      open_balance: BalanceForGoods::default(),
      receive: BalanceDelta { qty: 3.into(), cost: 10.into() },
      issue: BalanceDelta { qty: 3.into(), cost: 10.into() },
      close_balance: BalanceForGoods { qty: 0.into(), cost: 0.into() },
    };

    // assert_eq!(agr, res.items.1[0]);

    tmp_dir.close().expect("Can't remove tmp dir in test_issue_remainder");
  }

  #[actix_web::test]
  async fn store_test_issue_op_none() {
    let tmp_dir = TempDir::new().expect("Can't create tmp dir in test_issue_op_none");

    let wh = WHStorage::open(&tmp_dir.path()).unwrap();
    let mut db = wh.database;

    let start_d = dt("2022-10-10").expect("test_issue_op_none");
    let end_d = dt("2022-10-11").expect("test_issue_op_none");
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
        chrono::Utc::now(),
      ),
      // КОРРЕКТНАЯ ОПЕРАЦИЯ С ДВУМЯ NONE?
      OpMutation::new(id3, start_d, w1, None, G1, doc.clone(), None, None, chrono::Utc::now()),
    ];

    db.record_ops(&ops).expect("test_issue_op_none");

    // let st = DateTypeStoreGoodsId();
    // let res = st.get_report(start_d, end_d, w1, &mut db).expect("test_issue_op_none");

    let agr = AgregationStoreGoods {
      store: Some(w1),
      goods: Some(G1),
      batch: Some(doc.clone()),
      open_balance: BalanceForGoods::default(),
      receive: BalanceDelta { qty: 3.into(), cost: 10.into() },
      issue: BalanceDelta { qty: 3.into(), cost: 10.into() },
      close_balance: BalanceForGoods { qty: 0.into(), cost: 0.into() },
    };

    // assert_eq!(agr, res.items.1[0]);

    tmp_dir.close().expect("Can't remove tmp dir in test_issue_op_none");
  }

  #[actix_web::test]
  async fn store_test_receive_change_op() {
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
        chrono::Utc::now(),
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
        chrono::Utc::now(),
      ),
    ];

    db.record_ops(&ops_old).expect("test_receive_change_op");

    let old_check = Balance {
      date: dt("2022-10-01").expect("test_receive_change_op"),
      store: w1,
      goods: G1,
      batch: doc.clone(),
      number: BalanceForGoods { qty: 4.into(), cost: 40.into() },
    };

    let mut old_checkpoints = db
      .get_checkpoints_before_date(start_d, w1)
      .expect("test_receive_change_op")
      .into_iter();

    assert_eq!(Some(old_check), old_checkpoints.next());

    let ops_new = vec![OpMutation::new(
      id1,
      dt("2022-08-25").expect("test_receive_change_op"),
      w1,
      None,
      G1,
      doc.clone(),
      Some(InternalOperation::Receive(3.into(), 10.into())),
      Some(InternalOperation::Receive(4.into(), 100.into())),
      chrono::Utc::now(),
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
      .get_checkpoints_before_date(start_d, w1)
      .expect("test_receive_change_op")
      .into_iter();

    assert_eq!(Some(new_check), new_checkpoints.next());

    // let st = DateTypeStoreGoodsId();
    // let res = st.get_report(start_d, end_d, w1, &mut db).expect("test_receive_change_op");

    let agr = AgregationStoreGoods {
      store: Some(w1),
      goods: Some(G1),
      batch: Some(doc.clone()),
      open_balance: BalanceForGoods { qty: 5.into(), cost: 130.into() },
      receive: BalanceDelta::default(),
      issue: BalanceDelta { qty: 0.into(), cost: 0.into() },
      close_balance: BalanceForGoods { qty: 5.into(), cost: 130.into() },
    };

    // assert_eq!(res.items.1[0], agr);

    tmp_dir.close().expect("Can't remove tmp dir in test_receive_change_op");
  }
