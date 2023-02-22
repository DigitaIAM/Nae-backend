use std::{io, thread, time::Duration};

use crate::{
  animo::{
    db::AnimoDB,
    memory::{Memory, ID},
  },
  api,
  memories::MemoriesInFiles,
  services::Services,
  settings::{self, Settings},
  storage::SOrganizations,
  commutator::Application,
};

use utils::time::time_to_string;
use utils::json::JsonParams;
use std::sync::Arc;
use actix_web::{http::header::ContentType, test, web, App};
use futures::TryFutureExt;
use json::{object, JsonValue};
use rocksdb::{ColumnFamilyDescriptor, Options, IteratorMode};
use serde_json::json;
use tempfile::TempDir;
use uuid::Uuid;

use crate::warehouse::test_util::init;
use store::
{elements::{Mode, Batch, dt, OpMutation, InternalOperation, Balance, AgregationStoreGoods, AgregationStore},
 wh_storage::WHStorage,
 error::WHError,
 check_date_store_batch::CheckDateStoreBatch,
 balance::{BalanceForGoods, BalanceDelta},
 date_type_store_batch_id::DateTypeStoreBatchId,
 store_date_type_batch_id::StoreDateTypeBatchId};


const G1: Uuid = Uuid::from_u128(1);
const G2: Uuid = Uuid::from_u128(2);
const G3: Uuid = Uuid::from_u128(3);

#[actix_web::test]
  async fn app_store_test_incomplete_data() {
    let (tmp_dir, settings, db) = init();

    let (mut application, events_receiver) = Application::new(Arc::new(settings), Arc::new(db))
        .await
        .map_err(|e| io::Error::new(io::ErrorKind::Unsupported, e))
        .unwrap();

    let storage = SOrganizations::new(tmp_dir.path().join("companies"));
    application.storage = Some(storage.clone());

    application.register(MemoriesInFiles::new(application.clone(), "docs", storage.clone()));
    application.register(crate::inventory::service::Inventory::new(application.clone()));

    let app = test::init_service(
      App::new()
          .app_data(web::Data::new(application.clone()))
          .service(api::docs_create)
          .service(api::docs_update)
          .service(api::inventory_find)
          .default_service(web::route().to(api::not_implemented)),
    )
        .await;

    let goods = Uuid::from_u128(101);
    let storage = Uuid::from_u128(201);
    let oid = ID::from("99");

    let data: JsonValue = object! {
      _id: "",
      date: "2022-11-15",
      storage: storage.to_string(),
  };

    let req = test::TestRequest::post()
        .uri(&format!("/api/docs?oid={}&ctx=warehouse,receive", oid.to_base64()))
        .set_payload(data.dump())
        .insert_header(ContentType::json())
        .to_request();

    let response = test::call_and_read_body(&app, req).await;

    let result: serde_json::Value = serde_json::from_slice(&response).unwrap();

    let compare_data: JsonValue = object! {
      _id: result["_id"].as_str().unwrap(),
      date: "2022-11-15",
      storage: storage.to_string(),
  };

    // println!("RESULT: {result:#?}");

    assert_eq!(compare_data.dump(), result.to_string());
  }

  #[actix_web::test]
  async fn app_store_test_checkpoints() {
    let (tmp_dir, settings, db) = init();

    let (mut application, events_receiver) = Application::new(Arc::new(settings), Arc::new(db))
        .await
        .map_err(|e| io::Error::new(io::ErrorKind::Unsupported, e))
        .unwrap();

    let storage = SOrganizations::new(tmp_dir.path().join("companies"));
    application.storage = Some(storage.clone());

    application.register(MemoriesInFiles::new(application.clone(), "docs", storage.clone()));
    application.register(crate::inventory::service::Inventory::new(application.clone()));

    let app = test::init_service(
      App::new()
          // .app_data(web::Data::new(db.clone()))
          .app_data(web::Data::new(application.clone()))
          // .wrap(middleware::Logger::default())
          .service(api::docs_create)
          .service(api::docs_update)
          .service(api::inventory_find)
          .default_service(web::route().to(api::not_implemented)),
    )
        .await;

    let goods1 = Uuid::from_u128(101);
    let goods2 = Uuid::from_u128(102);
    let storage1 = Uuid::from_u128(201);
    let oid = ID::from("99");

    //receive0
    let data0: JsonValue = object! {
      _id: "",
      date: "2022-11-15",
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

    // println!("RESULT_0: {result0:#?}");

    assert_ne!("", result0["goods"][0]["_tid"].as_str().unwrap());

    //receive1
    let data1: JsonValue = object! {
      _id: "",
      date: "2022-12-18",
      storage: storage1.to_string(),
      goods: [
          {
              goods: goods2.to_string(),
              uom: "",
              qty: 2,
              price: 7,
              cost: 14,
              _tid: ""
          },
      ]
    };

    let req = test::TestRequest::post()
        .uri(&format!("/api/docs?oid={}&ctx=warehouse,receive", oid.to_base64()))
        .set_payload(data1.dump())
        .insert_header(ContentType::json())
        // .param("oid", oid.to_base64())
        // .param("document", "warehouse")
        // .param("document", "receive")
        .to_request();

    let response = test::call_and_read_body(&app, req).await;

    let result1: serde_json::Value = serde_json::from_slice(&response).unwrap();

    assert_ne!("", result0["goods"][0]["_tid"].as_str().unwrap());

    //report
    let from_date = "2023-01-05";
    let till_date = "2023-01-06";

    let req = test::TestRequest::get()
        .uri(&format!(
          "/api/inventory?oid={}&ctx=report&storage={}&from_date={}&till_date={}",
          oid.to_base64(),
          storage1.to_string(),
          from_date,
          till_date,
        ))
        .to_request();

    let response = test::call_and_read_body(&app, req).await;
    // println!("RESPONSE: {response:#?}");

    let result: serde_json::Value = serde_json::from_slice(&response).unwrap();
    // println!("REPORT: {:#?}", result["data"][0]["items"]);

    let example = json!([
     {
        "store": "00000000-0000-0000-0000-0000000000c9",
        "open_balance": "24",
        "receive": "0",
        "issue": "0",
        "close_balance": "24",
    },
    [
       {
         "store": "00000000-0000-0000-0000-0000000000c9",
         "goods": "00000000-0000-0000-0000-000000000065",
          "batch": {
              "date": "2022-11-15T00:00:00.000Z",
              "id": result["data"][0]["items"][1][0]["batch"]["id"].as_str().unwrap(),
          },
          "open_balance": {
              "cost": "10",
              "qty": "1",
          },
          "receive": {
              "cost": "0",
              "qty": "0",
          },
          "issue": {
              "cost": "0",
              "qty": "0",
          },
          "close_balance": {
              "cost": "10",
              "qty": "1",
          },
      },
       {
         "store": "00000000-0000-0000-0000-0000000000c9",
         "goods": "00000000-0000-0000-0000-000000000066",
          "batch": {
              "date": "2022-12-18T00:00:00.000Z",
              "id": result["data"][0]["items"][1][1]["batch"]["id"].as_str().unwrap(),
          },
          "open_balance": {
              "cost": "14",
              "qty": "2",
          },
          "receive": {
              "cost": "0",
              "qty": "0",
          },
          "issue": {
            "cost": "0",
            "qty": "0",
          },
          "close_balance": {
              "cost": "14",
              "qty": "2",
          },
      },
    ],
  ]);

    assert_eq!(result["data"][0]["items"], example);
  }

  #[actix_web::test]
  async fn app_store_test_move_checkpoints() {
    let (tmp_dir, settings, db) = init();

    let (mut application, events_receiver) = Application::new(Arc::new(settings), Arc::new(db))
        .await
        .map_err(|e| io::Error::new(io::ErrorKind::Unsupported, e))
        .unwrap();

    let storage = SOrganizations::new(tmp_dir.path().join("companies"));
    application.storage = Some(storage.clone());

    application.register(MemoriesInFiles::new(application.clone(), "docs", storage.clone()));
    application.register(crate::inventory::service::Inventory::new(application.clone()));

    let app = test::init_service(
      App::new()
          .app_data(web::Data::new(application.clone()))
          .service(api::docs_create)
          .service(api::docs_update)
          .service(api::inventory_find)
          .default_service(web::route().to(api::not_implemented)),
    )
        .await;

    let goods1 = Uuid::from_u128(101);
    let goods2 = Uuid::from_u128(102);
    let storage1 = Uuid::from_u128(201);
    let storage2 = Uuid::from_u128(202);
    // println!("STORAGE1 = {storage1:?}, STORAGE2 = {storage2:?}");
    let oid = ID::from("99");

    //receive0
    let data0: JsonValue = object! {
      _id: "",
      date: "2022-11-15",
      storage: storage1.to_string(),
      goods: [
          {
              goods: goods1.to_string(),
              uom: "",
              qty: 2,
              price: 15,
              cost: 30,
              _tid: ""
          },
      ]
  };

    let req = test::TestRequest::post()
        .uri(&format!("/api/docs?oid={}&ctx=warehouse,receive", oid.to_base64()))
        .set_payload(data0.dump())
        .insert_header(ContentType::json())
        .to_request();

    let response = test::call_and_read_body(&app, req).await;

    let result0: serde_json::Value = serde_json::from_slice(&response).unwrap();

    // println!("RESULT_0: {result0:#?}");

    assert_ne!("", result0["goods"][0]["_tid"].as_str().unwrap());

    //receive1
    let data1: JsonValue = object! {
      _id: "",
      date: "2022-12-18",
      storage: storage1.to_string(),
      goods: [
          {
              goods: goods2.to_string(),
              uom: "",
              qty: 2,
              price: 7,
              cost: 14,
              _tid: ""
          },
      ]
  };

    let req = test::TestRequest::post()
        .uri(&format!("/api/docs?oid={}&ctx=warehouse,receive", oid.to_base64()))
        .set_payload(data1.dump())
        .insert_header(ContentType::json())
        .to_request();

    let response = test::call_and_read_body(&app, req).await;

    let result1: serde_json::Value = serde_json::from_slice(&response).unwrap();

    assert_ne!("", result0["goods"][0]["_tid"].as_str().unwrap());

    //report for move
    let from_date = "2022-11-14";
    let till_date = "2022-11-16";

    let req = test::TestRequest::get()
        .uri(&format!(
          "/api/inventory?oid={}&ctx=report&storage={}&from_date={}&till_date={}",
          oid.to_base64(),
          storage1.to_string(),
          from_date,
          till_date,
        ))
        .to_request();

    let response = test::call_and_read_body(&app, req).await;

    let str = String::from_utf8_lossy(&response).to_string();
    let result: JsonValue = json::parse(&str).unwrap();

    let batch = &result["data"][0]["items"][1][0]["batch"];

    //move
    let data2: JsonValue = object! {
      _id: "",
      date: "2022-12-18",
      storage: storage1.to_string(),
      transfer: storage2.to_string(),
      goods: [
          {
              goods: goods1.to_string(),
              batch: batch.clone(),
              uom: "",
              qty: 1,
              price: 15,
              cost: 15,
              _tid: ""
          },
      ]
  };

    let req = test::TestRequest::post()
        .uri(&format!("/api/docs?oid={}&ctx=warehouse,transfer", oid.to_base64()))
        .set_payload(data2.dump())
        .insert_header(ContentType::json())
        .to_request();

    let response = test::call_and_read_body(&app, req).await;

    //report
    let from_date = "2023-01-05";
    let till_date = "2023-01-06";

    let req = test::TestRequest::get()
        .uri(&format!(
          "/api/inventory?oid={}&ctx=report&storage={}&from_date={}&till_date={}",
          oid.to_base64(),
          storage1.to_string(),
          from_date,
          till_date,
        ))
        .to_request();

    let response = test::call_and_read_body(&app, req).await;
    // println!("RESPONSE: {response:#?}");

    let result: serde_json::Value = serde_json::from_slice(&response).unwrap();
    // println!("REPORT: {:#?}", result["data"][0]["items"]);

    let example = json!([
     {
        "store": "00000000-0000-0000-0000-0000000000c9",
        "open_balance": "29",
        "receive": "0",
        "issue": "0",
        "close_balance": "29",
    },
    [
       {
         "store": "00000000-0000-0000-0000-0000000000c9",
         "goods": "00000000-0000-0000-0000-000000000065",
          "batch": {
              "date": "2022-11-15T00:00:00.000Z",
              "id": result["data"][0]["items"][1][0]["batch"]["id"].as_str().unwrap(),
          },
          "open_balance": {
            "qty": "1",
            "cost": "15",
          },
          "receive": {
            "qty": "0",
            "cost": "0",
          },
          "issue": {
            "qty": "0",
            "cost": "0",
          },
          "close_balance": {
            "qty": "1",
            "cost": "15",
          },
      },
       {
         "store": "00000000-0000-0000-0000-0000000000c9",
         "goods": "00000000-0000-0000-0000-000000000066",
          "batch": {
              "date": "2022-12-18T00:00:00.000Z",
              "id": result["data"][0]["items"][1][1]["batch"]["id"].as_str().unwrap(),
          },
          "open_balance": {
            "qty": "2",
            "cost": "14",
          },
          "receive": {
            "qty": "0",
            "cost": "0",
          },
          "issue": {
            "qty": "0",
            "cost": "0",
          },
          "close_balance": {
            "qty": "2",
            "cost": "14",
          },
      },
    ],
  ]);

    assert_eq!(result["data"][0]["items"], example);
  }

  #[actix_web::test]
  async fn app_store_test_change_move() {
    let (tmp_dir, settings, db) = init();

    let (mut application, events_receiver) = Application::new(Arc::new(settings), Arc::new(db))
        .await
        .map_err(|e| io::Error::new(io::ErrorKind::Unsupported, e))
        .unwrap();

    let storage = SOrganizations::new(tmp_dir.path().join("companies"));
    application.storage = Some(storage.clone());

    application.register(MemoriesInFiles::new(application.clone(), "docs", storage.clone()));
    application.register(crate::inventory::service::Inventory::new(application.clone()));

    let app = test::init_service(
      App::new()
          .app_data(web::Data::new(application.clone()))
          .service(api::docs_create)
          .service(api::docs_update)
          .service(api::inventory_find)
          .default_service(web::route().to(api::not_implemented)),
    )
        .await;

    let goods1 = Uuid::from_u128(101);
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
              qty: 3,
              price: 9,
              cost: 27,
              _tid: ""
          },
      ]
  };

    let req = test::TestRequest::post()
        .uri(&format!("/api/docs?oid={}&ctx=warehouse,receive", oid.to_base64()))
        .set_payload(data0.dump())
        .insert_header(ContentType::json())
        .to_request();

    let response = test::call_and_read_body(&app, req).await;

    let str = String::from_utf8_lossy(&response).to_string();

    let result0: JsonValue = json::parse(&str).unwrap();

    assert_ne!("", result0["goods"][0]["_tid"].as_str().unwrap());

    //report for move
    let from_date = "2023-01-17";
    let till_date = "2023-01-20";

    let req = test::TestRequest::get()
        .uri(&format!(
          "/api/inventory?oid={}&ctx=report&storage={}&from_date={}&till_date={}",
          oid.to_base64(),
          storage1.to_string(),
          from_date,
          till_date,
        ))
        .to_request();

    let response = test::call_and_read_body(&app, req).await;
    println!("RESPONSE: {response:?}");
    let str = String::from_utf8_lossy(&response).to_string();
    let result: JsonValue = json::parse(&str).unwrap();

    let batch = &result["data"][0]["items"][1][0]["batch"];

    // move
    let data1: JsonValue = object! {
      _id: "",
      date: "2023-01-19",
      storage: storage1.to_string(),
      transfer: storage2.to_string(),
      goods: [
        {
          goods: goods1.to_string(),
          batch: batch.clone(),
          uom: "",
          qty: 2,
          // price: 9,
          // cost: 18,
          _tid: "",
        }
      ]
  };

    let req = test::TestRequest::post()
        .uri(&format!("/api/docs?oid={}&ctx=warehouse,transfer", oid.to_base64()))
        .set_payload(data1.dump())
        .insert_header(ContentType::json())
        .to_request();

    let response = test::call_and_read_body(&app, req).await;

    // change
    let mut data2 = result0.clone();
    let id = data2.clone()["_id"].string();
    {
      let line = &mut data2["goods"][0];
      line["qty"] = 1.into();
      line["cost"] = 10.into();
    }

    // let id = result0["goods"][0]["_tid"].as_str().unwrap();

    // let data2: JsonValue = object! {
    //     _id: "",
    //     date: "2023-01-18",
    //     storage: storage1.to_string(),
    //     goods: [
    //         {
    //             goods: goods1.to_string(),
    //             uom: "",
    //             qty: 1,
    //             price: 10,
    //             cost: 10,
    //             _tid: id,
    //         }
    //     ]
    // };

    let req = test::TestRequest::post()
        .uri(&format!("/api/docs/update?oid={}&ctx=warehouse,receive&id={id}", oid.to_base64()))
        .set_payload(data2.dump())
        .insert_header(ContentType::json())
        .to_request();

    let response = test::call_and_read_body(&app, req).await;

    // println!("TEST_RESPONSE: {response:?}");

    let result: serde_json::Value = serde_json::from_slice(&response).unwrap();

    let compare: serde_json::Value = serde_json::from_str(&data2.dump()).unwrap();

    assert_eq!(compare, result);

    //report store1
    let from_date = "2023-01-17";
    let till_date = "2023-01-20";

    let req = test::TestRequest::get()
        .uri(&format!(
          "/api/inventory?oid={}&ctx=report&storage={}&from_date={}&till_date={}",
          oid.to_base64(),
          storage1.to_string(),
          from_date,
          till_date,
        ))
        .to_request();

    let response = test::call_and_read_body(&app, req).await;
    let result: serde_json::Value = serde_json::from_slice(&response).unwrap();

    let example = json!([
     {
      "store": &storage1.to_string(),
      "open_balance": "0",
      "receive": "10",
      "issue": "-20",
      "close_balance": "-10",
    },
    [
       {
        "store": &storage1.to_string(),
        "goods": &goods1.to_string(),
        "batch": {
          "date": result["data"][0]["items"][1][0]["batch"]["date"].as_str().unwrap(),
          "id": result["data"][0]["items"][1][0]["batch"]["id"].as_str().unwrap(),
        },
        "open_balance": {
          "cost": "0",
          "qty": "0",
        },
        "receive": {
          "cost": "10",
          "qty": "1",
        },
        "issue": {
          "cost": "-20",
          "qty": "-2",
        },
        "close_balance": {
          "cost": "-10",
          "qty": "-1",
        },
      },
    ],
  ]);

    // println!("REPORT: {:#?}", result["data"][0]["items"]);

    assert_eq!(result["data"][0]["items"], example);

    //report store2
    let from_date = "2023-01-17";
    let till_date = "2023-01-20";

    let req = test::TestRequest::get()
        .uri(&format!(
          "/api/inventory?oid={}&ctx=report&storage={}&from_date={}&till_date={}",
          oid.to_base64(),
          storage2.to_string(),
          from_date,
          till_date,
        ))
        .to_request();

    let response = test::call_and_read_body(&app, req).await;
    let result: serde_json::Value = serde_json::from_slice(&response).unwrap();

    let example = json!([
    {
      "store": &storage2.to_string(),
      "open_balance": "0",
      "receive": "20",
      "issue": "0",
      "close_balance": "20",
    },

    [
      {
        "store": &storage2.to_string(),
        "goods": &goods1.to_string(),
        "batch": {
          "date": "2023-01-18T00:00:00.000Z",
          "id": result["data"][0]["items"][1][0]["batch"]["id"].as_str().unwrap(),
        },
        "open_balance": {
          "cost": "0",
          "qty": "0",
        },
        "receive": {
          "cost": "20",
          "qty": "2",
        },
        "issue": {
          "cost": "0",
          "qty": "0",
        },
        "close_balance": {
          "cost": "20",
          "qty": "2",
        },
      },
    ],
  ]);

    // println!("REPORT: {:#?}", result["data"][0]["items"]);

    assert_eq!(result["data"][0]["items"], example);
  }

  #[actix_web::test]
  async fn app_store_test_move() {
    let (tmp_dir, settings, db) = init();

    let (mut application, events_receiver) = Application::new(Arc::new(settings), Arc::new(db))
        .await
        .map_err(|e| io::Error::new(io::ErrorKind::Unsupported, e))
        .unwrap();

    let storage = SOrganizations::new(tmp_dir.path().join("companies"));
    application.storage = Some(storage.clone());

    application.register(MemoriesInFiles::new(application.clone(), "docs", storage.clone()));
    application.register(crate::inventory::service::Inventory::new(application.clone()));

    let app = test::init_service(
      App::new()
          // .app_data(web::Data::new(db.clone()))
          .app_data(web::Data::new(application.clone()))
          // .wrap(middleware::Logger::default())
          .service(api::docs_create)
          .service(api::docs_update)
          .service(api::inventory_find)
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
              qty: 2,
              price: 9,
              cost: 18,
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

    //report for move
    let from_date = "2023-01-17";
    let till_date = "2023-01-20";

    let req = test::TestRequest::get()
        .uri(&format!(
          "/api/inventory?oid={}&ctx=report&storage={}&from_date={}&till_date={}",
          oid.to_base64(),
          storage1.to_string(),
          from_date,
          till_date,
        ))
        .to_request();

    let response = test::call_and_read_body(&app, req).await;

    let str = String::from_utf8_lossy(&response).to_string();
    let result: JsonValue = json::parse(&str).unwrap();

    let batch = &result["data"][0]["items"][1][0]["batch"];

    // move
    let data1: JsonValue = object! {
      _id: "",
      date: "2023-01-19",
      storage: storage1.to_string(),
      transfer: storage2.to_string(),
      goods: [
        {
          goods: goods1.to_string(),
          batch: batch.clone(),
          uom: "",
          qty: 1,
          price: 9,
          cost: 9,
          _tid: "",
        }
      ]
  };

    let req = test::TestRequest::post()
        .uri(&format!("/api/docs?oid={}&ctx=warehouse,transfer", oid.to_base64()))
        .set_payload(data1.dump())
        .insert_header(ContentType::json())
        .to_request();

    let response = test::call_and_read_body(&app, req).await;
    // println!("MOVE_RESPONSE: {response:?}");

    //report store1
    let from_date = "2023-01-17";
    let till_date = "2023-01-20";

    let req = test::TestRequest::get()
        .uri(&format!(
          "/api/inventory?oid={}&ctx=report&storage={}&from_date={}&till_date={}",
          oid.to_base64(),
          storage1.to_string(),
          from_date,
          till_date,
        ))
        .to_request();

    let response = test::call_and_read_body(&app, req).await;
    let result: serde_json::Value = serde_json::from_slice(&response).unwrap();

    let example = json!([
     {
      "store": &storage1.to_string(),
      "open_balance": "0",
      "receive": "34",
      "issue": "-9",
      "close_balance": "25",
    },
    [
       {
        "store": &storage1.to_string(),
        "goods": &goods1.to_string(),
        "batch": result["data"][0]["items"][1][0]["batch"],
        "open_balance": {
          "cost": "0",
          "qty": "0",
        },
        "receive": {
          "cost": "18",
          "qty": "2",
        },
        "issue": {
          "cost": "-9",
          "qty": "-1",
        },
        "close_balance": {
          "cost": "9",
          "qty": "1",
        },
      },
      {
        "store": &storage1.to_string(),
        "goods": &goods2.to_string(),
        "batch": result["data"][0]["items"][1][1]["batch"],
        "open_balance": {
          "cost": "0",
          "qty": "0",
        },
        "receive": {
            "cost": "16",
            "qty": "2",
        },
        "issue": {
          "cost": "0",
          "qty": "0",
        },
        "close_balance": {
          "cost": "16",
          "qty": "2",
        },
      },
    ],
  ]);

    println!("REPORT: {:#?}", result["data"]);

    assert_eq!(result["data"][0]["items"], example);

    //report store2
    let from_date = "2023-01-17";
    let till_date = "2023-01-20";

    let req = test::TestRequest::get()
        .uri(&format!(
          "/api/inventory?oid={}&ctx=report&storage={}&from_date={}&till_date={}",
          oid.to_base64(),
          storage2.to_string(),
          from_date,
          till_date,
        ))
        .to_request();

    let response = test::call_and_read_body(&app, req).await;
    let result: serde_json::Value = serde_json::from_slice(&response).unwrap();

    let example = json!([
    {
      "store": &storage2.to_string(),
      "open_balance": "0",
      "receive": "9",
      "issue": "0",
      "close_balance": "9",
    },

    [
      {
        "store": &storage2.to_string(),
        "goods": &goods1.to_string(),
        "batch": {
          "date": "2023-01-18T00:00:00.000Z",
          "id": result["data"][0]["items"][1][0]["batch"]["id"].as_str().unwrap(),
        },
        "open_balance": {
          "cost": "0",
          "qty": "0",
        },
        "receive": {
          "cost": "9",
          "qty": "1",
        },
        "issue": {
          "cost": "0",
          "qty": "0",
        },
        "close_balance": {
          "cost": "9",
          "qty": "1",
        },
      },
    ],
  ]);

    // println!("REPORT: {:#?}", result["data"][0]["items"]);

    assert_eq!(result["data"][0]["items"], example);
  }

  #[actix_web::test]
  async fn app_store_test_receive_many_stores() {
    let (tmp_dir, settings, db) = init();

    let (mut application, events_receiver) = Application::new(Arc::new(settings), Arc::new(db))
        .await
        .map_err(|e| io::Error::new(io::ErrorKind::Unsupported, e))
        .unwrap();

    let storage = SOrganizations::new(tmp_dir.path().join("companies"));
    application.storage = Some(storage.clone());

    application.register(MemoriesInFiles::new(application.clone(), "docs", storage.clone()));
    application.register(crate::inventory::service::Inventory::new(application.clone()));

    let app = test::init_service(
      App::new()
          .app_data(web::Data::new(application.clone()))
          .service(api::docs_create)
          .service(api::docs_update)
          .service(api::inventory_find)
          .default_service(web::route().to(api::not_implemented)),
    )
        .await;

    let goods1 = Uuid::from_u128(201);
    let goods2 = Uuid::from_u128(101);
    let storage1 = Uuid::from_u128(202);
    let storage2 = Uuid::from_u128(203);
    let oid = ID::from("99");

    //receive1
    let data1: JsonValue = object! {
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
      ]
  };

    let req = test::TestRequest::post()
        .uri(&format!("/api/docs?oid={}&ctx=warehouse,receive", oid.to_base64()))
        .set_payload(data1.dump())
        .insert_header(ContentType::json())
        .to_request();

    let response = test::call_and_read_body(&app, req).await;

    let result1: serde_json::Value = serde_json::from_slice(&response).unwrap();

    assert_ne!("", result1["goods"][0]["_tid"].as_str().unwrap());

    //receive2
    let data2: JsonValue = object! {
      _id: "",
      date: "2023-01-19",
      storage: storage2.to_string(),
      goods: [
          {
              goods: goods2.to_string(),
              uom: "",
              qty: 3,
              price: 8,
              cost: 24,
              _tid: ""
          }
      ]
  };

    let req = test::TestRequest::post()
        .uri(&format!("/api/docs?oid={}&ctx=warehouse,receive", oid.to_base64()))
        .set_payload(data2.dump())
        .insert_header(ContentType::json())
        .to_request();

    let response = test::call_and_read_body(&app, req).await;

    let result2: serde_json::Value = serde_json::from_slice(&response).unwrap();

    assert_ne!("", result2["goods"][0]["_tid"].as_str().unwrap());

    //report storage1
    let from_date = "2023-01-17";
    let till_date = "2023-01-20";

    let req = test::TestRequest::get()
        .uri(&format!(
          "/api/inventory?oid={}&ctx=report&storage={}&from_date={}&till_date={}",
          oid.to_base64(),
          storage1.to_string(),
          from_date,
          till_date,
        ))
        .to_request();

    let response = test::call_and_read_body(&app, req).await;
    // println!("RESPONSE: {response:#?}\n");

    let result: serde_json::Value = serde_json::from_slice(&response).unwrap();
    // println!("REPORT1: {result:#?}");

    let example1 = json!([
     {
       "store": storage1.to_string(),
       "open_balance": "0",
       "receive": "10",
       "issue": "0",
       "close_balance": "10",
    },
    [
       {
         "store": storage1.to_string(),
         "goods": goods1.to_string(),
         "batch": {
             "date": "2023-01-18T00:00:00.000Z",
             "id": result["data"][0]["items"][1][0]["batch"]["id"].as_str().unwrap(),
         },
         "open_balance": {
           "qty": "0",
           "cost": "0",
         },
         "receive": {
           "qty": "1",
           "cost": "10",
         },
         "issue": {
           "qty": "0",
           "cost": "0",
         },
         "close_balance": {
           "qty": "1",
           "cost": "10",
         },
       },
    ],
  ]);

    assert_eq!(result["data"][0]["items"], example1);

    //report storage2
    let from_date = "2023-01-17";
    let till_date = "2023-01-20";

    let req = test::TestRequest::get()
        .uri(&format!(
          "/api/inventory?oid={}&ctx=report&storage={}&from_date={}&till_date={}",
          oid.to_base64(),
          storage2.to_string(),
          from_date,
          till_date,
        ))
        .to_request();

    let response = test::call_and_read_body(&app, req).await;
    // println!("RESPONSE: {response:#?}\n");

    let result: serde_json::Value = serde_json::from_slice(&response).unwrap();
    // println!("REPORT2: {result:#?}");

    let example2 = json!([
     {
       "store": storage2.to_string(),
       "open_balance": "0",
       "receive": "24",
       "issue": "0",
       "close_balance": "24",
    },
    [
      {
        "store": storage2.to_string(),
        "goods": goods2.to_string(),
        "batch": {
            "date": "2023-01-19T00:00:00.000Z",
            "id": result["data"][0]["items"][1][0]["batch"]["id"].as_str().unwrap(),
        },
        "open_balance": {
          "qty": "0",
          "cost": "0",
        },
        "receive": {
          "qty": "3",
          "cost": "24",
        },
        "issue": {
          "qty": "0",
          "cost": "0",
        },
        "close_balance": {
          "qty": "3",
          "cost": "24",
        },
      },
    ],
  ]);

    assert_eq!(result["data"][0]["items"], example2);
  }

  #[actix_web::test]
  async fn app_store_test_receive_issue_change() {
    let (tmp_dir, settings, db) = init();

    let (mut application, events_receiver) = Application::new(Arc::new(settings), Arc::new(db))
        .await
        .map_err(|e| io::Error::new(io::ErrorKind::Unsupported, e))
        .unwrap();

    let storage = SOrganizations::new(tmp_dir.path().join("companies"));
    application.storage = Some(storage.clone());

    application.register(MemoriesInFiles::new(application.clone(), "docs", storage.clone()));
    application.register(crate::inventory::service::Inventory::new(application.clone()));

    let app = test::init_service(
      App::new()
          // .app_data(web::Data::new(db.clone()))
          .app_data(web::Data::new(application.clone()))
          // .wrap(middleware::Logger::default())
          .service(api::docs_create)
          .service(api::docs_update)
          .service(api::inventory_find)
          // .service(api::memory_modify)
          // .service(api::memory_query)
          .default_service(web::route().to(api::not_implemented)),
    )
        .await;

    let goods1 = Uuid::from_u128(101);
    let goods2 = Uuid::from_u128(201);
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

    let data = String::from_utf8_lossy(&response).to_string();
    let result0 = json::parse(&data).unwrap();

    let g1_tid = result0["goods"][0]["_tid"].as_str().unwrap();
    let g2_tid = result0["goods"][1]["_tid"].as_str().unwrap();

    assert_ne!("", g1_tid);
    assert_ne!("", g2_tid);

    let g1_batch = object! { id: g1_tid.clone(), date: "2023-01-18T00:00:00.000Z".to_string() };
    let g2_batch = object! { id: g2_tid.clone(), date: "2023-01-18T00:00:00.000Z".to_string() };

    // issue
    let data1: JsonValue = object! {
      _id: "",
      date: "2023-01-19",
      storage: storage1.to_string(),
      goods: [
          {
              goods: goods2.to_string(),
              batch: g2_batch.clone(),
              uom: "",
              qty: 1,
              // price: 0,
              // cost: 0,
              // _tid: result0["goods"][1]["_tid"].as_str().unwrap(),
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

    let data = String::from_utf8_lossy(&response).to_string();
    let result1 = json::parse(&data).unwrap();

    // change
    let mut data2 = result0.clone();
    let id = data2.clone()["_id"].string();
    {
      let line = &mut data2["goods"][0];
      line["qty"] = 2.into();
      line["cost"] = 20.into();
    }

    // println!(
    //   "UPDATE RECEIVE {}",
    //   format!("/api/docs/update?oid={}&ctx=warehouse,receive&id={id}", oid.to_base64())
    // );

    let req = test::TestRequest::post()
        .uri(&format!("/api/docs/update?oid={}&ctx=warehouse,receive&id={id}", oid.to_base64()))
        .set_payload(data2.to_string())
        .insert_header(ContentType::json())
        .to_request();

    let response = test::call_and_read_body(&app, req).await;

    let result: serde_json::Value = serde_json::from_slice(&response).unwrap();

    let compare: serde_json::Value = serde_json::from_str(&data2.to_string()).unwrap();

    assert_eq!(compare, result);

    //report
    let from_date = "2023-01-17";
    let till_date = "2023-01-20";

    let req = test::TestRequest::get()
        .uri(&format!(
          "/api/inventory?oid={}&ctx=report&storage={}&from_date={}&till_date={}",
          oid.to_base64(),
          storage1.to_string(),
          from_date,
          till_date,
        ))
        .to_request();

    let response = test::call_and_read_body(&app, req).await;
    // println!("RESPONSE: {response:#?}\n");

    let data = String::from_utf8_lossy(&response).to_string();
    let result: JsonValue = json::parse(&data).unwrap();
    // println!("REPORT: {result:#?}");

    let report: JsonValue = json::array![
     {
       "store": "00000000-0000-0000-0000-0000000000ca",
       "open_balance": "0",
       "receive": "36",
       "issue": "-8",
       "close_balance": "28",
    },
    [
       {
          "store": "00000000-0000-0000-0000-0000000000ca",
          "goods": goods1.to_string(),
          "batch": g1_batch.clone(),
          "open_balance": {
            "qty": "0",
            "cost": "0",
          },
          "receive": {
            "qty": "2",
            "cost": "20",
          },
          "issue": {
            "qty": "0",
            "cost": "0",
          },
          "close_balance": {
            "qty": "2",
            "cost": "20",
          },
      },
      {
        "store": "00000000-0000-0000-0000-0000000000ca",
        "goods": goods2.to_string(),
        "batch": g2_batch.clone(),
        "open_balance": {
          "qty": "0",
          "cost": "0",
        },
        "receive": {
          "qty": "2",
          "cost": "16",
        },
        "issue": {
          "qty": "-1",
          "cost": "-8",
        },
        "close_balance": {
          "qty": "1",
          "cost": "8",
        },
     },
    ],
  ];

    assert_eq!(result["data"][0]["items"].dump(), report.dump());
  }
