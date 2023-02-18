mod store_test_key_to_data;
mod store_test_app_incomplete_data;


use std::{io, thread, time::Duration};

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
  store::{date_type_store_batch_id::DateTypeStoreBatchId, wh_storage::WHStorage},
};

use super::{store_date_type_batch_id::StoreDateTypeBatchId, *};
use crate::utils::time::time_to_string;
use crate::warehouse::test_util::init;
use actix_web::{http::header::ContentType, test, web, App};
use futures::TryFutureExt;
use json::object;
use rocksdb::{ColumnFamilyDescriptor, Options};
use serde_json::json;
use tempfile::TempDir;
use uuid::Uuid;

#[actix_web::test]
async fn store_test_app_checkpoints() {
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
async fn store_test_app_move_checkpoints() {
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

// #[actix_web::test]
// async fn store_test_key_to_data() {
//   let date1 = dt("2022-12-15").unwrap();
//   let storage1 = Uuid::from_u128(201);
//   let goods1 = Uuid::from_u128(101);
//   let batch = Batch { id: Uuid::from_u128(102), date: date1 };

//   let key: Vec<u8> = []
//     .iter()
//     .chain((date1.timestamp() as u64).to_be_bytes().iter())
//     .chain(storage1.as_bytes().iter())
//     .chain(goods1.as_bytes().iter())
//     .chain((batch.date.timestamp() as u64).to_be_bytes().iter())
//     .chain(batch.id.as_bytes().iter())
//     .map(|b| *b)
//     .collect();

//   let (d, s, g, b) = CheckDateStoreBatch::key_to_data(key).unwrap();

//   // println!("{d:?}, {s:?}, {g:?}, {b:?}");

//   assert_eq!(date1, d);
//   assert_eq!(storage1, s);
//   assert_eq!(goods1, g);
//   assert_eq!(batch, b);
// }

#[actix_web::test]
async fn store_test_app_change_move() {
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
          price: 9,
          cost: 18,
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
    line["qty"] = 2.into();
    line["cost"] = 20.into();
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

  println!("TEST_RESPONSE: {response:?}");

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
      "receive": "34",
      "issue": "-9",
      "close_balance": "25",
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
async fn store_test_app_move() {
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
async fn store_test_app_receive_many_stores() {
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
async fn store_test_app_receive_issue_change() {
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

#[actix_web::test]
async fn store_test_get_wh_ops() -> Result<(), WHError> {
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

#[actix_web::test]
async fn store_test_get_aggregations_without_checkpoints() -> Result<(), WHError> {
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

#[actix_web::test]
async fn store_test_report() {
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

#[actix_web::test]
async fn store_test_issue_op_none() {
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
