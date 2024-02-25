mod test_init;

use test_init::init;

#[macro_use]
use serde_json::json;

use nae_backend::animo::{
  db::AnimoDB,
  memory::{Memory, ID},
};
use nae_backend::commutator::Application;
use nae_backend::memories::MemoriesInFiles;
use nae_backend::services::Services;
use nae_backend::storage::SOrganizations;

use actix_web::{
  http::header::ContentType,
  test::{call_and_read_body, init_service, TestRequest},
  web, App,
};

use json::object;
use json::JsonValue;
use nae_backend::api;
use std::io;
use std::sync::Arc;
use tempfile::{tempdir, TempDir};
use uuid::Uuid;

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
  application.register(nae_backend::inventory::service::Inventory::new(application.clone()));

  let app = init_service(
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

  let req = TestRequest::post()
    .uri(&format!("/api/docs?oid={}&ctx=warehouse,receive", oid.to_base64()))
    .set_payload(data0.dump())
    .insert_header(ContentType::json())
    .to_request();

  let response = call_and_read_body(&app, req).await;

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

  let req = TestRequest::post()
    .uri(&format!("/api/docs?oid={}&ctx=warehouse,receive", oid.to_base64()))
    .set_payload(data1.dump())
    .insert_header(ContentType::json())
    .to_request();

  let response = call_and_read_body(&app, req).await;

  let result1: serde_json::Value = serde_json::from_slice(&response).unwrap();

  assert_ne!("", result0["goods"][0]["_tid"].as_str().unwrap());

  //report for move
  let from_date = "2022-11-14";
  let till_date = "2022-11-16";

  let req = TestRequest::get()
    .uri(&format!(
      "/api/inventory?oid={}&ctx=report&storage={}&from_date={}&till_date={}",
      oid.to_base64(),
      storage1.to_string(),
      from_date,
      till_date,
    ))
    .to_request();

  let response = call_and_read_body(&app, req).await;

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

  let req = TestRequest::post()
    .uri(&format!("/api/docs?oid={}&ctx=warehouse,transfer", oid.to_base64()))
    .set_payload(data2.dump())
    .insert_header(ContentType::json())
    .to_request();

  let response = call_and_read_body(&app, req).await;

  //report
  let from_date = "2023-01-05";
  let till_date = "2023-01-06";

  let req = TestRequest::get()
    .uri(&format!(
      "/api/inventory?oid={}&ctx=report&storage={}&from_date={}&till_date={}",
      oid.to_base64(),
      storage1.to_string(),
      from_date,
      till_date,
    ))
    .to_request();

  let response = call_and_read_body(&app, req).await;
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
