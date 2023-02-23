mod test_init;

use test_init::init;

#[macro_use]
use serde_json::json;

use nae_backend::commutator::Application;
use nae_backend::animo::{
    db::AnimoDB,
    memory::{Memory, ID},
};
use nae_backend::storage::SOrganizations;
use nae_backend::memories::MemoriesInFiles;
use nae_backend::services::Services;

use actix_web::{
    web,
    App,
    test::{TestRequest, init_service, call_and_read_body},
    http::header::ContentType
};

use std::sync::Arc;
use std::io;
use nae_backend::api;
use json::JsonValue;
use json::object;
use uuid::Uuid;
use tempfile::{TempDir, tempdir};

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
    application.register(nae_backend::inventory::service::Inventory::new(application.clone()));

    let app = init_service(
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

    let req = TestRequest::post()
        .uri(&format!("/api/docs?oid={}&ctx=warehouse,receive", oid.to_base64()))
        .set_payload(data0.dump())
        .insert_header(ContentType::json())
        // .param("oid", oid.to_base64())
        // .param("document", "warehouse")
        // .param("document", "receive")
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
        // .param("oid", oid.to_base64())
        // .param("document", "warehouse")
        // .param("document", "receive")
        .to_request();

    let response = call_and_read_body(&app, req).await;

    let result1: serde_json::Value = serde_json::from_slice(&response).unwrap();

    assert_ne!("", result0["goods"][0]["_tid"].as_str().unwrap());

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