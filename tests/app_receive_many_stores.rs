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
async fn app_store_test_receive_many_stores() {
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

    let req = TestRequest::post()
        .uri(&format!("/api/docs?oid={}&ctx=warehouse,receive", oid.to_base64()))
        .set_payload(data1.dump())
        .insert_header(ContentType::json())
        .to_request();

    let response = call_and_read_body(&app, req).await;

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

    let req = TestRequest::post()
        .uri(&format!("/api/docs?oid={}&ctx=warehouse,receive", oid.to_base64()))
        .set_payload(data2.dump())
        .insert_header(ContentType::json())
        .to_request();

    let response = call_and_read_body(&app, req).await;

    let result2: serde_json::Value = serde_json::from_slice(&response).unwrap();

    assert_ne!("", result2["goods"][0]["_tid"].as_str().unwrap());

    //report storage1
    let from_date = "2023-01-17";
    let till_date = "2023-01-20";

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

    let req = TestRequest::get()
        .uri(&format!(
            "/api/inventory?oid={}&ctx=report&storage={}&from_date={}&till_date={}",
            oid.to_base64(),
            storage2.to_string(),
            from_date,
            till_date,
        ))
        .to_request();

    let response = call_and_read_body(&app, req).await;
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