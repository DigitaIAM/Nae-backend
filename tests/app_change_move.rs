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
use service::Services;

use service::utils::json::JsonParams;

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
async fn app_store_test_change_move() {
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

    let req = TestRequest::post()
        .uri(&format!("/api/docs?oid={}&ctx=warehouse,receive", oid.to_base64()))
        .set_payload(data0.dump())
        .insert_header(ContentType::json())
        .to_request();

    let response = call_and_read_body(&app, req).await;

    let str = String::from_utf8_lossy(&response).to_string();

    let result0: JsonValue = json::parse(&str).unwrap();

    assert_ne!("", result0["goods"][0]["_tid"].as_str().unwrap());

    //report for move
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

    let req = TestRequest::post()
        .uri(&format!("/api/docs?oid={}&ctx=warehouse,transfer", oid.to_base64()))
        .set_payload(data1.dump())
        .insert_header(ContentType::json())
        .to_request();

    let response = call_and_read_body(&app, req).await;

    // change
    let mut data2 = result0.clone();
    let id = data2.clone()["_id"].string();
    {
        let line = &mut data2["goods"][0];
        line["qty"] = 1.into();
        line["cost"] = 10.into();
    }

    let req = TestRequest::post()
        .uri(&format!("/api/docs/update?oid={}&ctx=warehouse,receive&id={id}", oid.to_base64()))
        .set_payload(data2.dump())
        .insert_header(ContentType::json())
        .to_request();

    let response = call_and_read_body(&app, req).await;

    // println!("TEST_RESPONSE: {response:?}");

    let result: serde_json::Value = serde_json::from_slice(&response).unwrap();

    let compare: serde_json::Value = serde_json::from_str(&data2.dump()).unwrap();

    assert_eq!(compare, result);

    //report store1
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