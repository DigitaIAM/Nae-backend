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

use utils::json::JsonParams;

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
async fn app_store_test_receive_issue_change() {
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

    let req = TestRequest::post()
        .uri(&format!("/api/docs?oid={}&ctx=warehouse,receive", oid.to_base64()))
        .set_payload(data0.dump())
        .insert_header(ContentType::json())
        // .param("oid", oid.to_base64())
        // .param("document", "warehouse")
        // .param("document", "receive")
        .to_request();

    let response = call_and_read_body(&app, req).await;

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

    let req = TestRequest::post()
        .uri(&format!("/api/docs?oid={}&ctx=warehouse,issue", oid.to_base64()))
        .set_payload(data1.dump())
        .insert_header(ContentType::json())
        // .param("oid", oid.to_base64())
        // .param("document", "warehouse")
        // .param("document", "receive")
        .to_request();

    let response = call_and_read_body(&app, req).await;

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

    let req = TestRequest::post()
        .uri(&format!("/api/docs/update?oid={}&ctx=warehouse,receive&id={id}", oid.to_base64()))
        .set_payload(data2.to_string())
        .insert_header(ContentType::json())
        .to_request();

    let response = call_and_read_body(&app, req).await;

    let result: serde_json::Value = serde_json::from_slice(&response).unwrap();

    let compare: serde_json::Value = serde_json::from_str(&data2.to_string()).unwrap();

    assert_eq!(compare, result);

    //report
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