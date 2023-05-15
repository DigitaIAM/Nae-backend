mod test_init;

use test_init::init;

#[macro_use]
use serde_json::json;

use nae_backend::animo::{db::AnimoDB, memory::Memory};
use nae_backend::commutator::Application;
use nae_backend::memories::MemoriesInFiles;
use nae_backend::storage::Workspaces;
use service::{Context, Services};

use service::utils::json::JsonParams;

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

use store::process_records::process_record;

#[actix_web::test]
async fn app_store_test_change_move() {
  let (tmp_dir, settings, db) = init();

  let (mut application, events_receiver) = Application::new(Arc::new(settings), Arc::new(db))
    .await
    .map_err(|e| io::Error::new(io::ErrorKind::Unsupported, e))
    .unwrap();

  let storage = Workspaces::new(tmp_dir.path().join("companies"));
  application.storage = Some(storage.clone());

  application.register(MemoriesInFiles::new(application.clone(), "memories", storage.clone()));
  application.register(nae_backend::inventory::service::Inventory::new(application.clone()));

  // let app = init_service(
  //   App::new()
  //     .app_data(web::Data::new(application.clone()))
  //     .service(api::docs_create)
  //     .service(api::docs_update)
  //     .service(api::inventory_find)
  //     .default_service(web::route().to(api::not_implemented)),
  // )
  // .await;

  // let goods1 = Uuid::from_u128(101);
  // let storage1 = Uuid::from_u128(201);
  // let storage2 = Uuid::from_u128(202);

  let oid = "yjmgJUmDo_kn9uxVi8s9Mj9mgGRJISxRt63wT46NyTQ";

  // Number	Категория товара	Name	Goods	Units of measure	Quantity	Date	Сounterparty	Recipient
  // 0		Американка пропилен 100мм	85	шт	8	19.12.2022	прием-передача Имакуловым Р.	склад

  //receive
  // let data0: JsonValue = object! {
  //     "Number": "0",
  //     "Категория товара": "",
  //     "Name": "Американка пропилен 100мм",
  //     "Goods": "85",
  //     "Units of measure": "шт",
  //     "Quantity": "8",
  //     "Date": "19.12.2022",
  //     "Сounterparty": "прием-передача Имакуловым Р.",
  //     "Recipient": "склад",
  // };

  let data0_vec = [
    "0",
    "производство",
    "Американка пропилен 100мм",
    "85",
    "шт",
    "8",
    "",
    "19.12.2022",
    "прием-передача Имакуловым Р.",
    "склад",
  ]
  .to_vec();

  let data0 = csv::StringRecord::from(data0_vec);

  let ctx = ["warehouse", "inventory"].to_vec();

  process_record(&application, &ctx, data0).unwrap();

  // let result = app.service("memories").find(object! {oid: oid, ctx: ctx, filter: filter})?;

  let filter = object! {};
  let params = object! {oid: oid, ctx: ctx, filter: filter};
  let result = application.service("memories").find(Context::local(), params).unwrap();

  // let result = application
  //   .service("memories").find(object! {oid: oid, ctx: ctx, filter: object! {}})
  //   // .await
  //   // .map_err(actix_web::error::ErrorInternalServerError)
  //   .unwrap();

  println!("test_result: {result:?}");

  // let req = TestRequest::post()
  //   .uri(&format!("/api/docs?oid={}&ctx=warehouse,receive", oid.to_base64()))
  //   .set_payload(data0.dump())
  //   .insert_header(ContentType::json())
  //   .to_request();
  //
  // let response = call_and_read_body(&app, req).await;
  //
  // let str = String::from_utf8_lossy(&response).to_string();
  //
  // let result0: JsonValue = json::parse(&str).unwrap();

  // let params: JsonValue = object! {"oid": oid, "ctx": ["warehouse", "receive"]};
  //
  // let result = app
  //   .service("memories")
  //   .create(data, params)
  //   .await
  //   .map_err(actix_web::error::ErrorInternalServerError)
  //   .unwrap();
  //
  // println!("mem_create_res {result:?}");

  // assert_ne!("", result0["goods"][0]["_tid"].as_str().unwrap());
  //
  // //report for move
  // let from_date = "2023-01-17";
  // let till_date = "2023-01-20";
  //
  // let req = TestRequest::get()
  //   .uri(&format!(
  //     "/api/inventory?oid={}&ctx=report&storage={}&from_date={}&till_date={}",
  //     oid.to_base64(),
  //     storage1.to_string(),
  //     from_date,
  //     till_date,
  //   ))
  //   .to_request();
  //
  // let response = call_and_read_body(&app, req).await;
  // println!("RESPONSE: {response:?}");
  // let str = String::from_utf8_lossy(&response).to_string();
  // let result: JsonValue = json::parse(&str).unwrap();
  //
  // let batch = &result["data"][0]["items"][1][0]["batch"];
  //
  // // move
  // let data1: JsonValue = object! {
  //     _id: "",
  //     date: "2023-01-19",
  //     storage: storage1.to_string(),
  //     transfer: storage2.to_string(),
  //     goods: [
  //       {
  //         goods: goods1.to_string(),
  //         batch: batch.clone(),
  //         uom: "",
  //         qty: 2,
  //         // price: 9,
  //         // cost: 18,
  //         _tid: "",
  //       }
  //     ]
  // };
  //
  // let req = TestRequest::post()
  //   .uri(&format!("/api/docs?oid={}&ctx=warehouse,transfer", oid.to_base64()))
  //   .set_payload(data1.dump())
  //   .insert_header(ContentType::json())
  //   .to_request();
  //
  // let response = call_and_read_body(&app, req).await;
  //
  // // change
  // let mut data2 = result0.clone();
  // let id = data2.clone()["_id"].string();
  // {
  //   let line = &mut data2["goods"][0];
  //   line["qty"] = 1.into();
  //   line["cost"] = 10.into();
  // }
  //
  // let req = TestRequest::post()
  //   .uri(&format!("/api/docs/update?oid={}&ctx=warehouse,receive&id={id}", oid.to_base64()))
  //   .set_payload(data2.dump())
  //   .insert_header(ContentType::json())
  //   .to_request();
  //
  // let response = call_and_read_body(&app, req).await;
  //
  // // println!("TEST_RESPONSE: {response:?}");
  //
  // let result: serde_json::Value = serde_json::from_slice(&response).unwrap();
  //
  // let compare: serde_json::Value = serde_json::from_str(&data2.dump()).unwrap();
  //
  // assert_eq!(compare, result);
  //
  // //report store1
  // let from_date = "2023-01-17";
  // let till_date = "2023-01-20";
  //
  // let req = TestRequest::get()
  //   .uri(&format!(
  //     "/api/inventory?oid={}&ctx=report&storage={}&from_date={}&till_date={}",
  //     oid.to_base64(),
  //     storage1.to_string(),
  //     from_date,
  //     till_date,
  //   ))
  //   .to_request();
  //
  // let response = call_and_read_body(&app, req).await;
  // let result: serde_json::Value = serde_json::from_slice(&response).unwrap();
  //
  // let example = json!([
  //    {
  //     "store": &storage1.to_string(),
  //     "open_balance": "0",
  //     "receive": "10",
  //     "issue": "-20",
  //     "close_balance": "-10",
  //   },
  //   [
  //      {
  //       "store": &storage1.to_string(),
  //       "goods": &goods1.to_string(),
  //       "batch": {
  //         "date": result["data"][0]["items"][1][0]["batch"]["date"].as_str().unwrap(),
  //         "id": result["data"][0]["items"][1][0]["batch"]["id"].as_str().unwrap(),
  //       },
  //       "open_balance": {
  //         "cost": "0",
  //         "qty": "0",
  //       },
  //       "receive": {
  //         "cost": "10",
  //         "qty": "1",
  //       },
  //       "issue": {
  //         "cost": "-20",
  //         "qty": "-2",
  //       },
  //       "close_balance": {
  //         "cost": "-10",
  //         "qty": "-1",
  //       },
  //     },
  //   ],
  // ]);
  //
  // // println!("REPORT: {:#?}", result["data"][0]["items"]);
  //
  // assert_eq!(result["data"][0]["items"], example);
  //
  // //report store2
  // let from_date = "2023-01-17";
  // let till_date = "2023-01-20";
  //
  // let req = TestRequest::get()
  //   .uri(&format!(
  //     "/api/inventory?oid={}&ctx=report&storage={}&from_date={}&till_date={}",
  //     oid.to_base64(),
  //     storage2.to_string(),
  //     from_date,
  //     till_date,
  //   ))
  //   .to_request();
  //
  // let response = call_and_read_body(&app, req).await;
  // let result: serde_json::Value = serde_json::from_slice(&response).unwrap();
  //
  // let example = json!([
  //   {
  //     "store": &storage2.to_string(),
  //     "open_balance": "0",
  //     "receive": "20",
  //     "issue": "0",
  //     "close_balance": "20",
  //   },
  //
  //   [
  //     {
  //       "store": &storage2.to_string(),
  //       "goods": &goods1.to_string(),
  //       "batch": {
  //         "date": "2023-01-18T00:00:00.000Z",
  //         "id": result["data"][0]["items"][1][0]["batch"]["id"].as_str().unwrap(),
  //       },
  //       "open_balance": {
  //         "cost": "0",
  //         "qty": "0",
  //       },
  //       "receive": {
  //         "cost": "20",
  //         "qty": "2",
  //       },
  //       "issue": {
  //         "cost": "0",
  //         "qty": "0",
  //       },
  //       "close_balance": {
  //         "cost": "20",
  //         "qty": "2",
  //       },
  //     },
  //   ],
  // ]);
  //
  // // println!("REPORT: {:#?}", result["data"][0]["items"]);
  //
  // assert_eq!(result["data"][0]["items"], example);
}
