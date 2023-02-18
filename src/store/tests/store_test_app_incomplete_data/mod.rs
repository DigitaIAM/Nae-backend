use std::{io, sync::Arc};

use actix_web::{http::header::ContentType, test, web, App};
use json::{object, JsonValue};
use uuid::Uuid;

use crate::{
  animo::memory::ID, api, commutator::Application, memories::MemoriesInFiles, services::Services,
  storage::SOrganizations, warehouse::test_util::init,
};

#[actix_web::test]
async fn store_test_app_incomplete_data() {
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
