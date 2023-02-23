mod test_init;

use test_init::init;

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
async fn app_store_test_incomplete_data() {
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

    let goods = Uuid::from_u128(101);
    let storage = Uuid::from_u128(201);
    let oid = ID::from("99");

    let data: JsonValue = object! {
      _id: "",
      date: "2022-11-15",
      storage: storage.to_string(),
  };

    let req = TestRequest::post()
        .uri(&format!("/api/docs?oid={}&ctx=warehouse,receive", oid.to_base64()))
        .set_payload(data.dump())
        .insert_header(ContentType::json())
        .to_request();

    let response = call_and_read_body(&app, req).await;

    let result: serde_json::Value = serde_json::from_slice(&response).unwrap();

    let compare_data: JsonValue = object! {
      _id: result["_id"].as_str().unwrap(),
      date: "2022-11-15",
      storage: storage.to_string(),
  };

    // println!("RESULT: {result:#?}");

    assert_eq!(compare_data.dump(), result.to_string());
}