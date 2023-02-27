extern crate actix_web;
extern crate nae_backend;
extern crate structopt;

mod test_init;

use test_init::init;

use std::sync::Arc;
use std::io;
// use actix_web::{
//     web,
//     App,
//     test::{TestRequest, init_service, call_and_read_body},
//     http::header::ContentType
// };

use nae_backend::{commutator::Application,
                  animo::memory::ID,
                  services::Services,
                  api,
                  use_cases::uc_005::receive_csv_to_json,
                  storage::SOrganizations,
                  memories::MemoriesInFiles};
use actix_web::{test::{TestRequest, call_and_read_body, init_service},
                web,
                App};

#[actix_web::test]
async fn app_store_test_from_csv() {
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
            .service(api::docs_find)
            .service(api::docs_create)
            // .service(api::docs_update)
            .service(api::inventory_find)
            .default_service(web::route().to(api::not_implemented)),
    )
        .await;

    receive_csv_to_json(&application, "/Users/g/projects/Nae-backend/tests/data/test_dista.csv").unwrap();

    let oid = ID::from("Midas-Plastics");

    //find a storage
    let req = TestRequest::get()
        .uri(&format!(
            "/api/docs/find?oid={}&ctx={}&filter={}",
            oid.to_base64(),
            "warehouse/receive/storage",
            "name:Склад Midas Plastics",
        ))
        .to_request();

    let response = call_and_read_body(&app, req).await;
    println!("STORAGE_RESPONSE: {response:#?}\n");

    //report
    let from_date = "2022-12-20";
    let till_date = "2022-12-22";

    let req = TestRequest::get()
        .uri(&format!(
            "/api/inventory?oid={}&ctx=report&storage={}&from_date={}&till_date={}",
            oid.to_base64(),
            uuid::Uuid::new_v4().to_string(),
            from_date,
            till_date,
        ))
        .to_request();

    let response = call_and_read_body(&app, req).await;
    println!("REPORT_RESPONSE: {response:#?}\n");

    // let data = String::from_utf8_lossy(&response).to_string();
    // let result: JsonValue = json::parse(&data).unwrap();
    // println!("REPORT: {result:#?}");
}