mod test_init;

use std::{io, sync::Arc};

use actix_web::{
  http::header::ContentType,
  test::{call_and_read_body, init_service, TestRequest},
  web, App,
};
use csv::{ReaderBuilder, Trim};
use json::{object, JsonValue};
use nae_backend::{
  animo::memory::ID, api, commutator::Application, memories::MemoriesInFiles, services::Services,
  storage::SOrganizations, use_cases::uc_002::new_import,
};
use rust_decimal::Decimal;
use test_init::init;
use utils::json::JsonParams;
use uuid::Uuid;

fn csv_to_json(path: &str) -> Vec<JsonValue> {
  let mut changes = Vec::with_capacity(1_000_000);

  let mut reader = ReaderBuilder::new().delimiter(b',').trim(Trim::All).from_path(path).unwrap();

  // ***

  let mut iter = reader.records();

  let mut iter_next = iter.next();

  while let Some(ref record) = iter_next {
    let record = record.as_ref().unwrap();

    let record_id = &record[0];
    if record_id.is_empty() {
      continue;
    }

    

  }

  // ***

  for record in reader.records() {
    let record = record.unwrap();

    let record_id = &record[0];
    if record_id.is_empty() {
      continue;
    }

    let label = &record[1];

    let goods_id = record[2].parse::<u128>().unwrap();
    let goods_id = Uuid::from_u128(goods_id);

    let uom = &record[3];

    let qty = record[4].parse::<Decimal>().unwrap();

    let date = &record[5];
    let date = format!("{}-{}-{}", &date[6..=9], &date[3..=4], &date[0..=1]);

    let counterparty = ID::from(&record[6]);

    let recipient = match &record[7] {
      "Склад Midas Plastics" => Uuid::from_u128(0),
      _ => unreachable!("unknown recipient"),
    };

    let _return = &record[8]; // why it needed?

    let object = object! {
        date: date,
        counterparty: counterparty.to_string(),
        storage: recipient.to_string(),
        goods: [
            {
                goods: goods_id.to_string(),
                goods_label: label,
                uom: uom,
                qty: qty.to_string(),
                cost: 0,
                _tid: "",
            },
        ]
    };
    changes.push(object);
  }
  changes
}

#[actix_web::test]
async fn app_csv_to_json() {
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

  // write data to db from csv file
  let records = csv_to_json("/Users/george/Nae-backend/src/use_cases/Copy_Dista.csv");
  let oid = ID::from("Midas-plastics");

  for record in records {

    let req = TestRequest::post()
    .uri(&format!("/api/docs?oid={}&ctx=warehouse,receive", oid.to_base64()))
    .set_payload(record.dump())
    .insert_header(ContentType::json())
    .to_request();

    let response = call_and_read_body(&app, req).await;
    println!("response: {response:?}");
  }

//report storage1
let from_date = "2022-12-20";
let till_date = "2022-12-22";

let req = TestRequest::get()
    .uri(&format!(
        "/api/inventory?oid={}&ctx=report&storage={}&from_date={}&till_date={}",
        oid,
        Uuid::from_u128(0).to_string(),
        from_date,
        till_date,
    ))
    .to_request();

let response = call_and_read_body(&app, req).await;
// println!("RESPONSE: {response:#?}\n");

let result: serde_json::Value = serde_json::from_slice(&response).unwrap();
println!("REPORT1: {result:#?}");

}
