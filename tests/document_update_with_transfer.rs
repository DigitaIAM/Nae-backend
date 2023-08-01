mod test_init;

use crate::test_init::{document_create, document_update, goods, init, receive, store};
use chrono::Utc;
use json::object;
use json::JsonValue;
use nae_backend::commutator::Application;
use nae_backend::memories::stock::find_items;
use nae_backend::memories::MemoriesInFiles;
use nae_backend::storage::Workspaces;
use service::utils::json::JsonParams;
use service::Services;
use std::str::FromStr;
use std::sync::Arc;
use store::batch::Batch;
use store::elements::dt;
use store::elements::ToJson;
use store::GetWarehouse;
use tantivy::HasLen;
use values::constants::_UUID;
use values::ID;

#[actix_web::test]
async fn update_document_with_transfer() {
  std::env::set_var("RUST_LOG", "debug,tantivy=off");
  env_logger::init();

  let (tmp_dir, settings, db) = init();

  let wss = Workspaces::new(tmp_dir.path().join("companies"));

  let (mut app, _) = Application::new(Arc::new(settings), Arc::new(db), wss).await.unwrap();

  app.register(MemoriesInFiles::new(app.clone(), "memories"));
  app.register(nae_backend::inventory::service::Inventory::new(app.clone()));

  let s1 = store(&app, "s1");
  let s2 = store(&app, "s2");
  let s3 = store(&app, "s3");
  let g1 = goods(&app, "g1");

  // create receive document
  let receive_doc = object! {
    date: "2023-01-01",
    counterparty: s1.to_string(),
    storage: s2.to_string(),
    number: "1",
  };

  let d1 = document_create(&app, receive_doc.clone(), vec!["warehouse", "receive", "document"]);

  // create receive operation
  let receive_obj = object! {
    document: d1["_id"].to_string(),
    goods: g1.to_string(),
    qty: object! {number: "3.0"},
    cost: object! {number: "0.3"},
  };

  let r1 = document_create(&app, receive_obj, vec!["warehouse", "receive"]);
  // log::debug!("receive_data: {:#?}", r1.dump());

  let r1_batch = Batch { id: r1["_uuid"].uuid().unwrap(), date: dt("2023-01-01").unwrap() };

  // create transfer document
  let mut transfer_doc = object! {
    date: "2023-01-02",
    from: s2.to_string(),
    into: s3.to_string(),
    number: "1",
  };

  let d2 = document_create(&app, transfer_doc.clone(), vec!["warehouse", "transfer", "document"]);

  // create transfer operation
  let transfer_obj = object! {
    document: d2["_id"].to_string(),
    goods: g1.to_string(),
    qty: object! {number: "3.0"},
    cost: object! {number: "0.3"},
  };

  let t1 = document_create(&app, transfer_obj, vec!["warehouse", "transfer"]);

  // change document
  transfer_doc["date"] = "2023-01-03".into();

  let d3 = document_update(
    &app,
    d2["_uuid"].string(),
    transfer_doc,
    vec!["warehouse", "transfer", "document"],
  );

  // get stock
  let wsid = ID::from("yjmgJUmDo_kn9uxVi8s9Mj9mgGRJISxRt63wT46NyTQ");

  let ws = app.wss.get(&wsid);

  let balances = app.warehouse().database.get_balance_for_all(Utc::now()).unwrap();

  // println!("balances: {balances:#?}");

  let filters = object! {};

  let result = find_items(&ws, &balances, &filters, 0).unwrap();

  println!("result: {result:#?}");

  let stock: Vec<(String, String)> = result["data"]
    .members()
    .map(|o| {
      (
        // o["batch"]["date"].string(),
        // o["storage"][_UUID].string(),
        o["_balance"]["qty"].string(),
        o["_balance"]["cost"].string(),
      )
    })
    .filter(|(q, c)| !q.is_empty() || !c.is_empty())
    .collect();

  // println!("stock: {stock:#?}");

  assert_eq!(stock[0], ("3.0".into(), "0.3".into()));
}
