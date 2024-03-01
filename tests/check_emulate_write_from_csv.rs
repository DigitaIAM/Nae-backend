mod test_init;

use json::{array, object};
use serde_json::from_str;
use std::io;
use std::sync::Arc;

use crate::test_init::{create_record, init};
use nae_backend::commutator::Application;
use nae_backend::memories::MemoriesInFiles;
use nae_backend::storage::Workspaces;
use service::utils::json::JsonParams;
use service::{Context, Services};
use store::process_records::process_warehouse_record;
use store::qty::Qty;
use store::GetWarehouse;
use values::c::_UUID;

#[actix_web::test]
async fn check_emulate_write_from_csv() {
  let (tmp_dir, settings, db) = init();

  let wss = Workspaces::new(tmp_dir.path().join("companies"));

  let (mut app, _) = Application::new(Arc::new(settings), Arc::new(db), wss).await.unwrap();

  let storage = Workspaces::new(tmp_dir.path().join("companies"));

  app.register(MemoriesInFiles::new(app.clone(), "memories"));
  app.register(nae_backend::inventory::service::Inventory::new(app.clone()));

  // write ops
  let mut data = new_data();
  create_record(&app, data).unwrap();

  // get result
  let oid = "yjmgJUmDo_kn9uxVi8s9Mj9mgGRJISxRt63wT46NyTQ";

  let stock = vec!["warehouse", "stock"];

  let mut filter = object! {};

  let params = object! {oid: oid, ctx: stock.clone(), filter: filter.clone()};

  let result = app.service("memories").find(Context::local(), params).unwrap();

  // log::debug!("test_result: {:#?}", result);

  let data = result["data"][0].clone();
  let qty: Qty = data["_balance"]["qty"].clone().try_into().unwrap();

  // assert_eq!(1.0, from_str::<f64>(qty).unwrap());

  // list of batches with balances
  let goods = data[_UUID].as_str().unwrap();

  filter["goods"] = goods.into();

  let params = object! {oid: oid, ctx: stock, filter: filter.clone()};

  let batches = app.service("memories").find(Context::local(), params).unwrap();

  // log::debug!("batches: {:#?}", batches);

  // operations
  let storage = batches["data"][0]["storage"].clone();
  let batch_id = batches["data"][0]["batch"]["id"].as_str().unwrap();
  let batch_date = batches["data"][0]["batch"]["date"].as_str().unwrap();
  // log::debug!("storage: {:#?}", storage);

  filter["dates"] =
    object! {"from": "2022-01-01", "till": chrono::offset::Utc::now().date_naive().to_string()};
  filter["storage"] = storage[_UUID].as_str().unwrap().into();
  filter["batch_id"] = batch_id.into();
  filter["batch_date"] = batch_date.into();

  let inventory_ctx = vec!["warehouse", "inventory"];
  let params = array![object! {oid: oid, ctx: inventory_ctx.clone(), filter: filter.clone()}];

  let balances = app.warehouse().database.get_balance_for_all(chrono::Utc::now()).unwrap();
  // log::debug!("balances: {balances:#?}");

  assert_eq!(1, balances.len());

  assert_eq!("цех".to_string(), storage["name"].string());
}

fn new_data<'a>() -> Vec<(Vec<&'a str>, Vec<&'a str>)> {
  let mut data = Vec::new();

  let transfer = vec!["warehouse", "transfer"];
  let receive = vec!["warehouse", "receive"];

  // transfer
  let data0_vec = vec![
    "167",
    "инструменты",
    "Штангенциркуль цифровой",
    "408",
    "шт",
    "1",
    "",
    "14.03.2023",
    "склад",
    "цех",
    "",
    "",
  ];

  data.push((data0_vec, transfer.clone()));

  // receive
  let data1_vec = vec![
    "LP1",
    "инструменты",
    "Штангенциркуль цифровой",
    "1279",
    "шт",
    "1.0",
    "",
    "13.03.2023",
    "AMETOV RUSTEM SHEVKETOVICH",
    "снабжение Бегбудиев Носир",
    "",
    "",
  ];

  data.push((data1_vec, receive));

  // new transfer
  let data2_vec = vec![
    "142",
    "инструменты",
    "Штангенциркуль цифровой",
    "408",
    "шт",
    "1",
    "",
    "14.03.2023",
    "снабжение Бегбудиев Носир",
    "склад",
    "",
    "",
  ];

  data.push((data2_vec, transfer));

  data
}
