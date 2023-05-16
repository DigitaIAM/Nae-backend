mod test_init;

use json::{array, object};
use serde_json::from_str;
use std::io;
use std::sync::Arc;
use test_init::init;

use crate::test_init::create_record;
use nae_backend::commutator::Application;
use nae_backend::memories::MemoriesInFiles;
use nae_backend::storage::Workspaces;
use service::{Context, Services};
use store::process_records::process_record;

#[actix_web::test]
async fn recalculating_many_ops() {
  let (tmp_dir, settings, db) = init();

  let (mut application, events_receiver) = Application::new(Arc::new(settings), Arc::new(db))
    .await
    .map_err(|e| io::Error::new(io::ErrorKind::Unsupported, e))
    .unwrap();

  let storage = Workspaces::new(tmp_dir.path().join("companies"));
  application.storage = Some(storage.clone());

  application.register(MemoriesInFiles::new(application.clone(), "memories", storage.clone()));
  application.register(nae_backend::inventory::service::Inventory::new(application.clone()));

  // write ops
  let mut data = new_data();
  create_record(&application, data).unwrap();

  // get result
  let oid = "yjmgJUmDo_kn9uxVi8s9Mj9mgGRJISxRt63wT46NyTQ";

  let stock_ctx = vec!["warehouse", "stock"];

  let mut filter = object! {};

  let params = object! {oid: oid, ctx: stock_ctx.clone(), filter: filter.clone()};

  let result = application.service("memories").find(Context::local(), params).unwrap();

  // println!("test_result: {:#?}", result);

  let data = result["data"][2].clone();
  let qty = data["_balance"]["qty"].as_str().unwrap();

  assert_eq!(300.0, from_str::<f64>(qty).unwrap());

  // list of batches with balances
  let goods = data["_uuid"].as_str().unwrap();

  filter["goods"] = goods.into();

  let params = object! {oid: oid, ctx: stock_ctx, filter: filter.clone()};

  let batches = application.service("memories").find(Context::local(), params).unwrap();

  // println!("batches: {:#?}", batches);

  println!("batches&qts");
  for b in batches["data"].members() {
    println!(
      "store {}\nbatch {}\nqty {}",
      b["storage"]["name"], b["batch"]["barcode"], b["_balance"]["qty"]
    );
  }

  assert_eq!(batches["data"].len(), 2);

  // // operations
  // let storage = batches["data"][0]["storage"]["_uuid"].as_str().unwrap();
  // let batch_id = batches["data"][0]["batch"]["id"].as_str().unwrap();
  // let batch_date = batches["data"][0]["batch"]["date"].as_str().unwrap();
  // // println!("storage: {:#?}", storage);
  //
  // filter["dates"] =
  //   object! {"from": "2022-01-01", "till": chrono::offset::Utc::now().date_naive().to_string()};
  // filter["storage"] = storage.into();
  // filter["batch_id"] = batch_id.into();
  // filter["batch_date"] = batch_date.into();
  //
  // let inventory_ctx = vec!["warehouse", "inventory"];
  // let params = array![object! {oid: oid, ctx: inventory_ctx.clone(), filter: filter.clone()}];
  // // println!("filter: {:#?}", filter);
  //
  // let report = application.service("inventory").find(Context::local(), params).unwrap();
  // // println!("report: {:#?}", report);
}

fn new_data<'a>() -> Vec<(Vec<&'a str>, Vec<&'a str>)> {
  let mut data = Vec::new();

  let transfer_ctx = vec!["warehouse", "transfer"];
  let receive_ctx = vec!["warehouse", "receive"];

  // transfer1
  let data0_vec = vec![
    "46",
    "производство",
    "Краситель полипропиленовый белый PP/F11481 Aksoy(Турция) (25кг)",
    "285",
    "кг",
    "25",
    "",
    "26.01.2023",
    "склад",
    "цех",
    "",
    "",
  ];

  data.push((data0_vec, transfer_ctx.clone()));

  // transfer2
  let data1_vec = vec![
    "49",
    "производство",
    "Краситель полипропиленовый белый PP/F11481 Aksoy(Турция) (25кг)",
    "285",
    "кг",
    "75",
    "",
    "26.01.2023",
    "склад",
    "цех",
    "",
    "",
  ];

  data.push((data1_vec, transfer_ctx.clone()));

  // transfer3
  let data2_vec = vec![
    "55",
    "производство",
    "Краситель полипропиленовый белый PP/F11481 Aksoy(Турция) (25кг)",
    "285",
    "кг",
    "25",
    "",
    "27.01.2023",
    "склад",
    "цех",
    "",
    "",
  ];

  data.push((data2_vec, transfer_ctx));

  // receive
  let data3_vec = vec![
    "L3Q",
    "производство",
    "Краситель полипропиленовый белый PP/F11481 Aksoy(Турция) (25кг)",
    "965",
    "кг",
    "300.0",
    "",
    "20.01.2023",
    "ИП ООО Telko Solution",
    "склад",
  ];

  data.push((data3_vec, receive_ctx));

  data
}
