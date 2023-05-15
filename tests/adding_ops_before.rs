mod test_init;

use json::object;
use serde_json::from_str;
use std::io;
use std::sync::Arc;
use test_init::init;

use nae_backend::commutator::Application;
use nae_backend::memories::MemoriesInFiles;
use nae_backend::storage::Workspaces;
use service::{Context, Services};
use store::process_records::process_record;

#[actix_web::test]
async fn inventory_test() {
  let (tmp_dir, settings, db) = init();

  let (mut application, events_receiver) = Application::new(Arc::new(settings), Arc::new(db))
    .await
    .map_err(|e| io::Error::new(io::ErrorKind::Unsupported, e))
    .unwrap();

  let storage = Workspaces::new(tmp_dir.path().join("companies"));
  application.storage = Some(storage.clone());

  application.register(MemoriesInFiles::new(application.clone(), "memories", storage.clone()));
  application.register(nae_backend::inventory::service::Inventory::new(application.clone()));

  let oid = "yjmgJUmDo_kn9uxVi8s9Mj9mgGRJISxRt63wT46NyTQ";

  // transfer
  let data0_vec = [
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
  ]
  .to_vec();

  let data0 = csv::StringRecord::from(data0_vec);

  let transfer_ctx = ["warehouse", "transfer"].to_vec();

  process_record(&application, &transfer_ctx, data0).unwrap();

  // receive
  let data1_vec = [
    "LP1",
    "инструменты",
    "Штангенциркуль цифровой",
    "1279",
    "шт",
    "1.0",
    "",
    "13.03.2023",
    "AMETOV RUSTEM SHEVKETOVICH",
    "Гагарина 36",
  ]
  .to_vec();

  let data1 = csv::StringRecord::from(data1_vec);

  let receive_ctx = ["warehouse", "receive"].to_vec();

  process_record(&application, &receive_ctx, data1).unwrap();

  // get result
  let stock_ctx = ["warehouse", "stock"].to_vec();
  let filter = object! {};
  let params = object! {oid: oid, ctx: stock_ctx, filter: filter};

  let result = application.service("memories").find(Context::local(), params).unwrap();

  // println!("test_result: {:#?}", result["data"][3]["_balance"]["qty"]);

  let qty = result["data"][3]["_balance"]["qty"].as_str().unwrap();

  assert_eq!(1.0, from_str::<f64>(qty).unwrap());
}
