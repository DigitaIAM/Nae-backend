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
use store::process_records::process_record;
use store::GetWarehouse;
use values::constants::_UUID;
use values::ID;

#[actix_web::test]
async fn check_csv_incomplete_data() {
  let (tmp_dir, settings, db) = init();

  let wss = Workspaces::new(tmp_dir.path().join("companies"));

  let (mut app, _) = Application::new(Arc::new(settings), Arc::new(db), wss).await.unwrap();

  let storage = Workspaces::new(tmp_dir.path().join("companies"));

  app.register(MemoriesInFiles::new(app.clone(), "memories"));
  app.register(nae_backend::inventory::service::Inventory::new(app.clone()));

  // write ops
  let mut data = new_data();
  create_record(&app, data).unwrap();

  let balances = app.warehouse().database.get_balance_for_all(chrono::Utc::now()).unwrap();
  log::debug!("balances: {balances:#?}");

  assert_eq!(0, balances.len());
}

fn new_data<'a>() -> Vec<(Vec<&'a str>, Vec<&'a str>)> {
  let mut data = Vec::new();

  let receive = vec!["warehouse", "receive"];

  // receive
  let data1_vec = vec!["1", "", "", "шт", "", "13.03.2023"];

  data.push((data1_vec, receive));

  data
}
