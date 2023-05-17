mod test_init;

use chrono::{DateTime, Utc};
use json::{array, object};
use serde_json::from_str;
use std::io;
use std::str::FromStr;
use std::sync::Arc;
use test_init::init;
use uuid::Uuid;

use crate::test_init::create_record;
use nae_backend::commutator::Application;
use nae_backend::memories::MemoriesInFiles;
use nae_backend::storage::Workspaces;
use service::utils::json::JsonParams;
use service::{Context, Services};
use store::balance::Cost;
use store::batch::Batch;
use store::elements::{dt, Goods, Mode, Qty, Store};
use store::operations::{InternalOperation, OpMutation};
use store::process_records::process_record;
use store::GetWarehouse;

const WID: &str = "yjmgJUmDo_kn9uxVi8s9Mj9mgGRJISxRt63wT46NyTQ";

#[actix_web::test]
async fn recalculating_many_ops() {
  let (tmp_dir, settings, db) = init();

  let (mut app, _) = Application::new(Arc::new(settings), Arc::new(db))
    .await
    .map_err(|e| io::Error::new(io::ErrorKind::Unsupported, e))
    .unwrap();

  let storage = Workspaces::new(tmp_dir.path().join("companies"));
  app.storage = Some(storage.clone());

  app.register(MemoriesInFiles::new(app.clone(), "memories", storage.clone()));
  app.register(nae_backend::inventory::service::Inventory::new(app.clone()));

  let s1 = store(&app, "s1");
  let s2 = store(&app, "s2");
  let g1 = goods(&app, "g1");

  // transfer 26.01.2023 склад > цех 25
  transfer(&app, "2023-01-26", s1, s2, g1, 25.into());

  // transfer 26.01.2023 склад > цех 75
  transfer(&app, "2023-01-26", s1, s2, g1, 75.into());

  // transfer 27.01.2023 склад > цех 75
  transfer(&app, "2023-01-26", s1, s2, g1, 25.into());

  // receive 20.01.2023 склад 300
  receive(&app, "2023-01-20", s1, g1, 300.into(), 30.into());
  // get result

  let balances = app.warehouse().database.get_balance_for_all(Utc::now()).unwrap();
  print!("balances: {balances:?}");
}

fn create(app: &Application, name: &str, ctx: Vec<&str>) -> Uuid {
  let data = app
    .service("memories")
    .create(
      Context::local(),
      json::object! {
        name: name
      },
      json::object! {
        oid: WID,
        ctx: ctx,
      },
    )
    .unwrap();

  data["_uuid"].uuid().unwrap()
}

fn store(app: &Application, name: &str) -> Uuid {
  create(app, name, vec!["warehouse", "storage"])
}

fn goods(app: &Application, name: &str) -> Uuid {
  create(app, name, vec!["warehouse", "goods"])
}

fn receive(app: &Application, date: &str, store: Store, goods: Goods, qty: Qty, cost: Cost) -> Uuid {
  let mut ops = vec![];

  let id = Uuid::new_v4();
  let date = dt(date).unwrap();
  ops.push(OpMutation {
    id: id.clone(),
    date: date.clone(),
    store,
    transfer: None,
    goods,
    batch: Batch { id, date },
    before: None,
    after: Some(InternalOperation::Receive(qty, cost)),
    is_dependent: false,
    dependant: vec![],
  });

  app.warehouse().mutate(&ops).unwrap();

  id
}

fn transfer(
  app: &Application,
  date: &str,
  from: Store,
  into: Store,
  goods: Goods,
  qty: Qty,
) -> Uuid {
  let mut ops = vec![];

  let id = Uuid::new_v4();
  ops.push(OpMutation {
    id,
    date: dt(date).unwrap(),
    store: from,
    transfer: Some(into),
    goods,
    batch: Batch::no(),
    before: None,
    after: Some(InternalOperation::Issue(qty, Cost::ZERO, Mode::Auto)),
    is_dependent: false,
    dependant: vec![],
  });

  app.warehouse().mutate(&ops).unwrap();

  id
}
