use json::JsonValue;
use std::sync::Arc;
use tempfile::{tempdir, TempDir};
use uuid::Uuid;

use nae_backend::animo::Animo;
use nae_backend::animo::Topology;
use nae_backend::animo::{db::AnimoDB, memory::Memory};
use nae_backend::commutator::Application;
use nae_backend::settings::Settings;
use nae_backend::warehouse::store_aggregation_topology::WHStoreAggregationTopology;
use nae_backend::warehouse::store_topology::WHStoreTopology;
use service::utils::json::JsonParams;
use service::{Context, Services};
use store::balance::Cost;
use store::batch::Batch;
use store::elements::{dt, Goods, Mode, Qty, Store};
use store::error::WHError;
use store::operations::{InternalOperation, OpMutation};
use store::process_records::process_record;
use store::GetWarehouse;

const WID: &str = "yjmgJUmDo_kn9uxVi8s9Mj9mgGRJISxRt63wT46NyTQ";

pub fn init() -> (TempDir, Settings, AnimoDB) {
  std::env::set_var("RUST_LOG", "actix_web=debug,nae_backend=debug");
  let _ = env_logger::builder().is_test(true).try_init();

  let tmp_dir = tempdir().unwrap();
  let tmp_path = tmp_dir.path().to_str().unwrap();

  let settings = Settings::test(tmp_path.into());

  let mut db: AnimoDB = Memory::init(tmp_path.into()).unwrap();
  let mut animo = Animo::default();

  let wh_store = Arc::new(WHStoreTopology());

  animo.register_topology(Topology::WarehouseStore(wh_store.clone()));
  animo.register_topology(Topology::WarehouseStoreAggregation(Arc::new(
    WHStoreAggregationTopology(wh_store.clone()),
  )));

  db.register_dispatcher(Arc::new(animo)).unwrap();
  (tmp_dir, settings, db)
}

pub fn create_record(
  app: &Application,
  records: Vec<(Vec<&str>, Vec<&str>)>,
) -> Result<(), WHError> {
  for (record, ctx) in records {
    let data = csv::StringRecord::from(record);

    process_record(app, &ctx, data)?;
  }

  Ok(())
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

pub fn store(app: &Application, name: &str) -> Uuid {
  create(app, name, vec!["warehouse", "storage"])
}

pub fn goods(app: &Application, name: &str) -> Uuid {
  create(app, name, vec!["warehouse", "goods"])
}

pub fn document_create(app: &Application, data: JsonValue, ctx: Vec<&str>) -> JsonValue {
  let data = app
    .service("memories")
    .create(
      Context::local(),
      data,
      json::object! {
        oid: WID,
        ctx: ctx,
      },
    )
    .unwrap();

  data
}

pub fn document_update(app: &Application, id: String, data: JsonValue, ctx: Vec<&str>) -> JsonValue {
  let data = app
    .service("memories")
    .patch(
      Context::local(),
      id,
      data,
      json::object! {
        oid: WID,
        ctx: ctx,
      },
    )
    .unwrap();

  data
}

pub fn receive(
  app: &Application,
  date: &str,
  store: Store,
  goods: Goods,
  qty: Qty,
  cost: Cost,
) -> Uuid {
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

pub fn delete(
  app: &Application,
  date: &str,
  store: Store,
  into: Option<Store>,
  goods: Goods,
  id: Uuid,
  batch: Batch,
  before: InternalOperation,
) {
  let mut ops = vec![];

  let date = dt(date).unwrap();
  ops.push(OpMutation {
    id: id.clone(),
    date: date.clone(),
    store,
    transfer: into,
    goods,
    batch,
    before: Some(before),
    after: None,
    is_dependent: false,
    dependant: vec![],
  });

  app.warehouse().mutate(&ops).unwrap();
}

pub fn transfer(
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
