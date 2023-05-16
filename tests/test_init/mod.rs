use std::sync::Arc;
use tempfile::{tempdir, TempDir};

use nae_backend::animo::Animo;
use nae_backend::animo::Topology;
use nae_backend::animo::{db::AnimoDB, memory::Memory};
use nae_backend::commutator::Application;
use nae_backend::settings::Settings;
use nae_backend::warehouse::store_aggregation_topology::WHStoreAggregationTopology;
use nae_backend::warehouse::store_topology::WHStoreTopology;
use store::error::WHError;
use store::process_records::process_record;

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
