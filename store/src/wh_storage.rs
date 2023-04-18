use super::{
  check_date_store_batch::CheckDateStoreBatch,
  date_type_store_batch_id::DateTypeStoreBatchId,
  db::Db,
  elements::{CheckpointTopology, OpMutation, OrderedTopology},
  error::WHError,
  store_date_type_batch_id::StoreDateTypeBatchId,
};
use rocksdb::{ColumnFamilyDescriptor, Options, DB};
use std::{path::Path, sync::Arc};

#[derive(Clone)]
pub struct WHStorage {
  pub database: Db,
}

impl WHStorage {
  pub fn mutate(&self, ops: &Vec<OpMutation>) -> Result<(), WHError> {
    Ok(self.database.record_ops(ops)?)
  }

  pub fn open<P: AsRef<Path>>(path: P) -> Result<Self, WHError> {
    std::fs::create_dir_all(&path).map_err(|e| WHError::new("Can't create folder for WHStorage"))?;

    let mut opts = Options::default();
    let mut cfs = Vec::new();

    let cf_names: Vec<&str> = vec![
      StoreDateTypeBatchId::cf_name(),
      DateTypeStoreBatchId::cf_name(),
      CheckDateStoreBatch::cf_name(),
      // CheckBatchStoreDate::cf_name(),
    ];

    for name in cf_names {
      let cf = ColumnFamilyDescriptor::new(name, opts.clone());
      cfs.push(cf);
    }

    opts.create_if_missing(true);
    opts.create_missing_column_families(true);

    let tmp_db = DB::open_cf_descriptors(&opts, &path, cfs)
      .expect("Can't open database in settings.database.inventory");
    let inner_db = Arc::new(tmp_db);

    let checkpoint_topologies: Vec<Box<dyn CheckpointTopology + Sync + Send>> = vec![
      Box::new(CheckDateStoreBatch { db: inner_db.clone() }),
      // Box::new(CheckBatchStoreDate { db: inner_db.clone() }),
    ];

    let ordered_topologies: Vec<Box<dyn OrderedTopology + Sync + Send>> = vec![
      Box::new(StoreDateTypeBatchId { db: inner_db.clone() }),
      Box::new(DateTypeStoreBatchId { db: inner_db.clone() }),
    ];

    let outer_db = Db {
      db: inner_db,
      checkpoint_topologies: Arc::new(checkpoint_topologies),
      ordered_topologies: Arc::new(ordered_topologies),
    };

    Ok(WHStorage { database: outer_db })
  }
}
