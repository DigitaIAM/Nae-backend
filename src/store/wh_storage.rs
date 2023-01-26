use super::{
  check_batch_store_date::CheckBatchStoreDate, check_date_store_batch::CheckDateStoreBatch,
  date_type_store_goods_id::DATE_TYPE_STORE_BATCH_ID,
  store_date_type_goods_id::STORE_DATE_TYPE_BATCH_ID, Checkpoint, Db, OpMutation, WHError,
};
use rocksdb::{ColumnFamilyDescriptor, Options, DB};
use std::{path::Path, sync::Arc};

#[derive(Clone)]
pub struct WHStorage {
  pub database: Db,
}

impl WHStorage {
  pub fn receive_operations(&self, ops: &Vec<OpMutation>) -> Result<(), WHError> {
    Ok(self.database.record_ops(ops)?)
  }

  pub fn open<P: AsRef<Path>>(path: P) -> Result<Self, WHError> {
    std::fs::create_dir_all(&path).map_err(|e| WHError::new("Can't create folder for WHStorage"))?;

    let mut opts = Options::default();
    let mut cfs = Vec::new();

    let mut cf_names: Vec<&str> = vec![
      STORE_DATE_TYPE_BATCH_ID,
      DATE_TYPE_STORE_BATCH_ID,
      CheckDateStoreBatch::cf_name(),
      CheckBatchStoreDate::cf_name(),
    ];
    // checkpoint_topologies.iter().for_each(|t| cf_names.push(t.cf_name()));

    for name in cf_names {
      let cf = ColumnFamilyDescriptor::new(name, opts.clone());
      cfs.push(cf);
    }

    opts.create_if_missing(true);
    opts.create_missing_column_families(true);

    let database = DB::open_cf_descriptors(&opts, &path, cfs)
      .expect("Can't open database in settings.database.inventory");
    let database = Arc::new(database);

    let checkpoint_topologies: Vec<Box<dyn Checkpoint + Sync + Send>> = vec![
      Box::new(CheckDateStoreBatch { db: database.clone() }),
      Box::new(CheckBatchStoreDate { db: database.clone() }),
    ];

    let db = Db { db: database, checkpoint_topologies: Arc::new(checkpoint_topologies) };

    Ok(WHStorage { database: db })
  }
}
