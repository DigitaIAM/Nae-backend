use super::{
  check_batch_store_date::CheckBatchStoreDate, check_date_store_batch::CheckDateStoreBatch,
  date_type_store_goods_id::DateTypeStoreGoodsId, store_date_type_goods_id::StoreDateTypeGoodsId,
  CheckpointTopology, Db, OpMutation, OrderedTopology, WHError,
};
use chrono::DateTime;
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
      StoreDateTypeGoodsId::cf_name(),
      DateTypeStoreGoodsId::cf_name(),
      CheckDateStoreBatch::cf_name(),
      CheckBatchStoreDate::cf_name(),
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
      Box::new(CheckBatchStoreDate { db: inner_db.clone() }),
    ];

    let ordered_topologies: Vec<Box<dyn OrderedTopology + Sync + Send>> = vec![
      Box::new(StoreDateTypeGoodsId { db: inner_db.clone() }),
      Box::new(DateTypeStoreGoodsId { db: inner_db.clone() }),
    ];

    let outer_db = Db {
      db: inner_db,
      checkpoint_topologies: Arc::new(checkpoint_topologies),
      ordered_topologies: Arc::new(ordered_topologies),
    };

    Ok(WHStorage { database: outer_db })
  }
}
