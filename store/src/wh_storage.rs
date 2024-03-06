use crate::checkpoints::CheckpointTopology;
use crate::operations::OpMutation;
use crate::ordered_topology::OrderedTopology;
use crate::topologies::store_batch_date_type_id::StoreBatchDateTypeId;
use crate::topologies::store_goods_date_type_id_batch::StoreGoodsDateTypeIdBatch;
use crate::{
  checkpoints::check_date_store_batch::CheckDateStoreBatch, db::Db, error::WHError,
  topologies::date_type_store_batch_id::DateTypeStoreBatchId,
  topologies::store_date_type_batch_id::StoreDateTypeBatchId,
};
use rocksdb::{ColumnFamilyDescriptor, Options, DB};
use std::{path::Path, sync::Arc};

#[derive(Clone)]
pub struct WHStorage {
  pub database: Db,
}

impl WHStorage {
  pub fn mutate(&self, ops: &Vec<OpMutation>) -> Result<(), WHError> {
    self.database.record_ops(ops)
  }

  pub fn open<P: AsRef<Path>>(path: P) -> Result<Self, WHError> {
    std::fs::create_dir_all(&path)
      .map_err(|_e| WHError::new("Can't create folder for WHStorage"))?;

    // let prefix_extractor = SliceTransform::create_fixed_prefix(1); // refuse this idea because we have many different prefix len
    let mut opts = Options::default();
    opts.create_if_missing(true);

    let cfs = match DB::list_cf(&opts, &path) {
      Ok(list) => list,
      Err(_) => Vec::new(),
    };
    let is_empty = cfs.is_empty();
    let cf_descriptors = cfs.into_iter().map(|name| {
      let cf_opts = Options::default();
      ColumnFamilyDescriptor::new(name, cf_opts)
    });

    // opts.create_missing_column_families(true);

    let tmp_db = DB::open_cf_descriptors(&opts, &path, cf_descriptors)
      .expect("Can't open database in settings.database.inventory");

    if is_empty {
      let cf_names: Vec<&str> = vec![
        StoreBatchDateTypeId::cf_name(),
        StoreDateTypeBatchId::cf_name(),
        DateTypeStoreBatchId::cf_name(),
        StoreGoodsDateTypeIdBatch::cf_name(),
        CheckDateStoreBatch::cf_name(),
        // CheckBatchStoreDate::cf_name(),
      ];

      for name in cf_names {
        let _ = tmp_db.create_cf(name, &opts);
      }
    }

    let inner_db = Arc::new(tmp_db);

    let checkpoint_topologies: Vec<Box<dyn CheckpointTopology + Sync + Send>> = vec![
      Box::new(CheckDateStoreBatch { db: inner_db.clone() }),
      // Box::new(CheckBatchStoreDate { db: inner_db.clone() }),
    ];

    let ordered_topologies: Vec<Box<dyn OrderedTopology + Sync + Send>> = vec![
      Box::new(StoreBatchDateTypeId { db: inner_db.clone() }),
      Box::new(StoreGoodsDateTypeIdBatch { db: inner_db.clone() }),
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
