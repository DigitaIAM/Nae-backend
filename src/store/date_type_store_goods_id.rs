use super::{
  Db, KeyValueStore, OpMutation, Op, Store, WHError, WareHouse, DATE_TYPE_STORE_BATCH_ID, UUID_MAX,
  UUID_NIL,
};
use chrono::{DateTime, Utc};
use rocksdb::{ColumnFamilyDescriptor, IteratorMode, Options, ReadOptions};

pub struct DateTypeStoreGoodsId();

impl WareHouse for DateTypeStoreGoodsId {
  fn get_ops(
    &self,
    start_d: DateTime<Utc>,
    end_d: DateTime<Utc>,
    wh: Store,
    db: &Db,
  ) -> Result<Vec<Op>, WHError> {
    let start_date = start_d.timestamp() as u64;
    let from: Vec<u8> = start_date
      .to_be_bytes()
      .iter()
      .chain(0_u8.to_be_bytes().iter())
      .chain(UUID_NIL.as_bytes().iter())
      .map(|b| *b)
      .collect();

    let end_date = end_d.timestamp() as u64;
    let till: Vec<u8> = end_date
      .to_be_bytes()
      .iter()
      .chain(u8::MAX.to_be_bytes().iter())
      .chain(UUID_MAX.as_bytes().iter())
      .map(|b| *b)
      .collect();

    let mut options = ReadOptions::default();
    options.set_iterate_range(from..till);

    if let Some(handle) = db.db.cf_handle(DATE_TYPE_STORE_BATCH_ID) {
      let iter = db.db.iterator_cf_opt(&handle, options, IteratorMode::Start);

      let mut res = Vec::new();

      for item in iter {
        let (_, value) = item?;
        let op = serde_json::from_slice(&value)?;
        res.push(op);
      }

      Ok(res)
    } else {
      Err(WHError::new("There are no operations in db"))
    }
  }

  fn put_op(&self, op: &OpMutation, db: &Db) -> Result<(), WHError> {
    if let Some(cf) = db.db.cf_handle(DATE_TYPE_STORE_BATCH_ID) {
      db.db.put_cf(&cf, op.date_type_store_batch_id(), op.value()?)?;

      Ok(())
    } else {
      Err(WHError::new("Can't get cf from db in fn put_op"))
    }
  }

  fn create_cf(&self, opts: Options) -> ColumnFamilyDescriptor {
    ColumnFamilyDescriptor::new(DATE_TYPE_STORE_BATCH_ID, opts)
  }
}
