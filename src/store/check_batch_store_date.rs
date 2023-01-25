use std::sync::Arc;

use super::{balance::BalanceForGoods, Balance, Checkpoint, Db, Op, Store, WHError};
use chrono::{DateTime, Utc};
use rocksdb::{BoundColumnFamily, DB};

const CF_NAME: &str = "cf_checkpoint_batch_store_date";
pub struct CheckBatchStoreDate {
  pub db: Arc<DB>,
}

impl CheckBatchStoreDate {
  pub fn cf_name() -> &'static str {
    CF_NAME
  }

  fn cf(&self) -> Result<Arc<BoundColumnFamily>, WHError> {
    if let Some(cf) = self.db.cf_handle(CheckBatchStoreDate::cf_name()) {
      Ok(cf)
    } else {
      Err(WHError::new("can't get CF"))
    }
  }
}

impl Checkpoint for CheckBatchStoreDate {
  fn key(&self, op: &Op, date: DateTime<Utc>) -> Vec<u8> {
    [].iter()
      .chain((op.batch.date.timestamp() as u64).to_be_bytes().iter())
      .chain(op.batch.id.as_bytes().iter())
      .chain(op.goods.as_bytes().iter())
      .chain(op.store.as_bytes().iter())
      .chain((date.timestamp() as u64).to_be_bytes().iter())
      .map(|b| *b)
      .collect()
  }

  fn get_balance(&self, key: &Vec<u8>) -> Result<BalanceForGoods, WHError> {
    match self.db.get_cf(&self.cf()?, key)? {
      Some(v) => Ok(serde_json::from_slice(&v)?),
      None => Ok(BalanceForGoods::default()),
    }
  }

  fn set_balance(&self, key: &Vec<u8>, balance: BalanceForGoods) -> Result<(), WHError> {
    self
      .db
      .put_cf(&self.cf()?, key, serde_json::to_string(&balance)?)
      .map_err(|_| WHError::new("Can't put to database"))
  }

  fn del_balance(&self, key: &Vec<u8>) -> Result<(), WHError> {
    self.db.delete_cf(&self.cf()?, key)?;
    Ok(())
  }

  fn get_checkpoints_before_date(
    &self,
    date: DateTime<Utc>,
    wh: Store,
  ) -> Result<Vec<Balance>, WHError> {
    Err(WHError::new("not supported"))
  }

  fn get_report(
    &self,
    start_date: DateTime<Utc>,
    end_date: DateTime<Utc>,
    wh: Store,
    db: &mut Db,
  ) -> Result<super::Report, WHError> {
    todo!()
  }
}
