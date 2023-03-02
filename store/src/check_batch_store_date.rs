use std::sync::Arc;

use super::{
  balance::BalanceForGoods,
  elements::{dt, first_day_next_month, Balance, CheckpointTopology, Op,
   OpMutation, Store, UUID_NIL},
  db::Db,
  error::WHError,
};
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

impl CheckpointTopology for CheckBatchStoreDate {
  fn key(&self, op: &Op, date: DateTime<Utc>) -> Vec<u8> {
    [].iter()
      .chain(op.goods.as_bytes().iter())
      .chain((op.batch.date.timestamp() as u64).to_be_bytes().iter())
      .chain(op.batch.id.as_bytes().iter())
      .chain(op.store.as_bytes().iter())
      .chain((date.timestamp() as u64).to_be_bytes().iter())
      .map(|b| *b)
      .collect()
  }

  fn key_checkpoint(&self, balance: &Balance, date_of_checkpoint: DateTime<Utc>) -> Vec<u8> {
    todo!()
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
    store: Store,
    date: DateTime<Utc>,
  ) -> Result<Vec<Balance>, WHError> {
    Err(WHError::new("Not supported"))
  }

  fn key_latest_checkpoint_date(&self) -> Vec<u8> {
    [].iter()
      .chain(UUID_NIL.as_bytes().iter())
      .chain(u64::MIN.to_be_bytes().iter())
      .chain(UUID_NIL.as_bytes().iter())
      .chain(UUID_NIL.as_bytes().iter())
      .chain(u64::MIN.to_be_bytes().iter())
      .map(|b| *b)
      .collect()
  }

  fn get_latest_checkpoint_date(&self) -> Result<DateTime<Utc>, WHError> {
    if let Some(bytes) = self
      .db
      .get_cf(&self.cf().map_err(|_| WHError::new("get cf()"))?, self.key_latest_checkpoint_date())
      .map_err(|_| WHError::new("key_latest_checkpoint_date()"))?
    {
      let date =
        serde_json::from_slice(&bytes).map_err(|_| WHError::new("get serde_json::from_slice"))?;
      Ok(DateTime::parse_from_rfc3339(date)?.into())
    } else {
      // Ok(DateTime::<Utc>::default())
      dt("1970-01-01")
    }
  }

  fn set_latest_checkpoint_date(&self, date: DateTime<Utc>) -> Result<(), WHError> {
    Ok(self.db.put_cf(
      &self.cf().map_err(|_| WHError::new("set cf()"))?,
      self.key_latest_checkpoint_date(),
      serde_json::to_string(&date).map_err(|_| WHError::new("set serde_json::from_slice"))?,
    )?)
  }

  fn actualize_balances(&self, current_date: DateTime<Utc>, latest_checkpoint_date: DateTime<Utc>) -> Result<(), WHError> {
    todo!()
  }
}
