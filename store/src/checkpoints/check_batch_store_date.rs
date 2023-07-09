use std::sync::Arc;

use crate::balance::Balance;
use crate::batch::Batch;
use crate::checkpoints::CheckpointTopology;
use crate::{
  balance::BalanceForGoods,
  elements::{dt, Goods, Store, UUID_NIL},
  error::WHError,
};
use chrono::{DateTime, Utc};
use rocksdb::{BoundColumnFamily, DB};
use std::collections::HashMap;
use uuid::Uuid;

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
  fn key(&self, store: Store, goods: Goods, batch: Batch, date: DateTime<Utc>) -> Vec<u8> {
    [].iter()
      .chain(batch.to_bytes(&goods).iter())
      .chain(store.as_bytes().iter())
      .chain((date.timestamp() as u64).to_be_bytes().iter())
      .copied()
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

  fn key_latest_checkpoint_date(&self) -> Vec<u8> {
    [].iter()
      .chain(UUID_NIL.as_bytes().iter())
      .chain(u64::MIN.to_be_bytes().iter())
      .chain(UUID_NIL.as_bytes().iter())
      .chain(UUID_NIL.as_bytes().iter())
      .chain(u64::MIN.to_be_bytes().iter())
      .copied()
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
      Ok(DateTime::parse_from_rfc3339(date)?.into()) // TODO store/read timestapm in binary format
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

  fn balances_for_store_goods(
    &self,
    _date: DateTime<Utc>,
    _store: Store,
    _goods: Goods,
  ) -> Result<(DateTime<Utc>, HashMap<Batch, BalanceForGoods>), WHError> {
    unimplemented!()
  }

  fn get_checkpoints_for_one_goods(
    &self,
    _store: Store,
    _goods: Goods,
    _date: DateTime<Utc>,
  ) -> Result<Vec<Balance>, WHError> {
    unimplemented!()
  }

  fn get_checkpoints_for_one_goods_with_date(
    &self,
    _store: Store,
    _goods: Goods,
    _date: DateTime<Utc>,
  ) -> Result<(DateTime<Utc>, HashMap<Uuid, BalanceForGoods>), WHError> {
    unimplemented!()
  }

  fn get_checkpoint_for_goods_and_batch(
    &self,
    _store: Store,
    _goods: Goods,
    _batch: &Batch,
    _date: DateTime<Utc>,
  ) -> Result<Option<Balance>, WHError> {
    unimplemented!()
  }

  fn get_checkpoints_for_all(
    &self,
    _date: DateTime<Utc>,
  ) -> Result<
    (DateTime<Utc>, HashMap<Store, HashMap<Goods, HashMap<Batch, BalanceForGoods>>>),
    WHError,
  > {
    unimplemented!()
  }

  fn get_checkpoints_for_many_goods(
    &self,
    _date: DateTime<Utc>,
    _goods: &Vec<Goods>,
  ) -> Result<(DateTime<Utc>, HashMap<Uuid, BalanceForGoods>), WHError> {
    unimplemented!()
  }

  fn get_checkpoints_for_one_storage_before_date(
    &self,
    _store: Store,
    _date: DateTime<Utc>,
  ) -> Result<Vec<Balance>, WHError> {
    Err(WHError::new("Not supported"))
  }

  fn get_checkpoints_for_all_storages_before_date(
    &self,
    _date: DateTime<Utc>,
  ) -> Result<Vec<Balance>, WHError> {
    unimplemented!()
  }
}
