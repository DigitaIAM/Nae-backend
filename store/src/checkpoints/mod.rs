pub mod check_batch_store_date;
pub mod check_date_store_batch;

use crate::balance::{Balance, BalanceForGoods};
use crate::batch::Batch;
use crate::elements::{first_day_next_month, Goods, Store, WHError};
use crate::operations::OpMutation;
use chrono::{DateTime, Utc};
use rocksdb::{BoundColumnFamily, IteratorMode, DB};
use std::collections::HashMap;
use std::sync::Arc;
use uuid::Uuid;

pub trait CheckpointTopology {
  fn key(&self, store: Store, goods: Goods, batch: Batch, date: DateTime<Utc>) -> Vec<u8>;
  fn key_to_data(&self, k: Vec<u8>) -> Result<(DateTime<Utc>, Store, Goods, Batch), WHError>;

  fn get_balance(&self, key: &Vec<u8>) -> Result<BalanceForGoods, WHError>;
  fn set_balance(&self, key: &Vec<u8>, balance: BalanceForGoods) -> Result<(), WHError>;
  fn del_balance(&self, key: &Vec<u8>) -> Result<(), WHError>;
  fn key_latest_checkpoint_date(&self) -> Vec<u8>;
  fn get_latest_checkpoint_date(&self) -> Result<DateTime<Utc>, WHError>;
  fn set_latest_checkpoint_date(&self, date: DateTime<Utc>) -> Result<(), WHError>;

  fn balances_for_store_goods(
    &self,
    date: DateTime<Utc>,
    store: Store,
    goods: Goods,
  ) -> Result<(DateTime<Utc>, HashMap<Batch, BalanceForGoods>), WHError>;

  fn get_checkpoints_for_one_goods(
    &self,
    store: Store,
    goods: Goods,
    date: DateTime<Utc>,
  ) -> Result<Vec<Balance>, WHError>;

  fn get_checkpoints_for_one_goods_with_date(
    &self,
    store: Store,
    goods: Goods,
    date: DateTime<Utc>,
  ) -> Result<(DateTime<Utc>, HashMap<Uuid, BalanceForGoods>), WHError>;

  fn get_checkpoint_for_goods_and_batch(
    &self,
    store: Store,
    goods: Goods,
    batch: &Batch,
    date: DateTime<Utc>,
  ) -> Result<Option<Balance>, WHError>;

  fn get_checkpoints_for_all(
    &self,
    date: DateTime<Utc>,
  ) -> Result<
    (DateTime<Utc>, HashMap<Store, HashMap<Goods, HashMap<Batch, BalanceForGoods>>>),
    WHError,
  >;

  fn get_checkpoints_for_many_goods(
    &self,
    date: DateTime<Utc>,
    goods: &Vec<Goods>,
  ) -> Result<(DateTime<Utc>, HashMap<Uuid, BalanceForGoods>), WHError>;

  fn checkpoint_update(&self, op: &OpMutation) -> Result<(), WHError> {
    // log::debug!("ops len: {}", ops.len());
    // for op in ops {
    log::debug!("================================");
    log::debug!("checkpoint_update {:#?}", op);

    // This assert do not pass
    // assert!(op.before.is_some() || op.after.is_some());

    let mut tmp_date = op.date;
    let mut check_point_date = op.date;

    let mut last_checkpoint_date = self.get_latest_checkpoint_date()?;

    // copy previous checkpoint to next one
    if last_checkpoint_date <= op.date {
      let old_checkpoints =
        self.get_checkpoints_for_all_storages_before_date(last_checkpoint_date)?;

      last_checkpoint_date = first_day_next_month(op.date);

      for old_checkpoint in old_checkpoints.iter() {
        let mut new_checkpoint_date = first_day_next_month(old_checkpoint.date);
        while new_checkpoint_date <= last_checkpoint_date {
          let key = self.key(
            old_checkpoint.store,
            old_checkpoint.goods,
            old_checkpoint.batch.clone(),
            new_checkpoint_date,
          );
          self.set_balance(&key, old_checkpoint.clone().number)?;
          new_checkpoint_date = first_day_next_month(new_checkpoint_date);
        }
      }
    }

    while check_point_date <= last_checkpoint_date {
      check_point_date = first_day_next_month(tmp_date);

      let key = self.key(op.store, op.goods, op.batch.clone(), check_point_date);

      let mut balance = self.get_balance(&key)?;
      log::debug!("balance on {check_point_date} {:#?} before operation {balance:?}", op.date);

      balance += op.to_delta(); // TODO: will fail at inventory operation
      log::debug!("{:?} > {balance:?}", op.to_delta());

      if balance.is_zero() {
        log::debug!("del_balance: {key:?}");
        self.del_balance(&key)?;
      } else {
        log::debug!("set_balance: {balance:?} {key:?}");
        self.set_balance(&key, balance)?;
      }
      tmp_date = check_point_date;

      if check_point_date == last_checkpoint_date {
        break;
      }
    }

    self.set_latest_checkpoint_date(check_point_date)?;
    // }

    Ok(())
  }

  fn get_checkpoints_for_one_storage_before_date(
    &self,
    store: Store,
    date: DateTime<Utc>,
  ) -> Result<Vec<Balance>, WHError>;

  fn get_checkpoints_for_all_storages_before_date(
    &self,
    date: DateTime<Utc>,
  ) -> Result<Vec<Balance>, WHError>;

  fn db(&self) -> Arc<DB>;

  fn cf(&self) -> Result<Arc<BoundColumnFamily>, WHError>;

  fn to_bytes(&self, balance: &BalanceForGoods) -> Result<Vec<u8>, WHError> {
    let mut bs = Vec::new();
    ciborium::ser::into_writer(&balance, &mut bs)?;
    Ok(bs)
  }

  fn from_bytes(&self, bytes: &[u8]) -> Result<BalanceForGoods, WHError> {
    Ok(ciborium::de::from_reader(bytes)?)
  }

  fn debug(&self) -> Result<(), WHError> {
    log::debug!("DEBUG: checkpoint_topology");
    let latest = self.key_latest_checkpoint_date();

    for record in self.db().full_iterator_cf(&self.cf()?, IteratorMode::Start) {
      let (k, value) = record?;
      let b = self.from_bytes(&value)?;
      let (date, store, goods, batch) = self.key_to_data(k.to_vec())?;
      if latest[..] == k[..] {
        log::debug!("latest checkpoint:");
      } else {
        log::debug!("checkpoint:");
      }
      log::debug!(
        "date: {:?}\nstore: {:?}\ngoods: {:?}\nbatch: {:?}\nbalance: {:?}",
        date,
        store,
        goods,
        batch,
        b
      );
    }

    Ok(())
  }
}
