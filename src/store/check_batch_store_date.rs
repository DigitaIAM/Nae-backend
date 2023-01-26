use std::sync::Arc;

use super::{
  balance::BalanceForGoods, first_day_next_month, Balance, CheckpointTopology, Db, Op, OpMutation,
  Store, WHError,
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
    Err(WHError::new("Not supported"))
  }

  fn data_update(
    &self,
    op: &OpMutation,
    last_checkpoint_date: DateTime<Utc>,
  ) -> Result<(), WHError> {
    // let cf = self.db.cf_handle(name).expect("option in change_checkpoint");

    let mut date = op.date;
    let mut check_point = op.date;

    // get iterator from first day of next month of given operation till 'last' check point
    while check_point < last_checkpoint_date {
      check_point = first_day_next_month(date);

      let key = self.key(&op.to_op(), check_point);

      let mut balance = self.get_balance(&key)?;

      balance += op.to_delta();

      // println!("CHECKPOINT: {balance:#?}");

      if balance.is_zero() {
        self.del_balance(&key)?;
      } else {
        self.set_balance(&key, balance)?;
      }
      date = check_point;
    }

    Ok(())
  }
}
