use std::sync::Arc;

use super::{
  balance::BalanceForGoods, first_day_current_month, first_day_next_month, max_batch, min_batch,
  Balance, CheckpointTopology, Db, Op, OpMutation, Store, WHError,
};
use chrono::{DateTime, Utc};
use rocksdb::{BoundColumnFamily, IteratorMode, ReadOptions, DB};

const CF_NAME: &str = "cf_checkpoint_date_store_batch";

pub struct CheckDateStoreBatch {
  pub db: Arc<DB>,
}

impl CheckDateStoreBatch {
  pub fn cf_name() -> &'static str {
    CF_NAME
  }

  fn cf(&self) -> Result<Arc<BoundColumnFamily>, WHError> {
    if let Some(cf) = self.db.cf_handle(CheckDateStoreBatch::cf_name()) {
      Ok(cf)
    } else {
      Err(WHError::new("can't get CF"))
    }
  }
}

impl CheckpointTopology for CheckDateStoreBatch {
  fn key(&self, op: &Op, date: DateTime<Utc>) -> Vec<u8> {
    [].iter()
      .chain((date.timestamp() as u64).to_be_bytes().iter())
      .chain(op.store.as_bytes().iter())
      .chain(op.goods.as_bytes().iter())
      .chain((op.batch.date.timestamp() as u64).to_be_bytes().iter())
      .chain(op.batch.id.as_bytes().iter())
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
    let mut result = Vec::new();

    let ts = u64::try_from(first_day_current_month(date).timestamp()).unwrap_or_default();

    let from: Vec<u8> = ts
      .to_be_bytes()
      .iter()
      .chain(wh.as_bytes().iter())
      .chain(min_batch().iter())
      .map(|b| *b)
      .collect();
    let till: Vec<u8> = ts
      .to_be_bytes()
      .iter()
      .chain(wh.as_bytes().iter())
      .chain(max_batch().iter())
      .map(|b| *b)
      .collect();

    let mut opts = ReadOptions::default();
    opts.set_iterate_range(from..till);

    let mut iter = self.db.iterator_cf_opt(&self.cf()?, opts, IteratorMode::Start);

    while let Some(res) = iter.next() {
      let (_, value) = res?;
      let balance = serde_json::from_slice(&value)?;
      println!("BAL: {balance:#?}");
      result.push(balance);
    }

    Ok(result)
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
