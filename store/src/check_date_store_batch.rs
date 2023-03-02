use std::sync::Arc;

use super::{
  balance::BalanceForGoods,
  elements::{dt, first_day_current_month, first_day_next_month, max_batch, min_batch,
   Balance, Batch, CheckpointTopology, Goods, Op, OpMutation, Store, UUID_NIL, UUID_MAX},
  db::Db,
  error::WHError,
};
use chrono::{DateTime, NaiveDateTime, Utc};
use rocksdb::{BoundColumnFamily, IteratorMode, ReadOptions, DB};
use uuid::Uuid;
use service::utils::time::timestamp_to_time;

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

  pub fn key_to_data(k: Vec<u8>) -> Result<(DateTime<Utc>, Store, Goods, Batch), WHError> {
    // u64 8 bytes
    // Uuid 16 bytes

    let ts = u64::from_be_bytes(k[0..=7].try_into().unwrap());
    let date = timestamp_to_time(ts)?;

    let store = Uuid::from_slice(&k[8..=23])?;
    let goods = Uuid::from_slice(&k[24..=39])?;

    let batch_id = Uuid::from_slice(&k[48..=63])?;

    let ts = u64::from_be_bytes(k[40..=47].try_into().unwrap());
    let batch =
      Batch { id: Uuid::from_slice(&k[48..=63])?, date: timestamp_to_time(ts)? };

    Ok((date, store, goods, batch))
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

  fn key_checkpoint(&self, balance: &Balance, date_of_checkpoint: DateTime<Utc>) -> Vec<u8> {
    [].iter()
    .chain((date_of_checkpoint.timestamp() as u64).to_be_bytes().iter())
    .chain(balance.store.as_bytes().iter())
    .chain(balance.goods.as_bytes().iter())
    .chain((balance.batch.date.timestamp() as u64).to_be_bytes().iter())
    .chain(balance.batch.id.as_bytes().iter())
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
    store: Store,
    date: DateTime<Utc>,
  ) -> Result<Vec<Balance>, WHError> {
    let mut result = Vec::new();

    let ts = u64::try_from(first_day_current_month(date).timestamp()).unwrap_or_default();

    let from: Vec<u8> = ts
      .to_be_bytes()
      .iter()
      // .chain(UUID_NIL.as_bytes().iter())
      .chain(store.as_bytes().iter())
      .chain(min_batch().iter())
      .map(|b| *b)
      .collect();
    let till: Vec<u8> = ts
      .to_be_bytes()
      .iter()
      // .chain(UUID_MAX.as_bytes().iter())
      .chain(store.as_bytes().iter())
      .chain(max_batch().iter())
      .map(|b| *b)
      .collect();

    let mut opts = ReadOptions::default();
    opts.set_iterate_range(from..till);

    let mut iter = self.db.iterator_cf_opt(&self.cf()?, opts, IteratorMode::Start);

    while let Some(res) = iter.next() {
      let (k, v) = res?;
      let b: BalanceForGoods = serde_json::from_slice(&v)?;
      // println!("BAL: {b:#?}");
      let (date, store, goods, batch) = CheckDateStoreBatch::key_to_data(k.to_vec())?;

      let balance = Balance { date, store, goods, batch, number: b };
      result.push(balance);
    }

    Ok(result)
  }

  fn key_latest_checkpoint_date(&self) -> Vec<u8> {
    [].iter()
      .chain(UUID_NIL.as_bytes().iter())
      .chain(u64::MIN.to_be_bytes().iter())
      .chain(UUID_NIL.as_bytes().iter())
      .chain(u64::MIN.to_be_bytes().iter())
      .chain(UUID_NIL.as_bytes().iter())
      .map(|b| *b)
      .collect()
  }

  fn get_latest_checkpoint_date(&self) -> Result<DateTime<Utc>, WHError> {
    if let Some(bytes) = self.db.get_cf(&self.cf()?, self.key_latest_checkpoint_date())? {
      let date = serde_json::from_slice(&bytes)?;
      Ok(DateTime::parse_from_rfc3339(date)?.into())
    } else {
      // Ok(DateTime::<Utc>::default())
      dt("1970-01-01")
    }
  }

  fn set_latest_checkpoint_date(&self, date: DateTime<Utc>) -> Result<(), WHError> {
    Ok(self.db.put_cf(
      &self.cf()?,
      self.key_latest_checkpoint_date(),
      serde_json::to_string(&date)?,
    )?)
  }
}
