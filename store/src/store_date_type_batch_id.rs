use std::{str::FromStr, sync::Arc};

use super::{
  balance::BalanceForGoods,
  elements::{InternalOperation, KeyValueStore, Op, OpMutation, OrderedTopology, Store,
             first_day_current_month, new_get_aggregations, Balance, Report},
  error::WHError,
  db::Db,
};
use chrono::{DateTime, Utc};
use json::array;
use rocksdb::{BoundColumnFamily, ColumnFamilyDescriptor, IteratorMode, Options, ReadOptions, DB};
use rust_decimal::Decimal;

const CF_NAME: &str = "cf_store_date_type_batch_id";

pub struct StoreDateTypeBatchId {
  pub db: Arc<DB>,
}

impl StoreDateTypeBatchId {
  pub fn cf_name() -> &'static str {
    CF_NAME
  }

  fn cf(&self) -> Result<Arc<BoundColumnFamily>, WHError> {
    if let Some(cf) = self.db.cf_handle(StoreDateTypeBatchId::cf_name()) {
      Ok(cf)
    } else {
      Err(WHError::new("can't get CF"))
    }
  }
}

impl OrderedTopology for StoreDateTypeBatchId {
  fn put(&self, op: &Op, balance: &BalanceForGoods) -> Result<(), WHError> {
    let key = self.key(op);
    // log::debug!("put {key:?}");
    // log::debug!("{op:?}");
    Ok(self.db.put_cf(&self.cf()?, key, self.to_bytes(op, balance))?)
  }

  fn get(&self, op: &Op) -> Result<Option<(Op, BalanceForGoods)>, WHError> {
    if let Some(bytes) = self.db.get_cf(&self.cf()?, self.key(&op))? {
      Ok(Some(self.from_bytes(&bytes)?))
    } else {
      Ok(None)
    }
  }

  fn del(&self, op: &Op) -> Result<(), WHError> {
    let key = self.key(op);
    // log::debug!("del {key:?}");
    // log::debug!("{op:?}");
    Ok(self.db.delete_cf(&self.cf()?, key)?)
  }

  fn balance_before(&self, op: &Op) -> Result<BalanceForGoods, WHError> {

    let expected_store: Vec<u8> = op.store.as_bytes().iter().map(|b| *b).collect();
    let expected_batch: Vec<u8> = op.batch().iter().map(|b| *b).collect();

    let key = self.key(op);

    let mut iter = self
      .db
      .iterator_cf(&self.cf()?, IteratorMode::From(&key, rocksdb::Direction::Reverse));

    while let Some(bytes) = iter.next() {
      let (k, v) = bytes?;

      if k[0..16] != expected_store || k[25..65] != expected_batch || k[0..] == key {
        continue;
      }

      let (op, balance) = self.from_bytes(&v)?;

      return Ok(balance);
    }

    Ok(BalanceForGoods::default())
  }

  fn operations_after(&self, op: &Op) -> Result<Vec<(Op, BalanceForGoods)>, WHError> {
    let mut res = Vec::new();

    let expected_store: Vec<u8> = op.store.as_bytes().iter().map(|b| *b).collect();
    let expected_batch: Vec<u8> = op.batch().iter().map(|b| *b).collect();

    let key = self.key(op);

    // TODO change iterator with range from..till?
    let mut iter = self.db.iterator_cf(
      &self.cf()?,
      IteratorMode::From(&key, rocksdb::Direction::Forward),
    );

    while let Some(bytes) = iter.next() {
      if let Ok((k, v)) = bytes {

        if k[0..16] != expected_store || k[25..65] != expected_batch || k[0..] == key {
          continue;
        }

        let (op, balance) = self.from_bytes(&v)?;

        res.push((op, balance));
      }
    }

    Ok(res)
  }

  fn create_cf(&self, opts: Options) -> ColumnFamilyDescriptor {
    ColumnFamilyDescriptor::new(StoreDateTypeBatchId::cf_name(), opts)
  }

  fn get_ops(
    &self,
    storage: Store,
    from_date: DateTime<Utc>,
    till_date: DateTime<Utc>,
  ) -> Result<Vec<Op>, WHError> {
    let from_date = from_date.timestamp() as u64;
    let from: Vec<u8> = storage
      .as_bytes()
      .iter()
      .chain(from_date.to_be_bytes().iter())
      .chain(0_u8.to_be_bytes().iter())
      .map(|b| *b)
      .collect();

    let till_date = till_date.timestamp() as u64;
    let till = storage
      .as_bytes()
      .iter()
      .chain(till_date.to_be_bytes().iter())
      .chain(u8::MAX.to_be_bytes().iter())
      .map(|b| *b)
      .collect();

      let mut options = ReadOptions::default();
      options.set_iterate_range(from..till);
  
      // store
      let expected: Vec<u8> = storage.as_bytes().iter().map(|b| *b).collect();
  
      // log::debug!("exp {expected:?}");
  
      let mut res = Vec::new();
  
      for item in self.db.iterator_cf_opt(&self.cf()?, options, IteratorMode::Start) {
        let (k, value) = item?;
  
        // log::debug!("k__ {k:?}");
        // log::debug!("k[0..16] {:?}", &k[0..16]);
  
        // || k[0..] == key
        if k[0..16] != expected {
          continue;
        }
  
        let (op, b) = self.from_bytes(&value)?;
  
        // log::debug!("k {k:?}");
        // log::debug!("o {op:?}");
        // log::debug!("b {b:?}");
  
        res.push(op);
      }
  
      Ok(res)
  }

  fn get_report(
    &self,
    db: &Db,
    storage: Store,
    from_date: DateTime<Utc>,
    till_date: DateTime<Utc>,
  ) -> Result<Report, WHError> {
    // log::debug!("STORE_DATE_TYPE_BATCH.get_report");
    let balances = db.get_checkpoints_before_date(storage, from_date)?;

    let ops = self.get_ops(storage, first_day_current_month(from_date), till_date)?;

    let items = new_get_aggregations(balances, ops, from_date);

    Ok(Report { from_date, till_date, items })
    // Err(WHError::new("test"))
  }

  fn key(&self, op: &Op) -> Vec<u8> {
    let ts = op.date.timestamp() as u64;

    let op_type = match op.op {
      InternalOperation::Receive(..) => 1_u8,
      InternalOperation::Issue(..) => 2_u8,
    };

    op.store
      .as_bytes()
      .iter()
      .chain(ts.to_be_bytes().iter())
      .chain(op_type.to_be_bytes().iter())
      .chain(op.batch().iter())
      .chain(op.id.as_bytes().iter())
      .map(|b| *b)
      .collect()
  }
}
