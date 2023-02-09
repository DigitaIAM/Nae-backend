use std::sync::Arc;

use super::{Db, InternalOperation, KeyValueStore, Op, OpMutation, OrderedTopology, Store, WHError};
use crate::store::{first_day_current_month, new_get_aggregations, Balance, Report};
use chrono::{DateTime, Utc};
use rocksdb::{BoundColumnFamily, ColumnFamilyDescriptor, IteratorMode, Options, ReadOptions, DB};

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
  fn put_op(&self, op: &OpMutation) -> Result<(), WHError> {
    if op.transfer.is_some() {
      self
        .db
        .put_cf(&self.cf()?, self.key(&op.dependent()?), op.dependent()?.value()?)?
    }

    Ok(self.db.put_cf(&self.cf()?, self.key(op), op.value()?)?)
  }

  fn delete_op(&self, op: &OpMutation) -> Result<(), WHError> {
    if op.transfer.is_some() {
      self.db.delete_cf(&self.cf()?, self.key(&op.dependent()?))?
    }

    Ok(self.db.delete_cf(&self.cf()?, self.key(op))?)
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

    let iter = self.db.iterator_cf_opt(&self.cf()?, options, IteratorMode::Start);

    let mut res = Vec::new();
    for item in iter {
      let (_, value) = item?;
      let op: OpMutation = serde_json::from_slice(&value)?;
      res.push(op.to_op());
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
    let balances = db.get_checkpoints_before_date(from_date)?;

    let ops = self.get_ops(storage, first_day_current_month(from_date), till_date)?;

    let items = new_get_aggregations(balances, ops, from_date);

    Ok(Report { from_date, till_date, items })
    // Err(WHError::new("test"))
  }

  fn data_update(&self, op: &OpMutation) -> Result<(), WHError> {
    if op.before.is_none() {
      self.put_op(op)
    } else {
      if let Ok(Some(bytes)) = self.db.get_cf(&self.cf()?, self.key(op)) {
        let o: OpMutation = serde_json::from_slice(&bytes)?;
        if op.before == o.after {
          self.put_op(op)
        } else if op.after.is_none() {
          self.delete_op(op)
        } else {
          return Err(WHError::new("Wrong 'before' state in operation"));
        }
      } else {
        return Err(WHError::new("There is no such operation in db"));
      }
    }
    // 1. before none after some - transfer
    // 2. before some after none - delete
    // 3. before some after some - change
  }

  fn key(&self, op: &OpMutation) -> Vec<u8> {
    let ts = op.date.timestamp() as u64;
    // if after == None, this operation will be recorded last (that's why op_type by default is 3)
    let mut op_type = 3_u8;

    if let Some(o) = &op.after {
      op_type = match o {
        InternalOperation::Receive(..) => 1_u8,
        InternalOperation::Issue(..) => 2_u8,
      };
    }

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
