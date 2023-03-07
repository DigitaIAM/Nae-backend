use std::{str::FromStr, sync::Arc};

use super::{
  balance::{BalanceDelta, BalanceForGoods},
  db::Db,
  elements::{
    first_day_current_month, new_get_aggregations, Balance, Batch, CheckpointTopology,
    InternalOperation, KeyValueStore, Mode, Op, OpMutation, OrderedTopology, Report, Store,
    UUID_MAX, UUID_NIL,
  },
  error::WHError,
};
use chrono::{DateTime, Utc};
use json::{array, JsonValue};
use rocksdb::{BoundColumnFamily, ColumnFamilyDescriptor, IteratorMode, Options, ReadOptions, DB};
use rust_decimal::Decimal;
use uuid::Uuid;
// use std::alloc::Global;
use crate::elements::Goods;
use std::convert::TryFrom;

const CF_NAME: &str = "cf_date_type_store_batch_id";
pub struct DateTypeStoreBatchId {
  pub db: Arc<DB>,
}

impl DateTypeStoreBatchId {
  pub fn cf_name() -> &'static str {
    CF_NAME
  }

  fn cf(&self) -> Result<Arc<BoundColumnFamily>, WHError> {
    if let Some(cf) = self.db.cf_handle(DateTypeStoreBatchId::cf_name()) {
      Ok(cf)
    } else {
      Err(WHError::new("can't get CF"))
    }
  }
}

impl OrderedTopology for DateTypeStoreBatchId {
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
    // log::debug!("DATE_TYPE_STORE_BATCH.balance_before");

    // store + batch
    let expected: Vec<u8> =
      op.store.as_bytes().iter().chain(op.batch().iter()).map(|b| *b).collect();

    let key = self.key(op);

    // log::debug!("key {key:?}");
    // log::debug!("exp {expected:?}");

    let mut iter = self
      .db
      .iterator_cf(&self.cf()?, IteratorMode::From(&key, rocksdb::Direction::Reverse));

    while let Some(bytes) = iter.next() {
      let (k, v) = bytes?;

      // log::debug!("k__ {k:?}");

      //date + optype + store + batch
      if k[9..65] != expected || k[0..] == key {
        continue;
      }

      let (op, balance) = self.from_bytes(&v)?;
      return Ok(balance);
    }

    return Ok(BalanceForGoods::default());
  }

  fn goods_balance_before(
    &self,
    op: &Op,
    balances: Vec<Balance>,
  ) -> Result<Vec<(Batch, BalanceForGoods)>, WHError> {
    let mut result = vec![];

    let ops =
      self.get_ops_for_goods(op.store, op.goods, first_day_current_month(op.date), op.date)?;

    let mut operations = vec![];

    for o in ops {
      if o.store == op.store && o.goods == op.goods {
        operations.push(o);
      }
    }

    let items = new_get_aggregations(balances, operations, op.date);

    for item in items.1 {
      result.push((item.batch.unwrap(), item.close_balance));
    }

    Ok(result)
  }

  fn operations_after(&self, op: &Op) -> Result<Vec<(Op, BalanceForGoods)>, WHError> {
    let mut res = Vec::new();

    // store + batch
    let expected: Vec<u8> =
      op.store.as_bytes().iter().chain(op.batch().iter()).map(|b| *b).collect();

    let key = self.key(op);

    // TODO change iterator with range from..till?
    let mut iter = self
      .db
      .iterator_cf(&self.cf()?, IteratorMode::From(&key, rocksdb::Direction::Forward));
    while let Some(bytes) = iter.next() {
      if let Ok((k, v)) = bytes {
        //date + optype + store + batch
        if k[9..=65] != expected || k[0..] == key {
          continue;
        }

        let (op, balance) = self.from_bytes(&v)?;

        res.push((op, balance));
      }
    }

    Ok(res)
  }

  fn create_cf(&self, opts: Options) -> ColumnFamilyDescriptor {
    ColumnFamilyDescriptor::new(DateTypeStoreBatchId::cf_name(), opts)
  }

  fn get_ops(
    &self,
    storage: Store,
    from_date: DateTime<Utc>,
    till_date: DateTime<Utc>,
  ) -> Result<Vec<Op>, WHError> {
    let from_date = from_date.timestamp() as u64;
    let from: Vec<u8> = from_date
      .to_be_bytes()
      .iter()
      .chain(0_u8.to_be_bytes().iter())
      .chain(UUID_NIL.as_bytes().iter())
      .chain(UUID_NIL.as_bytes().iter())
      .chain(UUID_NIL.as_bytes().iter())
      .chain(u64::MIN.to_be_bytes().iter())
      .chain(UUID_NIL.as_bytes().iter())
      .map(|b| *b)
      .collect();

    let till_date = till_date.timestamp() as u64;
    let till: Vec<u8> = till_date
      .to_be_bytes()
      .iter()
      .chain(u8::MAX.to_be_bytes().iter())
      .chain(UUID_MAX.as_bytes().iter())
      .chain(UUID_MAX.as_bytes().iter())
      .chain(UUID_MAX.as_bytes().iter())
      .chain(u64::MAX.to_be_bytes().iter())
      .chain(UUID_MAX.as_bytes().iter())
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
      // log::debug!("k[9..25] {:?}", &k[9..25]);

      // || k[0..] == key
      if k[9..25] != expected {
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

  fn get_ops_for_goods(
    &self,
    store: Store,
    goods: Goods,
    from_date: DateTime<Utc>,
    till_date: DateTime<Utc>,
  ) -> Result<Vec<Op>, WHError> {
    let ts_from = u64::try_from(from_date.timestamp()).unwrap_or_default();
    let from: Vec<u8> = ts_from
      .to_be_bytes()
      .iter()
      .chain(0_u8.to_be_bytes().iter())
      .chain(store.as_bytes().iter())
      .chain(goods.as_bytes().iter())
      .chain(UUID_NIL.as_bytes().iter())
      .chain(u64::MIN.to_be_bytes().iter())
      .chain(UUID_NIL.as_bytes().iter())
      .map(|b| *b)
      .collect();

    let ts_till = u64::try_from(till_date.timestamp()).unwrap_or_default();
    let till: Vec<u8> = ts_till
      .to_be_bytes()
      .iter()
      .chain(u8::MAX.to_be_bytes().iter())
      .chain(store.as_bytes().iter())
      .chain(goods.as_bytes().iter())
      .chain(UUID_MAX.as_bytes().iter())
      .chain(u64::MAX.to_be_bytes().iter())
      .chain(UUID_MAX.as_bytes().iter())
      .map(|b| *b)
      .collect();

    let mut options = ReadOptions::default();
    options.set_iterate_range(from..till);

    let expected: Vec<u8> = store.as_bytes().iter().map(|b| *b).collect();

    let mut res = Vec::new();

    for item in self.db.iterator_cf_opt(&self.cf()?, options, IteratorMode::Start) {
      let (k, value) = item?;

      if k[9..25] != expected {
        continue;
      }

      let (op, b) = self.from_bytes(&value)?;

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
    // log::debug!("DATE_TYPE_STORE_BATCH.get_report");

    let balances = db.get_checkpoints_before_date(storage, from_date)?;

    let ops = self.get_ops(storage, first_day_current_month(from_date), till_date)?;

    let items = new_get_aggregations(balances, ops, from_date);

    Ok(Report { from_date, till_date, items })
  }

  fn key(&self, op: &Op) -> Vec<u8> {
    let ts = op.date.timestamp() as u64;

    let op_type = match op.op {
      InternalOperation::Receive(..) => 1_u8,
      InternalOperation::Issue(..) => 2_u8,
    };

    ts.to_be_bytes()
      .iter()
      .chain(op_type.to_be_bytes().iter())
      .chain(op.store.as_bytes().iter())
      .chain(op.batch().iter())
      .chain(op.id.as_bytes().iter())
      .map(|b| *b)
      .collect()
  }
}
