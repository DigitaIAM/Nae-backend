use crate::{
  balance::BalanceForGoods,
  db::Db,
  elements::{first_day_current_month, Report, Store},
  error::WHError,
};

use crate::aggregations::get_aggregations;
use crate::batch::Batch;
use crate::elements::Goods;
use crate::elements::{UUID_MAX, UUID_NIL};
use crate::operations::Op;
use crate::ordered_topology::OrderedTopology;
use chrono::{DateTime, Utc};

use rocksdb::{BoundColumnFamily, ColumnFamilyDescriptor, IteratorMode, Options, ReadOptions, DB};
use std::sync::Arc;
use uuid::Uuid;

const CF_NAME: &str = "cf_store_date_type_batch_id";

pub struct StoreDateTypeBatchId {
  pub db: Arc<DB>,
}

impl StoreDateTypeBatchId {
  pub fn cf_name() -> &'static str {
    CF_NAME
  }
}

impl OrderedTopology for StoreDateTypeBatchId {
  fn put(
    &self,
    op: &Op,
    balance: &BalanceForGoods,
  ) -> Result<Option<(Op, BalanceForGoods)>, WHError> {
    if op.is_receive() && !op.is_dependent {
      debug_assert!(!op.batch.is_empty(), "{} | {:#?} | {:#?}", op.batch.is_empty(), op, balance);
    }
    debug_assert!(!op.op.is_zero(), "{} | {:#?} | {:#?}", op.batch.is_empty(), op, balance);

    let cf = self.cf()?;
    let key = self.key(op);
    // log::debug!("put {key:?}");
    // log::debug!("{op:?}");

    let result = match self.db.get_cf(&cf, &key)? {
      None => None,
      Some(bs) => Some(self.from_bytes(&bs)?),
    };

    self.db.put_cf(&cf, key, self.to_bytes(op, balance)?)?;

    Ok(result)
  }

  fn get(&self, op: &Op) -> Result<Option<(Op, BalanceForGoods)>, WHError> {
    if let Some(bytes) = self.db.get_cf(&self.cf()?, self.key(op))? {
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

  fn balance_before(&self, _op: &Op) -> Result<BalanceForGoods, WHError> {
    Err(WHError::new("Not supported"))
  }

  fn balance_on_op_or_before(&self, _op: &Op) -> Result<BalanceForGoods, WHError> {
    Err(WHError::new("Not supported"))
  }

  fn operation_after(&self, _op: &Op) -> Result<Option<(Op, BalanceForGoods)>, WHError> {
    Err(WHError::new("Not supported"))
  }

  fn operations_after(&self, _op: &Op) -> Result<Vec<(Op, BalanceForGoods)>, WHError> {
    Err(WHError::new("Not supported"))
  }

  fn create_cf(&self, opts: Options) -> ColumnFamilyDescriptor {
    ColumnFamilyDescriptor::new(StoreDateTypeBatchId::cf_name(), opts)
  }

  fn get_ops_for_storage(
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
      .copied()
      .collect();

    let till_date = till_date.timestamp() as u64;
    let till = storage
      .as_bytes()
      .iter()
      .chain(till_date.to_be_bytes().iter())
      .chain(u8::MAX.to_be_bytes().iter())
      .copied()
      .collect();

    let mut options = ReadOptions::default();
    options.set_iterate_range(from..till);

    // store
    let expected: Vec<u8> = storage.as_bytes().to_vec();

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

      let (op, _) = self.from_bytes(&value)?;

      // log::debug!("k {k:?}");
      // log::debug!("o {op:?}");
      // log::debug!("b {b:?}");

      res.push(op);
    }

    Ok(res)
  }

  fn get_ops_for_all(
    &self,
    _from_date: DateTime<Utc>,
    _till_date: DateTime<Utc>,
  ) -> Result<Vec<Op>, WHError> {
    Err(WHError::new("not implemented"))
  }

  fn get_ops_for_one_goods(
    &self,
    store: Store,
    goods: Goods,
    from_date: DateTime<Utc>,
    till_date: DateTime<Utc>,
  ) -> Result<Vec<Op>, WHError> {
    let ts_from = u64::try_from(from_date.timestamp()).unwrap_or_default();
    let from: Vec<u8> = store
      .as_bytes()
      .iter()
      .chain(ts_from.to_be_bytes().iter())
      .chain(0_u8.to_be_bytes().iter())
      .chain(goods.as_bytes().iter()) // part of batch
      .chain(UUID_NIL.as_bytes().iter()) // part of batch
      .chain(u64::MIN.to_be_bytes().iter()) // part of batch
      .chain(UUID_NIL.as_bytes().iter())
      .copied()
      .collect();

    let ts_till = u64::try_from(till_date.timestamp()).unwrap_or_default();
    let till: Vec<u8> = store
      .as_bytes()
      .iter()
      .chain(ts_till.to_be_bytes().iter())
      .chain(u8::MAX.to_be_bytes().iter())
      .chain(goods.as_bytes().iter()) // part of batch
      .chain(UUID_MAX.as_bytes().iter()) // part of batch
      .chain(u64::MAX.to_be_bytes().iter()) // part of batch
      .chain(UUID_MAX.as_bytes().iter())
      .copied()
      .collect();

    let mut options = ReadOptions::default();
    options.set_iterate_range(from..till);

    let expected_goods: Vec<u8> = goods.as_bytes().to_vec();

    let mut res = Vec::new();

    for item in self.db.iterator_cf_opt(&self.cf()?, options, IteratorMode::Start) {
      let (k, value) = item?;

      if k[25..41] != expected_goods {
        continue;
      }

      let (op, _) = self.from_bytes(&value)?;

      res.push(op);
    }

    Ok(res)
  }

  fn ops_for_store_goods_and_batch(
    &self,
    _store: Store,
    _goods: Goods,
    _batch: &Batch,
    _from_date: DateTime<Utc>,
    _till_date: DateTime<Utc>,
  ) -> Result<Vec<Op>, WHError> {
    Err(WHError::new("Not supported"))
  }

  fn get_ops_for_many_goods(
    &self,
    goods: &Vec<Goods>,
    from_date: DateTime<Utc>,
    till_date: DateTime<Utc>,
  ) -> Result<Vec<Op>, WHError> {
    // let goods: Vec<[u8; 16]> = goods.into_iter().as_slice().iter().map(|b| *b).collect();

    let mut byte_goods: Vec<Vec<u8>> = Vec::new();
    let _ = goods.iter().map(|g: &Goods| byte_goods.push(g.as_bytes().to_vec()));

    let ts_from = u64::try_from(from_date.timestamp()).unwrap_or_default();
    let from: Vec<u8> = UUID_NIL
      .as_bytes()
      .iter() // store
      .chain(ts_from.to_be_bytes().iter())
      .chain(0_u8.to_be_bytes().iter())
      .chain(UUID_NIL.as_bytes().iter()) // goods
      .chain(UUID_NIL.as_bytes().iter())
      .chain(u64::MIN.to_be_bytes().iter())
      .chain(UUID_NIL.as_bytes().iter())
      .copied()
      .collect();

    let ts_till = u64::try_from(till_date.timestamp()).unwrap_or_default();
    let till: Vec<u8> = UUID_MAX
      .as_bytes()
      .iter() // store
      .chain(ts_till.to_be_bytes().iter())
      .chain(u8::MAX.to_be_bytes().iter())
      .chain(UUID_MAX.as_bytes().iter()) // goods
      .chain(UUID_MAX.as_bytes().iter())
      .chain(u64::MAX.to_be_bytes().iter())
      .chain(UUID_MAX.as_bytes().iter())
      .copied()
      .collect();

    let mut options = ReadOptions::default();
    options.set_iterate_range(from..till);

    let mut res = Vec::new();

    for item in self.db.iterator_cf_opt(&self.cf()?, options, IteratorMode::Start) {
      let (k, value) = item?;

      if byte_goods.contains(&k[25..41].to_vec()) {
        let (op, _) = self.from_bytes(&value)?;
        res.push(op);
      }
    }

    Ok(res)
  }

  fn operations_for_store_goods(
    &self,
    _from: DateTime<Utc>,
    _till: &Op,
  ) -> Result<Vec<Op>, WHError> {
    Err(WHError::new("not implemented"))
  }

  fn get_report_for_storage(
    &self,
    db: &Db,
    storage: Store,
    from_date: DateTime<Utc>,
    till_date: DateTime<Utc>,
  ) -> Result<Report, WHError> {
    // log::debug!("STORE_DATE_TYPE_BATCH.get_report");
    let balances = db.get_checkpoints_for_one_storage_before_date(storage, from_date)?;

    let ops = self.get_ops_for_storage(storage, first_day_current_month(from_date), till_date)?;

    let items = get_aggregations(balances, ops, from_date);

    Ok(Report { from_date, till_date, items })
    // Err(WHError::new("test"))
  }

  fn key_build(
    &self,
    store: Store,
    goods: Goods,
    batch: Batch,
    date: i64,
    op_order: u8,
    op_id: Uuid,
    is_dependent: bool,
  ) -> Vec<u8> {
    assert!(date >= 0);
    let date = date as u64;
    let op_dependant = if is_dependent { 1_u8 } else { 0_u8 };

    store
      .as_bytes()
      .iter()
      .chain(date.to_be_bytes().iter())
      .chain(op_order.to_be_bytes().iter())
      .chain(batch.to_bytes(&goods).iter())
      .chain(op_id.as_bytes().iter())
      .chain(op_dependant.to_be_bytes().iter())
      .copied()
      .collect()
  }

  fn cf(&self) -> Result<Arc<BoundColumnFamily>, WHError> {
    if let Some(cf) = self.db.cf_handle(StoreDateTypeBatchId::cf_name()) {
      Ok(cf)
    } else {
      Err(WHError::new("can't get CF"))
    }
  }

  fn db(&self) -> Arc<DB> {
    self.db.clone()
  }
}
