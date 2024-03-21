use crate::{
  balance::BalanceForGoods,
  db::Db,
  elements::{first_day_current_month, Report, Store, UUID_MAX, UUID_NIL},
  error::WHError,
};

use crate::ordered_topology::OrderedTopology;

use crate::aggregations::{aggregations_for_store_goods_batch, aggregations_store_goods};
use crate::batch::Batch;
use crate::elements::Goods;
use crate::operations::Op;
use chrono::{DateTime, Utc};
use rocksdb::{BoundColumnFamily, ColumnFamilyDescriptor, IteratorMode, Options, ReadOptions, DB};
use std::convert::TryFrom;
use std::sync::Arc;
use uuid::Uuid;

const CF_NAME: &str = "cf_date_type_store_batch_id";
pub struct DateTypeStoreBatchId {
  pub db: Arc<DB>,
}

impl DateTypeStoreBatchId {
  pub fn cf_name() -> &'static str {
    CF_NAME
  }
}

impl OrderedTopology for DateTypeStoreBatchId {
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

    let before = match self.db.get_cf(&cf, &key)? {
      None => None,
      Some(bs) => Some(self.from_bytes(&bs)?),
    };

    self.db.put_cf(&self.cf()?, key, self.to_bytes(op, balance)?)?;

    Ok(before)
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

  fn operation_after(
    &self,
    _op: &Op,
    _exclude_virtual: bool,
  ) -> Result<Option<(Op, BalanceForGoods)>, WHError> {
    Err(WHError::new("Not supported"))
  }

  fn operations_after(&self, _op: &Op) -> Result<Vec<(Op, BalanceForGoods)>, WHError> {
    Err(WHError::new("Not supported"))
  }

  fn create_cf(&self, opts: Options) -> ColumnFamilyDescriptor {
    ColumnFamilyDescriptor::new(DateTypeStoreBatchId::cf_name(), opts)
  }

  fn ops_for_store(
    &self,
    store: Store,
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
      .copied()
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
      .copied()
      .collect();

    let mut options = ReadOptions::default();
    options.set_iterate_range(from..till);

    // store
    let expected: Vec<u8> = store.as_bytes().to_vec();

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

      let (op, _) = self.from_bytes(&value)?;

      // log::debug!("k {k:?}");
      // log::debug!("o {op:?}");
      // log::debug!("b {b:?}");

      res.push(op);
    }

    Ok(res)
  }

  fn ops(&self, from_date: DateTime<Utc>, till_date: DateTime<Utc>) -> Result<Vec<Op>, WHError> {
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
      .copied()
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
      .copied()
      .collect();

    let mut options = ReadOptions::default();
    options.set_iterate_range(from..till);

    let mut res = Vec::new();

    for item in self.db.iterator_cf_opt(&self.cf()?, options, IteratorMode::Start) {
      let (_, value) = item?;

      let (op, _) = self.from_bytes(&value)?;

      res.push(op);
    }

    Ok(res)
  }

  fn ops_for_store_goods(
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
      .copied()
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
      .copied()
      .collect();

    let mut options = ReadOptions::default();
    options.set_iterate_range(from..till);

    let expected_store: Vec<u8> = store.as_bytes().to_vec();
    let expected_goods: Vec<u8> = goods.as_bytes().to_vec();

    let mut res = Vec::new();

    for item in self.db.iterator_cf_opt(&self.cf()?, options, IteratorMode::Start) {
      let (k, value) = item?;

      if k[9..25] != expected_store || k[25..41] != expected_goods {
        continue;
      }

      let (op, _) = self.from_bytes(&value)?;

      if !op.op.is_zero() {
        res.push(op.clone());
      }

      for dependant in op.dependant {
        // println!("loading batch {:?}", batch);
        let (store, batch, op_order) = dependant.tuple();

        if let Some(bs) = self.db.get_cf(
          &self.cf()?,
          self.key_build(
            store,
            op.goods,
            batch,
            op.date.timestamp(),
            op_order,
            op.id,
            op.is_dependent,
          ),
        )? {
          let (dop, _) = self.from_bytes(&bs)?;
          // println!("dependant operation {:?}", dop);
          res.push(dop);
        } else {
          // TODO raise exception?
        }
      }
    }

    Ok(res)
  }

  fn ops_for_goods(
    &self,
    goods: &Vec<Goods>,
    from_date: DateTime<Utc>,
    till_date: DateTime<Utc>,
  ) -> Result<Vec<Op>, WHError> {
    // let goods: Vec<[u8; 16]> = goods.into_iter().as_slice().iter().map(|b| *b).collect();

    let byte_goods: Vec<Vec<u8>> = goods.iter().map(|g: &Goods| g.as_bytes().to_vec()).collect();

    let ts_from = u64::try_from(from_date.timestamp()).unwrap_or_default();
    let from: Vec<u8> = ts_from
      .to_be_bytes()
      .iter()
      .chain(0_u8.to_be_bytes().iter())
      .chain(UUID_NIL.as_bytes().iter()) // store
      .chain(UUID_NIL.as_bytes().iter()) // goods
      .chain(UUID_NIL.as_bytes().iter())
      .chain(u64::MIN.to_be_bytes().iter())
      .chain(UUID_NIL.as_bytes().iter())
      .copied()
      .collect();

    let ts_till = u64::try_from(till_date.timestamp()).unwrap_or_default();
    let till: Vec<u8> = ts_till
      .to_be_bytes()
      .iter()
      .chain(u8::MAX.to_be_bytes().iter())
      .chain(UUID_MAX.as_bytes().iter()) // store
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

  fn report_for_store(
    &self,
    db: &Db,
    store: Store,
    from_date: DateTime<Utc>,
    till_date: DateTime<Utc>,
  ) -> Result<Report, WHError> {
    let balances = db.checkpoints_for_store_before_date(store, from_date)?;

    let ops = self.ops_for_store(store, first_day_current_month(from_date), till_date)?;

    let items = aggregations_store_goods(balances, ops, from_date);

    Ok(Report { from_date, till_date, items })
  }

  // | ts | type | store | goods | batch | id | dependant |
  fn key_build(
    &self,
    store: Store,
    goods: Goods,
    batch: Batch,
    date: i64,
    op_order: u8,
    op_id: Uuid,
    is_dependant: bool,
  ) -> Vec<u8> {
    assert!(date >= 0);
    let date = date as u64;
    let op_dependant = if is_dependant { 1_u8 } else { 0_u8 };
    date
      .to_be_bytes()
      .iter()
      .chain(op_order.to_be_bytes().iter())
      .chain(store.as_bytes().iter())
      .chain(batch.to_bytes(&goods).iter())
      .chain(op_id.as_bytes().iter())
      .chain(op_dependant.to_be_bytes().iter())
      .copied()
      .collect()
  }

  fn cf(&self) -> Result<Arc<BoundColumnFamily>, WHError> {
    if let Some(cf) = self.db.cf_handle(DateTypeStoreBatchId::cf_name()) {
      Ok(cf)
    } else {
      Err(WHError::new("can't get CF"))
    }
  }

  fn db(&self) -> Arc<DB> {
    self.db.clone()
  }
}
