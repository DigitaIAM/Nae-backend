use crate::{
  balance::BalanceForGoods,
  db::Db,
  elements::{Report, Store},
  error::WHError,
};

use crate::batch::Batch;
use crate::elements::Goods;
use crate::elements::{UUID_MAX, UUID_NIL};
use crate::operations::Op;
use crate::ordered_topology::OrderedTopology;
use chrono::{DateTime, Utc};

use rocksdb::{
  BoundColumnFamily, ColumnFamilyDescriptor, Direction, IteratorMode, Options, ReadOptions, DB,
};
use std::sync::Arc;
use uuid::Uuid;

const CF_NAME: &str = "cf_store_batch_date_type_id";

pub struct StoreBatchDateTypeId {
  pub db: Arc<DB>,
}

impl StoreBatchDateTypeId {
  pub fn cf_name() -> &'static str {
    CF_NAME
  }
}

impl OrderedTopology for StoreBatchDateTypeId {
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

  fn balance_before(&self, op: &Op) -> Result<BalanceForGoods, WHError> {
    log::debug!("balance_before {:#?}", op);

    let key = self.key(op);

    let iter = self
      .db
      .iterator_cf(&self.cf()?, IteratorMode::From(&key, rocksdb::Direction::Reverse));

    for bytes in iter {
      let (k, v) = bytes?;

      if k[0..] == key {
        continue;
      }

      let (loaded_op, balance) = self.from_bytes(&v)?;

      log::debug!("fn_balance_before: loaded_op {loaded_op:#?}\nbalance {balance:#?}");

      if loaded_op.store != op.store || loaded_op.goods != op.goods || loaded_op.batch != op.batch {
        // log::debug!("break");
        break;
      }

      return Ok(balance);
    }

    Ok(BalanceForGoods::default())
  }

  fn balance_on_op_or_before(&self, op: &Op) -> Result<BalanceForGoods, WHError> {
    // println!("op {:#?}", op);

    let key = self.key(op);

    let iter = self
      .db
      .iterator_cf(&self.cf()?, IteratorMode::From(&key, rocksdb::Direction::Reverse));

    for bytes in iter {
      let (_k, v) = bytes?;

      let (loaded_op, balance) = self.from_bytes(&v)?;

      // println!("loaded_op {:#?}", loaded_op);

      if loaded_op.store != op.store || loaded_op.goods != op.goods || loaded_op.batch != op.batch {
        break;
      }

      // println!("{balance:#?}");

      return Ok(balance);
    }

    Ok(BalanceForGoods::default())
  }

  fn operation_after(
    &self,
    op: &Op,
    exclude_virtual: bool,
  ) -> Result<Option<(Op, BalanceForGoods)>, WHError> {
    // log::debug!("operations_after {op:#?}");

    let from = self.key(op);
    let till =
      self.key_build(op.store, op.goods, op.batch.clone(), i64::MAX, u8::MAX, UUID_MAX, true);

    // println!("key:");
    // for b in key.iter() {
    //   println!("{b:#010b}");
    // }

    let mut options = ReadOptions::default();
    options.set_iterate_range(from.clone()..till);

    // TODO change iterator with range from..till?
    let iter =
      self
        .db
        .iterator_cf_opt(&self.cf()?, options, IteratorMode::From(&from, Direction::Forward));

    for bytes in iter {
      if let Ok((k, v)) = bytes {
        if k[0..] == from {
          continue;
        }

        let (loaded_op, balance) = self.from_bytes(&v)?;

        // log::debug!("operations_after loaded {loaded_op:#?}\nbalance {balance:#?}");

        if loaded_op.store != op.store || loaded_op.goods != op.goods || loaded_op.batch != op.batch
        {
          // log::debug!("break");
          break;
        }

        if exclude_virtual && loaded_op.is_virtual() {
          continue;
        }

        return Ok(Some((loaded_op, balance)));
      }
    }

    Ok(None)
  }

  fn operations_after(&self, op: &Op) -> Result<Vec<(Op, BalanceForGoods)>, WHError> {
    // log::debug!("operations_after {op:#?}");

    let mut res = Vec::new();

    let key = self.key(op);
    let till =
      self.key_build(op.store, op.goods, op.batch.clone(), i64::MAX, u8::MAX, UUID_MAX, true);

    // println!("key:");
    // for b in key.iter() {
    //   println!("{b:#010b}");
    // }

    let mut options = ReadOptions::default();
    options.set_iterate_range(key.clone()..till);

    // TODO change iterator with range from..till?
    let iter =
      self
        .db
        .iterator_cf_opt(&self.cf()?, options, IteratorMode::From(&key, Direction::Forward));

    for bytes in iter {
      if let Ok((k, v)) = bytes {
        if k[0..] == key {
          continue;
        }

        // println!("load:");
        // for b in k.iter() {
        //   println!("{b:#010b}");
        // }

        let (loaded_op, balance) = self.from_bytes(&v)?;

        // log::debug!("operations_after loaded {loaded_op:#?}\nbalance {balance:#?}");

        if loaded_op.store != op.store || loaded_op.goods != op.goods || loaded_op.batch != op.batch
        {
          // log::debug!("break");
          break;
        }

        res.push((loaded_op, balance));
      }
    }

    Ok(res)
  }

  fn create_cf(&self, opts: Options) -> ColumnFamilyDescriptor {
    ColumnFamilyDescriptor::new(StoreBatchDateTypeId::cf_name(), opts)
  }

  fn get_ops_for_storage(
    &self,
    _storage: Store,
    _from_date: DateTime<Utc>,
    _till_date: DateTime<Utc>,
  ) -> Result<Vec<Op>, WHError> {
    Err(WHError::new("not implemented"))
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
    _store: Store,
    _goods: Goods,
    _from_date: DateTime<Utc>,
    _till_date: DateTime<Utc>,
  ) -> Result<Vec<Op>, WHError> {
    Err(WHError::new("not implemented"))
  }

  fn ops_for_store_goods_and_batch(
    &self,
    store: Store,
    goods: Goods,
    batch: &Batch,
    from_date: DateTime<Utc>,
    till_date: DateTime<Utc>,
  ) -> Result<Vec<Op>, WHError> {
    log::debug!("TODO STORE_BATCH_DATE_TYPE_ID::ops_for_store_goods_and_batch");

    let from: Vec<u8> =
      self.key_build(store, goods, batch.clone(), from_date.timestamp(), u8::MIN, UUID_NIL, false);
    let till: Vec<u8> =
      self.key_build(store, goods, batch.clone(), till_date.timestamp(), u8::MAX, UUID_MAX, true);

    let mut options = ReadOptions::default();
    options.set_iterate_range(from..till);

    let mut res = Vec::new();

    for item in self.db.iterator_cf_opt(&self.cf()?, options, IteratorMode::Start) {
      let (_k, value) = item?;

      let (op, _) = self.from_bytes(&value)?;

      assert!(!op.op.is_zero(), "{:#?}", op);

      if op.dependant.is_empty() {
        res.push(op);
      }

      // for dependant in op.dependant {
      //   println!("loading dependant {:?}", dependant);
      //
      //   let (store, batch, op_order) = dependant.tuple();
      //
      //   if let Some(bs) = self.db.get_cf(
      //     &self.cf()?,
      //     self.key_build(store, op.goods, batch, op.date, op_order, op.id, true),
      //   )? {
      //     let (dop, _) = self.from_bytes(&bs)?;
      //     println!("dependant operation {:?}", dop);
      //     res.push(dop);
      //   } else {
      //     // TODO raise exception?
      //   }
      // }
    }

    Ok(res)
  }

  fn get_ops_for_many_goods(
    &self,
    _goods: &Vec<Goods>,
    _from_date: DateTime<Utc>,
    _till_date: DateTime<Utc>,
  ) -> Result<Vec<Op>, WHError> {
    Err(WHError::new("not implemented"))
  }

  // operations for store+goods (return all batches)
  fn operations_for_store_goods(
    &self,
    _from: DateTime<Utc>,
    _till: &Op,
  ) -> Result<Vec<Op>, WHError> {
    Err(WHError::new("not implemented"))
  }

  fn get_report_for_storage(
    &self,
    _db: &Db,
    _storage: Store,
    _from_date: DateTime<Utc>,
    _till_date: DateTime<Utc>,
  ) -> Result<Report, WHError> {
    Err(WHError::new("not implemented"))
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
      .iter() // store
      .chain(batch.to_bytes(&goods).iter()) // batch
      .chain(date.to_be_bytes().iter()) // date
      .chain(op_order.to_be_bytes().iter()) // op order
      .chain(op_id.as_bytes().iter()) // op id
      .chain(op_dependant.to_be_bytes().iter())
      .copied()
      .collect()
  }

  fn cf(&self) -> Result<Arc<BoundColumnFamily>, WHError> {
    if let Some(cf) = self.db.cf_handle(StoreBatchDateTypeId::cf_name()) {
      Ok(cf)
    } else {
      Err(WHError::new("can't get CF"))
    }
  }

  fn db(&self) -> Arc<DB> {
    self.db.clone()
  }
}
