use crate::{
  balance::BalanceForGoods,
  db::Db,
  elements::{Report, Store},
  error::WHError,
};

use crate::batch::Batch;
use crate::elements::Goods;
use crate::elements::UUID_NIL;
use crate::operations::Op;
use crate::ordered_topology::OrderedTopology;
use chrono::{DateTime, Utc};

use db::RangeIterator;
use log::debug;
use rocksdb::{BoundColumnFamily, ColumnFamilyDescriptor, Options, DB};
use std::sync::Arc;
use uuid::Uuid;

const CF_NAME: &str = "cf_store_goods_date_type_id_batch";

pub struct StoreGoodsDateTypeIdBatch {
  pub db: Arc<DB>,
}

impl StoreGoodsDateTypeIdBatch {
  pub fn cf_name() -> &'static str {
    CF_NAME
  }
}

impl OrderedTopology for StoreGoodsDateTypeIdBatch {
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
    log::debug!("put put put {op:#?}\n > {balance:?}");

    let before = match self.db.get_cf(&cf, &key)? {
      None => None,
      Some(bs) => Some(self.from_bytes(&bs)?),
    };

    self.db.put_cf(&cf, key, self.to_bytes(op, balance)?)?;

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
    log::debug!("del del del {op:?}");
    Ok(self.db.delete_cf(&self.cf()?, key)?)
  }

  fn balance_before(&self, _op: &Op) -> Result<BalanceForGoods, WHError> {
    Err(WHError::new("not implemented"))
  }

  fn balance_on_op_or_before(&self, _op: &Op) -> Result<BalanceForGoods, WHError> {
    Err(WHError::new("not implemented"))
  }

  fn operation_after(
    &self,
    _op: &Op,
    _exclude_virtual: bool,
  ) -> Result<Option<(Op, BalanceForGoods)>, WHError> {
    Err(WHError::new("Not supported"))
  }

  fn operations_after(&self, _op: &Op) -> Result<Vec<(Op, BalanceForGoods)>, WHError> {
    Err(WHError::new("not implemented"))
  }

  fn create_cf(&self, opts: Options) -> ColumnFamilyDescriptor {
    ColumnFamilyDescriptor::new(StoreGoodsDateTypeIdBatch::cf_name(), opts)
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
    _store: Store,
    _goods: Goods,
    _batch: &Batch,
    _from_date: DateTime<Utc>,
    _till_date: DateTime<Utc>,
  ) -> Result<Vec<Op>, WHError> {
    Err(WHError::new("not implemented"))
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
    from: DateTime<Utc>,
    till_exclude: &Op,
  ) -> Result<Vec<Op>, WHError> {
    let bytes_from: Vec<u8> = self.key_build(
      till_exclude.store,
      till_exclude.goods,
      Batch::min(),
      from.timestamp(),
      u8::MIN,
      UUID_NIL,
      false,
    );
    let bytes_till: Vec<u8> = self.key_build(
      till_exclude.store,
      till_exclude.goods,
      Batch::max(),
      till_exclude.date.timestamp(),
      till_exclude.op.order(),
      till_exclude.id,
      till_exclude.is_dependent,
    );

    let mut res = Vec::new();

    for (_k, value) in (bytes_from.clone()..bytes_till.clone()).lookup(&self.db, &self.cf()?)? {
      let (op, b) = self.from_bytes(&value)?;

      // debug!("loaded_op {op:#?}\n > {b:?}");

      // exclude virtual nodes
      if op.dependant.is_empty() {
        res.push(op.clone());
      }

      // for dependant in op.dependant {
      //   // println!("loading batch {:?}", batch);
      //
      //   let (store, batch, op_order) = dependant.tuple();
      //
      //   if let Some(bs) = self.db.get_cf(
      //     &self.cf()?,
      //     self.key_build(store, op.goods, batch, op.date.timestamp(), op_order, op.id, true),
      //   )? {
      //     let (dop, _) = self.from_bytes(&bs)?;
      //     // println!("dependant operation {:?}", dop);
      //     res.push(dop);
      //   } else {
      //     // TODO raise exception?
      //   }
      // }
    }

    Ok(res)
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
      .chain(goods.as_bytes().iter()) // batch
      .chain(date.to_be_bytes().iter()) // date
      .chain(op_order.to_be_bytes().iter()) // op order
      .chain(op_id.as_bytes().iter()) // op id
      .chain(op_dependant.to_be_bytes().iter()) // op dependant
      .chain(batch.to_bytes(&goods).iter())
      .copied()
      .collect()
  }

  fn cf(&self) -> Result<Arc<BoundColumnFamily>, WHError> {
    if let Some(cf) = self.db.cf_handle(StoreGoodsDateTypeIdBatch::cf_name()) {
      Ok(cf)
    } else {
      Err(WHError::new("can't get CF"))
    }
  }

  fn db(&self) -> Arc<DB> {
    self.db.clone()
  }
}
