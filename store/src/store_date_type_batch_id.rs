use super::{
  balance::BalanceForGoods,
  db::Db,
  elements::{first_day_current_month, Report, Store},
  error::WHError,
};

use crate::agregations::{get_aggregations_for_one_goods, new_get_aggregations};
use crate::balance::Balance;
use crate::batch::Batch;
use crate::elements::{Goods, Qty};
use crate::elements::{UUID_MAX, UUID_NIL};
use crate::operations::Op;
use crate::ordered_topology::OrderedTopology;
use chrono::{DateTime, Utc};
use json::JsonValue;
use rocksdb::{BoundColumnFamily, ColumnFamilyDescriptor, IteratorMode, Options, ReadOptions, DB};
use std::sync::Arc;

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
  fn put(
    &self,
    op: &Op,
    balance: &BalanceForGoods,
  ) -> Result<Option<(Op, BalanceForGoods)>, WHError> {
    if op.is_receive() {
      if !op.is_dependent {
        debug_assert!(!op.batch.is_empty(), "{} | {:#?} | {:#?}", op.batch.is_empty(), op, balance);
      }
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
    println!("op {:#?}", op);
    // let expected_store: Vec<u8> = op.store.as_bytes().iter().map(|b| *b).collect();
    // let expected_goods_batch: Vec<u8> = op.batch().iter().map(|b| *b).collect();

    let key = self.key(op);

    let mut iter = self
      .db
      .iterator_cf(&self.cf()?, IteratorMode::From(&key, rocksdb::Direction::Reverse));

    while let Some(bytes) = iter.next() {
      let (k, v) = bytes?;

      if k[0..] == key {
        continue;
      }

      let (loaded_op, balance) = self.from_bytes(&v)?;

      println!("loaded_op {:#?}", loaded_op);

      if loaded_op.store != op.store {
        break;
      }

      if loaded_op.goods != op.goods || loaded_op.batch != op.batch {
        continue;
      }

      println!("{balance:#?}");

      return Ok(balance);
    }

    Ok(BalanceForGoods::default())
  }

  fn goods_balance_before(
    &self,
    op: &Op,
    balances: Vec<Balance>,
  ) -> Result<Vec<(Batch, BalanceForGoods)>, WHError> {
    let mut result = vec![];

    let ops =
      // self.get_ops_for_one_goods(op.store, op.goods, first_day_current_month(op.date), op.date)?;
    self.get_ops_before_op(op)?;

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

  fn operations_after(
    &self,
    op: &Op,
    no_batches: bool,
  ) -> Result<Vec<(Op, BalanceForGoods)>, WHError> {
    let mut res = Vec::new();

    let expected_store: Vec<u8> = op.store.as_bytes().iter().map(|b| *b).collect();
    // let expected_batch1: Vec<u8> = op.batch.to_bytes(&op.goods);
    // let expected_batch2: Vec<u8> = Batch::no().to_bytes(&op.goods);

    let no_batch = Batch::no();

    let key = self.key(op);

    // TODO change iterator with range from..till?
    let mut iter = self
      .db
      .iterator_cf(&self.cf()?, IteratorMode::From(&key, rocksdb::Direction::Forward));

    while let Some(bytes) = iter.next() {
      if let Ok((k, v)) = bytes {
        if k[0..16] != expected_store || k[0..] == key {
          continue;
        }

        let (loaded_op, balance) = self.from_bytes(&v)?;

        if loaded_op.store == op.store
          && loaded_op.goods == op.goods
          && (loaded_op.batch == op.batch || loaded_op.batch == no_batch)
        {
          let (op, balance) = self.from_bytes(&v)?;

          res.push((op, balance));
        }
      }
    }

    Ok(res)
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
    from_date: DateTime<Utc>,
    till_date: DateTime<Utc>,
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
      .map(|b| *b)
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
      .map(|b| *b)
      .collect();

    let mut options = ReadOptions::default();
    options.set_iterate_range(from..till);

    let expected_goods: Vec<u8> = goods.as_bytes().iter().map(|b| *b).collect();

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

  fn operations_for_store_goods(&self, from: DateTime<Utc>, till: &Op) -> Result<Vec<Op>, WHError> {
    log::debug!("TOPOLOGY: store_date_type_batch_id");
    let ts_from = u64::try_from(from.timestamp()).unwrap_or_default();
    let bytes_from: Vec<u8> = till
      .store
      .as_bytes()
      .iter()
      .chain(ts_from.to_be_bytes().iter())
      .chain(0_u8.to_be_bytes().iter())
      .chain(till.goods.as_bytes().iter()) // part of batch
      .chain(UUID_NIL.as_bytes().iter()) // part of batch
      .chain(u64::MIN.to_be_bytes().iter()) // part of batch
      .chain(UUID_NIL.as_bytes().iter())
      .map(|b| *b)
      .collect();

    let ts_till = u64::try_from(till.date.timestamp()).unwrap_or_default();
    let bytes_till: Vec<u8> = till
      .store
      .as_bytes()
      .iter()
      .chain(ts_till.to_be_bytes().iter())
      .chain(u8::MAX.to_be_bytes().iter())
      .chain(till.goods.as_bytes().iter()) // part of batch
      .chain(UUID_MAX.as_bytes().iter()) // part of batch
      .chain(u64::MAX.to_be_bytes().iter()) // part of batch
      .chain(UUID_MAX.as_bytes().iter())
      .map(|b| *b)
      .collect();

    let mut options = ReadOptions::default();
    options.set_iterate_range(bytes_from..bytes_till);

    // let expected_goods: Vec<u8> = till.goods.as_bytes().iter().map(|b| *b).collect();
    // let expected_uuid: Vec<u8> = till.id.as_bytes().iter().map(|b| *b).collect();
    // let expected_order: u8 = till.op.order();

    let mut res = Vec::new();

    for item in self.db.iterator_cf_opt(&self.cf()?, options, IteratorMode::Start) {
      let (k, value) = item?;

      let (op, _) = self.from_bytes(&value)?;

      // store filtered by prefix in range
      if op.goods != till.goods {
        continue;
      }

      if op.op.order() > till.op.order() {
        println!("exit by over order");
        break;
      }

      if op.id.as_bytes() >= till.id.as_bytes() {
        println!("exit by over uuid");
        break;
      }

      res.push(op.clone());
    }

    Ok(res)
  }

  fn get_ops_before_op(&self, op: &Op) -> Result<Vec<Op>, WHError> {
    let ts_from = u64::try_from(first_day_current_month(op.date).timestamp()).unwrap_or_default();
    let from: Vec<u8> = op
      .store
      .as_bytes()
      .iter()
      .chain(ts_from.to_be_bytes().iter())
      .chain(0_u8.to_be_bytes().iter())
      .chain(op.goods.as_bytes().iter()) // part of batch
      .chain(UUID_NIL.as_bytes().iter()) // part of batch
      .chain(u64::MIN.to_be_bytes().iter()) // part of batch
      .chain(UUID_NIL.as_bytes().iter())
      .map(|b| *b)
      .collect();

    let ts_till = u64::try_from(op.date.timestamp()).unwrap_or_default();
    let till: Vec<u8> = op
      .store
      .as_bytes()
      .iter()
      .chain(ts_till.to_be_bytes().iter())
      .chain(u8::MAX.to_be_bytes().iter())
      .chain(op.goods.as_bytes().iter()) // part of batch
      .chain(UUID_MAX.as_bytes().iter()) // part of batch
      .chain(u64::MAX.to_be_bytes().iter()) // part of batch
      .chain(UUID_MAX.as_bytes().iter())
      .map(|b| *b)
      .collect();

    let mut options = ReadOptions::default();
    options.set_iterate_range(from..till);

    let expected_goods: Vec<u8> = op.goods.as_bytes().iter().map(|b| *b).collect();
    let expected_op_id: Vec<u8> = op.id.as_bytes().iter().map(|b| *b).collect();

    let mut res = Vec::new();

    for item in self.db.iterator_cf_opt(&self.cf()?, options, IteratorMode::Start) {
      let (k, value) = item?;

      if k[25..41] != expected_goods {
        continue;
      }

      if k[65..81] == expected_op_id {
        break;
      }

      let (op, _) = self.from_bytes(&value)?;

      res.push(op);
    }

    Ok(res)
  }

  fn get_ops_for_one_goods_and_batch(
    &self,
    store: Store,
    goods: Goods,
    batch: &Batch,
    from_date: DateTime<Utc>,
    till_date: DateTime<Utc>,
  ) -> Result<Vec<Op>, WHError> {
    let ts_batch = u64::try_from(batch.date.timestamp()).unwrap_or_default();
    let ts_from = u64::try_from(from_date.timestamp()).unwrap_or_default();
    let from: Vec<u8> = store
      .as_bytes()
      .iter()
      .chain(ts_from.to_be_bytes().iter())
      .chain(0_u8.to_be_bytes().iter())
      // .chain(goods.as_bytes().iter())
      .chain(UUID_NIL.as_bytes().iter())
      .chain(u64::MIN.to_be_bytes().iter())
      .chain(UUID_NIL.as_bytes().iter())
      .map(|b| *b)
      .collect();

    let ts_till = u64::try_from(till_date.timestamp()).unwrap_or_default();
    let till: Vec<u8> = store
      .as_bytes()
      .iter()
      .chain(ts_till.to_be_bytes().iter())
      .chain(u8::MAX.to_be_bytes().iter())
      // .chain(goods.as_bytes().iter())
      .chain(UUID_MAX.as_bytes().iter())
      .chain(u64::MAX.to_be_bytes().iter())
      .chain(UUID_MAX.as_bytes().iter())
      .map(|b| *b)
      .collect();

    let mut options = ReadOptions::default();
    options.set_iterate_range(from..till);

    let expected_goods: Vec<u8> = goods.as_bytes().iter().map(|b| *b).collect();
    let expected_batch_date: Vec<u8> = ts_batch.to_be_bytes().iter().map(|b| *b).collect();
    let expected_batch_id: Vec<u8> = batch.id.as_bytes().iter().map(|b| *b).collect();

    let mut res = Vec::new();

    for item in self.db.iterator_cf_opt(&self.cf()?, options, IteratorMode::Start) {
      let (k, value) = item?;

      if k[25..41] == expected_goods
        && k[41..49] == expected_batch_date
        && k[49..65] == expected_batch_id
      {
        let (op, _) = self.from_bytes(&value)?;

        res.push(op);
      }
    }

    Ok(res)
  }

  fn get_ops_for_many_goods(
    &self,
    goods: &Vec<Goods>,
    from_date: DateTime<Utc>,
    till_date: DateTime<Utc>,
  ) -> Result<Vec<Op>, WHError> {
    // let goods: Vec<[u8; 16]> = goods.into_iter().as_slice().iter().map(|b| *b).collect();

    let mut byte_goods: Vec<Vec<u8>> = Vec::new();
    let _ = goods
      .iter()
      .map(|g: &Goods| byte_goods.push(g.as_bytes().iter().map(|b| *b).collect()));

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
      .map(|b| *b)
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
      .map(|b| *b)
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

  fn get_report_for_goods(
    &self,
    db: &Db,
    storage: Store,
    goods: Goods,
    batch: &Batch,
    from_date: DateTime<Utc>,
    till_date: DateTime<Utc>,
  ) -> Result<JsonValue, WHError> {
    println!("STORE_DATE_TYPE_BATCH.get_report_for_goods");
    let mut balances = Vec::new();

    if let Some(balance) = db.get_checkpoint_for_goods_and_batch(storage, goods, batch, from_date)? {
      balances.push(balance);
    }

    let ops = self.get_ops_for_one_goods_and_batch(
      storage,
      goods,
      batch,
      first_day_current_month(from_date),
      till_date,
    )?;

    let items = get_aggregations_for_one_goods(balances, ops, from_date, till_date)?;

    Ok(items)
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

    let items = new_get_aggregations(balances, ops, from_date);

    Ok(Report { from_date, till_date, items })
    // Err(WHError::new("test"))
  }

  fn key(&self, op: &Op) -> Vec<u8> {
    let ts = op.date.timestamp() as u64;

    let op_order = op.op.order();
    let op_dependant = if op.is_dependent { 1_u8 } else { 0_u8 };

    op.store
      .as_bytes()
      .iter()
      .chain(ts.to_be_bytes().iter())
      .chain(op_order.to_be_bytes().iter())
      .chain(op.batch().iter())
      .chain(op.id.as_bytes().iter())
      .chain(vec![op_dependant].iter())
      .map(|b| *b)
      .collect()
  }
}
