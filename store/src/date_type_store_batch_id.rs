use super::{
  balance::BalanceForGoods,
  db::Db,
  elements::{
    first_day_current_month, new_get_aggregations, Balance, Batch, InternalOperation, Op,
    OrderedTopology, Report, Store, UUID_MAX, UUID_NIL,
  },
  error::WHError,
};

use crate::elements::get_aggregations_for_one_goods;
use crate::elements::Goods;
use chrono::{DateTime, Utc};
use json::JsonValue;
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

  fn cf(&self) -> Result<Arc<BoundColumnFamily>, WHError> {
    if let Some(cf) = self.db.cf_handle(DateTypeStoreBatchId::cf_name()) {
      Ok(cf)
    } else {
      Err(WHError::new("can't get CF"))
    }
  }

  // ts | type | store | goods | batch | id
  fn key_build(
    &self,
    id: &Uuid,
    date: &DateTime<Utc>,
    store: &Store,
    goods: &Goods,
    batch: &Batch,
    op: &InternalOperation,
  ) -> Vec<u8> {
    let op_type = match op {
      InternalOperation::Receive(..) => 1_u8,
      InternalOperation::Issue(..) => 2_u8,
    };

    let ts = date.timestamp() as u64;
    ts.to_be_bytes()
      .iter()
      .chain(op_type.to_be_bytes().iter())
      .chain(store.as_bytes().iter())
      .chain(batch.to_bytes(goods).iter())
      .chain(id.as_bytes().iter())
      .map(|b| *b)
      .collect()
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

      let (_, balance) = self.from_bytes(&v)?;
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
      self.get_ops_for_one_goods(op.store, op.goods, first_day_current_month(op.date), op.date)?;

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

    // store + batch
    let expected1: Vec<u8> = op
      .store
      .as_bytes()
      .iter()
      .chain(op.batch.to_bytes(&op.goods).iter())
      .map(|b| *b)
      .collect();

    let expected2: Vec<u8> = op
      .store
      .as_bytes()
      .iter()
      .chain(Batch::no().to_bytes(&op.goods).iter())
      .map(|b| *b)
      .collect();

    let key = self.key(op);

    // TODO change iterator with range from..till?
    let mut iter = self
      .db
      .iterator_cf(&self.cf()?, IteratorMode::From(&key, rocksdb::Direction::Forward));
    while let Some(bytes) = iter.next() {
      if let Ok((k, v)) = bytes {
        //date + optype + store + batch
        if k[0..] == key {
          continue;
        }

        if k[9..=65] == expected1 || (no_batches && k[9..=65] == expected2) {
          let (op, balance) = self.from_bytes(&v)?;

          res.push((op, balance));
        }
      }
    }

    Ok(res)
  }

  fn create_cf(&self, opts: Options) -> ColumnFamilyDescriptor {
    ColumnFamilyDescriptor::new(DateTypeStoreBatchId::cf_name(), opts)
  }

  fn get_ops_for_all(
    &self,
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

    let mut res = Vec::new();

    for item in self.db.iterator_cf_opt(&self.cf()?, options, IteratorMode::Start) {
      let (_, value) = item?;

      let (op, _) = self.from_bytes(&value)?;

      res.push(op);
    }

    Ok(res)
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

      let (op, _) = self.from_bytes(&value)?;

      // log::debug!("k {k:?}");
      // log::debug!("o {op:?}");
      // log::debug!("b {b:?}");

      res.push(op);
    }

    Ok(res)
  }

  fn get_ops_for_one_goods(
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

    let expected_store: Vec<u8> = store.as_bytes().iter().map(|b| *b).collect();
    let expected_goods: Vec<u8> = goods.as_bytes().iter().map(|b| *b).collect();
    let expected_batch_date: Vec<u8> = ts_batch.to_be_bytes().iter().map(|b| *b).collect();
    let expected_batch_id: Vec<u8> = batch.id.as_bytes().iter().map(|b| *b).collect();

    let mut res = Vec::new();

    for item in self.db.iterator_cf_opt(&self.cf()?, options, IteratorMode::Start) {
      let (k, value) = item?;

      if k[9..25] != expected_store
        || k[25..41] != expected_goods
        || k[41..49] != expected_batch_date
        || k[49..65] != expected_batch_id
      {
        continue;
      }

      let (op, _) = self.from_bytes(&value)?;

      // if op.op.is_zero() {
      //   println!("zero operation {:?}", op);
      // }
      if !op.op.is_zero() {
        res.push(op.clone());
      }

      for batch in op.batches {
        // println!("loading batch {:?}", batch);
        if let Some(bs) = self.db.get_cf(
          &self.cf()?,
          self.key_build(&op.id, &op.date, &op.store, &op.goods, &batch, &op.op.clone()),
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

  fn get_ops_for_one_goods_and_batch(
    &self,
    store: Store,
    goods: Goods,
    batch: &Batch,
    from_date: DateTime<Utc>,
    till_date: DateTime<Utc>,
  ) -> Result<Vec<Op>, WHError> {
    println!("get_ops_for_one_goods_and_batch");
    let ts_from = u64::try_from(from_date.timestamp()).unwrap_or_default();
    let ts_batch = u64::try_from(batch.date.timestamp()).unwrap_or_default();
    let from: Vec<u8> = ts_from
      .to_be_bytes()
      .iter()
      .chain(0_u8.to_be_bytes().iter())
      .chain(store.as_bytes().iter())
      .chain(goods.as_bytes().iter())
      // .chain(UUID_NIL.as_bytes().iter())
      // .chain(ts_batch.to_be_bytes().iter())
      // .chain(batch.id.as_bytes().iter())
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
      // .chain(UUID_MAX.as_bytes().iter())
      // .chain(ts_batch.to_be_bytes().iter())
      // .chain(batch.id.as_bytes().iter())
      .chain(u64::MAX.to_be_bytes().iter())
      .chain(UUID_MAX.as_bytes().iter())
      .map(|b| *b)
      .collect();

    let mut options = ReadOptions::default();
    options.set_iterate_range(from..till);

    let expected_store: Vec<u8> = store.as_bytes().iter().map(|b| *b).collect();
    let expected_goods: Vec<u8> = goods.as_bytes().iter().map(|b| *b).collect();
    let expected_batch_date: Vec<u8> = ts_batch.to_be_bytes().iter().map(|b| *b).collect();
    let expected_batch_id: Vec<u8> = batch.id.as_bytes().iter().map(|b| *b).collect();

    let mut res = Vec::new();

    for item in self.db.iterator_cf_opt(&self.cf()?, options, IteratorMode::Start) {
      let (k, value) = item?;

      if k[9..25] != expected_store
        || k[25..41] != expected_goods
        || k[41..49] != expected_batch_date
        || k[49..65] != expected_batch_id
      {
        continue;
      }

      let (op, _) = self.from_bytes(&value)?;

      if op.op.is_zero() {
        println!("zero operation {:?}", op);
      }
      if !op.op.is_zero() {
        res.push(op.clone());
      }

      for batch in op.batches {
        println!("loading batch {:?}", batch);
        if let Some(bs) = self.db.get_cf(
          &self.cf()?,
          self.key_build(&op.id, &op.date, &op.store, &op.goods, &batch, &op.op.clone()),
        )? {
          let (dop, _) = self.from_bytes(&bs)?;
          println!("dependant operation {:?}", dop);
          res.push(dop);
        } else {
          // TODO raise exception?
        }
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

    let byte_goods: Vec<Vec<u8>> = goods
      .iter()
      .map(|g: &Goods| g.as_bytes().iter().map(|b| *b).collect())
      .collect();

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
      .map(|b| *b)
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

    // let items = new_get_aggregations(balances, ops, from_date);

    let items = get_aggregations_for_one_goods(balances, ops, from_date, till_date)?;

    // Ok(Report { from_date, till_date, items })
    Ok(items)
  }

  fn get_report_for_storage(
    &self,
    db: &Db,
    storage: Store,
    from_date: DateTime<Utc>,
    till_date: DateTime<Utc>,
  ) -> Result<Report, WHError> {
    let balances = db.get_checkpoints_for_one_storage_before_date(storage, from_date)?;

    let ops = self.get_ops(storage, first_day_current_month(from_date), till_date)?;

    let items = new_get_aggregations(balances, ops, from_date);

    Ok(Report { from_date, till_date, items })
  }

  fn key(&self, op: &Op) -> Vec<u8> {
    self.key_build(&op.id, &op.date, &op.store, &op.goods, &op.batch, &op.op)
  }
}
