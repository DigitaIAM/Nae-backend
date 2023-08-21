use bytecheck::CheckBytes;
use rkyv::{AlignedVec, Archive, Deserialize, Serialize};

use std::collections::HashMap;
use std::ops::{Add, Neg, Sub};
use std::sync::Arc;
use values::{ID, ID_BYTES, ID_MAX, ID_MIN};

use crate::animo::db::{AnimoDB, FromBytes, FromKVBytes, Snapshot, ToBytes, ToKVBytes};
use crate::animo::error::DBError;
use crate::animo::ops_manager::*;
use crate::animo::shared::*;
use crate::animo::*;
use crate::warehouse::balance::WHBalance;
use crate::warehouse::balance_operation::BalanceOperation;
use crate::warehouse::balance_operations::BalanceOps;
use crate::warehouse::primitive_types::*;
use crate::warehouse::store_topology::*;
use crate::warehouse::turnover::*;

// (time) + store + goods = (turnover, close_balance)

pub(crate) struct WHQueryStoreAggregation {
  prefix: usize,
  position: Vec<u8>,
  suffix: (usize, Vec<u8>),
}

impl WHQueryStoreAggregation {
  fn position_stores_at_prev_checkpoint(time: &Time) -> Self {
    WHQueryStoreAggregation {
      prefix: WHStoreAggregationTopology::position_prefix(),
      position: WHStoreAggregationTopology::position_stores_at_prev_checkpoint(time),
      suffix: (0, vec![]),
    }
  }

  fn position_stores_at_post_checkpoint(time: &Time) -> Self {
    let prefix = WHStoreAggregationTopology::position_prefix();
    let position = WHStoreAggregationTopology::position_stores_at_post_checkpoint(time);

    WHQueryStoreAggregation { prefix, position, suffix: (0, vec![]) }
  }

  fn existence(date: Time) -> Self {
    let prefix = ID_BYTES + 10;
    let position = WHStoreAggregationTopology::position(ID_MIN, ID_MIN, date);

    WHQueryStoreAggregation { prefix, position, suffix: (0, vec![]) }
  }
}

impl PositionInTopology for WHQueryStoreAggregation {
  fn prefix(&self) -> usize {
    self.prefix
  }

  fn position(&self) -> &Vec<u8> {
    &self.position
  }

  fn suffix(&self) -> &(usize, Vec<u8>) {
    &self.suffix
  }
}

#[derive(Debug)]
struct QueryWHCheckpoint {
  prefix: usize,
  position: Vec<u8>,
}

impl QueryWHCheckpoint {
  fn new(date: &Time) -> Self {
    QueryWHCheckpoint {
      prefix: WHStoreAggregationTopology::position_prefix(),
      position: WHStoreAggregationTopology::position_checkpoint(date),
    }
  }
}

impl PositionInTopology for QueryWHCheckpoint {
  fn prefix(&self) -> usize {
    self.prefix
  }

  fn position(&self) -> &Vec<u8> {
    &self.position
  }

  fn suffix(&self) -> &(usize, Vec<u8>) {
    todo!()
  }
}

impl QueryValue<WHCheckpoint> for QueryWHCheckpoint {
  fn closest_before(&self, s: &Snapshot) -> Option<(Vec<u8>, WHCheckpoint)> {
    LightIterator::preceding_values(s, self).next()
  }

  fn values_after<'a>(&'a self, s: &'a Snapshot<'a>) -> LightIterator<'a, WHCheckpoint> {
    following_light(s, &s.cf_values(), self)
  }
}

#[derive(Debug, Hash, Eq, PartialEq)]
pub struct WHStoreAggregationTopology(pub Arc<WHStoreTopology>);

impl AggregationTopology for WHStoreAggregationTopology {
  type DependantOn = WHStoreTopology;

  type InObj = WHBalance;
  type InOp = BalanceOperation;

  type InTObj = StoreBalance;
  type InTOp = StoreMovement;

  fn depends_on(&self) -> Arc<Self::DependantOn> {
    self.0.clone()
  }

  fn on_operation(
    &self,
    tx: &mut Txn,
    ops: &Vec<DeltaOp<Self::InObj, Self::InOp, Self::InTObj, Self::InTOp>>,
  ) -> Result<(), DBError> {
    let s = tx.s;
    let ops_manager = s.rf.ops_manager.clone();

    let ts = std::time::Instant::now();

    let max = 0;
    for op in ops {
      let delta = StoreDelta::from(op);

      //tx.ops_manager().write_aggregation_delta(tx, delta)?;

      let _first_checkpoint = delta.checkpoint();

      let query = QueryWHCheckpoint::new(&delta.date);

      let mut dates = vec![];
      for (_, checkpoint) in query.values_after(s) {
        // println!("found checkpoint {:?}", checkpoint.date);
        dates.push(checkpoint.date);
      }

      if dates.is_empty() {
        let checkpoint = WHStoreAggregationTopology::next_checkpoint(&delta.date);
        let position = WHStoreAggregationTopology::position(
          delta.store.into(),
          delta.goods.into(),
          checkpoint.clone(),
        );
        let point = WHCheckpoint::new(checkpoint.clone());

        // check if required to copy previous state of existence
        match query.closest_before(s) {
          None => {
            let v = delta.to_value();
            // println!("store new checkpoint {:?} {:?}", checkpoint, v);
            tx.update_value(&position, &v.aggregation)?;
            tx.update_value(&point.position, &point)?;
          },
          Some((_, prev)) => {
            // println!("found closest checkpoint {:?} before {:?}", prev.date, delta.date);
            let query = WHQueryStoreAggregation::existence(prev.date);

            // copy previous state of existence
            for data in ops_manager.values_after_heavy(s, &query) {
              let (_k, v): (_, WarehouseStock) = data;

              // println!("copy checkpoint {:?} {:?}", checkpoint, v);

              let position = WHStoreAggregationTopology::position(
                v.store.into(),
                v.goods.into(),
                checkpoint.clone(),
              );
              tx.update_value(&position, &v.aggregation)?;
            }
            tx.update_value(&point.position, &point)?;

            dates.push(checkpoint);
          },
        }
      }

      for date in dates {
        let position =
          WHStoreAggregationTopology::position(delta.store.into(), delta.goods.into(), date.clone());
        match tx.value::<AggregationAtCheckpoint>(&position)? {
          Some(value) => {
            let result = value.apply_aggregation(&delta.op)?;

            // println!("update checkpoint {:?} {:?}", date, result);

            // update value
            if result.is_zero() {
              tx.delete_value(&position)?;
            } else {
              tx.update_value(&position, &result)?;
            }
          },
          None => {
            let value = delta.to_value();
            // println!("store checkpoint {:?} {:?}", date, value);
            tx.update_value(&position, &value.aggregation)?;
          },
        }
      }
    }

    println!("max {} {:?}", max, ts.elapsed());

    Ok(())
  }
}

impl WHStoreAggregationTopology {
  pub fn stores_turnover(
    db: &AnimoDB,
    interval: TimeInterval,
  ) -> Result<MemoOfList<NamedValue<Store, Turnover<Money, MoneyOps>>>, DBError> {
    let s = db.snapshot();
    let mut tx = Txn::new(&s);

    let memo = WHStoreAggregationTopology::stores_turnover_tx(&mut tx, interval)?;

    tx.commit()?;

    Ok(memo)
  }

  fn stores_turnover_tx(
    tx: &mut Txn,
    interval: TimeInterval,
  ) -> Result<MemoOfList<NamedValue<Store, Turnover<Money, MoneyOps>>>, DBError> {
    log::debug!("listing stores at {:?}", interval);

    let checkpoint_from = WHStoreAggregationTopology::prev_checkpoint(&interval.from);
    let checkpoint_till = WHStoreAggregationTopology::next_checkpoint(&interval.till);

    log::debug!("checkpoint from {:?} > {:?}", interval.from, checkpoint_from);
    log::debug!("checkpoint till {:?} > {:?}", interval.till, checkpoint_till);

    log::debug!("***** get stores in checkpoints interval *****");

    let mut stores: HashMap<Store, NamedValue<Store, Turnover<Money, MoneyOps>>> =
      HashMap::with_capacity(17);

    // get stores in checkpoints interval
    for data in tx.values(
      &WHQueryStoreAggregation::position_stores_at_prev_checkpoint(&interval.from),
      &WHQueryStoreAggregation::position_stores_at_post_checkpoint(&interval.till),
    ) {
      let (key, point): (_, WarehouseStock) = data;
      log::debug!("key {:?}", key);
      log::debug!("point {:?}", point);
      let named = stores
        .entry(point.store)
        .or_insert(NamedValue::new(point.store, Turnover::default()));

      let aggregation = &mut named.value;

      log::debug!(
        "{:?} vs {:?} {:?} - {:?} {:?}",
        point.date,
        checkpoint_from,
        point.date.ts() == checkpoint_from.ts(),
        checkpoint_till,
        point.date.ts() == checkpoint_till.ts()
      );
      if point.date.ts() == checkpoint_from.ts() {
        aggregation.open += Money::from(point.aggregation.balance);
      } else if point.date.ts() == checkpoint_till.ts() {
        aggregation.ops += MoneyOps::from(point.aggregation.turnover);
        aggregation.close += Money::from(point.aggregation.balance);
      } else {
        aggregation.ops += MoneyOps::from(point.aggregation.turnover);
      }

      log::debug!("on full {:?}", aggregation);
    }

    log::debug!("***** subtract operations between [checkpoint_from, from) *****");
    log::debug!("aggregation: {:?}", stores);

    // subtract operations between [checkpoint_from, from)
    if checkpoint_from.ts() < interval.from.ts() {
      for (store_id, named) in stores.iter_mut() {
        let store_id = *store_id;

        let ops_from = WHQueryStoreOperation::start(store_id, &checkpoint_from);
        let ops_till = WHQueryStoreOperation::end_exclude(store_id, &interval.from);

        log::debug!("ops_from {:?}", ops_from.position());
        log::debug!("ops_till {:?}", ops_till.position());

        let aggregation = &mut named.value;

        // TODO make sure that StoreMovement match with topology configuration
        for data in tx.operations(&ops_from, &ops_till) {
          let (_, op): (_, BalanceOperation) = data;

          log::debug!("on pre {:?}", op);

          let change: MoneyOp = op.into();
          aggregation.open += Money::from(change.clone());
          aggregation.ops -= change;

          log::debug!("op before {:?}", aggregation);
        }
      }
    }

    log::debug!("***** subtract operations between (till, checkpoint_till] *****");
    log::debug!("aggregation: {:?}", stores);

    // subtract operations between (till, checkpoint_till]
    if checkpoint_till.ts() > interval.till.ts() {
      for (store_id, named) in stores.iter_mut() {
        let store_id = *store_id;

        let ops_from = WHQueryStoreOperation::start_exclude(store_id, &interval.till);
        let ops_till = WHQueryStoreOperation::end_exclude(store_id, &checkpoint_till);

        log::debug!("ops_from {:?}", ops_from.position());
        log::debug!("ops_till {:?}", ops_till.position());

        let aggregation = &mut named.value;

        for data in tx.operations(&ops_from, &ops_till) {
          // TODO make sure that StoreMovement match with topology configuration
          let (_, op): (_, BalanceOperation) = data;

          log::debug!("on post {:?}", op);

          let change: MoneyOp = op.into();
          aggregation.close -= Money::from(change.clone());
          aggregation.ops -= change;

          log::debug!("op after {:?}", aggregation);
        }
      }
    }

    log::debug!("aggregation: {:?}", stores);

    let items = stores.values().cloned().map(Memo::new).collect();
    Ok(MemoOfList::new(items))
  }

  // beginning of current month
  fn prev_checkpoint(time: &Time) -> Time {
    time.beginning_of_month()
  }

  // beginning of next month
  fn next_checkpoint(time: &Time) -> Time {
    time.beginning_of_next_month()
  }

  fn position_of_aggregation(store: Store, goods: Goods, time: Time) -> Vec<u8> {
    let checkpoint = WHStoreAggregationTopology::next_checkpoint(&time);
    WHStoreAggregationTopology::position(store.into(), goods.into(), checkpoint)
  }

  fn position_of_value(store: Store, goods: Goods, time: Time) -> Vec<u8> {
    WHStoreAggregationTopology::position(store.into(), goods.into(), time.end())
  }

  fn position_stores_at_prev_checkpoint(time: &Time) -> Vec<u8> {
    let checkpoint = WHStoreAggregationTopology::prev_checkpoint(time);
    WHStoreAggregationTopology::position(ID_MIN, ID_MIN, checkpoint)
  }

  fn position_stores_at_post_checkpoint(time: &Time) -> Vec<u8> {
    let checkpoint = WHStoreAggregationTopology::next_checkpoint(time);
    WHStoreAggregationTopology::position(ID_MAX, ID_MAX, checkpoint)
  }

  // fn position_goods_at_prev_checkpoint(store: Store, time: &Time) -> Vec<u8> {
  //   let checkpoint = WHStoreAggregationTopology::prev_checkpoint(time);
  //   WHStoreAggregationTopology::position(store.into(), ID_MIN, checkpoint)
  // }

  // fn position_goods_exclude(store: Store, time: &Time) -> Vec<u8> {
  //   let time = time.sub_quantum();
  //   WHStoreAggregationTopology::position(store.into(), ID_MAX, time)
  // }

  fn position_prefix() -> usize {
    ID_BYTES
  }

  fn position_checkpoint(time: &Time) -> Vec<u8> {
    let mut bs = Vec::with_capacity(ID_BYTES + 10);

    // operation prefix
    bs.extend_from_slice((*WH_AGGREGATION_CHECKPOINTS).as_slice());

    // define order by time
    bs.extend_from_slice(time.to_bytes().as_slice());

    bs
  }

  fn position(store: ID, goods: ID, time: Time) -> Vec<u8> {
    let mut bs = Vec::with_capacity((ID_BYTES * 3) + 10);

    // operation prefix
    bs.extend_from_slice((*WH_AGGREGATION_TOPOLOGY).as_slice());

    // define order by time
    bs.extend_from_slice(time.to_bytes().as_slice());

    // suffix
    bs.extend_from_slice(store.as_slice());
    bs.extend_from_slice(goods.as_slice());

    bs
  }

  fn suffix(store: ID, goods: ID) -> (usize, Vec<u8>) {
    let mut bs = Vec::with_capacity(ID_BYTES * 2);

    // suffix
    bs.extend_from_slice(store.as_slice());
    bs.extend_from_slice(goods.as_slice());

    (ID_BYTES + 10, bs)
  }

  fn decode_position_from_bytes(bs: &[u8]) -> Result<(ID, ID, Time), DBError> {
    let expected = (ID_BYTES * 3) + 10;
    if bs.len() != expected {
      Err(
        format!(
          "Warehouse store topology: incorrect number ({}) of bytes, expected {}",
          bs.len(),
          expected
        )
        .into(),
      )
    } else {
      let prefix: ID = bs[0..ID_BYTES].try_into()?;
      if prefix != *WH_AGGREGATION_TOPOLOGY {
        Err(
          format!("incorrect prefix id ({:?}), expected {:?}", prefix, *WH_AGGREGATION_TOPOLOGY)
            .into(),
        )
      } else {
        let date = Time::from_bytes(bs, ID_BYTES)?;
        let store = bs[(ID_BYTES + 10)..(2 * ID_BYTES + 10)].try_into()?;
        let goods = bs[(2 * ID_BYTES + 10)..(3 * ID_BYTES + 10)].try_into()?;

        Ok((store, goods, date))
      }
    }
  }
}

// #[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize, ImplBytes)]
#[derive(Clone, Archive, Deserialize, Serialize, Debug, PartialEq)]
// This will generate a PartialEq impl between our unarchived and archived types
// #[archive(compare(PartialEq))]
// To use the safe API, you have to derive CheckBytes for the archived type
#[archive_attr(derive(CheckBytes, Debug))]
pub struct AggregationAtCheckpoint {
  number_of_ops: i32,
  turnover: BalanceOps,
  balance: WHBalance,
}

impl ToBytes for AggregationAtCheckpoint {
  fn to_bytes(&self) -> Result<AlignedVec, DBError> {
    rkyv::to_bytes::<_, 1024>(self).map_err(|e| DBError::from(e.to_string()))
  }
}

impl FromBytes<Self> for AggregationAtCheckpoint {
  fn from_bytes(bs: &[u8]) -> Result<Self, DBError> {
    let archived = unsafe { rkyv::archived_root::<Self>(bs) };
    let value: Self = archived.deserialize(&mut rkyv::Infallible).unwrap();
    Ok(value)
    // match rkyv::check_archived_root::<Self>(bs) {
    //     Ok(archived) => {
    //         let value: Self = archived.deserialize(&mut rkyv::Infallible).unwrap();
    //         Ok(value)
    //     },
    //     Err(e) => Err(DBError::from(e.to_string()))
    // }
  }
}

impl AObject<CheckpointDelta> for AggregationAtCheckpoint {
  fn is_zero(&self) -> bool {
    self.number_of_ops == 0 && self.balance.is_zero()
  }

  fn apply_aggregation(&self, op: &CheckpointDelta) -> Result<Self, DBError> {
    Ok(AggregationAtCheckpoint {
      number_of_ops: self.number_of_ops + op.number_of_ops as i32,
      turnover: self.turnover.clone() + op.op.clone(),
      balance: self.balance.apply_aggregation(&op.op)?,
    })
  }
}

impl Add<Self> for AggregationAtCheckpoint {
  type Output = Self;

  fn add(self, rhs: Self) -> Self::Output {
    AggregationAtCheckpoint {
      number_of_ops: self.number_of_ops + rhs.number_of_ops, // TODO is it correct?
      turnover: self.turnover + rhs.turnover,
      balance: self.balance + rhs.balance,
    }
  }
}

impl Sub<Self> for AggregationAtCheckpoint {
  type Output = Self;

  fn sub(self, rhs: Self) -> Self::Output {
    AggregationAtCheckpoint {
      number_of_ops: self.number_of_ops - rhs.number_of_ops, // TODO is it correct?
      turnover: self.turnover - rhs.turnover,
      balance: self.balance - rhs.balance,
    }
  }
}

impl Neg for AggregationAtCheckpoint {
  type Output = Self;

  fn neg(self) -> Self::Output {
    AggregationAtCheckpoint {
      number_of_ops: self.number_of_ops,
      turnover: -self.turnover,
      balance: -self.balance,
    }
  }
}

// #[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize, ImplBytes)]
#[derive(Clone, Archive, Deserialize, Serialize, Debug, PartialEq)]
// This will generate a PartialEq impl between our unarchived and archived types
// #[archive(compare(PartialEq))]
// To use the safe API, you have to derive CheckBytes for the archived type
// #[archive_attr(derive(CheckBytes, Debug))]
pub struct CheckpointDelta {
  number_of_ops: i8,
  op: BalanceOps,
}

impl ToBytes for CheckpointDelta {
  fn to_bytes(&self) -> Result<AlignedVec, DBError> {
    todo!()
  }
}

impl FromBytes<Self> for CheckpointDelta {
  fn from_bytes(_bs: &[u8]) -> Result<Self, DBError> {
    todo!()
  }
}

impl AOperation<AggregationAtCheckpoint> for CheckpointDelta {
  fn to_value(&self) -> AggregationAtCheckpoint {
    AggregationAtCheckpoint {
      number_of_ops: self.number_of_ops as i32,
      turnover: self.op.clone(),
      balance: self.op.to_value(),
    }
  }
}

// #[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
#[derive(Debug, Clone)]
pub struct WarehouseStock {
  aggregation: AggregationAtCheckpoint,

  store: Store,
  goods: Goods,

  date: Time,
}

// #[derive(Debug, Clone, Serialize, Deserialize)]
#[derive(Clone, Archive, Deserialize, Serialize, Debug, PartialEq)]
// This will generate a PartialEq impl between our unarchived and archived types
// #[archive(compare(PartialEq))]
// To use the safe API, you have to derive CheckBytes for the archived type
// #[archive_attr(derive(CheckBytes, Debug))]
pub struct StoreDelta {
  op: CheckpointDelta,

  // TODO avoid serialization & deserialize of prefix & position
  prefix: usize,
  position: Vec<u8>,
  suffix: (usize, Vec<u8>),

  pub(crate) date: Time,

  pub(crate) store: Store,
  pub(crate) goods: Goods,
}

impl StoreDelta {
  fn new(number_of_ops: i8, store: Store, goods: Goods, date: Time, op: BalanceOps) -> Self {
    let prefix = WHStoreAggregationTopology::position_prefix();
    let position = WHStoreAggregationTopology::position_of_value(store, goods, date.clone());
    let suffix = WHStoreAggregationTopology::suffix(store.into(), goods.into());

    let op = CheckpointDelta { number_of_ops, op };
    StoreDelta { op, store, goods, date, prefix, position, suffix }
  }
}

impl From<&DeltaOp<WHBalance, BalanceOperation, StoreBalance, StoreMovement>> for StoreDelta {
  fn from(delta: &DeltaOp<WHBalance, BalanceOperation, StoreBalance, StoreMovement>) -> Self {
    if let Some(before) = delta.before.as_ref() {
      if let Some(after) = delta.after.as_ref() {
        StoreDelta::new(
          0,
          after.store,
          after.goods,
          after.date.clone(),
          BalanceOps::from(&after.op) - BalanceOps::from(&before.op),
        )
      } else {
        StoreDelta::new(
          -1,
          before.store,
          before.goods,
          before.date.clone(),
          -BalanceOps::from(&before.op),
        )
      }
    } else if let Some(after) = delta.after.as_ref() {
      StoreDelta::new(1, after.store, after.goods, after.date.clone(), BalanceOps::from(&after.op))
    } else {
      unreachable!("internal errors")
    }
  }
}

impl PositionInTopology for StoreDelta {
  fn prefix(&self) -> usize {
    self.prefix
  }

  fn position(&self) -> &Vec<u8> {
    &self.position
  }

  fn suffix(&self) -> &(usize, Vec<u8>) {
    &self.suffix
  }
}

#[derive(Clone, Archive, Deserialize, Serialize, Debug, PartialEq)]
// This will generate a PartialEq impl between our unarchived and archived types
// #[archive(compare(PartialEq))]
// To use the safe API, you have to derive CheckBytes for the archived type
// #[archive_attr(derive(CheckBytes, Debug))]
struct WHCheckpoint {
  date: Time,
  position: Vec<u8>,
  suffix: (usize, Vec<u8>),
}

impl WHCheckpoint {
  fn new(date: Time) -> Self {
    let position = WHStoreAggregationTopology::position_checkpoint(&date);

    WHCheckpoint { date, position, suffix: (0, vec![]) }
  }
}

impl PositionInTopology for WHCheckpoint {
  fn prefix(&self) -> usize {
    WHStoreAggregationTopology::position_prefix()
  }

  fn position(&self) -> &Vec<u8> {
    &self.position
  }

  fn suffix(&self) -> &(usize, Vec<u8>) {
    &self.suffix
  }
}

impl ToBytes for WHCheckpoint {
  fn to_bytes(&self) -> Result<AlignedVec, DBError> {
    rkyv::to_bytes::<_, 512>(self).map_err(|e| DBError::from(e.to_string()))
  }
}

impl FromBytes<Self> for WHCheckpoint {
  fn from_bytes(bs: &[u8]) -> Result<Self, DBError> {
    let archived = unsafe { rkyv::archived_root::<Self>(bs) };
    let value: Self = archived.deserialize(&mut rkyv::Infallible).unwrap();
    Ok(value)
  }
}

impl ACheckpoint for WHCheckpoint {}

impl AOperationInTopology<AggregationAtCheckpoint, CheckpointDelta, WHCheckpoint, WarehouseStock>
  for StoreDelta
{
  fn position_of_aggregation(&self) -> Result<Vec<u8>, DBError> {
    Ok(WHStoreAggregationTopology::position_of_aggregation(
      self.store,
      self.goods,
      self.date.clone(),
    ))
  }

  fn checkpoint(&self) -> WHCheckpoint {
    WHCheckpoint::new(self.date.clone())
  }

  fn operation(&self) -> CheckpointDelta {
    self.op.clone()
  }

  fn to_value(&self) -> WarehouseStock {
    let op = self.operation();
    WarehouseStock {
      aggregation: op.to_value(),
      store: self.store,
      goods: self.goods,
      date: self.date.clone(),
    }
  }
}

impl ToKVBytes for WarehouseStock {
  fn to_kv_bytes(&self) -> Result<(Vec<u8>, AlignedVec), DBError> {
    let k = WHStoreAggregationTopology::position_of_value(self.store, self.goods, self.date.clone());
    let v = self.aggregation.to_bytes()?;
    Ok((k, v))
  }
}

impl FromKVBytes<Self> for WarehouseStock {
  fn from_kv_bytes(k: &[u8], v: &[u8]) -> Result<WarehouseStock, DBError> {
    let (store, goods, date) = WHStoreAggregationTopology::decode_position_from_bytes(k)?;
    let value = AggregationAtCheckpoint::from_bytes(v)?;
    Ok(WarehouseStock {
      store: Store::from(store),
      goods: Goods::from(goods),
      date,
      aggregation: value,
    })
  }
}

impl AObjectInTopology<AggregationAtCheckpoint, CheckpointDelta, WHCheckpoint, StoreDelta>
  for WarehouseStock
{
  fn position(&self) -> Vec<u8> {
    WHStoreAggregationTopology::position_of_value(self.store, self.goods, self.date.clone())
  }

  fn value(&self) -> &AggregationAtCheckpoint {
    &self.aggregation
  }

  fn apply(&self, op: &StoreDelta) -> Result<Option<Self>, DBError> {
    let result = if self.store == op.store && self.goods == op.goods {
      // TODO && self.date >= op.date
      let value = self.aggregation.apply_aggregation(&op.op)?;
      Some(WarehouseStock {
        store: self.store,
        goods: self.goods,
        date: self.date.clone(),
        aggregation: value,
      })
    } else {
      None
    };
    Ok(result)
  }
}

#[cfg(test)]
mod tests {
  use super::*;
  use crate::warehouse::test_util::*;
  use crate::{animo::memory::Memory, settings::Settings};
  use chrono::DateTime;
  use criterion::Bencher;
  use dbase::{FieldValue, Record};
  use std::fs::File;
  use std::io::{BufWriter, Write};
  use std::thread;
  use std::time::{SystemTime, UNIX_EPOCH};

  #[test]
  #[ignore]
  fn test_warehouse_store_turnover() {
    let (tmp_dir, settings, db) = init();

    let wh1: Store = ID::from("wh1").into();
    let g1: Goods = ID::from("g1").into();
    let g2: Goods = ID::from("g2").into();

    log::debug!("MODIFY A");
    db.modify(incoming("A", "2022-05-27", wh1, g1, 10, Some(50))).expect("Ok");
    log::debug!("MODIFY B");
    db.modify(incoming("B", "2022-05-30", wh1, g1, 2, Some(10))).expect("Ok");
    log::debug!("MODIFY C");
    db.modify(outgoing("C", "2022-05-28", wh1, g1, 5, Some(25))).expect("Ok");

    // 2022-05-27   qty 10  cost    50  =   50  < 2022-05-27
    // 2022-05-28   qty -5  cost    -25 =   25  < 2022-05-28
    // 2022-05-30   qty 2   cost    10  =   35

    let interval = TimeInterval::new("2022-05-27", "2022-05-28").unwrap();

    log::debug!("READING [1] 2022-05-27 - 28");
    let stores = WHStoreAggregationTopology::stores_turnover(&db, interval.clone()).expect("Ok");
    assert_eq!(1, stores.len());

    let store_details = stores.get(0).unwrap().value();
    assert_eq!(wh1, store_details.name);
    assert_eq!(
      Turnover {
        open: Money(0.into()),
        ops: MoneyOps { incoming: Money(50.into()), outgoing: Money(25.into()) },
        close: Money(25.into()),
      },
      store_details.value
    );

    log::debug!("MODIFY D");
    db.modify(incoming("D", "2022-05-15", wh1, g2, 7, Some(11))).expect("Ok");

    // 2022-05-15   qty 7   cost    11  =   11
    // 2022-05-27   qty 10  cost    50  =   61  < 2022-05-27
    // 2022-05-28   qty -5  cost    -25 =   36  < 2022-05-28
    // 2022-05-30   qty 2   cost    10  =   46
    //                        0 + 71 - 25 = 46

    log::debug!("READING [2] 2022-05-27 - 28");
    let stores = WHStoreAggregationTopology::stores_turnover(&db, interval.clone()).expect("Ok");
    assert_eq!(1, stores.len());

    // Turnover { open: Money(0), ops: MoneyOps { incoming: Money(82), outgoing: Money(25) }, close: Money(57) }

    let store_details = stores.get(0).unwrap().value();
    assert_eq!(wh1, store_details.name);
    assert_eq!(
      Turnover {
        open: Money(11.into()),
        ops: MoneyOps { incoming: Money(50.into()), outgoing: Money(25.into()) },
        close: Money(36.into()),
      },
      store_details.value
    );

    log::debug!("DELETE D");
    db.modify(delete(incoming("D", "2022-05-15", wh1, g2, 7, Some(11))))
      .expect("Ok");

    // 2022-05-27   qty 10  cost    50  =   50  < 2022-05-27
    // 2022-05-28   qty -5  cost    -25 =   25  < 2022-05-28
    // 2022-05-30   qty 2   cost    10  =   35

    log::debug!("READING [3] 2022-05-27 - 28");
    let stores = WHStoreAggregationTopology::stores_turnover(&db, interval.clone()).expect("Ok");
    assert_eq!(1, stores.len());

    let store_details = stores.get(0).unwrap().value();
    assert_eq!(wh1, store_details.name);
    assert_eq!(
      Turnover {
        open: Money(0.into()),
        ops: MoneyOps { incoming: Money(50.into()), outgoing: Money(25.into()) },
        close: Money(25.into()),
      },
      store_details.value
    );

    // stop db and delete data folder
    db.close();
    tmp_dir.close().unwrap();
  }
}
