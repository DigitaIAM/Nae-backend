use crate::animo::db::{FromBytes, FromKVBytes, Snapshot};
use crate::animo::error::DBError;
use crate::animo::{
  ACheckpoint, AObject, AObjectInTopology, AOperation, AOperationInTopology, DeltaOp, Object,
  ObjectInTopology, Operation, OperationInTopology, Txn,
};
use rocksdb::{
  AsColumnFamilyRef, DBIteratorWithThreadMode, DBWithThreadMode, Direction, IteratorMode,
  MultiThreaded, ReadOptions,
};
use std::fmt::Debug;
use std::marker::PhantomData;

pub struct OpsManager();

pub trait PositionInTopology {
  fn prefix(&self) -> usize;
  fn position(&self) -> &Vec<u8>;
  fn suffix(&self) -> &(usize, Vec<u8>);
}

pub trait QueryValue<BV>: PositionInTopology {
  fn closest_before(&self, s: &Snapshot) -> Option<(Vec<u8>, BV)>;

  fn values_after<'a>(&'a self, s: &'a Snapshot<'a>) -> LightIterator<'a, BV>;
}

pub struct LightIterator<'a, O>(
  DBIteratorWithThreadMode<'a, DBWithThreadMode<MultiThreaded>>,
  &'a [u8],
  PhantomData<O>,
);

impl<'a, O> LightIterator<'a, O> {
  pub fn preceding_values(s: &'a Snapshot, query: &'a impl QueryValue<O>) -> Self {
    let key = query.position().as_slice();
    let prefix = &key[0..query.prefix()];
    let it = preceding(s, &s.cf_values(), key);
    LightIterator(it, prefix, PhantomData)
  }
}

impl<'a, O: FromBytes<O> + Debug> Iterator for LightIterator<'a, O> {
  type Item = (Vec<u8>, O);

  fn next(&mut self) -> Option<(Vec<u8>, O)> {
    match self.0.next() {
      None => None,
      Some(Ok((k, v))) => {
        // log::debug!("next {:?}", k);
        if self.1.len() <= k.len() && self.1 == &k[0..self.1.len()] {
          let record = O::from_bytes(&v).unwrap();
          Some((k.to_vec(), record))
        } else {
          None
        }
      },
      Some(Err(_)) => None,
    }
  }
}

pub struct HeavyIterator<'a, O> {
  it: DBIteratorWithThreadMode<'a, DBWithThreadMode<MultiThreaded>>,
  prefix: &'a [u8],
  suffix: &'a (usize, Vec<u8>),
  ph: PhantomData<O>,
}

impl<'a, O: FromKVBytes<O>> Iterator for HeavyIterator<'a, O> {
  type Item = (Vec<u8>, O);

  fn next(&mut self) -> Option<(Vec<u8>, O)> {
    loop {
      match self.it.next() {
        None => break None,
        Some(Ok((k, v))) => {
          // log::debug!("next {:?}", k);
          if self.prefix.len() <= k.len() && self.prefix == &k[0..self.prefix.len()] {
            if self.suffix.1.is_empty()
              || (self.suffix.0 + self.suffix.1.len() <= k.len()
                && self.suffix.1 == &k[self.suffix.0..(self.suffix.0 + self.suffix.1.len())])
            {
              let record = O::from_kv_bytes(&k, &v).unwrap();
              break Some((k.to_vec(), record));
            } else {
              continue;
            }
          } else {
            break None;
          }
        },
        Some(Err(_)) => break None,
      }
    }
  }
}

pub fn preceding<'a>(
  s: &'a Snapshot,
  cf_handle: &impl AsColumnFamilyRef,
  key: &[u8],
) -> DBIteratorWithThreadMode<'a, DBWithThreadMode<MultiThreaded>> {
  s.pit.iterator_cf_opt(
    cf_handle,
    ReadOptions::default(),
    IteratorMode::From(key, Direction::Reverse),
  )
}

// workaround for https://github.com/rust-lang/rust/issues/83701
pub fn following_light<'a, O, PIT>(
  s: &'a Snapshot<'a>,
  cf_handle: &impl AsColumnFamilyRef,
  pit: &'a PIT,
) -> LightIterator<'a, O>
where
  PIT: PositionInTopology,
{
  let key = pit.position().as_slice();
  let prefix = &key[0..pit.prefix()];
  let it = s.pit.iterator_cf_opt(
    cf_handle,
    ReadOptions::default(),
    IteratorMode::From(key, Direction::Forward),
  );
  LightIterator(it, prefix, PhantomData)
}

// workaround for https://github.com/rust-lang/rust/issues/83701
fn following_heavy<'a, O, PIT>(
  s: &'a Snapshot,
  cf_handle: &impl AsColumnFamilyRef,
  pit: &'a PIT,
) -> HeavyIterator<'a, O>
where
  PIT: PositionInTopology,
{
  let key = pit.position().as_slice();
  let prefix = &key[0..pit.prefix()];
  let suffix = pit.suffix();
  let it = s.pit.iterator_cf_opt(
    cf_handle,
    ReadOptions::default(),
    IteratorMode::From(key, Direction::Forward),
  );
  HeavyIterator { it, prefix, suffix, ph: PhantomData }
}

pub struct BetweenLightIterator<'a, O>(LightIterator<'a, O>, &'a Vec<u8>);

impl<'a, O: FromBytes<O> + Debug> Iterator for BetweenLightIterator<'a, O> {
  type Item = (Vec<u8>, O);

  fn next(&mut self) -> Option<(Vec<u8>, O)> {
    match self.0.next() {
      None => None,
      Some((k, v)) => {
        if &k <= self.1 {
          Some((k, v))
        } else {
          None
        }
      },
    }
  }
}

pub struct BetweenHeavyIterator<'a, O>(HeavyIterator<'a, O>, Vec<u8>);

impl<'a, O: FromKVBytes<O>> Iterator for BetweenHeavyIterator<'a, O> {
  type Item = (Vec<u8>, O);

  fn next(&mut self) -> Option<(Vec<u8>, O)> {
    match self.0.next() {
      None => None,
      Some((k, v)) => {
        if &k <= &self.1 {
          Some((k, v))
        } else {
          None
        }
      },
    }
  }
}

impl OpsManager {
  pub(crate) fn ops_between_light<'a, O, F, T>(
    &self,
    s: &'a Snapshot,
    from: &'a F,
    till: &'a T,
  ) -> BetweenLightIterator<'a, O>
  where
    F: PositionInTopology,
    T: PositionInTopology,
  {
    // TODO from.prefix() == till.prefix()
    let it = following_light(s, &s.cf_operations(), from);
    BetweenLightIterator(it, till.position())
  }

  pub(crate) fn values_before_heavy<'a, O: FromKVBytes<O>>(
    &self,
    s: &'a Snapshot,
    pit: &'a impl PositionInTopology,
  ) -> HeavyIterator<'a, O> {
    following_heavy(s, &s.cf_values(), pit)
  }

  pub(crate) fn values_after<'a, O: FromBytes<O>>(
    &self,
    s: &'a Snapshot,
    pit: &'a impl PositionInTopology,
  ) -> LightIterator<'a, O> {
    following_light(s, &s.cf_values(), pit)
  }

  pub(crate) fn values_after_heavy<'a, O: FromKVBytes<O>>(
    &self,
    s: &'a Snapshot,
    pit: &'a impl PositionInTopology,
  ) -> HeavyIterator<'a, O> {
    following_heavy(s, &s.cf_values(), pit)
  }

  pub(crate) fn values_between_heavy<'a, O>(
    &self,
    s: &'a Snapshot,
    from: &'a impl PositionInTopology,
    till: &'a impl PositionInTopology,
  ) -> BetweenHeavyIterator<'a, O> {
    BetweenHeavyIterator(following_heavy(s, &s.cf_values(), from), till.position().clone())
  }

  pub(crate) fn write_ops<BV, BO, TV, TO>(
    &self,
    tx: &mut Txn,
    deltas: &Vec<DeltaOp<BV, BO, TV, TO>>,
  ) -> Result<usize, DBError>
  where
    BV: Object<BO>,
    BO: Operation<BV>,

    TV: ObjectInTopology<BV, BO, TO>,
    TO: OperationInTopology<BV, BO, TV>,
  {
    let s = tx.s;
    let ops_manager = s.rf.ops_manager.clone();

    let mut max = 0;

    for ops in deltas {
      // calculate delta for propagation
      let delta = ops.delta();

      // store
      if let Some(after) = ops.after.as_ref() {
        tx.put_operation::<BV, BO, TV, TO>(after)?;
      } else if let Some(before) = ops.before.as_ref() {
        tx.del_operation::<BV, BO, TV, TO>(before)?;
      }

      let mut count = 0;

      // propagation
      for v in ops_manager.values_after(s, ops) {
        // workaround for rust issue: https://github.com/rust-lang/rust/issues/83701
        let (position, value): (_, BV) = v;

        // TODO get dependents and notify them

        log::debug!("updating value {:?} {:?}", value, position);

        // TODO: remove `.clone()`?
        let value = value + delta.clone();

        log::debug!("updated value {:?}", value);

        // store updated memo
        tx.update_value(&position, &value)?;

        count += 1;
      }

      if max < count {
        max = count;
      }
    }

    Ok(max)
  }

  pub(crate) fn write_aggregation_delta<BV, BO, TV, TC, TO>(
    &self,
    tx: &mut Txn,
    op: TO,
  ) -> Result<usize, DBError>
  where
    BV: AObject<BO> + Debug,
    BO: AOperation<BV> + Debug,
    TV: AObjectInTopology<BV, BO, TC, TO> + Debug,
    TC: ACheckpoint + Debug,
    TO: AOperationInTopology<BV, BO, TC, TV> + Debug,
  {
    let s = tx.s;
    let ops_manager = s.rf.ops_manager.clone();

    let position = op.position();
    let checkpoint = op.position_of_aggregation()?;

    log::debug!("propagate delta {:?} at {:?}", op, position);
    log::debug!("checkpoint {:?}", checkpoint);

    let mut count = 0;

    // propagation
    for v in ops_manager.values_after_heavy(s, &op) {
      // workaround for rust issue: https://github.com/rust-lang/rust/issues/83701
      let (position, value): (_, TV) = v;

      count += 1;

      // TODO get dependents and notify them

      log::debug!("next value {:?} at {:?}", value, position);

      match value.apply(&op)? {
        Some(result) => {
          let value = result.value();
          // update value
          if value.is_zero() {
            tx.delete_value(&position)?;
          } else {
            tx.update_value(&position, value)?;
          }
        },
        None => {},
      }
    }

    // make sure checkpoint exist
    match tx.value::<BV>(&checkpoint)? {
      None => {
        let tv = op.to_value();
        log::debug!("store new checkpoint {:?} at {:?}", tv.value(), position);
        // store checkpoint
        tx.update_value(&checkpoint, tv.value())?;
      },
      Some(_) => {}, // exist, updated above
    }

    Ok(count)
  }
}
