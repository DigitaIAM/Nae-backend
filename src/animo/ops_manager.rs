use std::collections::HashSet;
use std::fmt::Debug;
use std::marker::PhantomData;
use std::sync::Arc;
use rocksdb::{AsColumnFamilyRef, DB, DBIteratorWithThreadMode, DBWithThreadMode, Direction, Error, IteratorMode, MultiThreaded, ReadOptions};
use crate::animo::{AggregationDelta, Txn, Object, Operation};
use crate::error::DBError;
use crate::rocksdb::{FromBytes, Snapshot, ToBytes};

pub struct OpsManager {
    pub(crate) db: Arc<DB>,
}

pub(crate) struct ItemsIterator<'a,O>(DBIteratorWithThreadMode<'a, DBWithThreadMode<MultiThreaded>>,PhantomData<O>);

impl<'a,O> Iterator for ItemsIterator<'a,O> where O: FromBytes<O> {
    type Item = (Vec<u8>, O);

    fn next(&mut self) -> Option<(Vec<u8>, O)> {
        match self.0.next() {
            None => None,
            Some((k, v)) => {
                let record = O::from_bytes(&*v).unwrap();
                Some((k.to_vec(), record))
            }
        }
    }
}

fn preceding<'a,O>(s: &'a Snapshot, cf_handle: &impl AsColumnFamilyRef, key: &Vec<u8>) -> ItemsIterator<'a,O> {
    let it = s.pit.iterator_cf_opt(
        cf_handle,
        ReadOptions::default(),
        IteratorMode::From(key.as_slice(), Direction::Reverse)
    );

    ItemsIterator(it,PhantomData)
}

fn following<'a,O>(s: &'a Snapshot, cf_handle: &impl AsColumnFamilyRef, key: &Vec<u8>) -> ItemsIterator<'a,O> {
    let it = s.pit.iterator_cf_opt(
        cf_handle,
        ReadOptions::default(),
        IteratorMode::From(key.as_slice(), Direction::Forward)
    );

    ItemsIterator(it, PhantomData)
}

pub(crate) struct BetweenIterator<'a,O>(ItemsIterator<'a,O>, &'a Vec<u8>);

impl<'a,O> Iterator for BetweenIterator<'a,O> where O: FromBytes<O> {
    type Item = (Vec<u8>, O);

    fn next(&mut self) -> Option<(Vec<u8>, O)> {
        match self.0.next() {
            None => None,
            Some((k, v)) => {
                if &k >= self.1 {
                    Some((k, v))
                } else {
                    None
                }
            }
        }
    }
}

impl OpsManager {

    // pub(crate) fn ops_preceding<'a, O: FromBytes<O>>(&self, s: &'a Snapshot, position: &Vec<u8>) -> Result<ItemsIterator<'a,O>, DBError> {
    //     Ok(preceding(s, &s.cf_operations(), position))
    // }

    pub(crate) fn ops_following<'a, O: FromBytes<O>>(&self, s: &'a Snapshot, position: &Vec<u8>) -> Result<ItemsIterator<'a,O>, DBError> {
        Ok(following(s, &s.cf_operations(), position))
    }

    pub(crate) fn get_closest_memo<O: FromBytes<O>>(&self, s: &Snapshot, position: &Vec<u8>) -> Result<Option<(Vec<u8>, O)>, DBError> {
        Ok(preceding(s, &s.cf_memos(), position).next())
    }

    pub(crate) fn memos_after<'a,O>(&self, s: &'a Snapshot, position: &Vec<u8>) -> Result<ItemsIterator<'a,O>, DBError> {
        Ok(following(s, &s.cf_memos(), position))
    }

    pub(crate) fn ops_between<'a,O>(&self, s: &'a Snapshot, fm: &'a Vec<u8>, to: &'a Vec<u8>) -> BetweenIterator<'a,O> {
        BetweenIterator(preceding(s, &s.cf_operations(), fm), to)
    }

    pub(crate) fn write_ops<O, V>(&self, tx: &mut Txn, ops: Vec<O>) -> Result<(), DBError>
    where
        O: Operation<V> + FromBytes<O> + ToBytes,
        V: Object<V,O> + FromBytes<V> + ToBytes
    {
        for op in ops {
            // calculate delta for propagation
            let delta = if let Some(current) = tx.get_operation(&op)? {
                current.delta_between_operations(&op)
            } else {
                op.delta_after_operation()
            };

            // store
            tx.put_operation(&op)?;

            // propagation
            for memo in tx.memos_after::<V>(&op.position())? {
                // TODO get dependents and notify them

                memo.apply_delta(&delta);

                // store updated memo
                tx.put_memo(&memo)?;
            }
        }

        Ok(())
    }

    pub(crate) fn write_aggregation_delta<O,V>(&self, env: &Txn, delta: impl AggregationDelta<V>) -> Result<(), DBError>
        where
            O: Operation<V>,
            V: Object<V,O> + FromBytes<V> + ToBytes + Debug
    {
        let s = env.s;
        let db = self.db.clone();

        let local_topology_position = delta.position();
        let local_topology_checkpoint = delta.position_of_aggregation()?;
        let delta = delta.delta();

        debug!("propagate delta {:?} at {:?}", delta, local_topology_position);

        // propagation
        for (position, memo) in self.memos_after::<V>(s, &local_topology_position)? {
            // TODO get dependents and notify them

            debug!("next memo {:?} at {:?}", memo, position);

            let new_memo = memo.apply_delta(&delta);

            // store updated memo
            db.put_cf(&s.cf_memos(), &position, new_memo.to_bytes()?)?;
        }

        // make sure checkpoint exist
        match s.pit.get_cf(&s.cf_memos(), &local_topology_checkpoint)? {
            None => {
                // store checkpoint
                db.put_cf(&s.cf_memos(), &local_topology_checkpoint, delta.to_bytes()?)?;
            }
            Some(_) => {} // exist, nothing to do
        }

        Ok(())
    }
}