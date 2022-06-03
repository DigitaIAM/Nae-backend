use std::collections::HashSet;
use std::fmt::Debug;
use std::marker::PhantomData;
use std::sync::Arc;
use rocksdb::{AsColumnFamilyRef, DB, DBIteratorWithThreadMode, DBWithThreadMode, Direction, Error, IteratorMode, MultiThreaded, ReadOptions};
use crate::animo::{Txn, Object, Operation, AggregationOperation, ObjectInTopology, OperationInTopology, AggregationOperationInTopology, AggregationObjectInTopology, AggregationObject};
use crate::error::DBError;
use crate::rocksdb::{FromBytes, FromKVBytes, Snapshot, ToBytes, ToKVBytes};

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

fn preceding<'a,O>(s: &'a Snapshot, cf_handle: &impl AsColumnFamilyRef, key: Vec<u8>) -> ItemsIterator<'a,O> {
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

pub(crate) struct BetweenIterator<'a,O>(ItemsIterator<'a,O>, Vec<u8>);

impl<'a,O> Iterator for BetweenIterator<'a,O> where O: FromBytes<O> {
    type Item = (Vec<u8>, O);

    fn next(&mut self) -> Option<(Vec<u8>, O)> {
        match self.0.next() {
            None => None,
            Some((k, v)) => {
                if &k >= &self.1 {
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

    pub(crate) fn get_closest_memo<O: FromBytes<O>>(&self, s: &Snapshot, position: Vec<u8>) -> Result<Option<(Vec<u8>, O)>, DBError> {
        Ok(preceding(s, &s.cf_memos(), position).next())
    }

    pub(crate) fn memos_after<'a,O>(&self, s: &'a Snapshot, position: &Vec<u8>) -> ItemsIterator<'a,O> {
        following(s, &s.cf_memos(), position)
    }

    pub(crate) fn ops_between<'a,O>(&self, s: &'a Snapshot, fm: Vec<u8>, to: Vec<u8>) -> BetweenIterator<'a,O> {
        BetweenIterator(preceding(s, &s.cf_operations(), fm), to)
    }

    pub(crate) fn write_ops<BO,BV,TO,TV>(&self, tx: &mut Txn, ops: Vec<TO>) -> Result<(), DBError>
    where
        BV: Object<BO>,
        BO: Operation<BV>,

        TV: ObjectInTopology<BV,BO,TO>,
        TO: OperationInTopology<BV,BO,TV>,
    {
        for op in ops {
            // calculate delta for propagation
            let delta_op: BO = if let Some(current) = tx.get_operation::<BV,BO,TV,TO>(&op)? {
                current.delta_between(&op.operation())
            } else {
                op.operation()
            };

            // store
            tx.put_operation::<BV,BO,TV,TO>(&op)?;

            // propagation
            for (position, value) in tx.memos_after::<BV>(&op.position()) {
                // TODO get dependents and notify them

                value.apply(&delta_op);

                // store updated memo
                // TODO tx.update_value(&position, &value)?;
            }
        }

        Ok(())
    }

    pub(crate) fn write_aggregation_delta<BV,BO,TV,TO>(&self, tx: &mut Txn, op: TO) -> Result<(), DBError>
        where
            BV: AggregationObject<BO> + Debug,
            BO: AggregationOperation<BV> + Debug,
            TV: AggregationObjectInTopology<BV,BO,TO> + Debug,
            TO: AggregationOperationInTopology<BV,BO,TV> + Debug,
    {
        let local_topology_position = op.position();
        let local_topology_checkpoint = op.position_of_aggregation()?;

        debug!("propagate delta {:?} at {:?}", op, local_topology_position);

        // propagation
        for (position, value) in tx.memos_after::<BV>(&local_topology_position) {
            // TODO get dependents and notify them

            debug!("next memo {:?} at {:?}", value, position);

            let value = value.apply_aggregation(&op.operation())?;

            // store updated memo
            // TODO tx.update_value(&position, &value)?;
        }

        // make sure checkpoint exist
        match tx.get_memo::<BO>(&local_topology_checkpoint)? {
            None => {
                let value = op.to_value();
                // store checkpoint
                tx.put_value(&value)?;
            }
            Some(_) => {} // exist, updated above
        }

        Ok(())
    }
}