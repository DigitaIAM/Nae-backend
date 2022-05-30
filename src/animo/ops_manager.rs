use std::marker::PhantomData;
use std::sync::Arc;
use rocksdb::{AsColumnFamilyRef, DB, DBIteratorWithThreadMode, DBWithThreadMode, Direction, IteratorMode, MultiThreaded, ReadOptions};
use crate::animo::{Env, Object, Operation};
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


    pub(crate) fn write<O, V>(&self, env: &Env, local_topology_position: Vec<u8>, op: O) -> Result<(), DBError>
    where
        O: Operation<V> + FromBytes<O> + ToBytes,
        V: Object<V,O> + FromBytes<V> + ToBytes
    {
        let s = env.pit;
        let db = self.db.clone();

        // calculate delta for propagation
        let delta = if let Some(bs) = db.get_cf(&s.cf_operations(), &local_topology_position)? {
            let current = O::from_bytes(bs.as_slice())?;
            current.delta_between_operations(&op)
        } else {
            op.delta_after_operation()
        };

        // store
        db.put_cf(&s.cf_operations(), &local_topology_position, op.to_bytes()?)?;

        // propagation
        let mut it = self.memos_after::<V>(s, &local_topology_position)?;
        while let Some((r_position, current_value)) = it.next() {
            // TODO get dependents and notify them

            let new_value = current_value.apply_delta(&delta);

            // store updated memo
            db.put_cf(&s.cf_memos(), &r_position, new_value.to_bytes()?)?;
        }

        Ok(())
    }
}