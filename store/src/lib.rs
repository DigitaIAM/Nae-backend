use crate::error::WHError;
use rocksdb::{BoundColumnFamily, Direction, IteratorMode, DB};
use std::cmp::Ordering;
use std::sync::Arc;
use wh_storage::WHStorage;

pub mod aggregations;
pub mod balance;
pub mod batch;
pub mod checkpoints;
mod db;
pub mod elements;
pub mod error;
pub mod operations;
pub mod ordered_topology;
pub mod process_records;
pub mod topologies;
pub mod wh_storage;

pub trait GetWarehouse {
  fn warehouse(&self) -> WHStorage;
}

pub trait RangeIterator {
  fn lookup(
    &self,
    db: &Arc<DB>,
    cf: &Arc<BoundColumnFamily>,
  ) -> Result<Vec<(Vec<u8>, Vec<u8>)>, WHError>;
}

impl RangeIterator for std::ops::Range<Vec<u8>> {
  fn lookup(
    &self,
    db: &Arc<DB>,
    cf: &Arc<BoundColumnFamily>,
  ) -> Result<Vec<(Vec<u8>, Vec<u8>)>, WHError> {
    let mut result = vec![];

    let from = &self.start;
    let till = &self.end;

    for r in db.iterator_cf(cf, IteratorMode::From(from, Direction::Forward)) {
      let (k, v) = r?;
      if k.iter().as_slice().cmp(till) >= Ordering::Equal {
        break;
      }
      result.push((k.to_vec(), v.to_vec()))
    }

    Ok(result)
  }
}

impl RangeIterator for std::ops::RangeInclusive<Vec<u8>> {
  fn lookup(
    &self,
    db: &Arc<DB>,
    cf: &Arc<BoundColumnFamily>,
  ) -> Result<Vec<(Vec<u8>, Vec<u8>)>, WHError> {
    let mut result = vec![];

    let from = self.start();
    let till = self.end();

    for r in db.iterator_cf(cf, IteratorMode::From(from, Direction::Forward)) {
      let (k, v) = r?;
      if k.iter().as_slice().cmp(till) == Ordering::Greater {
        break;
      }
      result.push((k.to_vec(), v.to_vec()))
    }

    Ok(result)
  }
}
