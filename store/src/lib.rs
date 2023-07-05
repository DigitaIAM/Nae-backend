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
