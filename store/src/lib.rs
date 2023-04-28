extern crate chrono;
extern crate json;
extern crate rust_decimal;
extern crate service;
extern crate uuid;

use wh_storage::WHStorage;

pub mod agregations;
pub mod balance;
pub mod batch;
pub mod check_batch_store_date;
pub mod check_date_store_batch;
pub mod checkpoint_topology;
pub mod date_type_store_batch_id;
mod db;
pub mod elements;
pub mod error;
pub mod operations;
pub mod ordered_topology;
pub mod store_date_type_batch_id;
pub mod wh_storage;

pub trait GetWarehouse {
  fn warehouse(&self) -> WHStorage;
}
