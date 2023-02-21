extern crate uuid;
extern crate rust_decimal;
extern crate json;
extern crate rocksdb;
extern crate chrono;
extern crate utils;
extern crate errors;

use wh_storage::WHStorage;

mod balance;
mod check_batch_store_date;
mod check_date_store_batch;
mod date_type_store_batch_id;
mod db;
pub mod elements;
pub mod error;
mod store_date_type_batch_id;
pub mod wh_storage;

pub trait GetWarehouse {
    fn warehouse(&self) -> WHStorage;
}