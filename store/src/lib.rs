extern crate rust_decimal;

use wh_storage::WHStorage;

pub mod balance;
pub mod check_batch_store_date;
pub mod check_date_store_batch;
pub mod date_type_store_batch_id;
pub mod store_date_type_batch_id;
mod db;
pub mod elements;
pub mod error;
pub mod wh_storage;

pub trait GetWarehouse {
    fn warehouse(&self) -> WHStorage;
}

#[cfg(test)]
mod test;