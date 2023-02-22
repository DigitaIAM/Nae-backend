#[macro_use] extern crate quick_error;

mod auth;
mod commutator;
mod file;
mod inventory;
mod services;
mod settings;
mod storage;
mod websocket;
mod ws;

mod accounts;
mod animo;
mod api;
mod hr;
mod memories;
mod text_search;
mod use_cases;
pub mod warehouse;

mod hik;

#[cfg(test)]
mod test;