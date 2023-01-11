use crate::use_cases::write;
use crate::*;
use csv::{ReaderBuilder, Trim};
use json::JsonValue;
use std::fs::File;
use std::io::{BufRead, BufReader};

use rsfbclient::{prelude::*, FbError};

pub(crate) fn import(app: &Application) {
  // #[cfg(feature = "linking")]
  let mut conn = rsfbclient::builder_native()
    .with_dyn_link()
    .with_embedded()
    .db_name("./cases/erp.fdb")
    .user("SYSDBA")
    .connect()
    .unwrap();

  // #[cfg(feature = "pure_rust")]
  // let mut conn = rsfbclient::builder_pure_rust()
  //   .host("localhost")
  //   .db_name("erp.fdb")
  //   .user("SYSDBA")
  //   .pass("masterkey")
  //   .transaction(TransactionConfiguration {
  //     lock_resolution: TrLockResolution::NoWait,
  //     ..TransactionConfiguration::default()
  //   })
  //   .connect()?;

  let rows: Vec<(String, String)> = conn
    .query("select mon$attachment_name, mon$user from mon$attachments", ())
    .unwrap();

  for row in rows {
    println!("Attachment {}, user {}", row.0, row.1);
  }
}

pub(crate) fn report(app: &Application) {}
