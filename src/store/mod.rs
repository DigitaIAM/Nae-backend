#![allow(dead_code, unused_variables, unused_imports)]

use chrono::{DateTime, Datelike, Month, NaiveDate, Utc};
use json::{array, iterators::Members, JsonValue};
use rocksdb::{ColumnFamily, ColumnFamilyDescriptor, IteratorMode, Options, ReadOptions, DB};
use rust_decimal::{prelude::ToPrimitive, Decimal};
use serde::{Deserialize, Serialize};
// use tracing_subscriber::fmt::format::Json;
use std::{
  collections::{BTreeMap, HashMap},
  num,
  ops::{Add, AddAssign, Sub, SubAssign},
  str::FromStr,
  sync::Arc,
};
use tempfile::TempDir;
use uuid::{uuid, Uuid};

// use crate::error::WHError;
// use crate::store::error::WHError;

use chrono::ParseError;
use std::string::FromUtf8Error;

use crate::{commutator::Application, services, utils::json::JsonParams};

#[derive(Clone)]
pub struct WHStorage {
  pub database: Db,
}

impl WHStorage {
  pub fn receive_operations(&self, ops: &Vec<OpMutation>) -> Result<(), WHError> {
    Ok(self.database.record_ops(ops)?)
  }

  pub fn new() -> Result<Self, WHError> {
    std::fs::create_dir_all("./database/")
      .map_err(|e| WHError::new("Can't create folder in ./database/"))?;

    let mut opts = Options::default();
    let mut cfs = Vec::new();

    let cf_names: Vec<&str> = vec![
      STORE_DATE_TYPE_PARTY_ID,
      DATE_TYPE_STORE_PARTY_ID,
      CHECK_DATE_STORE_PARTY,
      CHECK_PARTY_STORE_DATE,
    ];

    for name in cf_names {
      let cf = ColumnFamilyDescriptor::new(name, opts.clone());
      cfs.push(cf);
    }

    opts.create_if_missing(true);
    opts.create_missing_column_families(true);
    let db = Db {
      db: Arc::new(
        DB::open_cf_descriptors(&opts, "./database/", cfs)
          .expect("Can't open database in ./database/"),
      ),
    };

    Ok(WHStorage { database: db })
  }
}

#[derive(Debug)]
pub struct WHError {
  message: String,
}

impl WHError {
  pub fn new(e: &str) -> Self {
    WHError { message: e.to_string() }
  }

  pub fn message(&self) -> String {
    self.message.clone()
  }
}

impl From<rocksdb::Error> for WHError {
  fn from(e: rocksdb::Error) -> Self {
    WHError { message: e.to_string() }
  }
}

impl From<serde_json::Error> for WHError {
  fn from(e: serde_json::Error) -> Self {
    WHError { message: e.to_string() }
  }
}

impl From<ParseError> for WHError {
  fn from(e: ParseError) -> Self {
    WHError { message: e.to_string() }
  }
}

impl From<FromUtf8Error> for WHError {
  fn from(e: FromUtf8Error) -> Self {
    WHError { message: e.to_string() }
  }
}

impl From<rust_decimal::Error> for WHError {
  fn from(e: rust_decimal::Error) -> Self {
    WHError { message: e.to_string() }
  }
}

impl From<uuid::Error> for WHError {
  fn from(e: uuid::Error) -> Self {
    WHError { message: e.to_string() }
  }
}

impl From<services::Error> for WHError {
  fn from(e: services::Error) -> Self {
    WHError { message: e.to_string() }
  }
}

type Goods = Uuid;
type Store = Uuid;
type Qty = Decimal;
type Cost = Decimal;

// trait TryFromStr {
//   fn try_from(value: Option<String>) -> Result<Self, WHError>;
// }

// impl TryFromStr for Uuid {
//   fn try_from(value: Option<String>) -> Result<Uuid, WHError> {
//     if let Some(v) = value {
//       Ok(Uuid::parse_str(v.as_str())?)
//     } else {
//       Err(WHError::new("Error parsing uuid from str"))
//     }
//   }
// }

// impl TryFromStr for Decimal {
//   fn try_from(value: Option<String>) -> Result<Self, WHError> {
//     if let Some(v) = value {
//       Ok(Decimal::from_str(v.as_str())?)
//     } else {
//       Err(WHError::new("Error parsing decimal from str"))
//     }
//   }
// }

const G1: Uuid = Uuid::from_u128(1);
const G2: Uuid = Uuid::from_u128(2);
const G3: Uuid = Uuid::from_u128(3);

const UUID_NIL: Uuid = uuid!("00000000-0000-0000-0000-000000000000");
const UUID_MAX: Uuid = uuid!("ffffffff-ffff-ffff-ffff-ffffffffffff");

const STORE_DATE_TYPE_PARTY_ID: &str = "cf_store_date_type_party_id";
const DATE_TYPE_STORE_PARTY_ID: &str = "cf_date_type_store_party_id";

const CHECK_DATE_STORE_PARTY: &str = "cf_check_date_store_party";
const CHECK_PARTY_STORE_DATE: &str = "cf_check_party_store_date";

#[derive(Eq, Ord, PartialOrd, Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Document {
  id: String,
  date: DateTime<Utc>,
}

impl Default for Document {
  fn default() -> Self {
    Self { id: Default::default(), date: Default::default() }
  }
}

trait WareHouse {
  fn put_op(&self, op: &OpMutation, db: &Db) -> Result<(), WHError>;

  fn create_cf(&self, opts: Options) -> ColumnFamilyDescriptor;

  fn get_ops(
    &self,
    start_d: DateTime<Utc>,
    end_d: DateTime<Utc>,
    wh: Store,
    db: &Db,
  ) -> Result<Vec<OpMutation>, WHError>;

  fn get_report(
    &self,
    start_date: DateTime<Utc>,
    end_date: DateTime<Utc>,
    wh: Store,
    db: &mut Db,
  ) -> Result<Report, WHError> {
    let checkpoints = db.search_last_checkpoints(start_date, wh)?;

    let ops = self.get_ops(first_day_current_month(start_date), end_date, wh, db)?;

    let mut old_balances = BTreeMap::new();

    for checkpoint in checkpoints {
      let balance = old_balances.entry(checkpoint.party()).or_insert(Balance {
        date: checkpoint.date,
        store: checkpoint.store,
        goods: checkpoint.goods,
        document: checkpoint.document.clone(),
        number: NumberForGoods::default(),
      });

      balance.number += &checkpoint.number;
    }

    let items = new_get_agregations(old_balances, ops, start_date);

    Ok(Report { start_date, end_date, items })
  }
}

struct DateTypeStoreGoodsId();
impl WareHouse for DateTypeStoreGoodsId {
  fn get_ops(
    &self,
    start_d: DateTime<Utc>,
    end_d: DateTime<Utc>,
    wh: Store,
    db: &Db,
  ) -> Result<Vec<OpMutation>, WHError> {
    let start_date = start_d.timestamp() as u64;
    let from: Vec<u8> = start_date
      .to_be_bytes()
      .iter()
      .chain(0_u8.to_be_bytes().iter())
      .chain(UUID_NIL.as_bytes().iter())
      .map(|b| *b)
      .collect();

    let end_date = end_d.timestamp() as u64;
    let till: Vec<u8> = end_date
      .to_be_bytes()
      .iter()
      .chain(u8::MAX.to_be_bytes().iter())
      .chain(UUID_MAX.as_bytes().iter())
      .map(|b| *b)
      .collect();

    let mut options = ReadOptions::default();
    options.set_iterate_range(from..till);

    if let Some(handle) = db.db.cf_handle(DATE_TYPE_STORE_PARTY_ID) {
      let iter = db.db.iterator_cf_opt(&handle, options, IteratorMode::Start);

      let mut res = Vec::new();

      for item in iter {
        let (_, value) = item?;
        let op = serde_json::from_slice(&value)?;
        res.push(op);
      }

      Ok(res)
    } else {
      Err(WHError::new("There are no operations in db"))
    }
  }

  fn put_op(&self, op: &OpMutation, db: &Db) -> Result<(), WHError> {
    if let Some(cf) = db.db.cf_handle(DATE_TYPE_STORE_PARTY_ID) {
      db.db.put_cf(&cf, op.date_type_store_party_id(), op.value()?)?;

      Ok(())
    } else {
      Err(WHError::new("Can't get cf from db in fn put_op"))
    }
  }

  fn create_cf(&self, opts: Options) -> ColumnFamilyDescriptor {
    ColumnFamilyDescriptor::new(DATE_TYPE_STORE_PARTY_ID, opts)
  }
}

struct StoreDateTypeGoodsId();
impl WareHouse for StoreDateTypeGoodsId {
  fn get_ops(
    &self,
    start_d: DateTime<Utc>,
    end_d: DateTime<Utc>,
    wh: Store,
    db: &Db,
  ) -> Result<Vec<OpMutation>, WHError> {
    let start_date = start_d.timestamp() as u64;
    let from: Vec<u8> = wh
      .as_bytes()
      .iter()
      .chain(start_date.to_be_bytes().iter())
      .chain(0_u8.to_be_bytes().iter())
      .map(|b| *b)
      .collect();

    let end_date = end_d.timestamp() as u64;
    let till = wh
      .as_bytes()
      .iter()
      .chain(end_date.to_be_bytes().iter())
      .chain(u8::MAX.to_be_bytes().iter())
      .map(|b| *b)
      .collect();

    let mut options = ReadOptions::default();
    options.set_iterate_range(from..till);

    if let Some(handle) = db.db.cf_handle(STORE_DATE_TYPE_PARTY_ID) {
      let iter = db.db.iterator_cf_opt(&handle, options, IteratorMode::Start);

      let mut res = Vec::new();
      for item in iter {
        let (_, value) = item?;
        let op = serde_json::from_slice(&value)?;
        res.push(op);
      }

      Ok(res)
    } else {
      Err(WHError::new("There are no operations in db"))
    }
  }

  fn put_op(&self, op: &OpMutation, db: &Db) -> Result<(), WHError> {
    if let Some(cf) = db.db.cf_handle(STORE_DATE_TYPE_PARTY_ID) {
      db.db.put_cf(&cf, op.store_date_type_party_id(), op.value()?)?;

      Ok(())
    } else {
      Err(WHError::new("Can't get cf from db in fn put_op"))
    }
  }

  fn create_cf(&self, opts: Options) -> ColumnFamilyDescriptor {
    ColumnFamilyDescriptor::new(STORE_DATE_TYPE_PARTY_ID, opts)
  }
}

pub fn dt(date: &str) -> Result<DateTime<Utc>, WHError> {
  let res = DateTime::parse_from_rfc3339(format!("{date}T00:00:00Z").as_str())?.into();

  Ok(res)
}

fn first_day_current_month(date: DateTime<Utc>) -> DateTime<Utc> {
  let naivedate = NaiveDate::from_ymd(date.year(), date.month(), 1).and_hms(0, 0, 0);
  DateTime::<Utc>::from_utc(naivedate, Utc)
}

fn first_day_next_month(date: DateTime<Utc>) -> DateTime<Utc> {
  let d = date.naive_local();
  let (year, month) = if d.month() == Month::December.number_from_month() {
    (d.year() + 1, Month::January.number_from_month())
  } else {
    (d.year(), d.month() + 1)
  };

  let naivedate = NaiveDate::from_ymd(year, month, 1).and_hms(0, 0, 0);
  DateTime::<Utc>::from_utc(naivedate, Utc)
}

fn min_party() -> Vec<u8> {
  UUID_NIL
    .as_bytes()
    .iter()
    .chain(u64::MIN.to_be_bytes().iter())
    .chain(UUID_NIL.as_bytes().iter())
    .map(|b| *b)
    .collect()
}

fn max_party() -> Vec<u8> {
  UUID_MAX
    .as_bytes()
    .iter()
    .chain(u64::MAX.to_be_bytes().iter())
    .chain(UUID_MAX.as_bytes().iter())
    .map(|b| *b)
    .collect()
}

#[derive(Clone)]
pub struct Db {
  db: Arc<DB>,
}

impl Db {
  fn put(&self, key: &Vec<u8>, value: &String) -> Result<(), WHError> {
    match self.db.put(key, value) {
      Ok(_) => Ok(()),
      Err(_) => Err(WHError::new("Can't put into a database")),
    }
  }

  fn get(&self, key: &Vec<u8>) -> Result<String, WHError> {
    match self.db.get(key) {
      Ok(Some(res)) => Ok(String::from_utf8(res)?),
      Ok(None) => Err(WHError::new("Can't get from database - no such value")),
      Err(_) => Err(WHError::new("Something wrong with getting from database")),
    }
  }

  fn find_checkpoint(&self, op: &OpMutation, name: &str) -> Result<Option<Balance>, WHError> {
    let bal = Balance {
      date: first_day_next_month(op.date),
      store: op.store,
      goods: op.goods,
      document: op.document.clone(),
      number: if let Some(o) = &op.after {
        match o {
          SOperation::Receive(n) => n.clone(),
          SOperation::Issue(n) => n.clone(),
        }
      } else {
        NumberForGoods::default()
      },
    };

    if let Some(cf) = self.db.cf_handle(name) {
      if let Ok(Some(v1)) = self.db.get_cf(&cf, bal.key(name)?) {
        let b = serde_json::from_slice(&v1)?;
        Ok(b)
      } else {
        Ok(None)
      }
    } else {
      Err(WHError::new("Can't get cf from db in fn find_checkpoint"))
    }
  }

  fn record_ops(&self, ops: &Vec<OpMutation>) -> Result<(), WHError> {
    let cf_names = DB::list_cf(&Options::default(), DB::path(&self.db))?;
    for op in ops {
      for name in &cf_names {
        if name == "default" {
          continue;
        } else if name == CHECK_DATE_STORE_PARTY || name == CHECK_PARTY_STORE_DATE {
          if op.before.is_none() {
            self.insert_checkpoint(&op, name)?;
          } else {
            self.change_checkpoint(&op, name)?;
          }
        } else {
          if let Some(cf) = self.db.cf_handle(name.as_str()) {
            if op.before.is_none() {
              self.db.put_cf(&cf, op.key(name)?, op.value()?)?;
            } else {
              if let Ok(Some(bytes)) = self.db.get_cf(&cf, op.key(name)?) {
                let o: OpMutation = serde_json::from_slice(&bytes)?;
                if op.before == o.after {
                  self.db.put_cf(&cf, op.key(name)?, op.value()?)?;
                } else {
                  return Err(WHError::new("Wrong 'before' state in operation"));
                }
              }
            }
          }
        }
      }
    }

    Ok(())
  }

  fn change_checkpoint(&self, op: &OpMutation, name: &str) -> Result<(), WHError> {
    let mut bal = Balance {
      date: first_day_next_month(op.date),
      store: op.store,
      goods: op.goods,
      document: op.document.clone(),
      number: NumberForGoods::default(),
    };

    let from = bal.key(name)?;
    let mut till: Vec<u8> = Vec::new();

    if name == CHECK_PARTY_STORE_DATE {
      till = op
        .party()
        .iter()
        .chain(op.store.as_bytes().iter())
        .chain(u64::MAX.to_be_bytes().iter())
        .map(|b| *b)
        .collect();
    } else if name == CHECK_DATE_STORE_PARTY {
      till = u64::MAX
        .to_be_bytes()
        .iter()
        .chain(op.store.as_bytes().iter())
        .chain(op.party().iter())
        .map(|b| *b)
        .collect();
    }

    let mut readopts = ReadOptions::default();
    readopts.set_iterate_range(from..till);
    let cf = self.db.cf_handle(name).expect("option in change_checkpoint");

    let mut iter = self.db.iterator_cf_opt(&cf, readopts, IteratorMode::Start);

    while let Some(res) = iter.next() {
      let (_, v) = res?;
      let mut b: Balance = serde_json::from_slice(&v)?;

      if b.document == op.document && b.store == op.store && b.goods == op.goods {
        if op.after.is_none() {
          bal.number += op.before.as_ref().expect("option in change_checkpoint");
          if bal.number.qty == Decimal::new(0, 0) {
            self.db.delete_cf(&cf, bal.key(name)?)?;
          } else {
            self.db.put_cf(&cf, bal.key(name)?, bal.value()?)?;
          }
        } else {
          b.number += &op.delta()?;

          self.db.put_cf(&cf, b.key(name)?, b.value()?)?;
        }
      }
    }

    Ok(())
  }

  fn insert_checkpoint(&self, op: &OpMutation, name: &str) -> Result<(), WHError> {
    let mut bal = Balance {
      date: first_day_current_month(op.date),
      store: op.store,
      goods: op.goods,
      document: op.document.clone(),
      number: NumberForGoods::default(),
    };

    let cf = self.db.cf_handle(name).expect("option in insert_checkpoint");

    let mut old = NumberForGoods::default();

    // найти предыдущий чекпоинт с помощью ключа баланса
    if let Ok(Some(v)) = self.db.get_cf(&cf, bal.key(name)?) {
      let b: Balance = serde_json::from_slice(&v)?;

      old += &b.number;
    }

    bal.date = first_day_next_month(op.date);

    if let Ok(Some(v)) = self.db.get_cf(&cf, bal.key(name)?) {
      let mut b: Balance = serde_json::from_slice(&v)?;

      if let Some(o) = &op.after {
        match o {
          SOperation::Receive(n) => {
            b.number += n;
          },
          SOperation::Issue(n) => {
            b.number -= n;
          },
        }
      } else {
        b.number = NumberForGoods::default();
      }

      if b.number.cost == Some(0.into())
        || b.number.cost.is_none()
        || b.number.qty == Decimal::new(0, 0)
      {
        self.db.delete_cf(&cf, b.key(name)?)?;
      } else {
        self.db.put_cf(&cf, b.key(name)?, b.value()?)?;
      }
    } else {
      if let Some(o) = &op.after {
        match o {
          SOperation::Receive(n) => {
            bal.number = bal.number + old + n.clone();
          },
          SOperation::Issue(n) => {
            bal.number = bal.number - old - n.clone();
          },
        }

        self.db.put_cf(&cf, bal.key(name)?, bal.value()?)?;
      }
    }

    Ok(())
  }

  fn search_last_checkpoints(
    &mut self,
    date: DateTime<Utc>,
    wh: Store,
  ) -> Result<Vec<Balance>, WHError> {
    let mut result = Vec::new();

    if let Some(cf) = self.db.cf_handle(CHECK_DATE_STORE_PARTY) {
      let ts = first_day_current_month(date).timestamp() as u64;

      let from: Vec<u8> = ts
        .to_be_bytes()
        .iter()
        .chain(wh.as_bytes().iter())
        .chain(min_party().iter())
        .map(|b| *b)
        .collect();
      let till: Vec<u8> = ts
        .to_be_bytes()
        .iter()
        .chain(wh.as_bytes().iter())
        .chain(max_party().iter())
        .map(|b| *b)
        .collect();
      let mut opts = ReadOptions::default();
      opts.set_iterate_range(from..till);

      let mut iter = self.db.iterator_cf_opt(&cf, opts, IteratorMode::Start);

      while let Some(res) = iter.next() {
        let (_, value) = res?;
        let balance = serde_json::from_slice(&value)?;
        result.push(balance);
      }
    } else {
      let opts = Options::default();
      self.db.create_cf(CHECK_DATE_STORE_PARTY, &opts)?;
    }

    Ok(result)
  }

  fn search_next_checkpoints(
    &mut self,
    date: DateTime<Utc>,
    wh: Store,
  ) -> Result<Vec<Balance>, WHError> {
    let mut result = Vec::new();

    if let Some(cf) = self.db.cf_handle(CHECK_DATE_STORE_PARTY) {
      let dt = date.timestamp() as u64;
      let from: Vec<u8> = dt
        .to_be_bytes()
        .iter()
        .chain(wh.as_bytes().iter())
        .chain(max_party().iter())
        .map(|b| *b)
        .collect();
      let mut iter =
        self.db.iterator_cf(&cf, IteratorMode::From(&from, rocksdb::Direction::Forward));
      while let Some(res) = iter.next() {
        let (_, value) = res?;
        let bal = serde_json::from_slice(&value)?;
        result.push(bal)
      }
    } else {
      let opts = Options::default();
      self.db.create_cf(CHECK_DATE_STORE_PARTY, &opts)?;
    }

    Ok(result)
  }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct NumberForGoods {
  qty: Qty,
  cost: Option<Cost>,
}

impl NumberForGoods {
  fn is_equal_operation(&self, oper: &Option<SOperation>) -> bool {
    if let Some(o) = oper {
      match o {
        SOperation::Receive(n) if self == n => return true,
        SOperation::Issue(n) if self == n => return true,
        &_ => (),
      }
    }

    false
  }
}

impl Default for NumberForGoods {
  fn default() -> Self {
    Self { qty: 0.into(), cost: Some(0.into()) }
  }
}

impl AddAssign<&Self> for NumberForGoods {
  fn add_assign(&mut self, rhs: &Self) {
    if let Some(c) = rhs.cost {
      self.cost = Some(self.cost.unwrap_or(0.into()) + c);
    }
    self.qty += rhs.qty;
  }
}

impl SubAssign<&Self> for NumberForGoods {
  fn sub_assign(&mut self, rhs: &Self) {
    if let Some(c) = rhs.cost {
      self.cost = Some(self.cost.unwrap_or(0.into()) - c);
    } else {
      if let Some(sc) = self.cost {
        let price = sc / self.qty;
        self.cost = Some(sc - price * rhs.qty);
      }
    }
    self.qty -= rhs.qty;
  }
}

impl Add for NumberForGoods {
  type Output = NumberForGoods;

  fn add(self, rhs: Self) -> Self::Output {
    NumberForGoods {
      qty: self.qty + rhs.qty,
      cost: Some(self.cost.unwrap_or(0.into()) + rhs.cost.unwrap_or(0.into())),
    }
  }
}

impl Sub for NumberForGoods {
  type Output = NumberForGoods;

  fn sub(self, rhs: Self) -> Self::Output {
    NumberForGoods {
      qty: self.qty - rhs.qty,
      cost: if let Some(c) = rhs.cost {
        Some(self.cost.unwrap_or(0.into()) - c)
      } else {
        if let Some(sc) = self.cost {
          let price = sc / self.qty;
          Some(sc - price * rhs.qty)
        } else {
          None
        }
      },
    }
  }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum SOperation {
  Receive(NumberForGoods),
  Issue(NumberForGoods),
}

impl Add<SOperation> for NumberForGoods {
  type Output = NumberForGoods;

  fn add(mut self, rhs: SOperation) -> Self::Output {
    match rhs {
      SOperation::Receive(n) => {
        self.cost = Some(self.cost.unwrap_or(0.into()) + n.cost.unwrap_or(0.into()));
        self.qty += n.qty;
      },
      SOperation::Issue(n) => {
        if let Some(c) = n.cost {
          self.cost = Some(self.cost.unwrap_or(0.into()) - c);
        } else {
          if let Some(sc) = self.cost {
            let price = sc / self.qty;
            self.cost = Some(sc - price * n.qty);
          }
        }
        self.qty -= n.qty;
      },
    }
    self
  }
}

impl AddAssign<&SOperation> for NumberForGoods {
  fn add_assign(&mut self, rhs: &SOperation) {
    match rhs {
      SOperation::Receive(n) => {
        self.qty += n.qty;
        self.cost = Some(self.cost.unwrap_or(0.into()) + n.cost.unwrap_or(0.into()));
      },
      SOperation::Issue(n) => {
        self.qty -= n.qty;
        if let Some(c) = n.cost {
          self.cost = Some(self.cost.unwrap_or(0.into()) - c);
        } else {
          if let Some(sc) = self.cost {
            let price = sc / self.qty;
            self.cost = Some(sc - price * n.qty);
          }
        }
      },
    }
  }
}

trait KeyValueStore {
  fn key(&self, s: &String) -> Result<Vec<u8>, WHError>;
  fn store_date_type_party_id(&self) -> Vec<u8>;
  fn date_type_store_party_id(&self) -> Vec<u8>;
  fn value(&self) -> Result<String, WHError>;
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct Balance {
  // key
  date: DateTime<Utc>,
  store: Store,
  goods: Goods,
  document: Document,
  // value
  number: NumberForGoods,
}

impl Default for Balance {
  fn default() -> Self {
    Self {
      date: Default::default(),
      store: Default::default(),
      goods: Default::default(),
      document: Default::default(),
      number: NumberForGoods::default(),
    }
  }
}

impl AddAssign<&OpMutation> for Balance {
  fn add_assign(&mut self, rhs: &OpMutation) {
    self.date = rhs.date;
    self.goods = rhs.goods;
    self.store = rhs.store;
    if let Some(o) = &rhs.after {
      self.number += o;
    }
  }
}

impl Balance {
  fn key_party_store_date(&self) -> Vec<u8> {
    let dt = self.date.timestamp() as u64;
    let key = self
      .party()
      .iter()
      .chain(self.store.as_bytes().iter())
      .chain(dt.to_be_bytes().iter())
      .map(|b| *b)
      .collect();
    key
  }

  fn key_date_store_party(&self) -> Vec<u8> {
    let dt = self.date.timestamp() as u64;
    let key = dt
      .to_be_bytes()
      .iter()
      .chain(self.store.as_bytes().iter())
      .chain(self.party().iter())
      .map(|b| *b)
      .collect();
    key
  }

  fn key(&self, s: &str) -> Result<Vec<u8>, WHError> {
    match s {
      CHECK_DATE_STORE_PARTY => Ok(self.key_date_store_party()),
      CHECK_PARTY_STORE_DATE => Ok(self.key_party_store_date()),
      _ => Err(WHError::new("Wrong Balance key type")),
    }
  }

  fn value(&self) -> Result<String, WHError> {
    Ok(serde_json::to_string(&self)?)
  }

  fn party(&self) -> Vec<u8> {
    let dt = self.document.date.timestamp() as u64;

    self
      .goods
      .as_bytes()
      .iter()
      .chain(dt.to_be_bytes().iter())
      .chain(self.document.id.as_bytes().iter())
      .map(|b| *b)
      .collect()
  }
}

struct Op {
  // key
  id: Uuid,
  date: DateTime<Utc>,
  store: Store,
  goods: Goods,
  document: Document,
  // value
  op: Option<SOperation>,
}

impl Op {
  fn party(&self) -> Vec<u8> {
    let dt = self.document.date.timestamp() as u64;

    self
      .goods
      .as_bytes()
      .iter()
      .chain(dt.to_be_bytes().iter())
      .chain(self.document.id.as_bytes().iter())
      .map(|b| *b)
      .collect()
  }
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub struct OpMutation {
  // key
  id: Uuid,
  date: DateTime<Utc>,
  store: Store,
  goods: Goods,
  document: Document,
  // value
  before: Option<SOperation>,
  after: Option<SOperation>,
}

impl OpMutation {
  fn new(
    id: Uuid,
    date: DateTime<Utc>,
    store: Store,
    goods: Goods,
    document: Document,
    before: Option<SOperation>,
    after: Option<SOperation>,
  ) -> OpMutation {
    OpMutation { id, date, store, goods, document, before, after }
  }

  fn party(&self) -> Vec<u8> {
    let dt = self.document.date.timestamp() as u64;

    self
      .goods
      .as_bytes()
      .iter()
      .chain(dt.to_be_bytes().iter())
      .chain(self.document.id.as_bytes().iter())
      .map(|b| *b)
      .collect()
  }

  fn delta(&self) -> Result<SOperation, WHError> {
    let old = self.before.clone().expect("option in delta");

    if let Some(new) = self.after.clone() {
      match new {
        SOperation::Receive(n) => match old {
          SOperation::Receive(o) => Ok(SOperation::Receive(n - o)),
          SOperation::Issue(o) => Err(WHError::new("Wrong op type in delta!")),
        },
        SOperation::Issue(n) => match old {
          SOperation::Receive(o) => Err(WHError::new("Wrong op type in delta!")),
          SOperation::Issue(o) => Ok(SOperation::Issue(n - o)),
        },
      }
    } else {
      Ok(old)
    }
  }

  fn new_from_ops(before: Option<Op>, after: Option<Op>) -> Result<OpMutation, WHError> {
    if let (Some(b), Some(a)) = (&before, &after) {
      Ok(OpMutation {
        id: a.id,
        date: a.date,
        store: a.store,
        goods: a.goods,
        document: a.document.clone(),
        before: b.op.clone(),
        after: a.op.clone(),
      })
    } else if let Some(b) = &before {
      Ok(OpMutation {
        id: b.id,
        date: b.date,
        store: b.store,
        goods: b.goods,
        document: b.document.clone(),
        before: b.op.clone(),
        after: None,
      })
    } else if let Some(a) = &after {
      Ok(OpMutation {
        id: a.id,
        date: a.date,
        store: a.store,
        goods: a.goods,
        document: a.document.clone(),
        before: None,
        after: a.op.clone(),
      })
    } else {
      Err(WHError::new("Both before and after states are None. It shouldn't happened"))
    }
  }
}

impl KeyValueStore for OpMutation {
  fn key(&self, s: &String) -> Result<Vec<u8>, WHError> {
    match s.as_str() {
      STORE_DATE_TYPE_PARTY_ID => Ok(self.store_date_type_party_id()),
      DATE_TYPE_STORE_PARTY_ID => Ok(self.date_type_store_party_id()),
      _ => Err(WHError::new("Wrong Op key type")),
    }
  }

  fn store_date_type_party_id(&self) -> Vec<u8> {
    let ts = self.date.timestamp() as u64;
    // if after == None, this operation will be recorded last (that's why op_type by default is 3)
    let mut op_type = 3_u8;

    if let Some(o) = &self.after {
      op_type = match o {
        SOperation::Receive(_) => 1_u8,
        SOperation::Issue(_) => 2_u8,
      };
    }

    let key = self
      .store
      .as_bytes()
      .iter()
      .chain(ts.to_be_bytes().iter())
      .chain(op_type.to_be_bytes().iter())
      .chain(self.party().iter())
      .chain(self.id.as_bytes().iter())
      .map(|b| *b)
      .collect();

    key
  }

  fn value(&self) -> Result<String, WHError> {
    Ok(serde_json::to_string(&self)?)
  }

  fn date_type_store_party_id(&self) -> Vec<u8> {
    let ts = self.date.timestamp() as u64;
    // if after == None, this operation will be recorded last (that's why op_type by default is 3)
    let mut op_type = 3_u8;

    if let Some(o) = &self.after {
      op_type = match o {
        SOperation::Receive(_) => 1_u8,
        SOperation::Issue(_) => 2_u8,
      };
    }

    let key = ts
      .to_be_bytes()
      .iter()
      .chain(op_type.to_be_bytes().iter())
      .chain(self.store.as_bytes().iter())
      .chain(self.party().iter())
      .chain(self.id.as_bytes().iter())
      .map(|b| *b)
      .collect();

    key
  }
}

enum ReturnType {
  Good(AgregationStoreGoods),
  Store(AgregationStore),
  Empty,
}

trait Agregation {
  fn check(&mut self, op: &OpMutation) -> ReturnType; // если операция валидна, вернет None, если нет - вернет свое значение и обнулит себя и выставит новые ключи
  fn apply_operation(&mut self, op: &OpMutation);
  fn apply_agregation(&mut self, agr: Option<&AgregationStoreGoods>);
  fn balance(&mut self, balance: Option<&Balance>) -> ReturnType; // имплементировать для трех возможных ситуаций
  fn is_applyable_for(&self, op: &OpMutation) -> bool;
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
struct AgregationStoreGoods {
  // ключ
  store: Option<Store>,
  goods: Option<Goods>,
  document: Option<Document>,
  // агрегация
  open_balance: NumberForGoods,
  receive: NumberForGoods,
  issue: NumberForGoods,
  close_balance: NumberForGoods,
}

#[derive(PartialEq, Debug, Clone)]
struct AgregationStore {
  // ключ (контекст)
  store: Option<Store>,
  // агрегация
  open_balance: Cost,
  receive: Cost,
  issue: Cost,
  close_balance: Cost,
}

impl AgregationStoreGoods {
  fn initialize(&mut self, op: &OpMutation) {
    self.store = Some(op.store);
    self.goods = Some(op.goods);
    self.open_balance = NumberForGoods::default();
    self.receive = NumberForGoods::default();
    self.issue = NumberForGoods::default();
    self.close_balance = NumberForGoods::default();
  }

  fn add_op_to_balance(&mut self, op: &OpMutation) {
    self.store = Some(op.store);
    self.goods = Some(op.goods);
    self.document = Some(op.document.clone());

    if let Some(o) = &op.after {
      match o {
        SOperation::Receive(n) => {
          self.open_balance += n;
          self.close_balance += n;
        },
        SOperation::Issue(n) => {
          self.open_balance -= n;
          self.close_balance -= n;
        },
      }
    } else {
      self.open_balance = NumberForGoods::default();
      self.close_balance = NumberForGoods::default();
    }
  }

  fn party(&self) -> Vec<u8> {
    let mut key = Vec::new();
    if let Some(doc) = &self.document {
      key = self
        .goods
        .expect("option in party")
        .as_bytes()
        .iter()
        .chain((doc.date.timestamp() as u64).to_be_bytes().iter())
        .chain(doc.id.as_bytes().iter())
        .map(|b| *b)
        .collect();
    }
    key
  }
}

impl Default for AgregationStoreGoods {
  fn default() -> Self {
    Self {
      store: None,
      goods: None,
      document: None,
      open_balance: NumberForGoods::default(),
      receive: NumberForGoods::default(),
      issue: NumberForGoods::default(),
      close_balance: NumberForGoods::default(),
    }
  }
}

impl AddAssign<&OpMutation> for AgregationStoreGoods {
  fn add_assign(&mut self, rhs: &OpMutation) {
    self.store = Some(rhs.store);
    self.goods = Some(rhs.goods);
    self.document = Some(rhs.document.clone());
    self.apply_operation(rhs);
  }
}

impl AgregationStore {
  fn initialize(&mut self, op: &OpMutation) {
    // задаем новый ключ
    self.store = Some(op.store);
    // обнуляем собственные значения
    self.open_balance = 0.into();
    self.receive = 0.into();
    self.issue = 0.into();
    self.close_balance = 0.into();
  }
}

impl Default for AgregationStore {
  fn default() -> Self {
    Self {
      store: None,
      open_balance: 0.into(),
      receive: 0.into(),
      issue: 0.into(),
      close_balance: 0.into(),
    }
  }
}

impl Agregation for AgregationStore {
  fn is_applyable_for(&self, op: &OpMutation) -> bool {
    todo!()
  }

  fn check(&mut self, op: &OpMutation) -> ReturnType {
    if let Some(store) = self.store {
      // проверяем валидна ли эта операция к агрегации
      if op.store == store {
        // если да, то выходим из функции
        ReturnType::Empty
      } else {
        // в противном случае клонируем собственное значение (агрегацию)
        let clone = self.clone();
        self.initialize(op);
        // возвращаем копию предыдущей агрегации
        ReturnType::Store(clone)
      }
    } else {
      self.initialize(op);
      ReturnType::Empty
    }
  }

  fn apply_operation(&mut self, op: &OpMutation) {
    if let Some(o) = &op.after {
      match o {
        SOperation::Receive(n) => {
          self.receive += n.cost.unwrap_or(0.into());
          self.close_balance += n.cost.unwrap_or(0.into());
        },
        SOperation::Issue(n) => {
          self.issue += n.cost.unwrap_or(0.into());
          self.close_balance -= n.cost.unwrap_or(0.into());
        },
      }
    }
  }

  fn balance(&mut self, balance: Option<&Balance>) -> ReturnType {
    todo!()
  }

  fn apply_agregation(&mut self, agr: Option<&AgregationStoreGoods>) {
    if let Some(agr) = agr {
      self.store = agr.store;
      self.open_balance += agr.open_balance.cost.unwrap_or(0.into());
      self.receive += agr.receive.cost.unwrap_or(0.into());
      self.issue += agr.issue.cost.unwrap_or(0.into());
      self.close_balance += agr.close_balance.cost.unwrap_or(0.into());
    }
  }
}

impl Agregation for AgregationStoreGoods {
  fn balance(&mut self, balance: Option<&Balance>) -> ReturnType {
    if let Some(b) = balance {
      if b.goods < self.goods.expect("option in fn balance") {
        // вернуть новую агрегацию с балансом без операций
        ReturnType::Good(AgregationStoreGoods {
          store: Some(b.store),
          goods: Some(b.goods),
          document: Some(b.document.clone()),
          open_balance: b.number.clone(),
          receive: NumberForGoods::default(),
          issue: NumberForGoods::default(),
          close_balance: b.number.clone(),
        })
      } else if b.goods > self.goods.expect("option in fn balance") {
        // None
        ReturnType::Empty
      } else {
        // вернуть обновленную агрегацию
        self.open_balance = b.number.clone();
        ReturnType::Good(self.clone())
      }
    } else {
      // None
      ReturnType::Empty
    }
  }

  fn check(&mut self, op: &OpMutation) -> ReturnType {
    if self.store.is_none() || self.goods.is_none() {
      self.initialize(op);
      ReturnType::Empty
    } else if op.store == self.store.expect("option in fn check")
      && op.goods == self.goods.expect("option in fn check")
    {
      ReturnType::Empty
    } else {
      let clone = self.clone();
      self.initialize(op);
      ReturnType::Good(clone)
    }
  }

  fn is_applyable_for(&self, op: &OpMutation) -> bool {
    if self.store.is_none() || self.goods.is_none() {
      false
    } else if op.store == self.store.expect("option in is_applyable_for")
      && op.goods == self.goods.expect("option in is_applyable_for")
    {
      true
    } else {
      false
    }
  }

  fn apply_operation(&mut self, op: &OpMutation) {
    if let Some(o) = &op.after {
      match o {
        SOperation::Receive(n) => {
          self.receive += n;
        },
        SOperation::Issue(n) => {
          if let Some(c) = n.cost {
            self.issue += n;
          } else {
            let issue = self.open_balance.clone() + self.receive.clone();
            let price = issue.cost.unwrap_or(0.into()) / issue.qty;
            self.issue.cost = Some(self.issue.cost.unwrap_or(0.into()) + price * n.qty);
            self.issue.qty += n.qty;
          }
        },
      }
      self.close_balance = self.open_balance.clone() + self.receive.clone() - self.issue.clone();
    } else {
      self.issue = self.open_balance.clone() + self.receive.clone();
      self.close_balance = NumberForGoods::default();
    }
  }

  fn apply_agregation(&mut self, agr: Option<&AgregationStoreGoods>) {
    todo!()
  }
}

impl KeyValueStore for AgregationStoreGoods {
  fn store_date_type_party_id(&self) -> Vec<u8> {
    todo!()
  }

  fn value(&self) -> Result<String, WHError> {
    Ok(serde_json::to_string(&self)?)
  }

  fn date_type_store_party_id(&self) -> Vec<u8> {
    todo!()
  }

  fn key(&self, s: &String) -> Result<Vec<u8>, WHError> {
    todo!()
  }
}

#[derive(Debug, PartialEq)]
struct Report {
  start_date: DateTime<Utc>,
  end_date: DateTime<Utc>,
  items: (AgregationStore, Vec<AgregationStoreGoods>),
}

fn new_get_agregations(
  balances: BTreeMap<Vec<u8>, Balance>,
  operations: Vec<OpMutation>,
  start_date: DateTime<Utc>,
) -> (AgregationStore, Vec<AgregationStoreGoods>) {
  let mut agregations = BTreeMap::new();
  let mut master_agregation = AgregationStore::default();

  for (product, balance) in balances {
    agregations.insert(
      product,
      AgregationStoreGoods {
        store: Some(balance.store),
        goods: Some(balance.goods),
        document: Some(balance.document),
        open_balance: balance.number.clone(),
        receive: NumberForGoods::default(),
        issue: NumberForGoods::default(),
        close_balance: balance.number,
      },
    );
  }

  for op in operations {
    if op.date < start_date {
      agregations
        .entry(op.party())
        .or_insert(AgregationStoreGoods::default())
        .add_op_to_balance(&op);
    } else {
      *agregations.entry(op.party()).or_insert(AgregationStoreGoods::default()) += &op;
    }
  }

  let mut res = Vec::new();

  for (_, agr) in agregations {
    master_agregation.apply_agregation(Some(&agr));
    res.push(agr);
  }

  (master_agregation, res)
}

pub fn receive_data(
  app: &Application,
  time: DateTime<Utc>,
  mut data: JsonValue,
  ctx: &Vec<String>,
  mut before: JsonValue,
) -> Result<JsonValue, WHError> {
  let store = data["storage"].uuid();
  let document = Document { id: data["_id"].string(), date: data["date"].date()? };

  // TODO make a vector from data["goods"] and iterate for it making Vec<Op> and send it to receive_operations()

  let mut before =
    json_to_ops(&mut before, store.clone(), document.clone(), time.clone(), ctx)?.into_iter();
  let mut after = json_to_ops(&mut data, store, document, time, ctx)?.into_iter();

  let mut b_op = before.next();
  let mut a_op = after.next();

  let mut ops: Vec<OpMutation> = Vec::new();

  while b_op.is_some() || a_op.is_some() {
    if let (Some(b), Some(a)) = (&b_op, &a_op) {
      if b.id == a.id && b.party() == a.party() {
        // create new OpMut with both (delta will be finded and propagated later in receive_operations())
        ops.push(OpMutation::new_from_ops(b_op, a_op)?);

        b_op = before.next();
        a_op = after.next();
      } else if b.party() > a.party() {
        // create new OpMut with a
        ops.push(OpMutation::new_from_ops(None, a_op)?);

        a_op = after.next();
      } else if b.party() < a.party() {
        //create new OpMut with b
        ops.push(OpMutation::new_from_ops(b_op, None)?);

        b_op = before.next();
      }
    } else if let Some(b) = &b_op {
      // create new OpMut with b
      ops.push(OpMutation::new_from_ops(b_op, None)?);

      b_op = before.next();
    } else if let Some(a) = &a_op {
      // create new OpMut with a
      ops.push(OpMutation::new_from_ops(None, a_op)?);

      a_op = after.next();
    }
  }

  app.warehouse.receive_operations(&ops)?;

  // return data with _tids
  Ok(data)
}

fn json_to_ops(
  data: &mut JsonValue,
  store: Uuid,
  document: Document,
  time: DateTime<Utc>,
  ctx: &Vec<String>,
) -> Result<Vec<Op>, WHError> {
  let mut ops = Vec::new();

  if *data != JsonValue::Null {
    for goods in data["goods"].members_mut() {
      let op = Op {
        // id: if _tid == None create new and inject _tid to data, else _tid
        id: if let Some(tid) = goods["_tid"].uuid_or_none() {
          tid
        } else {
          goods["_tid"] = JsonValue::String(Uuid::new_v4().to_string());
          goods["_tid"].uuid()
        },
        date: time,
        store,
        goods: goods["goods"].uuid(),
        document: document.clone(),
        op: if ctx == &vec!["warehouse".to_string(), "receive".to_string()] {
          Some(SOperation::Receive(NumberForGoods {
            qty: goods["qty"].number(),
            cost: Some(goods["cost"].number()),
          }))
        } else if ctx == &vec!["warehouse".to_string(), "issue".to_string()] {
          Some(SOperation::Issue(NumberForGoods {
            qty: goods["qty"].number(),
            cost: Some(goods["cost"].number()),
          }))
        } else {
          break;
        },
      };
      ops.push(op);
    }
  }
  Ok(ops)
}

#[cfg(test)]
mod tests {
  use std::io;

  use crate::{
    animo::{
      db::AnimoDB,
      memory::{Memory, ID},
    },
    api,
    docs::DocsFiles,
    services::{Error, Services},
    settings::{self, Settings},
    storage::SOrganizations,
  };

  use super::*;
  use crate::warehouse::test_util::init;
  use actix_web::{http::header::ContentType, test, web, App};
  use json::object;
  use rocksdb::{ColumnFamilyDescriptor, Options};
  use uuid::Uuid;

  #[actix_web::test]
  async fn store_test_put_ops_with_app() {
    let (tmp_dir, settings, db) = init();

    let (mut app, events_receiver) = Application::new(Arc::new(settings), Arc::new(db))
      .await
      .map_err(|e| io::Error::new(io::ErrorKind::Unsupported, e))
      .unwrap();

    let storage = SOrganizations::new("./data_test/companies/");
    app.storage = Some(storage.clone());

    app.register(DocsFiles::new(app.clone(), "docs", storage.clone()));

    let app = test::init_service(
      App::new()
        // .app_data(web::Data::new(db.clone()))
        .app_data(web::Data::new(app.clone()))
        // .wrap(middleware::Logger::default())
        .service(api::docs_create)
        // .service(api::memory_modify)
        // .service(api::memory_query)
        .default_service(web::route().to(api::not_implemented)),
    )
    .await;

    // let doc1 = Uuid::new_v4();
    let goods1 = Uuid::new_v4();
    let storage1 = Uuid::new_v4();
    let oid = ID::random();

    let mut data1: JsonValue = object! {
        _id: "2023-01-17T13:02:15.787Z",
        date: "2023-01-11",
        storage: storage1.to_string(),
        goods: [
            {
                goods: goods1.to_string(),
                uom: "",
                qty: 1,
                price: 10,
                cost: 10,
                _tid: ""
            },
            {
                goods: goods1.to_string(),
                uom: "",
                qty: 2,
                price: 8,
                cost: 16,
                _tid: ""
            }
        ]
    };

    let req = test::TestRequest::post()
      .uri(&format!("/api/docs?oid={}&document=warehouse,receive", oid.to_base64()))
      .set_payload(data1.dump())
      .insert_header(ContentType::json())
      // .param("oid", oid.to_base64())
      // .param("document", "warehouse")
      // .param("document", "receive")
      .to_request();

    let response = test::call_and_read_body(&app, req).await;

    let result: serde_json::Value = serde_json::from_slice(&response).unwrap();

    assert_ne!("", result["goods"][0]["_tid"].as_str().unwrap());
    assert_ne!("", result["goods"][1]["_tid"].as_str().unwrap());
  }

  #[actix_web::test]
    async fn store_test_receive_ops() {
      let tmpdir = TempDir::new().expect("Can't create tmp dir in test_get_wh_balance");

      let mut opts = Options::default();
      let mut cfs = Vec::new();

      let cf_names: Vec<&str> = vec![
        STORE_DATE_TYPE_PARTY_ID,
        DATE_TYPE_STORE_PARTY_ID,
        CHECK_DATE_STORE_PARTY,
        CHECK_PARTY_STORE_DATE,
      ];

      for name in cf_names {
        let cf = ColumnFamilyDescriptor::new(name, opts.clone());
        cfs.push(cf);
      }

      opts.create_if_missing(true);
      opts.create_missing_column_families(true);
      let mut db = Db {
        db: Arc::new(
          DB::open_cf_descriptors(&opts, &tmpdir, cfs).expect("Can't open db in test_receive_ops"),
        ),
      };

      let op_d = dt("2022-10-10").expect("test_receive_ops");
      let check_d = dt("2022-11-01").expect("test_receive_ops");
      let w1 = Uuid::new_v4();
      let party = Document { id: Uuid::new_v4().to_string(), date: op_d };

      let id1 = Uuid::from_u128(101);
      let id2 = Uuid::from_u128(102);
      let id3 = Uuid::from_u128(103);
      let id4 = Uuid::from_u128(104);

      let ops = vec![
        OpMutation::new(
          id1,
          op_d,
          w1,
          G1,
          party.clone(),
          None,
          Some(SOperation::Receive(NumberForGoods { qty: 3.into(), cost: Some(3000.into()) })),
        ),
        OpMutation::new(
          id2,
          op_d,
          w1,
          G1,
          party.clone(),
          None,
          Some(SOperation::Issue(NumberForGoods { qty: 1.into(), cost: Some(1000.into()) })),
        ),
        OpMutation::new(
          id3,
          op_d,
          w1,
          G2,
          party.clone(),
          None,
          Some(SOperation::Issue(NumberForGoods { qty: 2.into(), cost: Some(2000.into()) })),
        ),
        OpMutation::new(
          id4,
          op_d,
          w1,
          G2,
          party.clone(),
          None,
          Some(SOperation::Receive(NumberForGoods { qty: 2.into(), cost: Some(2000.into()) })),
        ),
      ];

      db.record_ops(&ops).expect("test_receive_ops");

      let balance = Balance {
        date: check_d,
        store: w1,
        goods: G1,
        document: party,
        number: NumberForGoods { qty: 2.into(), cost: Some(2000.into()) },
      };

      let b1 = db.find_checkpoint(&ops[0], CHECK_DATE_STORE_PARTY).expect("test_receive_ops");
      assert_eq!(b1, Some(balance.clone()));

      let b2 = db.find_checkpoint(&ops[0], CHECK_PARTY_STORE_DATE).expect("test_receive_ops");
      assert_eq!(b2, Some(balance));

      let b3 = db.find_checkpoint(&ops[2], CHECK_DATE_STORE_PARTY).expect("test_receive_ops");
      assert_eq!(b3, None);

      let b4 = db.find_checkpoint(&ops[2], CHECK_PARTY_STORE_DATE).expect("test_receive_ops");
      assert_eq!(b4, None);

      tmpdir.close().expect("Can't close tmp dir in test_receive_ops");
    }

    #[actix_web::test]
    async fn store_test_neg_balance_date_type_store_goods_id() {
      let tmpdir = TempDir::new().expect("Can't create tmp dir in test_get_neg_balance");
      let mut opts = Options::default();
      let cf = ColumnFamilyDescriptor::new(DATE_TYPE_STORE_PARTY_ID, opts.clone());
      opts.create_if_missing(true);
      opts.create_missing_column_families(true);
      let mut db = Db {
        db: Arc::new(
          DB::open_cf_descriptors(&opts, &tmpdir, vec![cf])
            .expect("Can't open db in test_get_neg_balance"),
        ),
      };

      let op_d = dt("2022-10-10").expect("test_get_neg_balance");
      let check_d = dt("2022-10-11").expect("test_get_neg_balance");
      let w1 = Uuid::new_v4();
      let party = Document { id: Uuid::new_v4().to_string(), date: op_d };

      let id1 = Uuid::from_u128(101);

      let ops = vec![OpMutation::new(
        id1,
        op_d,
        w1,
        G1,
        party.clone(),
        None,
        Some(SOperation::Issue(NumberForGoods { qty: 2.into(), cost: Some(2000.into()) })),
      )];

      db.record_ops(&ops).expect("test_get_neg_balance");

      let agr = AgregationStoreGoods {
        store: Some(w1),
        goods: Some(G1),
        document: Some(party.clone()),
        open_balance: NumberForGoods::default(),
        receive: NumberForGoods::default(),
        issue: NumberForGoods { qty: 2.into(), cost: Some(2000.into()) },
        close_balance: NumberForGoods { qty: (-2).into(), cost: Some((-2000).into()) },
      };

      let st = DateTypeStoreGoodsId();
      let res = st.get_report(op_d, check_d, w1, &mut db).expect("test_get_neg_balance");

      assert_eq!(res.items.1[0], agr);

      tmpdir.close().expect("Can't close tmp dir in test_get_neg_balance");
    }

    #[actix_web::test]
    async fn store_test_zero_balance_date_type_store_goods_id() {
      let tmpdir = TempDir::new().expect("Can't create tmp dir in test_get_zero_balance");
      let mut opts = Options::default();
      let cf = ColumnFamilyDescriptor::new(DATE_TYPE_STORE_PARTY_ID, opts.clone());
      opts.create_if_missing(true);
      opts.create_missing_column_families(true);
      let mut db = Db {
        db: Arc::new(
          DB::open_cf_descriptors(&opts, &tmpdir, vec![cf])
            .expect("Can't open db in test_get_zero_balance"),
        ),
      };

      let start_d = dt("2022-10-10").expect("test_get_zero_balance");
      let end_d = dt("2022-10-11").expect("test_get_zero_balance");
      let w1 = Uuid::new_v4();
      let party = Document { id: Uuid::new_v4().to_string(), date: start_d };

      let id1 = Uuid::from_u128(101);
      let id2 = Uuid::from_u128(102);

      let ops = vec![
        OpMutation::new(
          id1,
          start_d,
          w1,
          G1,
          party.clone(),
          None,
          Some(SOperation::Receive(NumberForGoods { qty: 3.into(), cost: Some(3000.into()) })),
        ),
        OpMutation::new(
          id2,
          start_d,
          w1,
          G1,
          party.clone(),
          None,
          Some(SOperation::Issue(NumberForGoods { qty: 3.into(), cost: Some(3000.into()) })),
        ),
      ];

      db.record_ops(&ops);

      let st = DateTypeStoreGoodsId();
      let res = st.get_report(start_d, end_d, w1, &mut db).expect("test_get_zero_balance");

      let agr = AgregationStoreGoods {
        store: Some(w1),
        goods: Some(G1),
        document: Some(party.clone()),
        open_balance: NumberForGoods::default(),
        receive: NumberForGoods { qty: 3.into(), cost: Some(3000.into()) },
        issue: NumberForGoods { qty: 3.into(), cost: Some(3000.into()) },
        close_balance: NumberForGoods::default(),
      };

      assert_eq!(res.items.1[0], agr);

      tmpdir.close().expect("Can't close tmp dir in test_get_zero_balance");
    }

    #[actix_web::test]
    async fn store_test_get_wh_ops_date_type_store_goods_id() {
      get_wh_ops(DateTypeStoreGoodsId()).expect("test_get_wh_ops_date_type_store_goods_id");
    }

    #[actix_web::test]
    async fn store_test_get_wh_ops_store_date_type_goods_id() {
      get_wh_ops(StoreDateTypeGoodsId()).expect("test_get_wh_ops_store_date_type_goods_id");
    }

    fn get_wh_ops(key: impl WareHouse) -> Result<(), WHError> {
      let tmpdir = TempDir::new().expect("Can't create tmp dir in test_get_wh_balance");
      let mut opts = Options::default();
      let cf = key.create_cf(opts.clone());
      opts.create_if_missing(true);
      opts.create_missing_column_families(true);
      let db = Db { db: Arc::new(DB::open_cf_descriptors(&opts, &tmpdir, vec![cf])?) };

      let start_d = dt("2022-10-10")?;
      let end_d = dt("2022-10-11")?;
      let w1 = Uuid::new_v4();
      let party = Document { id: Uuid::new_v4().to_string(), date: start_d };

      let id1 = Uuid::from_u128(101);
      let id2 = Uuid::from_u128(102);

      let ops = vec![
        OpMutation::new(
          id1,
          start_d,
          w1,
          G1,
          party.clone(),
          None,
          Some(SOperation::Receive(NumberForGoods { qty: 2.into(), cost: Some(2000.into()) })),
        ),
        OpMutation::new(
          id2,
          start_d,
          w1,
          G1,
          party.clone(),
          None,
          Some(SOperation::Receive(NumberForGoods { qty: 1.into(), cost: Some(1000.into()) })),
        ),
      ];

      for op in &ops {
        key.put_op(op, &db)?;
      }

      let res = key.get_ops(start_d, end_d, w1, &db)?;

      for i in 0..ops.len() {
        assert_eq!(ops[i], res[i]);
      }

      Ok(())
    }

    #[actix_web::test]
    async fn store_test_get_agregations_without_checkpoints_date_type_store_goods_id() {
      get_agregations_without_checkpoints(DateTypeStoreGoodsId()).expect("test_get_agregations");
    }

    #[actix_web::test]
    async fn store_test_get_agregations_without_checkpoints_store_date_type_goods_id() {
      get_agregations_without_checkpoints(StoreDateTypeGoodsId()).expect("test_get_agregations");
    }

    fn get_agregations_without_checkpoints(key: impl WareHouse) -> Result<(), WHError> {
      let tmpdir = TempDir::new().expect("Can't create tmp dir in test_get_wh_balance");
      let mut opts = Options::default();
      let cf = key.create_cf(opts.clone());
      opts.create_if_missing(true);
      opts.create_missing_column_families(true);
      let mut db = Db { db: Arc::new(DB::open_cf_descriptors(&opts, &tmpdir, vec![cf])?) };

      let op_d = dt("2022-10-10")?;
      let check_d = dt("2022-10-11")?;
      let w1 = Uuid::new_v4();
      let doc1 = Document { id: Uuid::new_v4().to_string(), date: dt("2022-10-09")? };
      let doc2 = Document { id: Uuid::new_v4().to_string(), date: op_d };

      let id1 = Uuid::from_u128(101);
      let id2 = Uuid::from_u128(102);
      let id3 = Uuid::from_u128(103);
      let id4 = Uuid::from_u128(104);

      let ops = vec![
        OpMutation::new(
          id1,
          op_d,
          w1,
          G1,
          doc1.clone(),
          None,
          Some(SOperation::Receive(NumberForGoods { qty: 3.into(), cost: Some(3000.into()) })),
        ),
        OpMutation::new(
          id2,
          op_d,
          w1,
          G1,
          doc1.clone(),
          None,
          Some(SOperation::Issue(NumberForGoods { qty: 1.into(), cost: Some(1000.into()) })),
        ),
        OpMutation::new(
          id3,
          op_d,
          w1,
          G2,
          doc2.clone(),
          None,
          Some(SOperation::Issue(NumberForGoods { qty: 2.into(), cost: Some(2000.into()) })),
        ),
        OpMutation::new(
          id4,
          op_d,
          w1,
          G2,
          doc2.clone(),
          None,
          Some(SOperation::Receive(NumberForGoods { qty: 2.into(), cost: Some(2000.into()) })),
        ),
      ];

      for op in &ops {
        key.put_op(op, &db)?;
      }

      let agregations = vec![
        AgregationStoreGoods {
          store: Some(w1),
          goods: Some(G1),
          document: Some(doc1.clone()),
          open_balance: NumberForGoods { qty: 0.into(), cost: Some(0.into()) },
          receive: NumberForGoods { qty: 3.into(), cost: Some(3000.into()) },
          issue: NumberForGoods { qty: 1.into(), cost: Some(1000.into()) },
          close_balance: NumberForGoods { qty: 2.into(), cost: Some(2000.into()) },
        },
        AgregationStoreGoods {
          store: Some(w1),
          goods: Some(G2),
          document: Some(doc2.clone()),
          open_balance: NumberForGoods { qty: 0.into(), cost: Some(0.into()) },
          receive: NumberForGoods { qty: 2.into(), cost: Some(2000.into()) },
          issue: NumberForGoods { qty: 2.into(), cost: Some(2000.into()) },
          close_balance: NumberForGoods { qty: 0.into(), cost: Some(0.into()) },
        },
      ];

      let res = key.get_report(op_d, check_d, w1, &mut db)?;
      let mut iter = res.items.1.into_iter();

      // println!("MANY BALANCES: {:#?}", res);

      for agr in agregations {
        assert_eq!(iter.next().expect("option in get_agregations"), agr);
      }
      assert_eq!(iter.next(), None);

      tmpdir.close().expect("Can't close tmp dir in store_test_get_wh_balance");

      Ok(())
    }

    #[actix_web::test]
    async fn store_test_op_iter() {
      let tmp_dir = TempDir::new().expect("Can't create tmp dir in test_op_iter");

      let db = Db { db: Arc::new(DB::open_default(&tmp_dir).expect("test_op_iter")) };

      let start_d = dt("2022-11-01").expect("test_op_iter");
      let w1 = Uuid::new_v4();
      let party = Document { id: Uuid::new_v4().to_string(), date: start_d };

      let id1 = Uuid::from_u128(101);
      let id2 = Uuid::from_u128(102);
      let id3 = Uuid::from_u128(103);
      let id4 = Uuid::from_u128(104);

      let ops = vec![
        OpMutation::new(
          id3,
          start_d,
          w1,
          G2,
          party.clone(),
          None,
          Some(SOperation::Receive(NumberForGoods { qty: 1.into(), cost: Some(1000.into()) })),
        ),
        OpMutation::new(
          id4,
          start_d,
          w1,
          G2,
          party.clone(),
          None,
          Some(SOperation::Receive(NumberForGoods { qty: 1.into(), cost: Some(1000.into()) })),
        ),
        OpMutation::new(
          id1,
          start_d,
          w1,
          G3,
          party.clone(),
          None,
          Some(SOperation::Issue(NumberForGoods {
            qty: Decimal::from_str("0.5").unwrap(),
            cost: Some(1500.into()),
          })),
        ),
        OpMutation::new(
          id2,
          start_d,
          w1,
          G3,
          party.clone(),
          None,
          Some(SOperation::Issue(NumberForGoods {
            qty: Decimal::from_str("0.5").unwrap(),
            cost: Some(1500.into()),
          })),
        ),
      ];

      for op in &ops {
        db.put(&op.store_date_type_party_id(), &op.value().expect("test_op_iter"))
          .expect("Can't put op in db in test_op_iter");
      }

      let iter = db.db.iterator(IteratorMode::Start);

      let mut res = Vec::new();

      for item in iter {
        let (_, v) = item.unwrap();
        let op = serde_json::from_slice(&v).unwrap();
        res.push(op);
      }

      for i in 0..ops.len() {
        assert_eq!(ops[i], res[i]);
      }

      tmp_dir.close().expect("Can't remove tmp dir in test_op_iter");
    }

    #[actix_web::test]
    async fn store_test_report() {
      let tmp_dir = TempDir::new().expect("Can't create tmp dir in test_report");

      let key1 = DateTypeStoreGoodsId();
      let key2 = StoreDateTypeGoodsId();

      let mut opts = Options::default();
      let mut cfs = Vec::new();

      let cf_names: Vec<&str> = vec![
        STORE_DATE_TYPE_PARTY_ID,
        DATE_TYPE_STORE_PARTY_ID,
        CHECK_DATE_STORE_PARTY,
        CHECK_PARTY_STORE_DATE,
      ];

      for name in cf_names {
        let cf = ColumnFamilyDescriptor::new(name, opts.clone());
        cfs.push(cf);
      }

      opts.create_if_missing(true);
      opts.create_missing_column_families(true);
      let mut db = Db {
        db: Arc::new(
          DB::open_cf_descriptors(&opts, &tmp_dir, cfs).expect("Can't open db in test_report"),
        ),
      };

      let start_d = dt("2022-11-07").expect("test_report");
      let end_d = dt("2022-11-08").expect("test_report");
      let w1 = Uuid::new_v4();
      let party = Document { id: Uuid::new_v4().to_string(), date: start_d };

      let ops = vec![
        OpMutation::new(
          Uuid::new_v4(),
          dt("2022-10-30").expect("test_report"),
          w1,
          G1,
          party.clone(),
          None,
          Some(SOperation::Receive(NumberForGoods { qty: 4.into(), cost: Some(4000.into()) })),
        ),
        OpMutation::new(
          Uuid::new_v4(),
          dt("2022-11-03").expect("test_report"),
          w1,
          G3,
          party.clone(),
          None,
          Some(SOperation::Receive(NumberForGoods { qty: 2.into(), cost: Some(6000.into()) })),
        ),
        OpMutation::new(
          Uuid::new_v4(),
          start_d,
          w1,
          G2,
          party.clone(),
          None,
          Some(SOperation::Receive(NumberForGoods { qty: 1.into(), cost: Some(1000.into()) })),
        ),
        OpMutation::new(
          Uuid::new_v4(),
          start_d,
          w1,
          G2,
          party.clone(),
          None,
          Some(SOperation::Receive(NumberForGoods { qty: 1.into(), cost: Some(1000.into()) })),
        ),
        OpMutation::new(
          Uuid::new_v4(),
          start_d,
          w1,
          G3,
          party.clone(),
          None,
          Some(SOperation::Issue(NumberForGoods {
            qty: Decimal::from_str("0.5").unwrap(),
            cost: Some(1500.into()),
          })),
        ),
        OpMutation::new(
          Uuid::new_v4(),
          start_d,
          w1,
          G3,
          party.clone(),
          None,
          Some(SOperation::Issue(NumberForGoods {
            qty: Decimal::from_str("0.5").unwrap(),
            cost: Some(1500.into()),
          })),
        ),
      ];

      db.record_ops(&ops).expect("test_report");

      let agr_store = AgregationStore {
        store: Some(w1),
        open_balance: 10000.into(),
        receive: 2000.into(),
        issue: 3000.into(),
        close_balance: 9000.into(),
      };

      let ex_items = vec![
        AgregationStoreGoods {
          store: Some(w1),
          goods: Some(G1),
          document: Some(party.clone()),
          open_balance: NumberForGoods { qty: 4.into(), cost: Some(4000.into()) },
          receive: NumberForGoods::default(),
          issue: NumberForGoods::default(),
          close_balance: NumberForGoods { qty: 4.into(), cost: Some(4000.into()) },
        },
        AgregationStoreGoods {
          store: Some(w1),
          goods: Some(G2),
          document: Some(party.clone()),
          open_balance: NumberForGoods::default(),
          receive: NumberForGoods { qty: 2.into(), cost: Some(2000.into()) },
          issue: NumberForGoods::default(),
          close_balance: NumberForGoods { qty: 2.into(), cost: Some(2000.into()) },
        },
        AgregationStoreGoods {
          store: Some(w1),
          goods: Some(G3),
          document: Some(party.clone()),
          open_balance: NumberForGoods { qty: 2.into(), cost: Some(6000.into()) },
          receive: NumberForGoods::default(),
          issue: NumberForGoods { qty: 1.into(), cost: Some(3000.into()) },
          close_balance: NumberForGoods { qty: 1.into(), cost: Some(3000.into()) },
        },
      ];

      let mut ex_map: HashMap<Goods, AgregationStoreGoods> = HashMap::new();

      for agr in ex_items.clone() {
        ex_map.insert(agr.goods.unwrap(), agr);
      }

      let report1 = key1.get_report(start_d, end_d, w1, &mut db).expect("test_report");
      let report2 = key2.get_report(start_d, end_d, w1, &mut db).expect("test_report");

      // println!("ITEMS: {:#?}", report1.items);

      assert_eq!(report1, report2);

      assert_eq!(report1.items.0, agr_store);
      assert_eq!(report1.items.1, ex_items);

      for item in report2.items.1 {
        assert_eq!(&item, ex_map.get(&item.goods.unwrap()).unwrap());
      }

      tmp_dir.close().expect("Can't remove tmp dir in test_report");
    }

    #[actix_web::test]
    async fn store_test_parties_date_type_store_goods_id() {
      let tmpdir = TempDir::new().expect("Can't create tmp dir in test_parties");
      let mut opts = Options::default();
      let cf = ColumnFamilyDescriptor::new(DATE_TYPE_STORE_PARTY_ID, opts.clone());
      opts.create_if_missing(true);
      opts.create_missing_column_families(true);
      let mut db = Db {
        db: Arc::new(
          DB::open_cf_descriptors(&opts, &tmpdir, vec![cf]).expect("Can't open db in test_parties"),
        ),
      };

      let start_d = dt("2022-10-10").expect("test_parties");
      let end_d = dt("2022-10-11").expect("test_parties");
      let w1 = Uuid::new_v4();
      let doc1 = Document { id: Uuid::new_v4().to_string(), date: dt("2022-10-08").expect("test_parties") };
      let doc2 = Document { id: Uuid::new_v4().to_string(), date: start_d };

      let id1 = Uuid::from_u128(101);
      let id2 = Uuid::from_u128(102);
      let id3 = Uuid::from_u128(102);

      let ops = vec![
        OpMutation::new(
          id1,
          start_d,
          w1,
          G1,
          doc1.clone(),
          None,
          Some(SOperation::Receive(NumberForGoods { qty: 3.into(), cost: Some(3000.into()) })),
        ),
        OpMutation::new(
          id2,
          start_d,
          w1,
          G1,
          doc2.clone(),
          None,
          Some(SOperation::Receive(NumberForGoods { qty: 4.into(), cost: Some(2000.into()) })),
        ),
        OpMutation::new(
          id3,
          start_d,
          w1,
          G1,
          doc2.clone(),
          None,
          Some(SOperation::Issue(NumberForGoods { qty: 1.into(), cost: Some(500.into()) })),
        ),
      ];

      db.record_ops(&ops).expect("test_parties");

      let st = DateTypeStoreGoodsId();
      let res = st.get_report(start_d, end_d, w1, &mut db).expect("test_parties");

      let agrs = vec![
        AgregationStoreGoods {
          store: Some(w1),
          goods: Some(G1),
          document: Some(doc1.clone()),
          open_balance: NumberForGoods::default(),
          receive: NumberForGoods { qty: 3.into(), cost: Some(3000.into()) },
          issue: NumberForGoods::default(),
          close_balance: NumberForGoods { qty: 3.into(), cost: Some(3000.into()) },
        },
        AgregationStoreGoods {
          store: Some(w1),
          goods: Some(G1),
          document: Some(doc2.clone()),
          open_balance: NumberForGoods::default(),
          receive: NumberForGoods { qty: 4.into(), cost: Some(2000.into()) },
          issue: NumberForGoods { qty: 1.into(), cost: Some(500.into()) },
          close_balance: NumberForGoods { qty: 3.into(), cost: Some(1500.into()) },
        },
      ];

      assert_eq!(res.items.1[0], agrs[0]);
      assert_eq!(res.items.1[1], agrs[1]);

      tmpdir.close().expect("Can't close tmp dir in test_parties");
    }

    #[actix_web::test]
    async fn store_test_issue_cost_none() {
      let tmp_dir = TempDir::new().expect("Can't create tmp dir in test_issue_cost_none");

      let mut opts = Options::default();
      let mut cfs = Vec::new();

      let cf_names: Vec<&str> = vec![
        STORE_DATE_TYPE_PARTY_ID,
        DATE_TYPE_STORE_PARTY_ID,
        CHECK_DATE_STORE_PARTY,
        CHECK_PARTY_STORE_DATE,
      ];

      for name in cf_names {
        let cf = ColumnFamilyDescriptor::new(name, opts.clone());
        cfs.push(cf);
      }

      opts.create_if_missing(true);
      opts.create_missing_column_families(true);
      let mut db = Db {
        db: Arc::new(
          DB::open_cf_descriptors(&opts, &tmp_dir, cfs)
            .expect("Can't open db in test_issue_cost_none"),
        ),
      };

      let start_d = dt("2022-10-10").expect("test_issue_cost_none");
      let end_d = dt("2022-10-11").expect("test_issue_cost_none");
      let w1 = Uuid::new_v4();

      let doc = Document { id: Uuid::new_v4().to_string(), date: start_d };

      let id1 = Uuid::from_u128(101);
      let id2 = Uuid::from_u128(102);

      let ops = vec![
        OpMutation::new(
          id1,
          start_d,
          w1,
          G1,
          doc.clone(),
          None,
          Some(SOperation::Receive(NumberForGoods { qty: 4.into(), cost: Some(2000.into()) })),
        ),
        OpMutation::new(
          id2,
          start_d,
          w1,
          G1,
          doc.clone(),
          None,
          Some(SOperation::Issue(NumberForGoods { qty: 1.into(), cost: None })),
        ),
      ];

      db.record_ops(&ops).expect("test_issue_cost_none");

      let st = DateTypeStoreGoodsId();
      let res = st.get_report(start_d, end_d, w1, &mut db).expect("test_issue_cost_none");

      let agr = AgregationStoreGoods {
        store: Some(w1),
        goods: Some(G1),
        document: Some(doc.clone()),
        open_balance: NumberForGoods::default(),
        receive: NumberForGoods { qty: 4.into(), cost: Some(2000.into()) },
        issue: NumberForGoods { qty: 1.into(), cost: Some(500.into()) },
        close_balance: NumberForGoods { qty: 3.into(), cost: Some(1500.into()) },
      };

      assert_eq!(agr, res.items.1[0]);

      tmp_dir.close().expect("Can't remove tmp dir in test_issue_cost_none");
    }

    #[actix_web::test]
    async fn store_test_receive_cost_none() {
      let tmp_dir = TempDir::new().expect("Can't create tmp dir in test_receive_cost_none");

      let mut opts = Options::default();
      let mut cfs = Vec::new();

      let cf_names: Vec<&str> = vec![
        STORE_DATE_TYPE_PARTY_ID,
        DATE_TYPE_STORE_PARTY_ID,
        CHECK_DATE_STORE_PARTY,
        CHECK_PARTY_STORE_DATE,
      ];

      for name in cf_names {
        let cf = ColumnFamilyDescriptor::new(name, opts.clone());
        cfs.push(cf);
      }

      opts.create_if_missing(true);
      opts.create_missing_column_families(true);
      let mut db = Db {
        db: Arc::new(
          DB::open_cf_descriptors(&opts, &tmp_dir, cfs)
            .expect("Can't open db in test_receive_cost_none"),
        ),
      };

      let start_d = dt("2022-10-10").expect("test_receive_cost_none");
      let end_d = dt("2022-10-11").expect("test_receive_cost_none");
      let w1 = Uuid::new_v4();

      let doc = Document { id: Uuid::new_v4().to_string(), date: start_d };

      let id1 = Uuid::from_u128(101);
      let id2 = Uuid::from_u128(102);

      let ops = vec![
        OpMutation::new(
          id1,
          start_d,
          w1,
          G1,
          doc.clone(),
          None,
          Some(SOperation::Receive(NumberForGoods { qty: 4.into(), cost: Some(2000.into()) })),
        ),
        OpMutation::new(
          id2,
          start_d,
          w1,
          G1,
          doc.clone(),
          None,
          Some(SOperation::Receive(NumberForGoods { qty: 1.into(), cost: None })),
        ),
      ];

      db.record_ops(&ops).expect("test_receive_cost_none");

      let st = DateTypeStoreGoodsId();
      let res = st.get_report(start_d, end_d, w1, &mut db).expect("test_receive_cost_none");

      let agr = AgregationStoreGoods {
        store: Some(w1),
        goods: Some(G1),
        document: Some(doc.clone()),
        open_balance: NumberForGoods::default(),
        receive: NumberForGoods { qty: 5.into(), cost: Some(2000.into()) },
        issue: NumberForGoods { qty: 0.into(), cost: Some(0.into()) },
        close_balance: NumberForGoods { qty: 5.into(), cost: Some(2000.into()) },
      };

      assert_eq!(agr, res.items.1[0]);

      tmp_dir.close().expect("Can't remove tmp dir in test_receive_cost_none");
    }

    #[actix_web::test]
    async fn store_test_issue_remainder() {
      let tmp_dir = TempDir::new().expect("Can't create tmp dir in test_issue_remainder");

      let mut opts = Options::default();
      let mut cfs = Vec::new();

      let cf_names: Vec<&str> = vec![
        STORE_DATE_TYPE_PARTY_ID,
        DATE_TYPE_STORE_PARTY_ID,
        CHECK_DATE_STORE_PARTY,
        CHECK_PARTY_STORE_DATE,
      ];

      for name in cf_names {
        let cf = ColumnFamilyDescriptor::new(name, opts.clone());
        cfs.push(cf);
      }

      opts.create_if_missing(true);
      opts.create_missing_column_families(true);
      let mut db = Db {
        db: Arc::new(
          DB::open_cf_descriptors(&opts, &tmp_dir, cfs)
            .expect("Can't open db in test_issue_remainder"),
        ),
      };

      let start_d = dt("2022-10-10").expect("test_issue_remainder");
      let end_d = dt("2022-10-11").expect("test_issue_remainder");
      let w1 = Uuid::new_v4();

      let doc = Document { id: Uuid::new_v4().to_string(), date: start_d };

      let id1 = Uuid::from_u128(101);
      let id2 = Uuid::from_u128(102);
      let id3 = Uuid::from_u128(103);

      println!("{id1}");
      println!("{id2}");

      let ops = vec![
        OpMutation::new(
          id1,
          start_d,
          w1,
          G1,
          doc.clone(),
          None,
          Some(SOperation::Receive(NumberForGoods { qty: 3.into(), cost: Some(10.into()) })),
        ),
        OpMutation::new(
          id2,
          start_d,
          w1,
          G1,
          doc.clone(),
          None,
          Some(SOperation::Issue(NumberForGoods { qty: 1.into(), cost: None })),
        ),
        OpMutation::new(
          id3,
          start_d,
          w1,
          G1,
          doc.clone(),
          None,
          Some(SOperation::Issue(NumberForGoods { qty: 2.into(), cost: None })),
        ),
      ];

      db.record_ops(&ops).expect("test_issue_remainder");

      let st = DateTypeStoreGoodsId();
      let res = st.get_report(start_d, end_d, w1, &mut db).expect("test_issue_remainder");

      // println!("HELLO: {:#?}", res.items.1);

      let agr = AgregationStoreGoods {
        store: Some(w1),
        goods: Some(G1),
        document: Some(doc.clone()),
        open_balance: NumberForGoods::default(),
        receive: NumberForGoods { qty: 3.into(), cost: Some(10.into()) },
        issue: NumberForGoods { qty: 3.into(), cost: Some(10.into()) },
        close_balance: NumberForGoods { qty: 0.into(), cost: Some(0.into()) },
      };

      assert_eq!(agr, res.items.1[0]);

      tmp_dir.close().expect("Can't remove tmp dir in test_issue_remainder");
    }

    #[actix_web::test]
    async fn store_test_issue_op_none() {
      let tmp_dir = TempDir::new().expect("Can't create tmp dir in test_issue_op_none");

      let mut opts = Options::default();
      let mut cfs = Vec::new();

      let cf_names: Vec<&str> = vec![
        STORE_DATE_TYPE_PARTY_ID,
        DATE_TYPE_STORE_PARTY_ID,
        CHECK_DATE_STORE_PARTY,
        CHECK_PARTY_STORE_DATE,
      ];

      for name in cf_names {
        let cf = ColumnFamilyDescriptor::new(name, opts.clone());
        cfs.push(cf);
      }

      opts.create_if_missing(true);
      opts.create_missing_column_families(true);
      let mut db = Db {
        db: Arc::new(
          DB::open_cf_descriptors(&opts, &tmp_dir, cfs).expect("Can't open db in test_issue_op_none"),
        ),
      };

      let start_d = dt("2022-10-10").expect("test_issue_op_none");
      let end_d = dt("2022-10-11").expect("test_issue_op_none");
      let w1 = Uuid::new_v4();

      let doc = Document { id: Uuid::new_v4().to_string(), date: start_d };

      let id1 = Uuid::from_u128(101);
      let id2 = Uuid::from_u128(102);
      let id3 = Uuid::from_u128(103);

      let ops = vec![
        OpMutation::new(
          id1,
          start_d,
          w1,
          G1,
          doc.clone(),
          None,
          Some(SOperation::Receive(NumberForGoods { qty: 3.into(), cost: Some(10.into()) })),
        ),
        // КОРРЕКТНАЯ ОПЕРАЦИЯ С ДВУМЯ NONE?
        OpMutation::new(id3, start_d, w1, G1, doc.clone(), None, None),
      ];

      db.record_ops(&ops).expect("test_issue_op_none");

      let st = DateTypeStoreGoodsId();
      let res = st.get_report(start_d, end_d, w1, &mut db).expect("test_issue_op_none");

      let agr = AgregationStoreGoods {
        store: Some(w1),
        goods: Some(G1),
        document: Some(doc.clone()),
        open_balance: NumberForGoods::default(),
        receive: NumberForGoods { qty: 3.into(), cost: Some(10.into()) },
        issue: NumberForGoods { qty: 3.into(), cost: Some(10.into()) },
        close_balance: NumberForGoods { qty: 0.into(), cost: Some(0.into()) },
      };

      assert_eq!(agr, res.items.1[0]);

      tmp_dir.close().expect("Can't remove tmp dir in test_issue_op_none");
    }

    #[actix_web::test]
    async fn store_test_receive_change_op() {
      let tmp_dir = TempDir::new().expect("Can't create tmp dir in test_receive_change_op");

      let mut opts = Options::default();
      let mut cfs = Vec::new();

      let cf_names: Vec<&str> = vec![
        STORE_DATE_TYPE_PARTY_ID,
        DATE_TYPE_STORE_PARTY_ID,
        CHECK_DATE_STORE_PARTY,
        CHECK_PARTY_STORE_DATE,
      ];

      for name in cf_names {
        let cf = ColumnFamilyDescriptor::new(name, opts.clone());
        cfs.push(cf);
      }

      opts.create_if_missing(true);
      opts.create_missing_column_families(true);
      let mut db = Db {
        db: Arc::new(
          DB::open_cf_descriptors(&opts, &tmp_dir, cfs)
            .expect("Can't open db in test_receive_change_op"),
        ),
      };

      let start_d = dt("2022-10-10").expect("test_receive_change_op");
      let end_d = dt("2022-10-11").expect("test_receive_change_op");
      let w1 = Uuid::new_v4();

      let doc = Document { id: Uuid::new_v4().to_string(), date: start_d };

      let id1 = Uuid::from_u128(101);

      let ops_old = vec![
        OpMutation::new(
          id1,
          dt("2022-08-25").expect("test_receive_change_op"),
          w1,
          G1,
          doc.clone(),
          None,
          Some(SOperation::Receive(NumberForGoods { qty: 3.into(), cost: Some(10.into()) })),
        ),
        OpMutation::new(
          id1,
          dt("2022-09-20").expect("test_receive_change_op"),
          w1,
          G1,
          doc.clone(),
          None,
          Some(SOperation::Receive(NumberForGoods { qty: 1.into(), cost: Some(30.into()) })),
        ),
      ];

      db.record_ops(&ops_old).expect("test_receive_change_op");

      let old_check = Balance {
        date: dt("2022-10-01").expect("test_receive_change_op"),
        store: w1,
        goods: G1,
        document: doc.clone(),
        number: NumberForGoods { qty: 4.into(), cost: Some(40.into()) },
      };

      let mut old_checkpoints = db
        .search_last_checkpoints(start_d, w1)
        .expect("test_receive_change_op")
        .into_iter();

      assert_eq!(Some(old_check), old_checkpoints.next());

      let ops_new = vec![OpMutation::new(
        id1,
        dt("2022-08-25").expect("test_receive_change_op"),
        w1,
        G1,
        doc.clone(),
        Some(SOperation::Receive(NumberForGoods { qty: 3.into(), cost: Some(10.into()) })),
        Some(SOperation::Receive(NumberForGoods { qty: 4.into(), cost: Some(100.into()) })),
      )];

      db.record_ops(&ops_new).expect("test_receive_change_op");

      let new_check = Balance {
        date: dt("2022-10-01").expect("test_receive_change_op"),
        store: w1,
        goods: G1,
        document: doc.clone(),
        number: NumberForGoods { qty: 5.into(), cost: Some(130.into()) },
      };

      let mut new_checkpoints = db
        .search_last_checkpoints(start_d, w1)
        .expect("test_receive_change_op")
        .into_iter();

      assert_eq!(Some(new_check), new_checkpoints.next());

      let st = DateTypeStoreGoodsId();
      let res = st.get_report(start_d, end_d, w1, &mut db).expect("test_receive_change_op");

      let agr = AgregationStoreGoods {
        store: Some(w1),
        goods: Some(G1),
        document: Some(doc.clone()),
        open_balance: NumberForGoods { qty: 5.into(), cost: Some(130.into()) },
        receive: NumberForGoods::default(),
        issue: NumberForGoods { qty: 0.into(), cost: Some(0.into()) },
        close_balance: NumberForGoods { qty: 5.into(), cost: Some(130.into()) },
      };

      assert_eq!(res.items.1[0], agr);

      tmp_dir.close().expect("Can't remove tmp dir in test_receive_change_op");
    }
}
