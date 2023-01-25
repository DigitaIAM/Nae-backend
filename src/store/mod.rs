#![allow(dead_code, unused_variables, unused_imports)]

mod balance;
mod check_batch_store_date;
mod check_date_store_batch;
mod date_type_store_goods_id;
mod error;
mod store_date_type_goods_id;

use chrono::{DateTime, Datelike, Month, NaiveDate, Utc};
use json::{array, iterators::Members, JsonValue};
use rocksdb::{ColumnFamily, ColumnFamilyDescriptor, IteratorMode, Options, ReadOptions, DB};
use rust_decimal::{prelude::ToPrimitive, Decimal};
use serde::{Deserialize, Serialize};
use std::ops::Neg;
use std::path::{Path, PathBuf};
use std::sync::RwLock;
use std::{
  collections::{BTreeMap, HashMap},
  num,
  ops::{Add, AddAssign, Sub, SubAssign},
  str::FromStr,
  sync::Arc,
};
use uuid::{uuid, Uuid};

use chrono::ParseError;
use std::string::FromUtf8Error;

use crate::settings::Settings;
use crate::{commutator::Application, services, utils::json::JsonParams};

use self::balance::{BalanceDelta, BalanceForGoods};
use self::check_batch_store_date::CheckBatchStoreDate;
use self::check_date_store_batch::CheckDateStoreBatch;
pub use self::error::WHError;

type Goods = Uuid;
type Store = Uuid;
type Qty = Decimal;
type Cost = Decimal;

const G1: Uuid = Uuid::from_u128(1);
const G2: Uuid = Uuid::from_u128(2);
const G3: Uuid = Uuid::from_u128(3);

const UUID_NIL: Uuid = uuid!("00000000-0000-0000-0000-000000000000");
const UUID_MAX: Uuid = uuid!("ffffffff-ffff-ffff-ffff-ffffffffffff");

const STORE_DATE_TYPE_BATCH_ID: &str = "cf_store_date_type_batch_id";
const DATE_TYPE_STORE_BATCH_ID: &str = "cf_date_type_store_batch_id";

// enum Topologies {

// }

#[derive(Clone)]
pub struct WHStorage {
  pub database: Db,
}

impl WHStorage {
  pub fn receive_operations(&self, ops: &Vec<OpMutation>) -> Result<(), WHError> {
    Ok(self.database.record_ops(ops)?)
  }

  pub fn open<P: AsRef<Path>>(path: P) -> Result<Self, WHError> {
    std::fs::create_dir_all(&path).map_err(|e| WHError::new("Can't create folder for WHStorage"))?;

    let mut opts = Options::default();
    let mut cfs = Vec::new();

    let mut cf_names: Vec<&str> = vec![
      STORE_DATE_TYPE_BATCH_ID,
      DATE_TYPE_STORE_BATCH_ID,
      CheckDateStoreBatch::cf_name(),
      CheckBatchStoreDate::cf_name(),
    ];
    // checkpoint_topologies.iter().for_each(|t| cf_names.push(t.cf_name()));

    for name in cf_names {
      let cf = ColumnFamilyDescriptor::new(name, opts.clone());
      cfs.push(cf);
    }

    opts.create_if_missing(true);
    opts.create_missing_column_families(true);

    let database = DB::open_cf_descriptors(&opts, &path, cfs)
      .expect("Can't open database in settings.database.inventory");
    let database = Arc::new(database);

    let checkpoint_topologies: Vec<Box<dyn Checkpoint + Sync + Send>> = vec![
      Box::new(CheckDateStoreBatch { db: database.clone() }),
      Box::new(CheckBatchStoreDate { db: database.clone() }),
    ];

    let db = Db { db: database, checkpoint_topologies: Arc::new(checkpoint_topologies) };

    Ok(WHStorage { database: db })
  }
}

#[derive(Eq, Ord, PartialOrd, Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Batch {
  id: Uuid,
  date: DateTime<Utc>,
}

impl Batch {
  fn new() -> Batch {
    Batch { id: Default::default(), date: Default::default() }
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
  ) -> Result<Vec<Op>, WHError>;

  fn get_report(
    &self,
    start_date: DateTime<Utc>,
    end_date: DateTime<Utc>,
    wh: Store,
    db: &mut Db,
  ) -> Result<Report, WHError> {
    // TODO move to trait Checkpoint
    let balances = db.get_checkpoints_before_date(start_date, wh)?;

    let ops = self.get_ops(first_day_current_month(start_date), end_date, wh, db)?;

    let items = new_get_agregations(balances, ops, start_date);

    Ok(Report { start_date, end_date, items })
  }
}

trait Checkpoint {
  // fn cf_name(&self) -> &str;

  fn key(&self, op: &Op, date_of_checkpoint: DateTime<Utc>) -> Vec<u8>;

  fn get_balance(&self, key: &Vec<u8>) -> Result<BalanceForGoods, WHError>;
  fn set_balance(&self, key: &Vec<u8>, balance: BalanceForGoods) -> Result<(), WHError>;
  fn del_balance(&self, key: &Vec<u8>) -> Result<(), WHError>;

  fn change_checkpoints(
    &self,
    op: &OpMutation,
    last_checkpoint_date: DateTime<Utc>,
  ) -> Result<(), WHError> {
    // let cf = self.db.cf_handle(name).expect("option in change_checkpoint");

    let mut date = op.date;
    let mut check_point = op.date;

    // get iterator from first day of next month of given operation till 'last' check point
    while check_point <= last_checkpoint_date {
      check_point = first_day_next_month(date);

      let key = self.key(&op.to_op(), check_point);

      let mut balance = self.get_balance(&key)?;

      balance += op.to_delta();

      if balance.is_zero() {
        self.del_balance(&key)?;
      } else {
        self.set_balance(&key, balance)?;
      }
      date = check_point;
    }

    Ok(())
  }

  fn get_checkpoints_before_date(
    &self,
    date: DateTime<Utc>,
    wh: Store,
  ) -> Result<Vec<Balance>, WHError>;

  fn get_report(
    &self,
    start_date: DateTime<Utc>,
    end_date: DateTime<Utc>,
    wh: Store,
    db: &mut Db,
  ) -> Result<Report, WHError>;
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

fn min_batch() -> Vec<u8> {
  UUID_NIL
    .as_bytes()
    .iter()
    .chain(u64::MIN.to_be_bytes().iter())
    .chain(UUID_NIL.as_bytes().iter())
    .map(|b| *b)
    .collect()
}

fn max_batch() -> Vec<u8> {
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
  checkpoint_topologies: Arc<Vec<Box<dyn Checkpoint + Sync + Send>>>,
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
      batch: op.batch.clone(),
      number: BalanceForGoods::default(),
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
    // let cf_names = DB::list_cf(&Options::default(), DB::path(&self.db))?;
    for op in ops {
      // for checkpoint_topology in self.db.checkpoint_topologies {
      //   checkpoint_topology.change_checkpoints(&op)?;
      // }

      // TODO review code
      // for name in &cf_names {
      //   if name == "default" {
      //     continue;
      //   } else if name == CHECK_DATE_STORE_BATCH || name == CHECK_BATCH_STORE_DATE {
      //     self.change_checkpoints(&op, name)?;
      //   } else {
      //     if let Some(cf) = self.db.cf_handle(name.as_str()) {
      //       if op.before.is_none() {
      //         self.db.put_cf(&cf, op.key(name)?, op.value()?)?;
      //       } else {
      //         if let Ok(Some(bytes)) = self.db.get_cf(&cf, op.key(name)?) {
      //           let o: OpMutation = serde_json::from_slice(&bytes)?;
      //           if op.before == o.after {
      //             self.db.put_cf(&cf, op.key(name)?, op.value()?)?;
      //           } else {
      //             return Err(WHError::new("Wrong 'before' state in operation"));
      //           }
      //         }
      //       }
      //     }
      //   }
      // }
    }

    Ok(())
  }

  fn get_checkpoints_before_date(
    &mut self,
    date: DateTime<Utc>,
    wh: Store,
  ) -> Result<Vec<Balance>, WHError> {
    for checkpoint_topology in self.checkpoint_topologies.iter() {
      match checkpoint_topology.get_checkpoints_before_date(date, wh) {
        Ok(result) => return Ok(result),
        Err(e) => {
          // TODO ignore only not supported
        },
      }
    }
    Err(WHError::new("can't get checkpoint before date"))
  }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct NumberForGoods {
  qty: Qty,
  cost: Option<Cost>,
}

impl NumberForGoods {
  fn to_delta(&self) -> BalanceDelta {
    BalanceDelta { qty: self.qty, cost: self.cost.unwrap_or_default() }
  }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum Mode {
  Auto,
  Manual,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum InternalOperation {
  Receive(Qty, Cost),
  Issue(Qty, Cost, Mode),
}

trait Operation {}

impl Into<BalanceDelta> for InternalOperation {
  fn into(self) -> BalanceDelta {
    match self {
      InternalOperation::Receive(qty, cost) => BalanceDelta { qty, cost },
      InternalOperation::Issue(qty, cost, mode) => BalanceDelta { qty: -qty, cost: -cost },
    }
  }
}

impl Add<InternalOperation> for BalanceForGoods {
  type Output = BalanceForGoods;

  fn add(mut self, rhs: InternalOperation) -> Self::Output {
    match rhs {
      InternalOperation::Receive(qty, cost) => {
        self.qty += qty;
        self.cost += cost;
      },
      InternalOperation::Issue(qty, cost, mode) => {
        self.qty -= qty;
        self.cost -= if mode == Mode::Manual {
          cost
        } else {
          match self.cost.checked_div(self.qty) {
            Some(price) => price * qty,
            None => 0.into(), // TODO handle error?
          }
        }
      },
    }
    self
  }
}

impl AddAssign<&InternalOperation> for BalanceForGoods {
  fn add_assign(&mut self, rhs: &InternalOperation) {
    match rhs {
      InternalOperation::Receive(qty, cost) => {
        self.qty += qty;
        self.cost += cost;
      },
      InternalOperation::Issue(qty, cost, mode) => {
        self.qty -= qty;
        self.cost -= if mode == &Mode::Manual {
          *cost
        } else {
          match self.cost.checked_div(self.qty) {
            Some(price) => price * *qty,
            None => 0.into(), // TODO handle error?
          }
        }
      },
    }
  }
}

trait KeyValueStore {
  fn key(&self, s: &String) -> Result<Vec<u8>, WHError>;
  fn store_date_type_batch_id(&self) -> Vec<u8>;
  fn date_type_store_batch_id(&self) -> Vec<u8>;
  fn value(&self) -> Result<String, WHError>;
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct Balance {
  // key
  date: DateTime<Utc>,
  store: Store,
  goods: Goods,
  batch: Batch,
  // value
  number: BalanceForGoods,
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
  fn key_batch_store_date(&self) -> Vec<u8> {
    let dt = self.date.timestamp() as u64;
    let key = self
      .batch()
      .iter()
      .chain(self.store.as_bytes().iter())
      .chain(dt.to_be_bytes().iter())
      .map(|b| *b)
      .collect();
    key
  }

  fn key_date_store_batch(&self) -> Vec<u8> {
    let dt = self.date.timestamp() as u64;
    let key = dt
      .to_be_bytes()
      .iter()
      .chain(self.store.as_bytes().iter())
      .chain(self.batch().iter())
      .map(|b| *b)
      .collect();
    key
  }

  fn key(&self, s: &str) -> Result<Vec<u8>, WHError> {
    match s {
      CHECK_DATE_STORE_BATCH => Ok(self.key_date_store_batch()),
      CHECK_BATCH_STORE_DATE => Ok(self.key_batch_store_date()),
      _ => Err(WHError::new("Wrong Balance key type")),
    }
  }

  fn value(&self) -> Result<String, WHError> {
    Ok(serde_json::to_string(&self)?)
  }

  fn batch(&self) -> Vec<u8> {
    let dt = self.batch.date.timestamp() as u64;

    self
      .goods
      .as_bytes()
      .iter()
      .chain(dt.to_be_bytes().iter())
      .chain(self.batch.id.as_bytes().iter())
      .map(|b| *b)
      .collect()
  }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct Op {
  // key
  id: Uuid,
  date: DateTime<Utc>,
  store: Store,
  goods: Goods,
  batch: Batch,

  transfer: Option<Store>,

  // value
  op: InternalOperation,
  event: String,
}

impl Op {
  fn to_delta(&self) -> BalanceDelta {
    self.op.clone().into()
  }
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub struct OpMutation {
  // key
  id: Uuid,
  date: DateTime<Utc>,
  store: Store,
  transfer: Option<Store>,
  goods: Goods,
  batch: Batch,
  // value
  before: Option<InternalOperation>,
  after: Option<InternalOperation>,
  event: String,
}

impl Default for OpMutation {
  fn default() -> Self {
    Self {
      id: Default::default(),
      date: Default::default(),
      store: Default::default(),
      transfer: Default::default(),
      goods: Default::default(),
      batch: Batch::new(),
      before: None,
      after: None,
      event: Default::default(),
    }
  }
}

impl OpMutation {
  fn new(
    id: Uuid,
    date: DateTime<Utc>,
    store: Store,
    transfer: Option<Store>,
    goods: Goods,
    batch: Batch,
    before: Option<InternalOperation>,
    after: Option<InternalOperation>,
    event: DateTime<Utc>,
  ) -> OpMutation {
    OpMutation { id, date, store, transfer, goods, batch, before, after, event: event.to_string() }
  }

  fn receive_new(
    id: Uuid,
    date: DateTime<Utc>,
    store: Store,
    goods: Goods,
    batch: Batch,
    qty: Qty,
    cost: Cost,
  ) -> OpMutation {
    OpMutation {
      id,
      date,
      store,
      transfer: None,
      goods,
      batch,
      before: None,
      after: Some(InternalOperation::Receive(qty, cost)),
      event: chrono::Utc::now().to_string(),
    }
  }

  fn to_op(&self) -> Op {
    if let Some(op) = self.after.as_ref() {
      Op {
        id: self.id.clone(),
        date: self.date.clone(),
        store: self.store.clone(),
        goods: self.goods.clone(),
        batch: self.batch.clone(),
        transfer: self.transfer.clone(),
        op: op.clone(),
        event: self.event.clone(),
      }
    } else {
      todo!()
    }
  }

  fn batch(&self) -> Vec<u8> {
    let dt = self.batch.date.timestamp() as u64;

    self
      .goods
      .as_bytes()
      .iter()
      .chain(dt.to_be_bytes().iter())
      .chain(self.batch.id.as_bytes().iter())
      .map(|b| *b)
      .collect()
  }

  fn to_delta(&self) -> BalanceDelta {
    let n: BalanceDelta = self.after.as_ref().map(|i| i.clone().into()).unwrap_or_default();
    let o: BalanceDelta = self.before.as_ref().map(|i| i.clone().into()).unwrap_or_default();

    n - o
  }

  fn new_from_ops(before: Option<Op>, after: Option<Op>) -> OpMutation {
    if let (Some(b), Some(a)) = (&before, &after) {
      OpMutation {
        id: a.id,
        date: a.date,
        store: a.store,
        transfer: a.transfer,
        goods: a.goods,
        batch: a.batch.clone(),
        before: Some(b.op.clone()),
        after: Some(a.op.clone()),
        event: a.event.clone(),
      }
    } else if let Some(b) = &before {
      OpMutation {
        id: b.id,
        date: b.date,
        store: b.store,
        transfer: b.transfer,
        goods: b.goods,
        batch: b.batch.clone(),
        before: Some(b.op.clone()),
        after: None,
        event: b.event.clone(),
      }
    } else if let Some(a) = &after {
      OpMutation {
        id: a.id,
        date: a.date,
        store: a.store,
        transfer: a.transfer,
        goods: a.goods,
        batch: a.batch.clone(),
        before: None,
        after: Some(a.op.clone()),
        event: a.event.clone(),
      }
    } else {
      OpMutation::default()
    }
  }
}

impl KeyValueStore for OpMutation {
  fn key(&self, s: &String) -> Result<Vec<u8>, WHError> {
    match s.as_str() {
      STORE_DATE_TYPE_BATCH_ID => Ok(self.store_date_type_batch_id()),
      DATE_TYPE_STORE_BATCH_ID => Ok(self.date_type_store_batch_id()),
      _ => Err(WHError::new("Wrong Op key type")),
    }
  }

  fn store_date_type_batch_id(&self) -> Vec<u8> {
    let ts = self.date.timestamp() as u64;
    // if after == None, this operation will be recorded last (that's why op_type by default is 3)
    let mut op_type = 3_u8;

    if let Some(o) = &self.after {
      op_type = match o {
        InternalOperation::Receive(..) => 1_u8,
        InternalOperation::Issue(..) => 2_u8,
      };
    }

    let key = self
      .store
      .as_bytes()
      .iter()
      .chain(ts.to_be_bytes().iter())
      .chain(op_type.to_be_bytes().iter())
      .chain(self.batch().iter())
      .chain(self.id.as_bytes().iter())
      .map(|b| *b)
      .collect();

    key
  }

  fn value(&self) -> Result<String, WHError> {
    Ok(serde_json::to_string(&self)?)
  }

  fn date_type_store_batch_id(&self) -> Vec<u8> {
    let ts = self.date.timestamp() as u64;
    // if after == None, this operation will be recorded last (that's why op_type by default is 3)
    let mut op_type = 3_u8;

    if let Some(o) = &self.after {
      op_type = match o {
        InternalOperation::Receive(..) => 1_u8,
        InternalOperation::Issue(..) => 2_u8,
      };
    }

    let key = ts
      .to_be_bytes()
      .iter()
      .chain(op_type.to_be_bytes().iter())
      .chain(self.store.as_bytes().iter())
      .chain(self.batch().iter())
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
  fn apply_operation(&mut self, op: &Op);
  fn apply_agregation(&mut self, agr: Option<&AgregationStoreGoods>);
  fn balance(&mut self, balance: Option<&Balance>) -> ReturnType; // имплементировать для трех возможных ситуаций
  fn is_applyable_for(&self, op: &OpMutation) -> bool;
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
struct AgregationStoreGoods {
  // ключ
  store: Option<Store>,
  goods: Option<Goods>,
  batch: Option<Batch>,
  // агрегация
  open_balance: BalanceForGoods,
  receive: BalanceDelta,
  issue: BalanceDelta,
  close_balance: BalanceForGoods,
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
    self.open_balance = BalanceForGoods::default();
    self.receive = BalanceDelta::default();
    self.issue = BalanceDelta::default();
    self.close_balance = BalanceForGoods::default();
  }

  fn add_to_open_balance(&mut self, op: &Op) {
    self.store = Some(op.store);
    self.goods = Some(op.goods);
    self.batch = Some(op.batch.clone());

    let delta = op.to_delta();

    self.open_balance += delta.clone();
    self.close_balance += delta;
  }

  fn batch(&self) -> Vec<u8> {
    let mut key = Vec::new();
    if let Some(doc) = &self.batch {
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
      batch: None,
      open_balance: BalanceForGoods::default(),
      receive: BalanceDelta::default(),
      issue: BalanceDelta::default(),
      close_balance: BalanceForGoods::default(),
    }
  }
}

impl AddAssign<&Op> for AgregationStoreGoods {
  fn add_assign(&mut self, rhs: &Op) {
    self.store = Some(rhs.store);
    self.goods = Some(rhs.goods);
    self.batch = Some(rhs.batch.clone());
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

  fn apply_operation(&mut self, op: &Op) {
    match &op.op {
      InternalOperation::Receive(qty, cost) => {
        self.receive += cost;
        self.close_balance += cost;
      },
      InternalOperation::Issue(qty, cost, mode) => {
        self.issue += cost;
        self.close_balance -= cost;
      },
    }
  }

  fn balance(&mut self, balance: Option<&Balance>) -> ReturnType {
    todo!()
  }

  fn apply_agregation(&mut self, agr: Option<&AgregationStoreGoods>) {
    if let Some(agr) = agr {
      self.store = agr.store;
      self.open_balance += agr.open_balance.cost;
      self.receive += agr.receive.cost;
      self.issue += agr.issue.cost;
      self.close_balance += agr.close_balance.cost;
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
          batch: Some(b.batch.clone()),
          open_balance: b.number.clone(),
          receive: BalanceDelta::default(),
          issue: BalanceDelta::default(),
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

  fn apply_operation(&mut self, op: &Op) {
    match &op.op {
      InternalOperation::Receive(qty, cost) => {
        self.receive.qty += qty;
        self.receive.cost += cost;
      },
      InternalOperation::Issue(qty, cost, mode) => {
        self.issue.qty -= qty;
        if mode == &Mode::Auto {
          let balance = self.open_balance.clone() + self.receive.clone();
          let cost = match balance.cost.checked_div(balance.qty) {
            Some(price) => price * qty,
            None => 0.into(), // TODO handle error?
          };
          self.issue.cost -= cost;
        } else {
          self.issue.cost -= cost;
        }
      },
    }
    self.close_balance = self.open_balance.clone() + self.receive.clone() + self.issue.clone();
  }

  fn apply_agregation(&mut self, agr: Option<&AgregationStoreGoods>) {
    todo!()
  }
}

impl KeyValueStore for AgregationStoreGoods {
  fn store_date_type_batch_id(&self) -> Vec<u8> {
    todo!()
  }

  fn value(&self) -> Result<String, WHError> {
    Ok(serde_json::to_string(&self)?)
  }

  fn date_type_store_batch_id(&self) -> Vec<u8> {
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
  balances: Vec<Balance>,
  operations: Vec<Op>,
  start_date: DateTime<Utc>,
) -> (AgregationStore, Vec<AgregationStoreGoods>) {
  let key = |store: &Store, goods: &Goods, batch: &Batch| -> Vec<u8> {
    [].iter()
      .chain(store.as_bytes().iter())
      .chain(goods.as_bytes().iter())
      .chain((batch.date.timestamp() as u64).to_be_bytes().iter())
      .chain(batch.id.as_bytes().iter())
      .map(|b| *b)
      .collect()
  };

  let keyFor = |op: &Op| -> Vec<u8> { key(&op.store, &op.goods, &op.batch) };

  let mut agregations = BTreeMap::new();
  let mut master_agregation = AgregationStore::default();

  for balance in balances {
    agregations.insert(
      key(&balance.store, &balance.goods, &balance.batch),
      AgregationStoreGoods {
        store: Some(balance.store),
        goods: Some(balance.goods),
        batch: Some(balance.batch),

        open_balance: balance.number.clone(),
        receive: BalanceDelta::default(),
        issue: BalanceDelta::default(),
        close_balance: balance.number,
      },
    );
  }

  for op in operations {
    if op.date < start_date {
      agregations
        .entry(keyFor(&op))
        .or_insert(AgregationStoreGoods::default())
        .add_to_open_balance(&op);
    } else {
      *agregations.entry(keyFor(&op)).or_insert(AgregationStoreGoods::default()) += &op;
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

  let mut before = json_to_ops(&mut before, store.clone(), time.clone(), ctx)?.into_iter();

  // let tmp = json_to_ops(&mut data, store, time, ctx)?;
  // println!("AFTER: {tmp:#?}");
  // let mut after = tmp.into_iter();

  let mut after = json_to_ops(&mut data, store, time, ctx)?;

  let mut ops: Vec<OpMutation> = Vec::new();

  while let Some(ref b) = before.next() {
    if let Some(a) = after.remove_entry(&b.0) {
      ops.push(OpMutation::new_from_ops(Some(b.1.clone()), Some(a.1)));
    } else {
      ops.push(OpMutation::new_from_ops(Some(b.1.clone()), None));
    }
  }

  let mut after = after.into_iter();

  while let Some(ref a) = after.next() {
    ops.push(OpMutation::new_from_ops(None, Some(a.1.clone())));
  }

  // while b_op.is_some() || a_op.is_some() {
  //   if let (Some(b), Some(a)) = (&b_op, &a_op) {
  //     if b.id == a.id && b.batch() == a.batch() {
  //       // create new OpMut with both (delta will be finded and propagated later in receive_operations())
  //       ops.push(OpMutation::new_from_ops(b_op, a_op)?);

  //       b_op = before.next();
  //       a_op = after.next();
  //     } else if b.batch() > a.batch() {
  //       // create new OpMut with a
  //       ops.push(OpMutation::new_from_ops(None, a_op)?);

  //       a_op = after.next();
  //     } else if b.batch() < a.batch() {
  //       //create new OpMut with b
  //       ops.push(OpMutation::new_from_ops(b_op, None)?);

  //       b_op = before.next();
  //     }
  //   } else if let Some(b) = &b_op {
  //     // create new OpMut with b
  //     ops.push(OpMutation::new_from_ops(b_op, None)?);

  //     b_op = before.next();
  //   } else if let Some(a) = &a_op {
  //     // create new OpMut with a
  //     ops.push(OpMutation::new_from_ops(None, a_op)?);

  //     a_op = after.next();
  //   }
  // }

  app.warehouse.receive_operations(&ops)?;

  Ok(data)
}

fn json_to_ops(
  data: &mut JsonValue,
  store: Uuid,
  time: DateTime<Utc>,
  ctx: &Vec<String>,
) -> Result<HashMap<String, Op>, WHError> {
  let mut ops = HashMap::new();

  if *data != JsonValue::Null {
    let d_date = data["date"].date()?;
    for goods in data["goods"].members_mut() {
      let op = Op {
        id: if let Some(tid) = goods["_tid"].uuid_or_none() {
          tid
        } else {
          goods["_tid"] = JsonValue::String(Uuid::new_v4().to_string());
          goods["_tid"].uuid()
        },
        date: d_date,
        store,
        transfer: goods["transfer"].uuid_or_none(),
        goods: goods["goods"].uuid(),
        batch: Batch { id: goods["_tid"].uuid(), date: d_date },
        op: if ctx == &vec!["warehouse".to_string(), "receive".to_string()] {
          InternalOperation::Receive(goods["qty"].number(), goods["cost"].number())
        } else if ctx == &vec!["warehouse".to_string(), "issue".to_string()] {
          let (cost, mode) = if let Some(cost) = goods["cost"].number_or_none() {
            (cost, Mode::Manual)
          } else {
            (0.into(), Mode::Auto)
          };
          InternalOperation::Issue(goods["qty"].number(), cost, mode)
        } else {
          break;
        },
        event: time.to_string(),
      };
      ops.insert(goods["_tid"].to_string(), op);
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
    store::date_type_store_goods_id::DateTypeStoreGoodsId,
  };

  use super::{store_date_type_goods_id::StoreDateTypeGoodsId, *};
  use crate::warehouse::test_util::init;
  use actix_web::{http::header::ContentType, test, web, App};
  use futures::TryFutureExt;
  use json::object;
  use rocksdb::{ColumnFamilyDescriptor, Options};
  use tempfile::TempDir;
  use uuid::Uuid;

  #[actix_web::test]
  async fn store_test_app_move() {
    let (tmp_dir, settings, db) = init();

    let (mut application, events_receiver) = Application::new(Arc::new(settings), Arc::new(db))
      .await
      .map_err(|e| io::Error::new(io::ErrorKind::Unsupported, e))
      .unwrap();

    let storage = SOrganizations::new(tmp_dir.path().join("companies"));
    application.storage = Some(storage.clone());

    application.register(DocsFiles::new(application.clone(), "docs", storage.clone()));

    let app = test::init_service(
      App::new()
        // .app_data(web::Data::new(db.clone()))
        .app_data(web::Data::new(application.clone()))
        // .wrap(middleware::Logger::default())
        .service(api::docs_create)
        .service(api::docs_update)
        // .service(api::memory_modify)
        // .service(api::memory_query)
        .default_service(web::route().to(api::not_implemented)),
    )
    .await;

    let goods1 = Uuid::from_u128(101);
    let goods2 = Uuid::from_u128(102);
    let storage1 = Uuid::from_u128(201);
    let storage2 = Uuid::from_u128(202);
    let oid = ID::from("99");

    //receive
    let data0: JsonValue = object! {
        _id: "",
        date: "2023-01-18",
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
                goods: goods2.to_string(),
                uom: "",
                qty: 2,
                price: 8,
                cost: 16,
                _tid: ""
            }
        ]
    };

    let req = test::TestRequest::post()
      .uri(&format!("/api/docs?oid={}&ctx=warehouse,receive", oid.to_base64()))
      .set_payload(data0.dump())
      .insert_header(ContentType::json())
      // .param("oid", oid.to_base64())
      // .param("document", "warehouse")
      // .param("document", "receive")
      .to_request();

    let response = test::call_and_read_body(&app, req).await;

    let result0: serde_json::Value = serde_json::from_slice(&response).unwrap();

    assert_ne!("", result0["goods"][0]["_tid"].as_str().unwrap());
    assert_ne!("", result0["goods"][1]["_tid"].as_str().unwrap());

    // move
    let id = result0["goods"][0]["_tid"].as_str().unwrap();

    let data1: JsonValue = object! {
        _id: "",
        date: "2023-01-18",
        storage: storage1.to_string(),
        transfer: storage2.to_string(),
        goods: [
            {
                goods: goods1.to_string(),
                uom: "",
                qty: 2,
                price: 10,
                cost: 20,
                _tid: id,
            }
        ]
    };

    let req = test::TestRequest::post()
      .uri(&format!("/api/docs/{id}?oid={}&ctx=warehouse,receive", oid.to_base64()))
      .set_payload(data1.dump())
      .insert_header(ContentType::json())
      .to_request();

    let response = test::call_and_read_body(&app, req).await;

    let result: serde_json::Value = serde_json::from_slice(&response).unwrap();

    let compare: serde_json::Value = serde_json::from_str(&data1.dump()).unwrap();

    assert_eq!(compare, result);

    let x = DateTypeStoreGoodsId();

    let report = x
      .get_report(
        dt("2023-01-17").unwrap(),
        dt("2023-01-20").unwrap(),
        storage1,
        &mut application.warehouse.database,
      )
      .unwrap();

    println!("REPORT: {report:#?}");
  }

  #[actix_web::test]
  async fn store_test_app_receive_issue_change() {
    let (tmp_dir, settings, db) = init();

    let (mut application, events_receiver) = Application::new(Arc::new(settings), Arc::new(db))
      .await
      .map_err(|e| io::Error::new(io::ErrorKind::Unsupported, e))
      .unwrap();

    let storage = SOrganizations::new(tmp_dir.path().join("companies"));
    application.storage = Some(storage.clone());

    application.register(DocsFiles::new(application.clone(), "docs", storage.clone()));

    let app = test::init_service(
      App::new()
        // .app_data(web::Data::new(db.clone()))
        .app_data(web::Data::new(application.clone()))
        // .wrap(middleware::Logger::default())
        .service(api::docs_create)
        .service(api::docs_update)
        // .service(api::memory_modify)
        // .service(api::memory_query)
        .default_service(web::route().to(api::not_implemented)),
    )
    .await;

    let goods1 = Uuid::from_u128(201);
    let goods2 = Uuid::from_u128(101);
    let storage1 = Uuid::from_u128(202);
    let oid = ID::from("99");

    //receive
    let data0: JsonValue = object! {
        _id: "",
        date: "2023-01-18",
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
                goods: goods2.to_string(),
                uom: "",
                qty: 2,
                price: 8,
                cost: 16,
                _tid: ""
            }
        ]
    };

    let req = test::TestRequest::post()
      .uri(&format!("/api/docs?oid={}&ctx=warehouse,receive", oid.to_base64()))
      .set_payload(data0.dump())
      .insert_header(ContentType::json())
      // .param("oid", oid.to_base64())
      // .param("document", "warehouse")
      // .param("document", "receive")
      .to_request();

    let response = test::call_and_read_body(&app, req).await;

    let result0: serde_json::Value = serde_json::from_slice(&response).unwrap();

    assert_ne!("", result0["goods"][0]["_tid"].as_str().unwrap());
    assert_ne!("", result0["goods"][1]["_tid"].as_str().unwrap());

    // issue
    let data1: JsonValue = object! {
        _id: "",
        date: "2023-01-18",
        storage: storage1.to_string(),
        goods: [
            {
                goods: goods2.to_string(),
                uom: "",
                qty: 1,
                // price: 0,
                // cost: 0,
                _tid: result0["goods"][1]["_tid"].as_str().unwrap(),
            },
        ]
    };

    let req = test::TestRequest::post()
      .uri(&format!("/api/docs?oid={}&ctx=warehouse,issue", oid.to_base64()))
      .set_payload(data1.dump())
      .insert_header(ContentType::json())
      // .param("oid", oid.to_base64())
      // .param("document", "warehouse")
      // .param("document", "receive")
      .to_request();

    let response = test::call_and_read_body(&app, req).await;

    let result1: serde_json::Value = serde_json::from_slice(&response).unwrap();

    // change
    let id = result0["goods"][0]["_tid"].as_str().unwrap();

    let data2: JsonValue = object! {
        _id: "",
        date: "2023-01-18",
        storage: storage1.to_string(),
        goods: [
            {
                goods: goods1.to_string(),
                uom: "",
                qty: 2,
                price: 10,
                cost: 20,
                _tid: id,
            }
        ]
    };

    let req = test::TestRequest::post()
      .uri(&format!("/api/docs/{id}?oid={}&ctx=warehouse,receive", oid.to_base64()))
      .set_payload(data2.dump())
      .insert_header(ContentType::json())
      .to_request();

    let response = test::call_and_read_body(&app, req).await;

    let result: serde_json::Value = serde_json::from_slice(&response).unwrap();

    let compare: serde_json::Value = serde_json::from_str(&data2.dump()).unwrap();

    assert_eq!(compare, result);

    let x = DateTypeStoreGoodsId();

    let report = x
      .get_report(
        dt("2023-01-17").unwrap(),
        dt("2023-01-20").unwrap(),
        storage1,
        &mut application.warehouse.database,
      )
      .unwrap();

    println!("REPORT: {report:#?}");
  }

  #[actix_web::test]
  async fn store_test_receive_ops() {
    let tmp_dir = TempDir::new().expect("Can't create tmp dir in test_get_wh_balance");

    let mut wh = WHStorage::open(&tmp_dir.path()).unwrap();
    let mut db = wh.database;

    let op_d = dt("2022-10-10").expect("test_receive_ops");
    let check_d = dt("2022-11-01").expect("test_receive_ops");
    let w1 = Uuid::new_v4();
    let party = Batch { id: Uuid::new_v4(), date: op_d };

    let id1 = Uuid::from_u128(101);
    let id2 = Uuid::from_u128(102);
    let id3 = Uuid::from_u128(103);
    let id4 = Uuid::from_u128(104);

    let ops = vec![
      OpMutation::receive_new(id1, op_d, w1, G1, party.clone(), 3.into(), 3000.into()),
      OpMutation::new(
        id2,
        op_d,
        w1,
        None,
        G1,
        party.clone(),
        None,
        Some(InternalOperation::Issue(1.into(), 1000.into(), Mode::Manual)),
        chrono::Utc::now(),
      ),
      OpMutation::new(
        id3,
        op_d,
        w1,
        None,
        G2,
        party.clone(),
        None,
        Some(InternalOperation::Issue(2.into(), 2000.into(), Mode::Manual)),
        chrono::Utc::now(),
      ),
      OpMutation::new(
        id4,
        op_d,
        w1,
        None,
        G2,
        party.clone(),
        None,
        Some(InternalOperation::Receive(2.into(), 2000.into())),
        chrono::Utc::now(),
      ),
    ];

    db.record_ops(&ops).expect("test_receive_ops");

    let balance = Balance {
      date: check_d,
      store: w1,
      goods: G1,
      batch: party,
      number: BalanceForGoods { qty: 2.into(), cost: 2000.into() },
    };

    // for checkpoint_topology in db. ...
    todo!();
    // let b1 = db.find_checkpoint(&ops[0], CHECK_DATE_STORE_BATCH).expect("test_receive_ops");
    // assert_eq!(b1, Some(balance.clone()));

    // let b2 = db.find_checkpoint(&ops[0], CHECK_BATCH_STORE_DATE).expect("test_receive_ops");
    // assert_eq!(b2, Some(balance));

    // let b3 = db.find_checkpoint(&ops[2], CHECK_DATE_STORE_BATCH).expect("test_receive_ops");
    // assert_eq!(b3, None);

    // let b4 = db.find_checkpoint(&ops[2], CHECK_BATCH_STORE_DATE).expect("test_receive_ops");
    // assert_eq!(b4, None);

    tmp_dir.close().expect("Can't close tmp dir in test_receive_ops");
  }

  #[actix_web::test]
  async fn store_test_neg_balance_date_type_store_goods_id() {
    let tmp_dir = TempDir::new().expect("Can't create tmp dir in test_get_neg_balance");

    let wh = WHStorage::open(&tmp_dir.path()).unwrap();
    let mut db = wh.database;

    let op_d = dt("2022-10-10").expect("test_get_neg_balance");
    let check_d = dt("2022-10-11").expect("test_get_neg_balance");
    let w1 = Uuid::new_v4();
    let party = Batch { id: Uuid::new_v4(), date: op_d };

    let id1 = Uuid::from_u128(101);

    let ops = vec![OpMutation::new(
      id1,
      op_d,
      w1,
      None,
      G1,
      party.clone(),
      None,
      Some(InternalOperation::Issue(2.into(), 2000.into(), Mode::Manual)),
      chrono::Utc::now(),
    )];

    db.record_ops(&ops).expect("test_get_neg_balance");

    let agr = AgregationStoreGoods {
      store: Some(w1),
      goods: Some(G1),
      batch: Some(party.clone()),
      open_balance: BalanceForGoods::default(),
      receive: BalanceDelta::default(),
      issue: BalanceDelta { qty: 2.into(), cost: 2000.into() },
      close_balance: BalanceForGoods { qty: (-2).into(), cost: (-2000).into() },
    };

    let st = DateTypeStoreGoodsId();
    let res = st.get_report(op_d, check_d, w1, &mut db).expect("test_get_neg_balance");

    assert_eq!(res.items.1[0], agr);

    tmp_dir.close().expect("Can't close tmp dir in test_get_neg_balance");
  }

  #[actix_web::test]
  async fn store_test_zero_balance_date_type_store_goods_id() {
    let tmp_dir = TempDir::new().expect("Can't create tmp dir in test_get_zero_balance");

    let wh = WHStorage::open(&tmp_dir.path()).unwrap();
    let mut db = wh.database;

    let start_d = dt("2022-10-10").expect("test_get_zero_balance");
    let end_d = dt("2022-10-11").expect("test_get_zero_balance");
    let w1 = Uuid::new_v4();
    let party = Batch { id: Uuid::new_v4(), date: start_d };

    let id1 = Uuid::from_u128(101);
    let id2 = Uuid::from_u128(102);

    let ops = vec![
      OpMutation::receive_new(id1, start_d, w1, G1, party.clone(), 3.into(), 3000.into()),
      OpMutation::new(
        id2,
        start_d,
        w1,
        None,
        G1,
        party.clone(),
        None,
        Some(InternalOperation::Issue(3.into(), 3000.into(), Mode::Manual)),
        chrono::Utc::now(),
      ),
    ];

    db.record_ops(&ops);

    let st = DateTypeStoreGoodsId();
    let res = st.get_report(start_d, end_d, w1, &mut db).expect("test_get_zero_balance");

    let agr = AgregationStoreGoods {
      store: Some(w1),
      goods: Some(G1),
      batch: Some(party.clone()),
      open_balance: BalanceForGoods::default(),
      receive: BalanceDelta { qty: 3.into(), cost: 3000.into() },
      issue: BalanceDelta { qty: 3.into(), cost: 3000.into() },
      close_balance: BalanceForGoods::default(),
    };

    assert_eq!(res.items.1[0], agr);

    tmp_dir.close().expect("Can't close tmp dir in test_get_zero_balance");
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
    let tmp_dir = TempDir::new().expect("Can't create tmp dir in test_get_wh_balance");

    let wh = WHStorage::open(&tmp_dir.path()).unwrap();
    let mut db = wh.database;

    let start_d = dt("2022-10-10")?;
    let end_d = dt("2022-10-11")?;
    let w1 = Uuid::new_v4();
    let party = Batch { id: Uuid::new_v4(), date: start_d };

    let id1 = Uuid::from_u128(101);
    let id2 = Uuid::from_u128(102);

    let ops = vec![
      OpMutation::new(
        id1,
        start_d,
        w1,
        None,
        G1,
        party.clone(),
        None,
        Some(InternalOperation::Receive(2.into(), 2000.into())),
        chrono::Utc::now(),
      ),
      OpMutation::new(
        id2,
        start_d,
        w1,
        None,
        G1,
        party.clone(),
        None,
        Some(InternalOperation::Receive(1.into(), 1000.into())),
        chrono::Utc::now(),
      ),
    ];

    for op in &ops {
      key.put_op(op, &db)?;
    }

    let res = key.get_ops(start_d, end_d, w1, &db)?;

    for i in 0..ops.len() {
      assert_eq!(ops[i].to_op(), res[i]);
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
    let tmp_dir = TempDir::new().expect("Can't create tmp dir in test_get_wh_balance");

    let wh = WHStorage::open(&tmp_dir.path()).unwrap();
    let mut db = wh.database;

    let op_d = dt("2022-10-10")?;
    let check_d = dt("2022-10-11")?;
    let w1 = Uuid::new_v4();
    let doc1 = Batch { id: Uuid::new_v4(), date: dt("2022-10-09")? };
    let doc2 = Batch { id: Uuid::new_v4(), date: op_d };

    let id1 = Uuid::from_u128(101);
    let id2 = Uuid::from_u128(102);
    let id3 = Uuid::from_u128(103);
    let id4 = Uuid::from_u128(104);

    let ops = vec![
      OpMutation::new(
        id1,
        op_d,
        w1,
        None,
        G1,
        doc1.clone(),
        None,
        Some(InternalOperation::Receive(3.into(), 3000.into())),
        chrono::Utc::now(),
      ),
      OpMutation::new(
        id2,
        op_d,
        w1,
        None,
        G1,
        doc1.clone(),
        None,
        Some(InternalOperation::Issue(1.into(), 1000.into(), Mode::Manual)),
        chrono::Utc::now(),
      ),
      OpMutation::new(
        id3,
        op_d,
        w1,
        None,
        G2,
        doc2.clone(),
        None,
        Some(InternalOperation::Issue(2.into(), 2000.into(), Mode::Manual)),
        chrono::Utc::now(),
      ),
      OpMutation::new(
        id4,
        op_d,
        w1,
        None,
        G2,
        doc2.clone(),
        None,
        Some(InternalOperation::Receive(2.into(), 2000.into())),
        chrono::Utc::now(),
      ),
    ];

    for op in &ops {
      key.put_op(op, &db)?;
    }

    let agregations = vec![
      AgregationStoreGoods {
        store: Some(w1),
        goods: Some(G1),
        batch: Some(doc1.clone()),
        open_balance: BalanceForGoods::default(),
        receive: BalanceDelta { qty: 3.into(), cost: 3000.into() },
        issue: BalanceDelta { qty: 1.into(), cost: 1000.into() },
        close_balance: BalanceForGoods { qty: 2.into(), cost: 2000.into() },
      },
      AgregationStoreGoods {
        store: Some(w1),
        goods: Some(G2),
        batch: Some(doc2.clone()),
        open_balance: BalanceForGoods::default(),
        receive: BalanceDelta { qty: 2.into(), cost: 2000.into() },
        issue: BalanceDelta { qty: 2.into(), cost: 2000.into() },
        close_balance: BalanceForGoods::default(),
      },
    ];

    let res = key.get_report(op_d, check_d, w1, &mut db)?;
    let mut iter = res.items.1.into_iter();

    // println!("MANY BALANCES: {:#?}", res);

    for agr in agregations {
      assert_eq!(iter.next().expect("option in get_agregations"), agr);
    }
    assert_eq!(iter.next(), None);

    tmp_dir.close().expect("Can't close tmp dir in store_test_get_wh_balance");

    Ok(())
  }

  #[actix_web::test]
  async fn store_test_op_iter() {
    let tmp_dir = TempDir::new().expect("Can't create tmp dir in test_op_iter");

    let wh = WHStorage::open(&tmp_dir.path()).unwrap();
    let mut db = wh.database;

    let start_d = dt("2022-11-01").expect("test_op_iter");
    let w1 = Uuid::new_v4();
    let party = Batch { id: Uuid::new_v4(), date: start_d };

    let id1 = Uuid::from_u128(101);
    let id2 = Uuid::from_u128(102);
    let id3 = Uuid::from_u128(103);
    let id4 = Uuid::from_u128(104);

    let ops = vec![
      OpMutation::new(
        id3,
        start_d,
        w1,
        None,
        G2,
        party.clone(),
        None,
        Some(InternalOperation::Receive(1.into(), 1000.into())),
        chrono::Utc::now(),
      ),
      OpMutation::new(
        id4,
        start_d,
        w1,
        None,
        G2,
        party.clone(),
        None,
        Some(InternalOperation::Receive(1.into(), 1000.into())),
        chrono::Utc::now(),
      ),
      OpMutation::new(
        id1,
        start_d,
        w1,
        None,
        G3,
        party.clone(),
        None,
        Some(InternalOperation::Issue(Decimal::from_str("0.5").unwrap(), 1500.into(), Mode::Manual)),
        chrono::Utc::now(),
      ),
      OpMutation::new(
        id2,
        start_d,
        w1,
        None,
        G3,
        party.clone(),
        None,
        Some(InternalOperation::Issue(Decimal::from_str("0.5").unwrap(), 1500.into(), Mode::Manual)),
        chrono::Utc::now(),
      ),
    ];

    for op in &ops {
      db.put(&op.store_date_type_batch_id(), &op.value().expect("test_op_iter"))
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

    let wh = WHStorage::open(&tmp_dir.path()).unwrap();
    let mut db = wh.database;

    let start_d = dt("2022-11-07").expect("test_report");
    let end_d = dt("2022-11-08").expect("test_report");
    let w1 = Uuid::new_v4();
    let party = Batch { id: Uuid::new_v4(), date: start_d };

    let ops = vec![
      OpMutation::new(
        Uuid::new_v4(),
        dt("2022-10-30").expect("test_report"),
        w1,
        None,
        G1,
        party.clone(),
        None,
        Some(InternalOperation::Receive(4.into(), 4000.into())),
        chrono::Utc::now(),
      ),
      OpMutation::new(
        Uuid::new_v4(),
        dt("2022-11-03").expect("test_report"),
        w1,
        None,
        G3,
        party.clone(),
        None,
        Some(InternalOperation::Receive(2.into(), 6000.into())),
        chrono::Utc::now(),
      ),
      OpMutation::new(
        Uuid::new_v4(),
        start_d,
        w1,
        None,
        G2,
        party.clone(),
        None,
        Some(InternalOperation::Receive(1.into(), 1000.into())),
        chrono::Utc::now(),
      ),
      OpMutation::new(
        Uuid::new_v4(),
        start_d,
        w1,
        None,
        G2,
        party.clone(),
        None,
        Some(InternalOperation::Receive(1.into(), 1000.into())),
        chrono::Utc::now(),
      ),
      OpMutation::new(
        Uuid::new_v4(),
        start_d,
        w1,
        None,
        G3,
        party.clone(),
        None,
        Some(InternalOperation::Issue(Decimal::from_str("0.5").unwrap(), 1500.into(), Mode::Manual)),
        chrono::Utc::now(),
      ),
      OpMutation::new(
        Uuid::new_v4(),
        start_d,
        w1,
        None,
        G3,
        party.clone(),
        None,
        Some(InternalOperation::Issue(Decimal::from_str("0.5").unwrap(), 1500.into(), Mode::Manual)),
        chrono::Utc::now(),
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
        batch: Some(party.clone()),
        open_balance: BalanceForGoods { qty: 4.into(), cost: 4000.into() },
        receive: BalanceDelta::default(),
        issue: BalanceDelta::default(),
        close_balance: BalanceForGoods { qty: 4.into(), cost: 4000.into() },
      },
      AgregationStoreGoods {
        store: Some(w1),
        goods: Some(G2),
        batch: Some(party.clone()),
        open_balance: BalanceForGoods::default(),
        receive: BalanceDelta { qty: 2.into(), cost: 2000.into() },
        issue: BalanceDelta::default(),
        close_balance: BalanceForGoods { qty: 2.into(), cost: 2000.into() },
      },
      AgregationStoreGoods {
        store: Some(w1),
        goods: Some(G3),
        batch: Some(party.clone()),
        open_balance: BalanceForGoods { qty: 2.into(), cost: 6000.into() },
        receive: BalanceDelta::default(),
        issue: BalanceDelta { qty: 1.into(), cost: 3000.into() },
        close_balance: BalanceForGoods { qty: 1.into(), cost: 3000.into() },
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
    let tmp_dir = TempDir::new().expect("Can't create tmp dir in test_parties");

    let wh = WHStorage::open(&tmp_dir.path()).unwrap();
    let mut db = wh.database;

    let start_d = dt("2022-10-10").expect("test_parties");
    let end_d = dt("2022-10-11").expect("test_parties");
    let w1 = Uuid::new_v4();
    let doc1 = Batch { id: Uuid::new_v4(), date: dt("2022-10-08").expect("test_parties") };
    let doc2 = Batch { id: Uuid::new_v4(), date: start_d };

    let id1 = Uuid::from_u128(101);
    let id2 = Uuid::from_u128(102);
    let id3 = Uuid::from_u128(102);

    let ops = vec![
      OpMutation::new(
        id1,
        start_d,
        w1,
        None,
        G1,
        doc1.clone(),
        None,
        Some(InternalOperation::Receive(3.into(), 3000.into())),
        chrono::Utc::now(),
      ),
      OpMutation::new(
        id2,
        start_d,
        w1,
        None,
        G1,
        doc2.clone(),
        None,
        Some(InternalOperation::Receive(4.into(), 2000.into())),
        chrono::Utc::now(),
      ),
      OpMutation::new(
        id3,
        start_d,
        w1,
        None,
        G1,
        doc2.clone(),
        None,
        Some(InternalOperation::Issue(1.into(), 500.into(), Mode::Manual)),
        chrono::Utc::now(),
      ),
    ];

    db.record_ops(&ops).expect("test_parties");

    let st = DateTypeStoreGoodsId();
    let res = st.get_report(start_d, end_d, w1, &mut db).expect("test_parties");

    let agrs = vec![
      AgregationStoreGoods {
        store: Some(w1),
        goods: Some(G1),
        batch: Some(doc1.clone()),
        open_balance: BalanceForGoods::default(),
        receive: BalanceDelta { qty: 3.into(), cost: 3000.into() },
        issue: BalanceDelta::default(),
        close_balance: BalanceForGoods { qty: 3.into(), cost: 3000.into() },
      },
      AgregationStoreGoods {
        store: Some(w1),
        goods: Some(G1),
        batch: Some(doc2.clone()),
        open_balance: BalanceForGoods::default(),
        receive: BalanceDelta { qty: 4.into(), cost: 2000.into() },
        issue: BalanceDelta { qty: 1.into(), cost: 500.into() },
        close_balance: BalanceForGoods { qty: 3.into(), cost: 1500.into() },
      },
    ];

    assert_eq!(res.items.1[0], agrs[0]);
    assert_eq!(res.items.1[1], agrs[1]);

    tmp_dir.close().expect("Can't close tmp dir in test_parties");
  }

  #[actix_web::test]
  async fn store_test_issue_cost_none() {
    let tmp_dir = TempDir::new().expect("Can't create tmp dir in test_issue_cost_none");

    let wh = WHStorage::open(&tmp_dir.path()).unwrap();
    let mut db = wh.database;

    let start_d = dt("2022-10-10").expect("test_issue_cost_none");
    let end_d = dt("2022-10-11").expect("test_issue_cost_none");
    let w1 = Uuid::new_v4();

    let doc = Batch { id: Uuid::new_v4(), date: start_d };

    let id1 = Uuid::from_u128(101);
    let id2 = Uuid::from_u128(102);

    let ops = vec![
      OpMutation::new(
        id1,
        start_d,
        w1,
        None,
        G1,
        doc.clone(),
        None,
        Some(InternalOperation::Receive(4.into(), 2000.into())),
        chrono::Utc::now(),
      ),
      OpMutation::new(
        id2,
        start_d,
        w1,
        None,
        G1,
        doc.clone(),
        None,
        Some(InternalOperation::Issue(1.into(), 0.into(), Mode::Auto)),
        chrono::Utc::now(),
      ),
    ];

    db.record_ops(&ops).expect("test_issue_cost_none");

    let st = DateTypeStoreGoodsId();
    let res = st.get_report(start_d, end_d, w1, &mut db).expect("test_issue_cost_none");

    let agr = AgregationStoreGoods {
      store: Some(w1),
      goods: Some(G1),
      batch: Some(doc.clone()),
      open_balance: BalanceForGoods::default(),
      receive: BalanceDelta { qty: 4.into(), cost: 2000.into() },
      issue: BalanceDelta { qty: 1.into(), cost: 500.into() },
      close_balance: BalanceForGoods { qty: 3.into(), cost: 1500.into() },
    };

    assert_eq!(agr, res.items.1[0]);

    tmp_dir.close().expect("Can't remove tmp dir in test_issue_cost_none");
  }

  #[actix_web::test]
  async fn store_test_receive_cost_none() {
    let tmp_dir = TempDir::new().expect("Can't create tmp dir in test_receive_cost_none");

    let wh = WHStorage::open(&tmp_dir.path()).unwrap();
    let mut db = wh.database;

    let start_d = dt("2022-10-10").expect("test_receive_cost_none");
    let end_d = dt("2022-10-11").expect("test_receive_cost_none");
    let w1 = Uuid::new_v4();

    let doc = Batch { id: Uuid::new_v4(), date: start_d };

    let id1 = Uuid::from_u128(101);
    let id2 = Uuid::from_u128(102);

    let ops = vec![
      OpMutation::new(
        id1,
        start_d,
        w1,
        None,
        G1,
        doc.clone(),
        None,
        Some(InternalOperation::Receive(4.into(), 2000.into())),
        chrono::Utc::now(),
      ),
      OpMutation::new(
        id2,
        start_d,
        w1,
        None,
        G1,
        doc.clone(),
        None,
        Some(InternalOperation::Receive(1.into(), 0.into())),
        chrono::Utc::now(),
      ),
    ];

    db.record_ops(&ops).expect("test_receive_cost_none");

    let st = DateTypeStoreGoodsId();
    let res = st.get_report(start_d, end_d, w1, &mut db).expect("test_receive_cost_none");

    let agr = AgregationStoreGoods {
      store: Some(w1),
      goods: Some(G1),
      batch: Some(doc.clone()),
      open_balance: BalanceForGoods::default(),
      receive: BalanceDelta { qty: 5.into(), cost: 2000.into() },
      issue: BalanceDelta { qty: 0.into(), cost: 0.into() },
      close_balance: BalanceForGoods { qty: 5.into(), cost: 2000.into() },
    };

    assert_eq!(agr, res.items.1[0]);

    tmp_dir.close().expect("Can't remove tmp dir in test_receive_cost_none");
  }

  #[actix_web::test]
  async fn store_test_issue_remainder() {
    let tmp_dir = TempDir::new().expect("Can't create tmp dir in test_issue_remainder");

    let wh = WHStorage::open(&tmp_dir.path()).unwrap();
    let mut db = wh.database;

    let start_d = dt("2022-10-10").expect("test_issue_remainder");
    let end_d = dt("2022-10-11").expect("test_issue_remainder");
    let w1 = Uuid::new_v4();

    let doc = Batch { id: Uuid::new_v4(), date: start_d };

    let id1 = Uuid::from_u128(101);
    let id2 = Uuid::from_u128(102);
    let id3 = Uuid::from_u128(103);

    let ops = vec![
      OpMutation::new(
        id1,
        start_d,
        w1,
        None,
        G1,
        doc.clone(),
        None,
        Some(InternalOperation::Receive(3.into(), 10.into())),
        chrono::Utc::now(),
      ),
      OpMutation::new(
        id2,
        start_d,
        w1,
        None,
        G1,
        doc.clone(),
        None,
        Some(InternalOperation::Issue(1.into(), 0.into(), Mode::Auto)),
        chrono::Utc::now(),
      ),
      OpMutation::new(
        id3,
        start_d,
        w1,
        None,
        G1,
        doc.clone(),
        None,
        Some(InternalOperation::Issue(2.into(), 0.into(), Mode::Auto)),
        chrono::Utc::now(),
      ),
    ];

    db.record_ops(&ops).expect("test_issue_remainder");

    let st = DateTypeStoreGoodsId();
    let res = st.get_report(start_d, end_d, w1, &mut db).expect("test_issue_remainder");

    // println!("HELLO: {:#?}", res.items.1);

    let agr = AgregationStoreGoods {
      store: Some(w1),
      goods: Some(G1),
      batch: Some(doc.clone()),
      open_balance: BalanceForGoods::default(),
      receive: BalanceDelta { qty: 3.into(), cost: 10.into() },
      issue: BalanceDelta { qty: 3.into(), cost: 10.into() },
      close_balance: BalanceForGoods { qty: 0.into(), cost: 0.into() },
    };

    assert_eq!(agr, res.items.1[0]);

    tmp_dir.close().expect("Can't remove tmp dir in test_issue_remainder");
  }

  #[actix_web::test]
  async fn store_test_issue_op_none() {
    let tmp_dir = TempDir::new().expect("Can't create tmp dir in test_issue_op_none");

    let wh = WHStorage::open(&tmp_dir.path()).unwrap();
    let mut db = wh.database;

    let start_d = dt("2022-10-10").expect("test_issue_op_none");
    let end_d = dt("2022-10-11").expect("test_issue_op_none");
    let w1 = Uuid::new_v4();

    let doc = Batch { id: Uuid::new_v4(), date: start_d };

    let id1 = Uuid::from_u128(101);
    let id2 = Uuid::from_u128(102);
    let id3 = Uuid::from_u128(103);

    let ops = vec![
      OpMutation::new(
        id1,
        start_d,
        w1,
        None,
        G1,
        doc.clone(),
        None,
        Some(InternalOperation::Receive(3.into(), 10.into())),
        chrono::Utc::now(),
      ),
      // КОРРЕКТНАЯ ОПЕРАЦИЯ С ДВУМЯ NONE?
      OpMutation::new(id3, start_d, w1, None, G1, doc.clone(), None, None, chrono::Utc::now()),
    ];

    db.record_ops(&ops).expect("test_issue_op_none");

    let st = DateTypeStoreGoodsId();
    let res = st.get_report(start_d, end_d, w1, &mut db).expect("test_issue_op_none");

    let agr = AgregationStoreGoods {
      store: Some(w1),
      goods: Some(G1),
      batch: Some(doc.clone()),
      open_balance: BalanceForGoods::default(),
      receive: BalanceDelta { qty: 3.into(), cost: 10.into() },
      issue: BalanceDelta { qty: 3.into(), cost: 10.into() },
      close_balance: BalanceForGoods { qty: 0.into(), cost: 0.into() },
    };

    assert_eq!(agr, res.items.1[0]);

    tmp_dir.close().expect("Can't remove tmp dir in test_issue_op_none");
  }

  #[actix_web::test]
  async fn store_test_receive_change_op() {
    let tmp_dir = TempDir::new().expect("Can't create tmp dir in test_receive_change_op");

    let wh = WHStorage::open(&tmp_dir.path()).unwrap();
    let mut db = wh.database;

    let start_d = dt("2022-10-10").expect("test_receive_change_op");
    let end_d = dt("2022-10-11").expect("test_receive_change_op");
    let w1 = Uuid::new_v4();

    let doc = Batch { id: Uuid::new_v4(), date: start_d };

    let id1 = Uuid::from_u128(101);

    let ops_old = vec![
      OpMutation::new(
        id1,
        dt("2022-08-25").expect("test_receive_change_op"),
        w1,
        None,
        G1,
        doc.clone(),
        None,
        Some(InternalOperation::Receive(3.into(), 10.into())),
        chrono::Utc::now(),
      ),
      OpMutation::new(
        id1,
        dt("2022-09-20").expect("test_receive_change_op"),
        w1,
        None,
        G1,
        doc.clone(),
        None,
        Some(InternalOperation::Receive(1.into(), 30.into())),
        chrono::Utc::now(),
      ),
    ];

    db.record_ops(&ops_old).expect("test_receive_change_op");

    let old_check = Balance {
      date: dt("2022-10-01").expect("test_receive_change_op"),
      store: w1,
      goods: G1,
      batch: doc.clone(),
      number: BalanceForGoods { qty: 4.into(), cost: 40.into() },
    };

    let mut old_checkpoints = db
      .get_checkpoints_before_date(start_d, w1)
      .expect("test_receive_change_op")
      .into_iter();

    assert_eq!(Some(old_check), old_checkpoints.next());

    let ops_new = vec![OpMutation::new(
      id1,
      dt("2022-08-25").expect("test_receive_change_op"),
      w1,
      None,
      G1,
      doc.clone(),
      Some(InternalOperation::Receive(3.into(), 10.into())),
      Some(InternalOperation::Receive(4.into(), 100.into())),
      chrono::Utc::now(),
    )];

    db.record_ops(&ops_new).expect("test_receive_change_op");

    let new_check = Balance {
      date: dt("2022-10-01").expect("test_receive_change_op"),
      store: w1,
      goods: G1,
      batch: doc.clone(),
      number: BalanceForGoods { qty: 5.into(), cost: 130.into() },
    };

    let mut new_checkpoints = db
      .get_checkpoints_before_date(start_d, w1)
      .expect("test_receive_change_op")
      .into_iter();

    assert_eq!(Some(new_check), new_checkpoints.next());

    let st = DateTypeStoreGoodsId();
    let res = st.get_report(start_d, end_d, w1, &mut db).expect("test_receive_change_op");

    let agr = AgregationStoreGoods {
      store: Some(w1),
      goods: Some(G1),
      batch: Some(doc.clone()),
      open_balance: BalanceForGoods { qty: 5.into(), cost: 130.into() },
      receive: BalanceDelta::default(),
      issue: BalanceDelta { qty: 0.into(), cost: 0.into() },
      close_balance: BalanceForGoods { qty: 5.into(), cost: 130.into() },
    };

    assert_eq!(res.items.1[0], agr);

    tmp_dir.close().expect("Can't remove tmp dir in test_receive_change_op");
  }
}
