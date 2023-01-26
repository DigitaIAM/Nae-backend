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
use self::date_type_store_goods_id::DateTypeStoreGoodsId;
pub use self::error::WHError;
use self::store_date_type_goods_id::StoreDateTypeGoodsId;

type Goods = Uuid;
type Store = Uuid;
type Qty = Decimal;
type Cost = Decimal;

const G1: Uuid = Uuid::from_u128(1);
const G2: Uuid = Uuid::from_u128(2);
const G3: Uuid = Uuid::from_u128(3);

const UUID_NIL: Uuid = uuid!("00000000-0000-0000-0000-000000000000");
const UUID_MAX: Uuid = uuid!("ffffffff-ffff-ffff-ffff-ffffffffffff");

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
      StoreDateTypeGoodsId::cf_name(),
      DateTypeStoreGoodsId::cf_name(),
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

    let tmp_db = DB::open_cf_descriptors(&opts, &path, cfs)
      .expect("Can't open database in settings.database.inventory");
    let inner_db = Arc::new(tmp_db);

    let checkpoint_topologies: Vec<Box<dyn CheckpointTopology + Sync + Send>> = vec![
      Box::new(CheckDateStoreBatch { db: inner_db.clone() }),
      Box::new(CheckBatchStoreDate { db: inner_db.clone() }),
    ];

    let store_topologies: Vec<Box<dyn StoreTopology + Sync + Send>> = vec![
      Box::new(DateTypeStoreGoodsId { db: inner_db.clone() }),
      Box::new(StoreDateTypeGoodsId { db: inner_db.clone() }),
    ];

    let outer_db = Db {
      db: inner_db,
      checkpoint_topologies: Arc::new(checkpoint_topologies),
      store_topologies: Arc::new(store_topologies),
    };

    Ok(WHStorage { database: outer_db })
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
trait StoreTopology {
  fn put_op(&self, op: &OpMutation) -> Result<(), WHError>;

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

    println!("BALANCES: {balances:#?}");

    let ops = self.get_ops(first_day_current_month(start_date), end_date, wh, db)?;

    println!("OPS: {ops:#?}");

    let items = new_get_agregations(balances, ops, start_date);

    Ok(Report { start_date, end_date, items })
  }

  fn data_update(&self, op: &OpMutation) -> Result<(), WHError>;
}

trait CheckpointTopology {
  // fn cf_name(&self) -> &str;

  fn key(&self, op: &Op, date_of_checkpoint: DateTime<Utc>) -> Vec<u8>;

  fn get_balance(&self, key: &Vec<u8>) -> Result<BalanceForGoods, WHError>;
  fn set_balance(&self, key: &Vec<u8>, balance: BalanceForGoods) -> Result<(), WHError>;
  fn del_balance(&self, key: &Vec<u8>) -> Result<(), WHError>;

  fn data_update(&self, op: &OpMutation, last_checkpoint_date: DateTime<Utc>)
    -> Result<(), WHError>;

  fn get_checkpoints_before_date(
    &self,
    date: DateTime<Utc>,
    wh: Store,
  ) -> Result<Vec<Balance>, WHError>;
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
  checkpoint_topologies: Arc<Vec<Box<dyn CheckpointTopology + Sync + Send>>>,
  store_topologies: Arc<Vec<Box<dyn StoreTopology + Sync + Send>>>,
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
    for op in ops {
      for checkpoint_topology in self.checkpoint_topologies.iter() {
        checkpoint_topology.data_update(op, dt("2023-02-01")?)?;
      }

      // if let Some(cf) = self.db.cf_handle(DATE_TYPE_STORE_BATCH_ID) {
      //   self.db.put_cf(&cf, op.key(&DATE_TYPE_STORE_BATCH_ID.to_string())?, op.value()?)?;
      // }

      // if let Some(cf) = self.db.cf_handle(STORE_DATE_TYPE_BATCH_ID) {
      //   self.db.put_cf(&cf, op.key(&STORE_DATE_TYPE_BATCH_ID.to_string())?, op.value()?)?;
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
          if e.message() == "Not supported".to_string() {
            continue;
          } else {
            return Err(e);
          }
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
mod test;
