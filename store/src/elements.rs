#![allow(dead_code, unused_variables, unused_imports)]

use chrono::{DateTime, Datelike, Month, NaiveDate, Utc};
use json::{array, iterators::Members, object, JsonValue};
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

pub use super::error::WHError;
use super::{
  balance::{BalanceDelta, BalanceForGoods},
  check_batch_store_date::CheckBatchStoreDate,
  check_date_store_batch::CheckDateStoreBatch,
  date_type_store_batch_id::DateTypeStoreBatchId,
  db::Db,
  store_date_type_batch_id::StoreDateTypeBatchId,
};
use service::utils::time::{date_to_string, time_to_string};

use crate::GetWarehouse;
use service::Services;

use service::utils::json::JsonParams;
use std::cmp::max;

pub type Goods = Uuid;
pub type Store = Uuid;
pub(crate) type Qty = Decimal;
pub type Cost = Decimal;

pub(crate) const UUID_NIL: Uuid = uuid!("00000000-0000-0000-0000-000000000000");
pub(crate) const UUID_MAX: Uuid = uuid!("ffffffff-ffff-ffff-ffff-ffffffffffff");

pub trait ToJson {
  fn to_json(&self) -> JsonValue;
}

impl ToJson for Uuid {
  fn to_json(&self) -> JsonValue {
    JsonValue::String(self.to_string())
  }
}

impl ToJson for DateTime<Utc> {
  fn to_json(&self) -> JsonValue {
    JsonValue::String(date_to_string(*self))
  }
}

impl ToJson for Decimal {
  fn to_json(&self) -> JsonValue {
    JsonValue::String(self.to_string())
  }
}

impl ToJson for String {
  fn to_json(&self) -> JsonValue {
    JsonValue::String(self.clone())
  }
}

#[derive(Hash, Eq, Ord, PartialOrd, Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Batch {
  pub id: Uuid,
  pub date: DateTime<Utc>,
}

impl Batch {
  fn new() -> Batch {
    Batch { id: Uuid::new_v4(), date: DateTime::<Utc>::MAX_UTC }
  }

  pub fn is_empty(&self) -> bool {
    self.id == UUID_NIL
  }

  fn from_json(json: &JsonValue) -> Result<Self, WHError> {
    if json.is_object() {
      Ok(Batch { id: json["id"].uuid()?, date: json["date"].date_with_check()? })
    } else {
      Err(WHError::new("fn from_json for Batch failed"))
    }
  }

  fn to_barcode(&self) -> String {
    let date = self.date.to_string();
    let mut id: String = self.id.to_string().chars().filter(|c| *c >= '0' && *c <= '9').collect();
    while id.len() < 5 {
      id.push('0');
    }
    format!("2{}{}{}{}", &date[2..4], &date[5..7], &date[8..10], &id[0..5])
  }
}

impl ToJson for Batch {
  fn to_json(&self) -> JsonValue {
    let barcode = self.to_barcode();
    object! {
      barcode: barcode.to_json(),
      id: self.id.to_json(),
      date: self.date.to_json()
    }
  }
}

pub trait OrderedTopology {
  fn put(&self, op: &Op, balance: &BalanceForGoods) -> Result<(), WHError>;
  fn get(&self, op: &Op) -> Result<Option<(Op, BalanceForGoods)>, WHError>;
  fn del(&self, op: &Op) -> Result<(), WHError>;

  fn balance_before(&self, op: &Op) -> Result<BalanceForGoods, WHError>;
  fn goods_balance_before(
    &self,
    op: &Op,
    balances: Vec<Balance>,
  ) -> Result<Vec<(Batch, BalanceForGoods)>, WHError>;

  fn operations_after(&self, op: &Op) -> Result<Vec<(Op, BalanceForGoods)>, WHError>;

  fn create_cf(&self, opts: Options) -> ColumnFamilyDescriptor;

  fn get_ops(
    &self,
    storage: Store,
    from_date: DateTime<Utc>,
    till_date: DateTime<Utc>,
  ) -> Result<Vec<Op>, WHError>;

  fn get_ops_for_all(
    &self,
    from_date: DateTime<Utc>,
    till_date: DateTime<Utc>,
  ) -> Result<Vec<Op>, WHError>;

  fn get_ops_for_one_goods(
    &self,
    store: Store,
    goods: Goods,
    from_date: DateTime<Utc>,
    till_date: DateTime<Utc>,
  ) -> Result<Vec<Op>, WHError>;

  fn get_ops_for_one_goods_and_batch(
    &self,
    store: Store,
    goods: Goods,
    batch: &Batch,
    from_date: DateTime<Utc>,
    till_date: DateTime<Utc>,
  ) -> Result<Vec<Op>, WHError>;

  fn get_ops_for_many_goods(
    &self,
    goods: &Vec<Goods>,
    from_date: DateTime<Utc>,
    till_date: DateTime<Utc>,
  ) -> Result<Vec<Op>, WHError>;

  fn get_report_for_goods(
    &self,
    db: &Db,
    storage: Store,
    goods: Goods,
    batch: &Batch,
    from_date: DateTime<Utc>,
    till_date: DateTime<Utc>,
  ) -> Result<JsonValue, WHError>;

  fn get_report_for_storage(
    &self,
    db: &Db,
    storage: Store,
    from_date: DateTime<Utc>,
    till_date: DateTime<Utc>,
  ) -> Result<Report, WHError>;

  fn key(&self, op: &Op) -> Vec<u8>;

  fn data_update(
    &self,
    op: &OpMutation,
    balances: Vec<Balance>,
  ) -> Result<Vec<OpMutation>, WHError> {
    self.mutate_op(op, balances)

    // TODO review logic after enable transaction
    // if op.before.is_none() {
    //   if let Ok(None) = self.get(&op.to_op()) {
    //     self.mutate_op(op, balances)
    //   } else {
    //     let err = WHError::new("Wrong 'before' state, expected something");
    //     log::debug!("ERROR: {err:?}");
    //     return Err(err);
    //   }
    // } else {
    //   if let Ok(Some((o, balance))) = self.get(&op.to_op()) {
    //     // let (o, balance) = self.from_bytes(&bytes)?;
    //     if Some(o.op) == op.before {
    //       self.mutate_op(op, balances)
    //     } else {
    //       let err = WHError::new("Wrong 'before' state in operation: {o.op:?}");
    //       log::debug!("ERROR: {err:?}");
    //       return Err(err);
    //     }
    //   } else {
    //     let err = WHError::new("There is no such operation in db");
    //     log::debug!("ERROR: {err:?}");
    //     return Err(err);
    //   }
    // }
  }

  fn mutate_op(
    &self,
    op_mut: &OpMutation,
    balances: Vec<Balance>,
  ) -> Result<Vec<OpMutation>, WHError> {
    let mut ops: Vec<Op> = vec![];
    let mut result: Vec<OpMutation> = vec![];

    ops.push(op_mut.to_op());

    while ops.len() > 0 {
      let mut op = ops.remove(0);

      log::debug!("processing {:?}", op);

      let mut batches = vec![];

      if op.is_issue() && op.batch.is_empty() && !op.is_dependent {
        // calculate balance
        let before_balance: Vec<(Batch, BalanceForGoods)> =
          self.goods_balance_before(&op, balances.clone())?;

        println!("BEFORE_BALANCE: {:?}", before_balance);

        let mut qty = match op.op {
          InternalOperation::Receive(_, _) => unreachable!(),
          InternalOperation::Issue(qty, _, _) => qty,
        };

        for (batch, balance) in before_balance {
          if balance.qty <= Decimal::ZERO {
            continue;
          } else if qty >= balance.qty {
            batches.push(batch.clone());

            let mut new = op.clone();
            new.is_dependent = true;
            new.batch = batch;
            new.op = InternalOperation::Issue(balance.qty, balance.cost, Mode::Auto);
            println!("NEW_OP partly: qty {qty} balance {balance:?} op {new:?}");
            ops.push(new);

            qty -= balance.qty;

            println!("NEW_OP: qty {:?}", qty);
          } else {
            batches.push(batch.clone());

            let mut new = op.clone();
            new.is_dependent = true;
            new.batch = batch;
            new.op = InternalOperation::Issue(qty, qty * (balance.cost / balance.qty), Mode::Auto);
            println!("NEW_OP full: qty {qty} balance {balance:?} op {new:?}");
            ops.push(new);

            qty -= qty;
          }

          if qty <= Decimal::ZERO {
            break;
          }
        }

        println!("qty left {qty}");

        // todo!("update op with qty");
        op.op = match op.op {
          InternalOperation::Receive(_, _) => unreachable!(),
          InternalOperation::Issue(q, c, m) => InternalOperation::Issue(qty, qty * (c / q), m), // TODO make sure that q is not ZERO
        };

        op.batches = batches.clone();

        // calculate balance
        let before_balance: BalanceForGoods = self.balance_before(&op)?; // Vec<(Batch, BalanceForGoods)>
        let (calculated_op, new_balance) = self.evaluate(&before_balance, &op);

        let current_balance =
          if let Some((o, b)) = self.get(&op)? { b } else { BalanceForGoods::default() };

        log::debug!("_before_balance: {before_balance:?}");
        log::debug!("_calculated_op: {calculated_op:?}");
        log::debug!("_current_balance: {current_balance:?}");
        log::debug!("_new_balance: {new_balance:?}");

        // store update op with balance or delete
        if calculated_op.is_zero() && batches.is_empty() {
          self.del(&calculated_op)?;
        } else {
          //   self.put(&calculated_op, &new_balance, batches)?;
          self.put(&calculated_op, &new_balance)?;
          result.push(OpMutation {
            id: calculated_op.id,
            date: calculated_op.date,
            store: calculated_op.store,
            transfer: calculated_op.store_into,
            goods: calculated_op.goods,
            batch: calculated_op.batch.clone(),
            before: None,
            after: Some(calculated_op.op.clone()),
            is_dependent: calculated_op.is_dependent,
            batches: calculated_op.batches.clone(),
          });
        }

        // if next op have dependant add it to ops
        if let Some(dep) = calculated_op.dependent() {
          log::debug!("_new dependent: {dep:?}");
          ops.push(dep);
        }
      } else {
        // calculate balance
        let before_balance: BalanceForGoods = self.balance_before(&op)?; // Vec<(Batch, BalanceForGoods)>
        let (calculated_op, new_balance) = self.evaluate(&before_balance, &op);

        let current_balance =
          if let Some((o, b)) = self.get(&op)? { b } else { BalanceForGoods::default() };

        log::debug!("before_balance: {before_balance:?}");
        log::debug!("calculated_op: {calculated_op:?}");
        log::debug!("current_balance: {current_balance:?}");
        log::debug!("new_balance: {new_balance:?}");

        // store update op with balance or delete
        if calculated_op.is_zero() {
          self.del(&calculated_op)?;
        } else {
          //   self.put(&calculated_op, &new_balance, batches)?;
          self.put(&calculated_op, &new_balance)?;
          result.push(OpMutation {
            id: calculated_op.id,
            date: calculated_op.date,
            store: calculated_op.store,
            transfer: calculated_op.store_into,
            goods: calculated_op.goods,
            batch: calculated_op.batch.clone(),
            before: None,
            after: Some(calculated_op.op.clone()),
            is_dependent: calculated_op.is_dependent,
            batches: calculated_op.batches.clone(),
          });
        }

        // if next op have dependant add it to ops
        if let Some(dep) = calculated_op.dependent() {
          log::debug!("new dependent: {dep:?}");
          ops.push(dep);
        }

        // propagate delta
        if !current_balance.delta(&new_balance).is_zero() {
          let mut before_balance = new_balance;
          for (next_operation, next_current_balance) in self.operations_after(&calculated_op)? {
            let (calc_op, new_balance) = self.evaluate(&before_balance, &next_operation);
            if calc_op.is_zero() {
              self.del(&calc_op)?;
            } else {
              //   self.put(&calc_op, &new_balance, batches)?;
              self.put(&calc_op, &new_balance)?;
            }

            // if next op have dependant add it to ops
            if let Some(dep) = calc_op.dependent() {
              log::debug!("update dependent: {dep:?}");
              ops.push(dep);
            }

            if !next_current_balance.delta(&new_balance).is_zero() {
              break;
            }

            before_balance = new_balance;
          }
        }
      }
    }

    Ok(result)
  }

  fn evaluate(&self, balance: &BalanceForGoods, op: &Op) -> (Op, BalanceForGoods) {
    match &op.op {
      InternalOperation::Receive(q, c) => {
        (op.clone(), BalanceForGoods { qty: balance.qty + q, cost: balance.cost + c })
      },
      InternalOperation::Issue(q, c, m) => {
        let mut cost = c.clone();
        let op = if m == &Mode::Auto {
          cost = match balance.cost.checked_div(balance.qty) {
            Some(price) => price * q,
            None => 0.into(), // TODO raise exeption?
          };
          Op {
            id: op.id,
            date: op.date,
            store: op.store,
            goods: op.goods,
            batch: op.batch.clone(),
            store_into: op.store_into,
            op: InternalOperation::Issue(q.clone(), cost.clone(), m.clone()),
            is_dependent: op.is_dependent,
            batches: op.batches.clone(),
          }
        } else {
          op.clone()
        };

        (op, BalanceForGoods { qty: balance.qty - q, cost: balance.cost - cost })
      },
    }
  }

  fn to_bytes(&self, op: &Op, balance: &BalanceForGoods) -> String {
    // let b = vec![];
    // for batch in batches {
    //     b.push(batch.to_json());
    // }
    array![op.to_json(), balance.to_json()].dump()
  }

  fn from_bytes(&self, bytes: &[u8]) -> Result<(Op, BalanceForGoods), WHError> {
    let data = String::from_utf8_lossy(bytes).to_string();
    let array = json::parse(&data)?;

    if array.is_array() {
      let op = Op::from_json(array[0].clone())?;
      let balance = BalanceForGoods::from_json(array[1].clone())?;

      //   let mut batches = vec![];
      //   if array[2].is_array() {
      //       for b in array[2].members() {
      //         batches.push(Batch::from_json(b)?);
      //       }
      //   }

      Ok((op, balance))
    } else {
      Err(WHError::new("unexpected structure"))
    }
  }

  fn get_balances(
    &self,
    from_date: DateTime<Utc>,
    till_date: DateTime<Utc>,
    goods: &Vec<Goods>,
    checkpoints: HashMap<Uuid, BalanceForGoods>,
  ) -> Result<HashMap<Uuid, BalanceForGoods>, WHError> {
    let mut result = checkpoints.clone();

    // get operations between checkpoint date and requested date
    let ops = self.get_ops_for_many_goods(goods, from_date, till_date)?;

    for op in ops {
      result.entry(op.goods).and_modify(|bal| *bal += &op.op).or_insert(match &op.op {
        InternalOperation::Receive(q, c) => BalanceForGoods { qty: q.clone(), cost: c.clone() },
        InternalOperation::Issue(q, c, _) => BalanceForGoods { qty: -q.clone(), cost: -c.clone() },
      });
    }

    Ok(result)
  }

  fn get_balances_for_all(
    &self,
    from_date: DateTime<Utc>,
    till_date: DateTime<Utc>,
    checkpoints: HashMap<Store, HashMap<Goods, HashMap<Batch, BalanceForGoods>>>,
  ) -> Result<HashMap<Store, HashMap<Goods, HashMap<Batch, BalanceForGoods>>>, WHError> {
    let mut result = checkpoints;

    // get operations between checkpoint date and requested date
    let ops = self.get_ops_for_all(from_date, till_date)?;

    for op in ops {
      result
        .entry(op.store)
        .or_insert_with(|| HashMap::new())
        .entry(op.goods)
        .or_insert_with(|| HashMap::new())
        .entry(op.batch)
        .and_modify(|bal| *bal += &op.op)
        .or_insert_with(|| BalanceForGoods::default() + op.op);
    }

    // TODO remove zero balances

    Ok(result)
  }
}

pub trait CheckpointTopology {
  // fn cf_name(&self) -> &str;

  fn key(&self, op: &Op, date_of_checkpoint: DateTime<Utc>) -> Vec<u8>;
  fn key_checkpoint(&self, balance: &Balance, date_of_checkpoint: DateTime<Utc>) -> Vec<u8>;

  fn get_balance(&self, key: &Vec<u8>) -> Result<BalanceForGoods, WHError>;
  fn set_balance(&self, key: &Vec<u8>, balance: BalanceForGoods) -> Result<(), WHError>;
  fn del_balance(&self, key: &Vec<u8>) -> Result<(), WHError>;
  fn key_latest_checkpoint_date(&self) -> Vec<u8>;
  fn get_latest_checkpoint_date(&self) -> Result<DateTime<Utc>, WHError>;
  fn set_latest_checkpoint_date(&self, date: DateTime<Utc>) -> Result<(), WHError>;
  fn get_checkpoints_for_one_goods(
    &self,
    store: Store,
    goods: Goods,
    date: DateTime<Utc>,
  ) -> Result<Vec<Balance>, WHError>;

  fn get_checkpoint_for_goods_and_batch(
    &self,
    store: Store,
    goods: Goods,
    batch: &Batch,
    date: DateTime<Utc>,
  ) -> Result<Option<Balance>, WHError>;

  fn get_checkpoints_for_all(
    &self,
    date: DateTime<Utc>,
  ) -> Result<
    (DateTime<Utc>, HashMap<Store, HashMap<Goods, HashMap<Batch, BalanceForGoods>>>),
    WHError,
  >;

  fn get_checkpoints_for_many_goods(
    &self,
    date: DateTime<Utc>,
    goods: &Vec<Goods>,
  ) -> Result<(DateTime<Utc>, HashMap<Uuid, BalanceForGoods>), WHError>;

  fn checkpoint_update(&self, ops: Vec<OpMutation>) -> Result<(), WHError> {
    log::debug!("ops len: {}", ops.len());
    for op in ops {
      log::debug!("================================");
      log::debug!("checkpoint_update {:?} {:?} {:?} {:?}", op.store, op.goods, op.batch, op.after);
      let mut tmp_date = op.date;
      let mut check_point_date = op.date;
      let mut last_checkpoint_date = self.get_latest_checkpoint_date()?;

      if last_checkpoint_date <= op.date {
        let old_checkpoints =
          self.get_checkpoints_for_all_storages_before_date(last_checkpoint_date)?;

        last_checkpoint_date = first_day_next_month(op.date);

        for old_checkpoint in old_checkpoints.iter() {
          let mut new_checkpoint_date = first_day_next_month(old_checkpoint.date);
          while new_checkpoint_date <= last_checkpoint_date {
            let key = self.key_checkpoint(old_checkpoint, new_checkpoint_date);
            self.set_balance(&key, old_checkpoint.clone().number)?;
            new_checkpoint_date = first_day_next_month(new_checkpoint_date);
          }
        }
      }

      while check_point_date <= last_checkpoint_date {
        check_point_date = first_day_next_month(tmp_date);

        let key = self.key(&op.to_op(), check_point_date);

        let mut balance = self.get_balance(&key)?;
        log::debug!("balance on {check_point_date} before operation {balance:?}");
        balance += op.to_delta();
        log::debug!("dates: op {} last checkpoint {last_checkpoint_date}", op.date);
        log::debug!("balance after {:?} : {balance:?}", op.after);

        if balance.is_zero() {
          log::debug!("del_balance: {key:?}");
          self.del_balance(&key)?;
        } else {
          log::debug!("set_balance: {balance:?} {key:?}");
          self.set_balance(&key, balance)?;
        }
        tmp_date = check_point_date;

        if check_point_date == last_checkpoint_date {
          break;
        }
      }

      self.set_latest_checkpoint_date(check_point_date)?;
    }

    Ok(())
  }

  fn get_checkpoints_for_one_storage_before_date(
    &self,
    store: Store,
    date: DateTime<Utc>,
  ) -> Result<Vec<Balance>, WHError>;

  fn get_checkpoints_for_all_storages_before_date(
    &self,
    date: DateTime<Utc>,
  ) -> Result<Vec<Balance>, WHError>;
}

pub fn dt(date: &str) -> Result<DateTime<Utc>, WHError> {
  let res = DateTime::parse_from_rfc3339(format!("{date}T00:00:00Z").as_str())?.into();

  Ok(res)
}

pub(crate) fn first_day_current_month(date: DateTime<Utc>) -> DateTime<Utc> {
  let date = NaiveDate::from_ymd_opt(date.year(), date.month(), 1)
    .unwrap()
    .and_hms_opt(0, 0, 0)
    .unwrap();
  DateTime::<Utc>::from_utc(date, Utc)
}

pub(crate) fn first_day_next_month(date: DateTime<Utc>) -> DateTime<Utc> {
  let d = date.naive_local();
  let (year, month) = if d.month() == Month::December.number_from_month() {
    (d.year() + 1, Month::January.number_from_month())
  } else {
    (d.year(), d.month() + 1)
  };

  let date = NaiveDate::from_ymd_opt(year, month, 1).unwrap().and_hms_opt(0, 0, 0).unwrap();
  DateTime::<Utc>::from_utc(date, Utc)
}

pub(crate) fn min_batch() -> Vec<u8> {
  UUID_NIL
    .as_bytes()
    .iter()
    .chain(u64::MIN.to_be_bytes().iter())
    .chain(UUID_NIL.as_bytes().iter())
    .map(|b| *b)
    .collect()
}

pub(crate) fn max_batch() -> Vec<u8> {
  UUID_MAX
    .as_bytes()
    .iter()
    .chain(u64::MAX.to_be_bytes().iter())
    .chain(UUID_MAX.as_bytes().iter())
    .map(|b| *b)
    .collect()
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
  // TODO Inventory(Qty, Cost), // actual qty, calculated qty; calculated qty +/- delta = actual qty (delta qty, delta cost)
  Receive(Qty, Cost),     // FROM // TODO Option<Store>
  Issue(Qty, Cost, Mode), // INTO // TODO Option<Store>
}

impl ToJson for InternalOperation {
  fn to_json(&self) -> JsonValue {
    // JsonValue::String(serde_json::to_string(&self).unwrap_or_default())

    match self {
      InternalOperation::Receive(q, c) => {
        object! {
          type: JsonValue::String("Receive".to_string()),
          qty: q.to_json(),
          cost: c.to_json(),
        }
      },
      InternalOperation::Issue(q, c, m) => {
        object! {
          type: JsonValue::String("Issue".to_string()),
          qty: q.to_json(),
          cost: c.to_json(),
          mode: match m {
            Mode::Auto => JsonValue::String("Auto".to_string()),
            Mode::Manual => JsonValue::String("Manual".to_string()),
          }
        }
      },
    }
  }
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
            None => 0.into(), // TODO handle errors?
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
            None => 0.into(), // TODO handle errors?
          }
        }
      },
    }
  }
}

pub(crate) trait KeyValueStore {
  fn key(&self, s: &String) -> Result<Vec<u8>, WHError>;
  fn store_date_type_batch_id(&self) -> Vec<u8>;
  fn date_type_store_batch_id(&self) -> Vec<u8>;
  fn value(&self) -> Result<String, WHError>;
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Balance {
  // key
  pub date: DateTime<Utc>,
  pub store: Store,
  pub goods: Goods,
  pub batch: Batch,
  // value
  pub number: BalanceForGoods,
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
  fn zero_balance() -> Self {
    Balance {
      date: Default::default(),
      store: Default::default(),
      goods: Default::default(),
      batch: Batch { id: Default::default(), date: Default::default() },
      number: Default::default(),
    }
  }

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

  pub(crate) fn key(&self, s: &str) -> Result<Vec<u8>, WHError> {
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
pub struct Op {
  // key
  pub id: Uuid,
  pub date: DateTime<Utc>,
  pub store: Store, // TODO: from_store: Option<Store>
  pub goods: Goods,
  pub batch: Batch,

  pub store_into: Option<Store>, // TODO: to_store: Option<Store>

  // TODO operation_store = Receive > to_store, Issue > from_store
  // TODO contra_store = Receive > from_store, Issue > from_store

  // TODO not_allowed = from_store = None, to_store = None
  // TODO is_receive  = from_store = None, to_store = Some
  // TODO is_issue    = from_store = Some, to_store = None
  // TODO is_transfer = from_store = Some, to_store = Some

  // value
  pub op: InternalOperation, // TODO qty, cost, mode

  pub is_dependent: bool,
  pub batches: Vec<Batch>,
}

impl Op {
  pub(crate) fn qty(&self) -> Decimal {
    match self.op {
      InternalOperation::Receive(q, _) => q,
      InternalOperation::Issue(q, _, _) => q,
    }
  }

  pub(crate) fn cost(&self) -> Decimal {
    match self.op {
      InternalOperation::Receive(_, c) => c,
      InternalOperation::Issue(_, c, _) => c,
    }
  }

  pub(crate) fn from_json(data: JsonValue) -> Result<Self, WHError> {
    let op = &data["op"];

    let operation = match op["type"].as_str() {
      Some("Receive") => InternalOperation::Receive(op["qty"].number(), op["cost"].number()),
      Some("Issue") => {
        let mode = if op["mode"].as_str() == Some("Auto") { Mode::Auto } else { Mode::Manual };
        InternalOperation::Issue(op["qty"].number(), op["cost"].number(), mode)
      },
      _ => return Err(WHError::new(&format!("unknown operation type {}", op["type"]))),
    };

    let mut batches = vec![];

    match &data["batches"] {
      JsonValue::Array(array) => {
        for batch in array {
          batches.push(Batch { id: batch["id"].uuid()?, date: batch["date"].date_with_check()? });
        }
      },
      _ => (),
    }

    let op = Op {
      id: data["id"].uuid()?,
      date: data["date"].date_with_check()?,
      store: data["store"].uuid()?,
      goods: data["goods"].uuid()?,
      batch: Batch {
        id: data["batch"]["id"].uuid()?,
        date: data["batch"]["date"].date_with_check()?,
      },
      store_into: data["transfer"].uuid_or_none(),
      op: operation,
      is_dependent: data["is_dependent"].boolean(),
      batches,
    };
    Ok(op)
  }

  fn to_delta(&self) -> BalanceDelta {
    self.op.clone().into()
  }

  pub(crate) fn batch(&self) -> Vec<u8> {
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

  fn dependent(&self) -> Option<Op> {
    // if self.is_dependent {
    //   None
    // } else
    if let Some(store_into) = self.store_into {
      match &self.op {
        InternalOperation::Issue(q, c, m) => Some(Op {
          id: self.id,
          date: self.date,
          store: store_into,
          goods: self.goods,
          batch: self.batch.clone(),
          store_into: Some(self.store),
          op: InternalOperation::Receive(q.clone(), c.clone()),
          is_dependent: true,
          batches: vec![],
        }),
        _ => None,
      }
    } else {
      None
    }
  }

  fn is_zero(&self) -> bool {
    match &self.op {
      InternalOperation::Receive(q, c) => q.is_zero() && c.is_zero(),
      InternalOperation::Issue(q, c, _) => q.is_zero() && c.is_zero(),
    }
  }

  pub fn is_issue(&self) -> bool {
    match self.op {
      InternalOperation::Receive(_, _) => false,
      InternalOperation::Issue(_, _, _) => true,
    }
  }
}

impl ToJson for Op {
  fn to_json(&self) -> JsonValue {
    object! {
      id: self.id.to_json(),
      date: self.date.to_json(),
      store: self.store.to_json(),
      goods: self.goods.to_json(),
      batch: self.batch.to_json(),
      transfer: match self.store_into {
        Some(t) => t.to_json(),
        None => JsonValue::Null,
      },
      op: self.op.to_json(),
    }
  }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct OpMutation {
  // key
  id: Uuid,
  pub(crate) date: DateTime<Utc>,
  pub(crate) store: Store,
  transfer: Option<Store>,
  pub(crate) goods: Goods,
  pub(crate) batch: Batch,
  // value
  before: Option<InternalOperation>,
  after: Option<InternalOperation>,

  pub is_dependent: bool,
  pub batches: Vec<Batch>,
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
      is_dependent: false,
      batches: vec![],
    }
  }
}

impl OpMutation {
  pub fn new(
    id: Uuid,
    date: DateTime<Utc>,
    store: Store,
    transfer: Option<Store>,
    goods: Goods,
    batch: Batch,
    before: Option<InternalOperation>,
    after: Option<InternalOperation>,
  ) -> OpMutation {
    OpMutation {
      id,
      date,
      store,
      transfer,
      goods,
      batch,
      before,
      after,
      is_dependent: false,
      batches: vec![],
    }
  }

  pub fn receive_new(
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
      is_dependent: false,
      batches: vec![],
    }
  }

  fn value(&self) -> Result<String, WHError> {
    Ok(serde_json::to_string(&self)?)
  }

  pub fn to_op(&self) -> Op {
    if let Some(op) = self.after.as_ref() {
      Op {
        id: self.id.clone(),
        date: self.date.clone(),
        store: self.store.clone(),
        goods: self.goods.clone(),
        batch: self.batch.clone(),
        store_into: self.transfer.clone(),
        op: op.clone(),
        is_dependent: self.is_dependent,
        batches: self.batches.clone(),
      }
    } else {
      Op {
        id: self.id.clone(),
        date: self.date.clone(),
        store: self.store.clone(),
        goods: self.goods.clone(),
        batch: self.batch.clone(),
        store_into: self.transfer.clone(),
        op: if let Some(b) = self.before.clone() {
          b
        } else {
          InternalOperation::Receive(0.into(), 0.into())
        },
        is_dependent: self.is_dependent,
        batches: self.batches.clone(),
      }
    }
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
        transfer: a.store_into,
        goods: a.goods,
        batch: a.batch.clone(),
        before: Some(b.op.clone()),
        after: Some(a.op.clone()),
        is_dependent: a.is_dependent,
        batches: a.batches.clone(),
      }
    } else if let Some(b) = &before {
      OpMutation {
        id: b.id,
        date: b.date,
        store: b.store,
        transfer: b.store_into,
        goods: b.goods,
        batch: b.batch.clone(),
        before: Some(b.op.clone()),
        after: None,
        is_dependent: b.is_dependent,
        batches: b.batches.clone(),
      }
    } else if let Some(a) = &after {
      OpMutation {
        id: a.id,
        date: a.date,
        store: a.store,
        transfer: a.store_into,
        goods: a.goods,
        batch: a.batch.clone(),
        before: None,
        after: Some(a.op.clone()),
        is_dependent: a.is_dependent,
        batches: a.batches.clone(),
      }
    } else {
      OpMutation::default()
    }
  }

  fn dependent(&self) -> Option<OpMutation> {
    if let Some(transfer) = self.transfer {
      let before = match self.before.clone() {
        Some(b) => Some(b),
        None => None,
      };
      // TODO check if cost in operation already calculated - No!
      match self.after.as_ref() {
        Some(InternalOperation::Issue(q, c, _)) => Some(OpMutation {
          id: self.id,
          date: self.date,
          store: transfer,
          transfer: None,
          goods: self.goods,
          batch: self.batch.clone(),
          before,
          after: Some(InternalOperation::Receive(q.clone(), c.clone())),
          is_dependent: self.is_dependent,
          batches: self.batches.clone(),
        }),
        _ => None,
      }
    } else {
      None
    }
  }

  pub fn is_issue(&self) -> bool {
    match &self.after {
      Some(o) => match o {
        InternalOperation::Receive(_, _) => false,
        InternalOperation::Issue(_, _, _) => true,
      },
      None => false,
    }
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

#[derive(Clone, Debug, PartialEq)]
pub struct AgregationStoreGoods {
  // ключ
  pub store: Option<Store>,
  pub goods: Option<Goods>,
  pub batch: Option<Batch>,
  // агрегация
  pub open_balance: BalanceForGoods,
  pub receive: BalanceDelta,
  pub issue: BalanceDelta,
  pub close_balance: BalanceForGoods,
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

  fn is_zero(&self) -> bool {
    self.open_balance.is_zero()
      && self.receive.is_zero()
      && self.issue.is_zero()
      && self.close_balance.is_zero()
  }
}

impl ToJson for AgregationStoreGoods {
  fn to_json(&self) -> JsonValue {
    if let (Some(s), Some(g), Some(b)) = (self.store, self.goods, &self.batch) {
      object! {
        store: s.to_json(),
        goods: g.to_json(),
        batch: b.to_json(),
        open_balance: self.open_balance.to_json(),
        receive: self.receive.to_json(),
        issue: self.issue.to_json(),
        close_balance: self.close_balance.to_json(),
      }
    } else {
      JsonValue::Null
    }
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

#[derive(PartialEq, Debug, Clone)]
pub struct AgregationStore {
  // ключ (контекст)
  pub store: Option<Store>,
  // агрегация
  pub open_balance: Cost,
  pub receive: Cost,
  pub issue: Cost,
  pub close_balance: Cost,
}

impl ToJson for AgregationStore {
  fn to_json(&self) -> JsonValue {
    if let Some(s) = self.store {
      object! {
        store: s.to_json(),
        open_balance: self.open_balance.to_json(),
        receive: self.receive.to_json(),
        issue: self.issue.to_json(),
        close_balance: self.close_balance.to_json(),
      }
    } else {
      JsonValue::Null
    }
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
        self.issue -= cost;
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
            None => 0.into(), // TODO handle errors?
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

  // is it ok to make this with to_json() method?
  fn value(&self) -> Result<String, WHError> {
    Ok(self.to_json().dump())
  }

  fn date_type_store_batch_id(&self) -> Vec<u8> {
    todo!()
  }

  fn key(&self, s: &String) -> Result<Vec<u8>, WHError> {
    todo!()
  }
}

#[derive(Debug, PartialEq)]
pub struct Report {
  pub from_date: DateTime<Utc>,
  pub till_date: DateTime<Utc>,
  pub items: (AgregationStore, Vec<AgregationStoreGoods>),
}

impl ToJson for Report {
  fn to_json(&self) -> JsonValue {
    let mut arr = JsonValue::new_array();

    for agr in self.items.1.iter() {
      arr.push(agr.to_json());
    }

    object! {
      from_date: time_to_naive_string(self.from_date),
      till_date: time_to_naive_string(self.till_date),
      items: vec![self.items.0.to_json(), arr]
    }
  }
}

fn time_to_naive_string(time: DateTime<Utc>) -> String {
  let mut res = time.to_string();
  res.split_off(10);
  res
}

pub(crate) fn get_aggregations_for_one_goods(
  balances: Vec<Balance>,
  operations: Vec<Op>,
  start_date: DateTime<Utc>,
  end_date: DateTime<Utc>,
) -> Result<JsonValue, WHError> {
  let mut result: Vec<JsonValue> = vec![];

  let balance = match balances.len() {
    0 => Balance::zero_balance(),
    1 => balances[0].clone(),
    _ => unreachable!("Two or more balances for one goods and batch"),
  };

  result.push(object! {
    date: time_to_naive_string(start_date),
    type: JsonValue::String("open_balance".to_string()),
    _id: Uuid::new_v4().to_json(),
    qty: balance.number.qty.to_json(),
    cost: balance.number.cost.to_json(),
  });

  let mut open_balance = balance.number.clone();

  let mut close_balance = BalanceForGoods::default();

  let mut op_iter = operations.iter();

  while let Some(op) = op_iter.next() {
    if op.date < start_date {
      open_balance += op.to_delta();
    } else {
      close_balance += op.to_delta();
    }
    result.push(
      object! {
          date: op.date.to_json(),
          type: if op.is_issue() { JsonValue::String("issue".to_string()) } else { JsonValue::String("receive".to_string()) },
          _id: op.id.to_json(),
          qty: op.qty().to_json(),
          cost: op.cost().to_json(),
        }
    );
  }

  result[0]["qty"] = open_balance.qty.to_json();
  result[0]["cost"] = open_balance.cost.to_json();

  close_balance.qty += open_balance.qty;
  close_balance.cost += open_balance.cost;

  result.push(object! {
    date: time_to_naive_string(end_date),
    type: JsonValue::String("close_balance".to_string()),
    _id: Uuid::new_v4().to_json(),
    qty: close_balance.qty.to_json(),
    cost: close_balance.cost.to_json(),
  });

  Ok(JsonValue::Array(result))
}

pub(crate) fn new_get_aggregations(
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

  let key_for = |op: &Op| -> Vec<u8> { key(&op.store, &op.goods, &op.batch) };

  let mut aggregations = BTreeMap::new();
  let mut master_aggregation = AgregationStore::default();

  for balance in balances {
    aggregations.insert(
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
      aggregations
        .entry(key_for(&op))
        .or_insert(AgregationStoreGoods::default())
        .add_to_open_balance(&op);
    } else {
      *aggregations.entry(key_for(&op)).or_insert(AgregationStoreGoods::default()) += &op;
    }
  }

  let mut res = Vec::new();

  for (_, agr) in aggregations {
    if !agr.is_zero() {
      master_aggregation.apply_agregation(Some(&agr));
      res.push(agr);
    }
  }

  (master_aggregation, res)
}

pub fn receive_data(
  app: &(impl GetWarehouse + Services),
  time: DateTime<Utc>,
  data: JsonValue,
  ctx: &Vec<String>,
  before: JsonValue,
) -> Result<JsonValue, WHError> {
  // TODO if structure of input Json is invalid, should return it without changes and save it to memories anyway
  // If my data was corrupted, should rewrite it and do the operations
  // TODO tests with invalid structure of incoming JsonValue
  log::debug!("BEFOR: {:?}", before.dump());
  log::debug!("AFTER: {:?}", data.dump());

  let old_data = data.clone();
  let mut new_data = data.clone();
  let mut new_before = before.clone();

  let mut before = match json_to_ops(app, &mut new_before, time.clone(), ctx) {
    Ok(res) => res,
    Err(e) => {
      println!("_WHERROR_ BEFORE: {}", e.message());
      println!("{}", data.dump());
      return Ok(old_data);
    },
  };

  let mut after = match json_to_ops(app, &mut new_data, time, ctx) {
    Ok(res) => res,
    Err(e) => {
      println!("_WHERROR_ AFTER: {}", e.message());
      println!("{}", data.dump());
      return Ok(old_data);
    },
  };

  log::debug!("OPS BEFOR: {before:?}");
  log::debug!("OPS AFTER: {after:?}");

  let mut before = before.into_iter();

  let mut ops: Vec<OpMutation> = Vec::new();

  while let Some(ref b) = before.next() {
    if let Some(a) = after.remove_entry(&b.0) {
      if a.1.store == b.1.store
        && a.1.goods == b.1.goods
        && a.1.batch == b.1.batch
        && a.1.date == b.1.date
        && a.1.store_into == b.1.store_into
      {
        ops.push(OpMutation::new_from_ops(Some(b.1.clone()), Some(a.1)));
      } else {
        ops.push(OpMutation::new_from_ops(Some(b.1.clone()), None));
        ops.push(OpMutation::new_from_ops(None, Some(a.1)));
      }
    } else {
      ops.push(OpMutation::new_from_ops(Some(b.1.clone()), None));
    }
  }

  let mut after = after.into_iter();

  while let Some(ref a) = after.next() {
    ops.push(OpMutation::new_from_ops(None, Some(a.1.clone())));
  }

  log::debug!("OPS: {:?}", ops);

  if ops.is_empty() {
    Ok(old_data)
  } else {
    app.warehouse().mutate(&ops)?;
    Ok(new_data)
  }
}

fn json_to_ops(
  app: &impl Services,
  data: &mut JsonValue,
  time: DateTime<Utc>,
  ctx: &Vec<String>,
) -> Result<HashMap<String, Op>, WHError> {
  // log::debug!("json_to_ops {data:?}");

  let mut ops = HashMap::new();

  if !data.is_object() {
    return Ok(ops);
  }

  if ctx.get(0) != Some(&"warehouse".to_string()) || ctx.len() != 2 {
    return Ok(ops);
  }

  let type_of_operation = match ctx.get(1) {
    Some(str) => str.clone(),
    _ => return Ok(ops),
  };

  let doc_ctx = vec!["warehouse", &type_of_operation, "document"];

  let oid = app.service("companies").find(object! {limit: 1, skip: 0})?;
  // log::debug!("OID: {:?}", oid["data"][0]["_id"]);

  let oid = oid["data"][0]["_id"].as_str().unwrap_or_default();

  let params = object! {oid: oid, ctx: doc_ctx, enrich: false };
  let document = match app.service("memories").get(data["document"].string(), params) {
    Ok(d) => d,
    Err(_) => return Ok(ops), // TODO handle IO error differently!!!!
  };

  log::debug!("DOCUMENT: {:?}", document.dump());

  let date = match document["date"].date_with_check() {
    Ok(d) => d,
    Err(_) => return Ok(ops),
  };

  let (store_from, store_into) = if type_of_operation == "transfer" {
    let store_from = if data["storage_from"].string() == "" {
      match resolve_store(app, oid, &document, "from") {
        Ok(uuid) => uuid,
        Err(_) => return Ok(ops), // TODO handle errors better, allow to catch only 'not found'
      }
    } else {
      match resolve_store(app, oid, &data, "storage_from") {
        Ok(uuid) => uuid,
        Err(_) => return Ok(ops), // TODO handle errors better, allow to catch only 'not found'
      }
    };

    let store_into = if data["storage_into"].string() == "" {
      match resolve_store(app, oid, &document, "into") {
        Ok(uuid) => uuid,
        Err(_) => return Ok(ops), // TODO handle errors better, allow to catch only 'not found'
      }
    } else {
      match resolve_store(app, oid, &data, "storage_into") {
        Ok(uuid) => uuid,
        Err(_) => return Ok(ops), // TODO handle errors better, allow to catch only 'not found'
      }
    };

    (store_from, Some(store_into))
  } else {
    let store_from = match resolve_store(app, oid, &document, "storage") {
      Ok(uuid) => uuid,
      Err(_) => return Ok(ops), // TODO handle errors better, allow to catch only 'not found'
    };
    (store_from, None)
  };

  println!("store_from: {store_from:?} store_into: {store_into:?}");

  let params = object! {oid: oid, ctx: vec!["goods"] };
  let item = match app.service("memories").get(data["goods"].string(), params) {
    Ok(d) => d,
    Err(_) => return Ok(ops), // TODO handle IO error differently!!!!
  };

  let goods = match item["_uuid"].uuid_or_none() {
    Some(uuid) => uuid,
    None => return Ok(ops),
  };

  // log::debug!("before op");

  let op = match type_of_operation.as_str() {
    "receive" | "inventory" => {
      let qty = data["qty"]["number"].number_or_none();
      let cost = data["cost"]["number"].number_or_none();

      if qty.is_none() && cost.is_none() {
        return Ok(ops);
      } else {
        InternalOperation::Receive(qty.unwrap_or_default(), cost.unwrap_or_default())
      }
    },
    "transfer" | "dispatch" => {
      let qty = data["qty"]["number"].number_or_none();
      let cost = data["cost"]["number"].number_or_none();

      if qty.is_none() && cost.is_none() {
        return Ok(ops);
      } else {
        let (cost, mode) =
          if let Some(cost) = cost { (cost, Mode::Manual) } else { (0.into(), Mode::Auto) };
        InternalOperation::Issue(qty.unwrap_or_default(), cost, mode)
      }
    },
    _ => return Ok(ops),
  };

  log::debug!("after op {op:?}");

  let tid = if let Some(tid) = data["_uuid"].uuid_or_none() {
    tid
  } else {
    let tid = Uuid::new_v4();
    data["_uuid"] = JsonValue::String(tid.to_string());
    tid
  };

  let batch = if type_of_operation == "receive" {
    Batch { id: tid, date }
  } else if type_of_operation == "inventory" {
    // TODO try to read batch from document
    Batch { id: tid, date }
  } else {
    match &data["batch"] {
      JsonValue::Object(d) => {
        // let params = object! {oid: oid["data"][0]["_id"].as_str(), ctx: vec!["warehouse", "receive", "document"] };
        // let doc_from = app.service("memories").get(d["_id"].string(), params)?;
        Batch { id: data["batch"]["_uuid"].uuid()?, date: data["batch"]["date"].date_with_check()? }
      },
      _ => Batch { id: UUID_NIL, date: dt("1970-01-01")? },
    }
  };

  data["batch"] = object! {
    "_uuid": batch.id.to_string(),
    "date": time_to_string(batch.date),
    "barcode": batch.to_barcode(),
  };

  let mut batches = vec![];

  match &data["batches"] {
    JsonValue::Array(array) => {
      for batch in array {
        batches.push(Batch { id: batch["id"].uuid()?, date: batch["date"].date_with_check()? });
      }
    },
    _ => (),
  }

  let op = Op {
    id: tid,
    date,
    store: store_from,
    store_into,
    goods,
    batch,
    op,
    is_dependent: data["is_dependent"].boolean(),
    batches: batches.clone(),
  };

  ops.insert(tid.to_string(), op);

  Ok(ops)
}

fn resolve_store(
  app: &impl Services,
  oid: &str,
  document: &JsonValue,
  name: &str,
) -> Result<Uuid, service::error::Error> {
  let store_id = document[name].string();

  log::debug!("store_id {name} {store_id:?}");

  let params = object! {oid: oid, ctx: vec!["warehouse", "storage"] };
  let storage = app.service("memories").get(store_id, params)?;
  log::debug!("storage {:?}", storage.dump());
  storage["_uuid"].uuid()
}
