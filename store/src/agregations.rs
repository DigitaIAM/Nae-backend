use crate::balance::{Balance, BalanceDelta, BalanceForGoods};
use crate::batch::Batch;
use crate::elements::{
  time_to_naive_string, Cost, Goods, KeyValueStore, Mode, ReturnType, Store, ToJson, WHError,
};
use crate::operations::{InternalOperation, Op, OpMutation};
use chrono::{DateTime, Utc};
use json::{object, JsonValue};
use std::collections::BTreeMap;
use std::ops::AddAssign;
use uuid::Uuid;

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

  fn apply_agregation(&mut self, agr: Option<&AgregationStoreGoods>) {
    if let Some(agr) = agr {
      self.store = agr.store;
      self.open_balance += agr.open_balance.cost;
      self.receive += agr.receive.cost;
      self.issue += agr.issue.cost;
      self.close_balance += agr.close_balance.cost;
    }
  }

  fn balance(&mut self, balance: Option<&Balance>) -> ReturnType {
    todo!()
  }

  fn is_applyable_for(&self, op: &OpMutation) -> bool {
    todo!()
  }
}

impl Agregation for AgregationStoreGoods {
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
}

impl KeyValueStore for AgregationStoreGoods {
  fn key(&self, s: &String) -> Result<Vec<u8>, WHError> {
    todo!()
  }

  fn store_date_type_batch_id(&self) -> Vec<u8> {
    todo!()
  }

  fn date_type_store_batch_id(&self) -> Vec<u8> {
    todo!()
  }

  // is it ok to make this with to_json() method?
  fn value(&self) -> Result<String, WHError> {
    Ok(self.to_json().dump())
  }
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
