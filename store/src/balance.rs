use super::{
  elements::{Cost, Qty, ToJson},
  error::WHError,
};

use chrono::{DateTime, Utc};
use json::{object, JsonValue};
use serde::{Deserialize, Serialize};
use std::ops::{Add, AddAssign, Sub};

use crate::batch::Batch;
use crate::elements::{Goods, Mode, Store};
use crate::operations::{InternalOperation, OpMutation};
use service::utils::json::JsonParams;

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct BalanceForGoods {
  pub qty: Qty,
  pub cost: Cost,
}

impl BalanceForGoods {
  pub fn is_zero(&self) -> bool {
    self.qty.is_zero() && self.cost.is_zero()
  }

  pub fn delta(&self, other: &BalanceForGoods) -> BalanceDelta {
    BalanceDelta { qty: other.qty - self.qty, cost: other.cost - self.cost }
  }

  pub(crate) fn from_json(data: JsonValue) -> Result<Self, WHError> {
    Ok(BalanceForGoods { qty: data["qty"].number(), cost: data["cost"].number() })
  }
}

impl ToJson for BalanceForGoods {
  fn to_json(&self) -> JsonValue {
    object! {
      qty: self.qty.to_string(),
      cost: self.cost.to_string(),
    }
  }
}

impl AddAssign<BalanceDelta> for BalanceForGoods {
  fn add_assign(&mut self, rhs: BalanceDelta) {
    self.qty += rhs.qty;
    self.cost += rhs.cost;
  }
}

impl Add<BalanceDelta> for BalanceForGoods {
  type Output = Self;

  fn add(self, rhs: BalanceDelta) -> Self::Output {
    BalanceForGoods { qty: self.qty + rhs.qty, cost: self.cost + rhs.cost }
  }
}

impl AddAssign for BalanceForGoods {
  fn add_assign(&mut self, rhs: Self) {
    self.qty += rhs.qty;
    self.cost += rhs.cost;
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

#[derive(Default, Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct BalanceDelta {
  pub qty: Qty,
  pub cost: Cost,
}
impl BalanceDelta {
  pub(crate) fn is_zero(&self) -> bool {
    self.qty.is_zero() && self.cost.is_zero()
  }
}

impl ToJson for BalanceDelta {
  fn to_json(&self) -> JsonValue {
    object! {
      qty: self.qty.to_string(),
      cost: self.cost.to_string(),
    }
  }
}

impl AddAssign<Self> for BalanceDelta {
  fn add_assign(&mut self, rhs: Self) {
    self.qty += rhs.qty;
    self.cost += rhs.cost;
  }
}

impl Sub for BalanceDelta {
  type Output = BalanceDelta;

  fn sub(self, rhs: Self) -> Self::Output {
    BalanceDelta { qty: self.qty - rhs.qty, cost: self.cost - rhs.cost }
  }
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
  pub(crate) fn zero_balance() -> Self {
    Balance {
      date: Default::default(),
      store: Default::default(),
      goods: Default::default(),
      batch: Batch { id: Default::default(), date: Default::default() },
      number: Default::default(),
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
