use super::{elements::ToJson, error::WHError};

use chrono::{DateTime, Utc};
use json::{object, JsonValue};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use std::ops::{Add, AddAssign, Neg, Sub, SubAssign};
use std::str::FromStr;

use crate::batch::Batch;
use crate::elements::{Goods, Mode, Store, UUID_NIL};
use crate::operations::{InternalOperation, OpMutation};
use crate::qty::{Qty, Uom};
use service::utils::json::JsonParams;

#[derive(Debug, Clone, PartialEq)]
pub struct Price(Decimal, Uom);

impl Price {
  pub const ZERO: Price = Price(Decimal::ZERO, Uom::In(UUID_NIL, None));
  pub const ERROR: Price = Price(Decimal::NEGATIVE_ONE, Uom::In(UUID_NIL, None));

  pub fn number(&self) -> Decimal {
    self.0
  }

  pub fn uom(&self) -> Uom {
    self.1.clone()
  }

  // pub fn cost(&self, qty: Qty, name: &Uom) -> Cost {
  //   if let Some(lower) = qty.lowering(name) {
  //     (lower * self.0).round_dp(2).into()
  //   } else {
  //     Cost::ERROR
  //   }
  // }
}

impl From<Price> for Decimal {
  fn from(val: Price) -> Self {
    val.0
  }
}

// impl From<Decimal> for Price {
//   fn from(number: Decimal) -> Self {
//     Price(number)
//   }
// }

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Cost(Decimal);

impl Cost {
  pub const ZERO: Cost = Cost(Decimal::ZERO);
  pub const ERROR: Cost = Cost(Decimal::NEGATIVE_ONE);

  pub fn price(&self, qty: &Qty, name: &Uom) -> Price {
    if qty.is_zero() {
      Price::ZERO
    } else {
      if let Some(lower) = qty.lowering(name) {
        log::debug!("_qty {qty:?}\n_lower {lower:?}");
        Price((self.0 / lower.number).round_dp(5).into(), name.clone())
      } else {
        Price::ERROR
      }
    }
  }

  pub const fn is_zero(&self) -> bool {
    self.0.is_zero()
  }
}

impl ToJson for Cost {
  fn to_json(&self) -> JsonValue {
    self.0.to_json()
  }
}

impl From<Cost> for Decimal {
  fn from(val: Cost) -> Self {
    val.0
  }
}

impl From<i32> for Cost {
  fn from(number: i32) -> Self {
    Cost(number.into())
  }
}

impl TryFrom<&str> for Cost {
  type Error = rust_decimal::Error;

  fn try_from(number: &str) -> Result<Self, Self::Error> {
    Ok(Cost(Decimal::from_str(number)?))
  }
}

impl From<Decimal> for Cost {
  fn from(number: Decimal) -> Self {
    Cost(number)
  }
}

impl AddAssign<Cost> for Cost {
  fn add_assign(&mut self, rhs: Cost) {
    self.0 += rhs.0
  }
}

impl AddAssign<&Cost> for Cost {
  fn add_assign(&mut self, rhs: &Cost) {
    self.0 += rhs.0
  }
}

impl SubAssign<Cost> for Cost {
  fn sub_assign(&mut self, rhs: Cost) {
    self.0 -= rhs.0
  }
}

impl SubAssign<&Cost> for Cost {
  fn sub_assign(&mut self, rhs: &Cost) {
    self.0 -= rhs.0
  }
}

impl Add<Cost> for Cost {
  type Output = Cost;

  fn add(self, rhs: Cost) -> Self::Output {
    (self.0 + rhs.0).into()
  }
}

impl Sub<Cost> for Cost {
  type Output = Cost;

  fn sub(self, rhs: Cost) -> Self::Output {
    (self.0 - rhs.0).into()
  }
}

impl Neg for Cost {
  type Output = Cost;

  fn neg(self) -> Self::Output {
    (-self.0).into()
  }
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct BalanceForGoods {
  pub qty: Qty,
  pub cost: Cost,
}

impl BalanceForGoods {
  pub fn price(&self, name: &Uom) -> Price {
    self.cost.price(&self.qty, name)
  }

  pub fn is_zero(&self) -> bool {
    self.qty.is_zero() && self.cost.is_zero()
  }

  pub fn delta(&self, other: &BalanceForGoods) -> BalanceDelta {
    BalanceDelta { qty: &other.qty - &self.qty, cost: other.cost - self.cost }
  }

  pub(crate) fn from_json(data: JsonValue) -> Result<Self, WHError> {
    let qty = data["qty"].clone().try_into()?;
    Ok(BalanceForGoods { qty, cost: data["cost"].number().into() })
  }

  pub fn apply(&mut self, op: &InternalOperation) {
    match op {
      InternalOperation::Inventory(b, d, ..) => {
        self.qty += &d.qty;
        self.cost += d.cost;
        assert_eq!(b, self);
      },
      InternalOperation::Receive(qty, cost) => {
        self.qty += qty;
        self.cost += cost;
      },
      InternalOperation::Issue(qty, cost, _mode) => {
        self.qty -= qty;
        self.cost -= cost;
      },
    }
  }
}

impl ToJson for BalanceForGoods {
  fn to_json(&self) -> JsonValue {
    let qty: JsonValue = (&self.qty).into();
    object! {
      qty: qty,
      cost: self.cost.to_json(),
    }
  }
}

impl AddAssign<BalanceDelta> for BalanceForGoods {
  fn add_assign(&mut self, rhs: BalanceDelta) {
    self.qty += &rhs.qty;
    self.cost += rhs.cost;
  }
}

impl Add<BalanceDelta> for BalanceForGoods {
  type Output = Self;

  fn add(self, rhs: BalanceDelta) -> Self::Output {
    BalanceForGoods { qty: &self.qty + &rhs.qty, cost: self.cost + rhs.cost }
  }
}

impl AddAssign for BalanceForGoods {
  fn add_assign(&mut self, rhs: Self) {
    self.qty += &rhs.qty;
    self.cost += rhs.cost;
  }
}

impl Add<InternalOperation> for BalanceForGoods {
  type Output = BalanceForGoods;

  fn add(mut self, rhs: InternalOperation) -> Self::Output {
    match rhs {
      InternalOperation::Inventory(_, d, mode) => {
        self.qty += &d.qty;
        self.cost += if mode == Mode::Manual { d.cost } else { d.qty.cost(&self) }
      },
      InternalOperation::Receive(qty, cost) => {
        self.qty += &qty;
        self.cost += cost;
      },
      InternalOperation::Issue(qty, cost, mode) => {
        self.qty -= &qty;
        self.cost -= if mode == Mode::Manual { cost } else { qty.cost(&self) }
      },
    }
    self
  }
}

#[derive(Default, Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct BalanceDelta {
  pub qty: Qty,
  pub cost: Cost,
}

impl BalanceDelta {
  pub(crate) fn is_zero(&self) -> bool {
    self.qty.is_zero() && self.cost.is_zero()
  }

  pub(crate) fn new(qty: &Qty, cost: &Cost) -> Self {
    BalanceDelta { qty: qty.clone(), cost: cost.clone() }
  }
}

impl ToJson for BalanceDelta {
  fn to_json(&self) -> JsonValue {
    object! {
      qty: Into::<JsonValue>::into(&self.qty),
      cost: self.cost.to_json(),
    }
  }
}

impl AddAssign<Self> for BalanceDelta {
  fn add_assign(&mut self, rhs: Self) {
    self.qty += &rhs.qty;
    self.cost += rhs.cost;
  }
}

impl Sub for BalanceDelta {
  type Output = BalanceDelta;

  fn sub(self, rhs: Self) -> Self::Output {
    BalanceDelta { qty: &self.qty - &rhs.qty, cost: self.cost - rhs.cost }
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
    if let Some((o, _)) = &rhs.after {
      self.number.apply(o);
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
      .copied()
      .collect()
  }
}
