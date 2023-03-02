use json::{object, JsonValue};
use rust_decimal::{prelude::ToPrimitive, Decimal};
use serde::{Deserialize, Serialize};
use std::ops::Neg;
use std::path::Path;
use std::{
  collections::{BTreeMap, HashMap},
  num,
  ops::{Add, AddAssign, Sub, SubAssign},
  str::FromStr,
  sync::Arc,
};

use super::{Cost, InternalOperation, Op, Qty, ToJson};

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct BalanceForGoods {
  pub qty: Qty,
  pub cost: Cost,
}

impl BalanceForGoods {
  pub fn is_zero(&self) -> bool {
    self.qty.is_zero() && self.cost.is_zero()
  }

  pub fn get_new_balance(&self, op: &Op) -> BalanceForGoods {
    match op.op {
      InternalOperation::Receive(q, c) => BalanceForGoods { qty: self.qty + q, cost: self.cost + c },
      InternalOperation::Issue(q, c, _) => {
        BalanceForGoods { qty: self.qty - q, cost: self.cost - c }
      },
    }
  }

  pub fn delta(&self, other: &BalanceForGoods) -> Option<BalanceDelta> {
    if self != other {
      Some(BalanceDelta { qty: other.qty - self.qty, cost: other.cost - self.cost })
    } else {
      None
    }
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

// impl Neg for BalanceForGoods {
//   type Output = BalanceForGoods;
//   fn neg(self) -> Self::Output {
//     self.qty = -self.qty;
//     self.qty = -self.cost;
//     self
//   }
// }

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

// impl SubAssign<BalanceDelta> for BalanceForGoods {
//   fn sub_assign(&mut self, rhs: BalanceDelta) {
//     self.qty -= rhs.qty;
//     self.cost -= rhs.cost;
//   }
// }

// impl Add for BalanceForGoods {
//   type Output = BalanceDelta;

//   fn add(self, rhs: Self) -> Self::Output {
//     BalanceDelta {
//       qty: self.qty + rhs.qty,
//       cost: self.cost + rhs.cost,
//     }
//   }
// }

// impl Sub for BalanceForGoods {
//   type Output = BalanceDelta;

//   fn sub(self, rhs: Self) -> Self::Output {
//     BalanceDelta { qty: self.qty - rhs.qty, cost: self.cost - rhs.cost }
//   }
// }

#[derive(Default, Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct BalanceDelta {
  pub qty: Qty,
  pub cost: Cost,
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