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

use super::{Cost, Qty};

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct BalanceForGoods {
  pub qty: Qty,
  pub cost: Cost,
}

impl BalanceForGoods {
  pub fn is_zero(&self) -> bool {
    self.qty.is_zero() && self.cost.is_zero()
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
    BalanceForGoods {
      qty: self.qty + rhs.qty,
      cost: self.cost + rhs.cost,
    }
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