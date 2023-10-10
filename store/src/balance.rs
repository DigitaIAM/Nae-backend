use super::{elements::ToJson, error::WHError};
use std::cmp::Ordering;

use chrono::{DateTime, Utc};
use json::{object, JsonValue};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use std::ops::{Add, AddAssign, Deref, Neg, Sub, SubAssign};
use std::str::FromStr;

use crate::batch::Batch;
use crate::elements::{Goods, Mode, Store, UUID_NIL};
use crate::operations::{InternalOperation, OpMutation};
use crate::qty::{Number, Qty, QtyDelta, Uom};
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
        Price((self.0 / lower).round_dp(5).into(), name.clone())
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

impl AddAssign<QtyDelta> for BalanceForGoods {
  fn add_assign(&mut self, rhs: QtyDelta) {
    let mut positive = vec![];
    let mut negative = vec![];
    if let Some(before) = rhs.before {
      match before {
        InternalOperation::Inventory(_, _, _) => todo!(),
        InternalOperation::Receive(q, c) => negative.push((q, c)),
        InternalOperation::Issue(q, c, _) => positive.push((q, c)),
      }
    }
    if let Some(after) = rhs.after {
      match after {
        InternalOperation::Inventory(_, _, _) => todo!(),
        InternalOperation::Receive(q, c) => positive.push((q, c)),
        InternalOperation::Issue(q, c, _) => negative.push((q, c)),
      }
    }

    let mut ls = self.qty.inner.clone();

    for (q, c) in negative {
      // println!("negative qty {q:?}");
      let mut rs = q.inner;

      loop {
        if rs.is_empty() {
          break;
        }

        if ls.is_empty() {
          ls = rs.into_iter().map(|n| -n).collect();

          break;
        }

        let mut r = rs.remove(0);
        // println!("right {r:?}");
        for i in 0..ls.len() {
          if r.is_zero() {
            break;
          }
          // let l = &ls[i];
          let l = ls[i].clone();
          // println!("left {l:?}");
          if &l.name == &r.name {
            ls.remove(i);
            let product = l.number() - r.number();
            log::debug!("ADD_ASSIGN: {:?} - {:?} = {:?}", l.number(), r.number(), product);
            r.number = Decimal::ZERO;
            if product.is_sign_positive() {
              ls.push(Number::new_named(product, l.name.clone()));
            } else if product.is_sign_negative() {
              rs.push(Number::new_named(-product, l.name.clone()));
            }
          }
        }

        if r.is_zero() {
          continue;
        }

        let mut named: Vec<(usize, Uom)> = ls
          .iter()
          .enumerate()
          .map(|(i, l)| (i, l.common(&r)))
          // .map(|(i, l)| {
          //   if let Some(common) = l.common(&r) {
          //     (i, Some(common))
          //   } else if l.base() == r.base() {
          //     (i, Some(l.base()))
          //   } else {
          //     (i, None)
          //   }
          // })
          .filter(|(_, l)| l.is_some())
          .map(|(i, l)| (i, l.unwrap()))
          .collect();

        named.sort_by(|(li, ln), (ri, rn)| {
          let l_depth = ln.depth();
          let r_depth = rn.depth();

          if r_depth == l_depth {
            if let (Some(l_number), Some(r_number)) = (ln.number(), rn.number()) {
              r_number.cmp(&l_number)
            } else {
              Ordering::Equal
            }
          } else {
            r_depth.cmp(&l_depth)
          }
        });

        if named.is_empty() {
          ls.push(-r);
        } else {
          let (i, common) = named.remove(0); // TODO can be

          let l = ls.remove(i);

          let ll = l.lowering(&common).unwrap();
          let rl = r.lowering(&common).unwrap();

          let product = ll.number() - rl.number();
          log::debug!("ADD_ASSIGN: {:?} - {:?} = {:?}", ll.number(), rl.number(), product);
          r.number = Decimal::ZERO;
          if product.is_sign_positive() {
            ls.append(&mut Number::new_named(product, l.name.clone()).elevate_to_uom(&l.name).inner);
          } else if product.is_sign_negative() {
            rs.append(
              &mut Number::new_named(-product, l.name.clone()).elevate_to_uom(&l.name).inner,
            );
          }
        }
      }
      self.qty = Qty::new(ls.clone());
      self.cost -= &c;
    }

    for (q, c) in positive {
      self.qty += &q;
      self.cost += &c;
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

// impl AddAssign<&InternalOperation> for BalanceForGoods {
//   fn add_assign(&mut self, rhs: &InternalOperation) {
//     match rhs {
//       InternalOperation::Inventory(_, d, mode) => {
//         self.qty += d.qty;
//         self.cost +=
//           if mode == &Mode::Manual { d.cost } else { self.cost.price(self.qty).cost(d.qty) }
//       },
//       InternalOperation::Receive(qty, cost) => {
//         self.qty += qty;
//         self.cost += cost;
//       },
//       InternalOperation::Issue(qty, cost, mode) => {
//         self.qty -= qty;
//         self.cost -= if mode == &Mode::Manual { *cost } else { self.cost.price(self.qty).cost(*qty) }
//       },
//     }
//   }
// }

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

  pub(crate) fn to_delta(&self, rhs: &Self) -> BalanceDelta {
    BalanceDelta { qty: self.qty.to_delta(&rhs.qty), cost: rhs.cost - self.cost }
  }

  // pub(crate) fn relax(&self, balance: &Qty) -> Self {
  //   BalanceDelta { qty: self.qty.relax(balance), cost: self.cost }
  // }
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

#[cfg(test)]
mod tests {
  use crate::balance::{BalanceForGoods, Cost};
  use crate::operations::InternalOperation;
  use crate::qty::{Number, Qty, QtyDelta};
  use rust_decimal::Decimal;
  use uuid::Uuid;

  fn balance_plus_delta(balance: BalanceForGoods, delta: QtyDelta, check: BalanceForGoods) {
    let mut result = balance.clone();

    result += delta;
    println!("balance_plus_delta {result:?}");

    assert_eq!(check, result);
  }
  #[test]
  fn add_QtyDelta_to_BalanceForGoods() {
    let uom0 = Uuid::new_v4();
    let uom1 = Uuid::new_v4();
    let uom2 = Uuid::new_v4();

    // 3 + (3 d 1) = 1
    balance_plus_delta(
      BalanceForGoods {
        qty: Qty::new(vec![Number::new(Decimal::from(3), uom1, None)]),
        cost: Cost::from(Decimal::from(3)),
      },
      QtyDelta {
        before: Some(InternalOperation::Receive(
          Qty::new(vec![Number::new(Decimal::from(3), uom1, None)]),
          Cost::from(Decimal::from(3)),
        )),
        after: Some(InternalOperation::Receive(
          Qty::new(vec![Number::new(Decimal::from(1), uom1, None)]),
          Cost::from(Decimal::from(1)),
        )),
      },
      BalanceForGoods {
        qty: Qty::new(vec![Number::new(Decimal::from(1), uom1, None)]),
        cost: Cost::from(Decimal::from(1)),
      },
    );

    // 1 + (2 d 0) = -1
    balance_plus_delta(
      BalanceForGoods {
        qty: Qty::new(vec![Number::new(Decimal::from(1), uom1, None)]),
        cost: Cost::from(Decimal::from(1)),
      },
      QtyDelta {
        before: Some(InternalOperation::Receive(
          Qty::new(vec![Number::new(Decimal::from(2), uom1, None)]),
          Cost::from(Decimal::from(2)),
        )),
        after: Some(InternalOperation::Receive(
          Qty::new(vec![Number::new(Decimal::from(0), uom1, None)]),
          Cost::from(Decimal::from(0)),
        )),
      },
      BalanceForGoods {
        qty: Qty::new(vec![Number::new(Decimal::from(-1), uom1, None)]),
        cost: Cost::from(Decimal::from(-1)),
      },
    );

    // 2 + (2 d -3) = -3
    balance_plus_delta(
      BalanceForGoods {
        qty: Qty::new(vec![Number::new(Decimal::from(2), uom1, None)]),
        cost: Cost::from(Decimal::from(2)),
      },
      QtyDelta {
        before: Some(InternalOperation::Receive(
          Qty::new(vec![Number::new(Decimal::from(2), uom1, None)]),
          Cost::from(Decimal::from(2)),
        )),
        after: Some(InternalOperation::Receive(
          Qty::new(vec![Number::new(Decimal::from(-3), uom1, None)]),
          Cost::from(Decimal::from(-3)),
        )),
      },
      BalanceForGoods {
        qty: Qty::new(vec![Number::new(Decimal::from(-3), uom1, None)]),
        cost: Cost::from(Decimal::from(-3)),
      },
    );

    // 1 + 0 = 1
    balance_plus_delta(
      BalanceForGoods {
        qty: Qty::new(vec![Number::new(Decimal::from(1), uom1, None)]),
        cost: Cost::from(Decimal::from(1)),
      },
      QtyDelta {
        before: Some(InternalOperation::Receive(
          Qty::new(vec![Number::new(Decimal::from(0), uom1, None)]),
          Cost::from(Decimal::from(0)),
        )),
        after: Some(InternalOperation::Receive(
          Qty::new(vec![Number::new(Decimal::from(0), uom1, None)]),
          Cost::from(Decimal::from(0)),
        )),
      },
      BalanceForGoods {
        qty: Qty::new(vec![Number::new(Decimal::from(1), uom1, None)]),
        cost: Cost::from(Decimal::from(1)),
      },
    );

    // (1 0f 3) + ((1 of 3) d (1 of 4)) = (1 of 4)
    balance_plus_delta(
      BalanceForGoods {
        qty: Qty::new(vec![Number::new(
          Decimal::from(1),
          uom0,
          Some(Box::new(Number::new(Decimal::from(3), uom1, None))),
        )]),
        cost: Cost::from(Decimal::from(3)),
      },
      QtyDelta {
        before: Some(InternalOperation::Receive(
          Qty::new(vec![Number::new(
            Decimal::from(1),
            uom0,
            Some(Box::new(Number::new(Decimal::from(3), uom1, None))),
          )]),
          Cost::from(Decimal::from(3)),
        )),
        after: Some(InternalOperation::Receive(
          Qty::new(vec![Number::new(
            Decimal::from(1),
            uom0,
            Some(Box::new(Number::new(Decimal::from(4), uom1, None))),
          )]),
          Cost::from(Decimal::from(4)),
        )),
      },
      BalanceForGoods {
        qty: Qty::new(vec![Number::new(
          Decimal::from(1),
          uom0,
          Some(Box::new(Number::new(Decimal::from(4), uom1, None))),
        )]),
        cost: Cost::from(Decimal::from(4)),
      },
    );

    // s1 receive (1 of 3)
    // s1 -> s2 transfer 3
    // change s1 receive from (1 of 3) to (1 of 4)

    // s1 (0) + ((1 of 3) d (1 of 4)) = (1 of 4)
    balance_plus_delta(
      BalanceForGoods { qty: Qty::new(vec![]), cost: Cost::from(Decimal::from(0)) },
      QtyDelta {
        before: Some(InternalOperation::Receive(
          Qty::new(vec![Number::new(
            Decimal::from(-1),
            uom0,
            Some(Box::new(Number::new(Decimal::from(3), uom1, None))),
          )]),
          Cost::from(Decimal::from(3)),
        )),
        after: Some(InternalOperation::Receive(
          Qty::new(vec![Number::new(
            Decimal::from(1),
            uom0,
            Some(Box::new(Number::new(Decimal::from(4), uom1, None))),
          )]),
          Cost::from(Decimal::from(4)),
        )),
      },
      BalanceForGoods {
        qty: Qty::new(vec![Number::new(
          Decimal::from(1),
          uom0,
          Some(Box::new(Number::new(Decimal::from(4), uom1, None))),
        )]),
        cost: Cost::from(Decimal::from(4)),
      },
    );
  }
}
