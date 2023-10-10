use crate::balance::{BalanceDelta, BalanceForGoods, Cost, Price};
use crate::elements::{ToJson, UUID_NIL};
use crate::error::WHError;
use crate::operations::InternalOperation;
use actix_web::body::MessageBody;
use json::{object, JsonValue};
use rust_decimal::prelude::{ToPrimitive, Zero};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use service::utils::json::JsonParams;
use std::collections::VecDeque;
use std::ops::{Add, AddAssign, Deref, Div, Mul, Neg, Sub, SubAssign};
use uuid::Uuid;

#[derive(Clone, Debug, PartialOrd, Eq, Hash, Serialize, Deserialize)]
pub enum Uom {
  In(Uuid, Option<Box<Number<Uom>>>),
}

#[derive(Clone, Debug, Default, PartialEq, PartialOrd, Eq, Hash, Serialize, Deserialize)]
pub struct Number<N> {
  pub number: Decimal,
  pub name: N,
}

#[derive(Clone, Debug, Default, PartialEq, PartialOrd, Eq, Hash, Serialize, Deserialize)]
pub struct Qty {
  pub inner: Vec<Number<Uom>>,
}

#[derive(Debug)]
pub struct QtyDelta {
  pub(crate) before: Option<InternalOperation>,
  pub(crate) after: Option<InternalOperation>,
}

impl QtyDelta {
  pub fn delta(&self) -> BalanceDelta {
    if let Some(before) = self.before.as_ref() {
      if let Some(after) = self.after.as_ref() {
        let before: BalanceDelta = before.clone().into();
        let after: BalanceDelta = after.clone().into();
        // after - before
        before.to_delta(&after)
      } else {
        let before: BalanceDelta = before.clone().into();

        BalanceDelta { qty: -before.qty, cost: -before.cost }
      }
    } else if let Some(after) = self.after.as_ref() {
      after.clone().into()
    } else {
      BalanceDelta::default()
    }
  }
}

trait Convert {
  type Into;
  fn convert(&mut self, into: Self) -> Result<Vec<Self::Into>, WHError>;
}

impl Convert for Vec<(Decimal, Uuid)> {
  type Into = Number<Uom>;

  fn convert(&mut self, into: Self) -> Result<Vec<Self::Into>, WHError> {
    let mut result: Vec<Number<Uom>> = Vec::new();

    if self.len() >= into.len() {
      result.push(Number::from_vec(self.clone())?);
      return Ok(result);
    }

    let mut from = self;
    let mut into = into;

    let start_index = from.len() - 1;

    // if uoms are not the same, return empty vec
    if from[start_index].1 != into[start_index].1 {
      return Ok(result);
    }

    // let mut from_iter = from[start_index..].iter();
    let mut into_iter = into[start_index..].iter();
    let mut index = start_index;

    while let Some((into_number, _)) = into_iter.next() {
      if let Some((from_number, from_uuid)) = from.get(index) {
        if from_number == into_number {
          if let Some((_, next_uuid)) = into.get(index + 1) {
            from.push((Decimal::ONE, *next_uuid));
          }
        } else if from_number > into_number {
          // div with remainder and create two Qty objects
          if let (Some(div), rem) =
            ((from_number / into_number).to_u128(), from_number % into_number)
          {
            if rem > Decimal::ZERO {
              let mut tmp = from.clone();
              tmp[index] = (rem, *from_uuid);
              result.push(Number::from_vec(tmp)?);
              from[index] = ((from_number - rem) / Decimal::from(div), *from_uuid);
            } else {
              from[index] = (from_number / Decimal::from(div), *from_uuid);
            }
            if let Some((_, next_uuid)) = into.get(index + 1) {
              from.push((Decimal::from(div), *next_uuid));
            }
          }
        }
      }

      index += 1;
    }

    result.push(Number::from_vec(from.clone())?);
    Ok(result)
  }
}

impl Uom {
  fn uuid(&self) -> Uuid {
    match self {
      Uom::In(uuid, _) => uuid.clone(),
    }
  }

  fn named(&self) -> Option<Box<Number<Uom>>> {
    match self {
      Uom::In(_, qty) => qty.clone(),
    }
  }

  fn uom(&self) -> Uom {
    match self {
      Uom::In(uuid, qty) => {
        if let Some(q) = qty {
          q.name.clone()
        } else {
          Uom::In(uuid.clone(), None)
        }
      },
    }
  }

  fn is_some(&self) -> bool {
    match self {
      Uom::In(_, qty) => {
        if let Some(_) = qty {
          true
        } else {
          false
        }
      },
    }
  }

  pub(crate) fn depth(&self) -> usize {
    let mut result = 0;
    let mut data = self.named().clone();

    while let Some(qty) = data.as_ref() {
      result += 1;
      data = qty.name.named();
    }

    result
  }

  pub fn number(&self) -> Option<Decimal> {
    if let Some(named) = self.named() {
      Some(named.number)
    } else {
      None
    }
  }
}

impl PartialEq for Uom {
  fn eq(&self, other: &Self) -> bool {
    match self {
      Uom::In(left_id, left_qty) => match other {
        Uom::In(right_id, right_qty) => {
          if let (Some(l), Some(r)) = (left_qty, right_qty) {
            if left_id == right_id && l == r {
              true
            } else {
              false
            }
          } else if left_qty.is_none() && right_qty.is_none() {
            left_id == right_id
          } else {
            false
          }
        },
      },
    }
  }
}

impl Into<JsonValue> for &Number<Uom> {
  fn into(self) -> JsonValue {
    let mut data = self.clone();
    let mut head = object! {};
    head["number"] = data.number.to_json();
    head["uom"] = data.name.uuid().to_json();
    let mut current = &mut head;

    while let Some(qty) = data.named() {
      let mut tmp = object! {};
      tmp["number"] = qty.number.to_json();
      tmp["in"] = data.name.uuid().to_json();

      tmp["uom"] = qty.name.uuid().to_json();

      current["uom"] = tmp.clone();
      current = &mut current["uom"];

      data = *qty;
    }
    head
  }
}

impl TryInto<Number<Uom>> for JsonValue {
  type Error = WHError;

  fn try_into(self) -> Result<Number<Uom>, Self::Error> {
    if !self.is_object() || self.is_empty() {
      return Err(WHError::new("JsonValue is not an object/is empty"));
    }

    let uom = &self["uom"];

    if self["number"].is_null() || uom.is_null() {
      return Err(WHError::new("Incomplete data"));
    }

    if !uom.is_object() && uom.uuid_or_none().is_none() {
      return Err(WHError::new("Wrong data (id instead of uuid) or null"));
    }

    let mut data = &self;

    let mut root = Number { number: data["number"].number(), name: Uom::In(UUID_NIL, None) };

    let mut head = &mut root;

    while data.is_object() {
      let uom = &data["uom"];
      let mut tmp = Number { number: Decimal::ZERO, name: Uom::In(UUID_NIL, None) };

      if !uom.is_object() || !uom["_uuid"].is_null() {
        let uom_uuid = if let Some(uuid) = uom.uuid_or_none() {
          uuid
        } else if let Some(uuid) = uom["_uuid"].uuid_or_none() {
          uuid
        } else {
          return Err(WHError::new("No uuid for 'uom'"));
        };

        head.name = Uom::In(uom_uuid, None);
        break;
      } else {
        tmp.number = uom["number"].number();
        tmp.name = Uom::In(UUID_NIL, None);
        let in_uuid = if let Some(uuid) = uom["in"].uuid_or_none() {
          uuid
        } else if let Some(uuid) = uom["in"]["_uuid"].uuid_or_none() {
          uuid
        } else {
          return Err(WHError::new("No uuid for 'in'"));
        };

        head.name = Uom::In(in_uuid, Some(Box::new(tmp)));
        head = match &mut head.name {
          Uom::In(_, ref mut qty) => qty.as_mut().unwrap(),
          _ => unreachable!(),
        };
      }
      data = uom;
    }

    Ok(root)
  }
}

impl Number<Uom> {
  pub fn new(number: Decimal, uuid: Uuid, inner: Option<Box<Number<Uom>>>) -> Self {
    Number { number, name: Uom::In(uuid, inner) }
  }

  pub fn new_named(number: Decimal, name: Uom) -> Self {
    Number { number, name }
  }

  fn from_vec(data: Vec<(Decimal, Uuid)>) -> Result<Self, WHError> {
    if data.len() < 1 {
      return Err(WHError::new("empty vector"));
    }
    let mut data = data.clone();
    data.reverse();

    let mut root = Number { number: data[0].0, name: Uom::In(data[0].1, None) };

    let mut head = &mut root;

    for (num, id) in data[1..].iter() {
      let tmp = Number { number: *num, name: Uom::In(*id, None) };
      head.name = Uom::In(head.name.uuid(), Some(Box::new(tmp)));
      head = match &mut head.name {
        Uom::In(_, ref mut qty) => qty.as_mut().unwrap(),
      };
    }

    Ok(root)
  }

  pub(crate) fn named(&self) -> Option<Box<Number<Uom>>> {
    self.name.named()
  }

  pub(crate) fn uuid(&self) -> Uuid {
    match &self.name {
      Uom::In(uuid, _) => uuid.clone(),
    }
  }

  pub fn number(&self) -> Decimal {
    self.number.clone()
  }

  fn to_vec(&self) -> Vec<(Decimal, Uuid)> {
    let mut result = Vec::new();

    result.insert(0, (self.number, self.name.uuid()));

    let mut current = self.clone();

    while let Some(qty) = current.named() {
      result.insert(0, (qty.number, qty.name.uuid()));
      current = *qty;
    }

    result
  }

  fn simplify(data: &mut Vec<(Decimal, Uuid)>, index: usize) {
    while data.len() > index + 1 {
      if let (Some(popped), Some(last)) = (data.pop(), data.last_mut()) {
        let (last_number, last_uuid) = *last;
        *last = (popped.0 * last_number, last_uuid);
      }
    }
  }

  // fn subtract(
  //   full: Vec<(Decimal, Uuid)>,
  //   mut left: Vec<(Decimal, Uuid)>,
  //   right: Vec<(Decimal, Uuid)>,
  // ) -> Result<Vec<Self>, WHError> {
  //   if let (Some(l_last), Some(r_last)) = (left.last_mut(), right.last()) {
  //     let (last_number, last_uuid) = *l_last;
  //     let number = last_number - r_last.0;
  //     if number == Decimal::ZERO {
  //       return Ok(Vec::new());
  //     }
  //     *l_last = (number, last_uuid);
  //   }
  //
  //   left.convert(full)
  // }

  pub fn is_zero(&self) -> bool {
    self.number.is_zero()
  }

  fn is_positive(&self) -> bool {
    if self.number > Decimal::ZERO {
      true
    } else {
      false
    }
  }

  fn is_negative(&self) -> bool {
    if self.number < Decimal::ZERO {
      true
    } else {
      false
    }
  }

  fn get_common(mut big: Number<Uom>, mut small: Number<Uom>) -> Option<Uom> {
    if small.name.depth() > 0 {
      while let Some(s) = small.named() {
        while let Some(b) = big.named() {
          if big.name == small.name {
            return Some(big.name);
          }
          big = *b;
          if big.named().is_none() {
            if big.name == small.name {
              return Some(big.name);
            }
          }
        }
        small = *s;
      }
    } else {
      while let Some(b) = big.named() {
        if big.name == small.name {
          return Some(big.name);
        }
        big = *b;
        if big.named().is_none() {
          if big.name == small.name {
            return Some(big.name);
          }
        }
      }
    }

    None
  }

  pub(crate) fn common(&self, rhs: &Self) -> Option<Uom> {
    if self.name == rhs.name {
      return Some(self.name.clone());
    } else {
      let mut left = self.clone();
      let mut right = rhs.clone();
      let left_depth = left.name.depth();
      let right_depth = right.name.depth();

      if left_depth == right_depth {
        while let (Some(l), Some(r)) = (left.named(), right.named()) {
          println!("{l:?} vs {r:?}");
          if left.name == right.name {
            return Some(left.name);
          } else if l.name == r.name {
            return Some(l.name);
          }
          left = *l;
          right = *r;
        }
      } else if left_depth > right_depth {
        return Self::get_common(left, right);
      } else {
        return Self::get_common(right, left);
      }
    }

    None
  }

  pub(crate) fn lowering(&self, name: &Uom) -> Option<Number<Uom>> {
    if &self.name == name {
      return Some(self.clone());
    }

    let mut result = Number::new(Decimal::ZERO, name.uuid(), name.named());

    let mut tmp = self.clone();

    while let Some(mut inner_qty) = tmp.named() {
      inner_qty.number *= tmp.number;

      if &inner_qty.name == name {
        result.number += inner_qty.number;
        break;
      }

      if inner_qty.named().is_none() {
        return None;
      }

      tmp = *inner_qty;
    }

    Some(result)
  }

  fn elevate_to_balance(&self, balance: &Qty) -> Qty {
    let mut result = Qty::new(vec![self.clone()]);
    let mut prev = self.clone();

    for b in balance.inner.iter() {
      if let Some(_common) = self.common(b) {
        if b.name.depth() > prev.name.depth() {
          result = self.elevate_to_uom(&b.name);
        }
      }
    }

    result
  }

  pub fn elevate_to_uom(&self, uom: &Uom) -> Qty {
    let mut result = Qty::new(vec![]);

    if &self.name == uom {
      result.inner.push(self.clone());
      return result;
    }

    if self.name.depth() >= uom.depth() {
      result.inner.push(self.clone());
      return result;
    }

    let mut tmp = uom.clone();

    while let Some(inner) = tmp.named() {
      if self.name == inner.name {
        if self.number.abs() < inner.number.abs() {
          result.inner.push(self.clone());
        } else {
          let amount = (self.number / inner.number).trunc();
          let new_number = Number::new(amount, tmp.uuid(), tmp.named());

          // println!("{:?} / {:?} = {:?}", self.number, inner.number, amount);
          if (amount / Decimal::ONE) >= Decimal::ONE {
            let mut upper_numbers = new_number.elevate_to_uom(uom).inner;
            // println!("upper_numbers {upper_numbers:?}");
            result.inner.append(&mut upper_numbers);
          } else {
            result.inner.push(new_number);
          }

          let differ = self.number - inner.number * amount;
          if !differ.is_zero() {
            result.inner.push(Number::new(differ, inner.uuid(), inner.named()));
          }
        }
        break;
      }

      tmp = inner.name;
    }

    if result.inner.is_empty() {
      result.inner.push(self.clone());
    }

    // log::debug!("result = {result:?}");
    result
  }

  pub(crate) fn base(&self) -> Uom {
    let mut result = self.name.clone();

    let mut iter = self.clone();

    while let Some(inner) = iter.named() {
      result = inner.name.clone();
      iter = *inner;
    }

    result
  }
}

impl Neg for Number<Uom> {
  type Output = Self;

  fn neg(self) -> Self::Output {
    Number { number: -(self.number), name: self.name }
  }
}

// impl Sub for &Number<Uom> {
//   type Output = Qty;
//
//   fn sub(self, rhs: Self) -> Self::Output {
//     let mut left = self.to_vec();
//     let mut right = rhs.to_vec();
//     let mut index = 0;
//
//     for (l, r) in left.iter().zip(right.iter()) {
//       // compare name and number parts if it's not the last value
//       if index + 1 < left.len() && index + 1 < right.len() {
//         if l != r {
//           return Qty { inner: vec![self.clone(), -(rhs.clone())] };
//         }
//         index += 1;
//       } else {
//         // compare only name part if it's the last value
//         let (_, left_id) = l;
//         let (_, right_id) = r;
//         if left_id != right_id {
//           return Qty { inner: vec![self.clone(), -(rhs.clone())] };
//         }
//       }
//     }
//
//     let full = left.clone();
//
//     if left.len() > index + 1 {
//       Number::simplify(&mut left, index);
//     } else if right.len() > index + 1 {
//       Number::simplify(&mut right, index);
//     }
//
//     let mut result = Qty { inner: Vec::new() };
//
//     if left.len() == right.len() {
//       result.inner = Number::subtract(full, left, right).unwrap_or_default();
//     }
//
//     result
//   }
// }

impl Mul<Decimal> for Number<Uom> {
  type Output = Decimal;

  fn mul(self, rhs: Decimal) -> Self::Output {
    let mut sum_qty = self.number;
    let mut data = self;

    while let Some(qty) = data.named() {
      sum_qty *= qty.number;
      data = *qty;
    }

    sum_qty * rhs
  }
}

impl Div<Number<Uom>> for Decimal {
  type Output = Self;

  fn div(self, rhs: Number<Uom>) -> Self::Output {
    let mut sum_qty = rhs.number;
    let mut data = rhs;

    while let Some(qty) = data.named() {
      sum_qty *= qty.number;
      data = *qty;
    }

    self / sum_qty
  }
}

impl Qty {
  pub fn new(inner: Vec<Number<Uom>>) -> Self {
    Qty { inner }
  }

  fn sort(&mut self) {
    // self.inner.sort_by(|a, b| a.number.cmp(&b.number));
    self.inner.sort_by(|a, b| b.name.depth().cmp(&a.name.depth()))
  }

  pub fn inner(&self) -> &Vec<Number<Uom>> {
    &self.inner
  }
  pub fn is_positive(&self) -> bool {
    for qty in &self.inner {
      if !qty.is_positive() {
        return false;
      }
    }
    true
  }

  pub fn is_negative(&self) -> bool {
    for qty in &self.inner {
      if qty.is_positive() || qty.is_zero() {
        return false;
      }
    }
    true
  }

  pub fn is_zero(&self) -> bool {
    if self.inner.is_empty() {
      true
    } else {
      for qty in &self.inner {
        if !qty.number.is_zero() {
          return false;
        }
      }
      true
    }
  }

  pub(crate) fn lowering(&self, name: &Uom) -> Option<Number<Uom>> {
    let mut result = Number::new(Decimal::ZERO, name.uuid(), name.named());

    for qty in &self.inner {
      if let Some(sum) = qty.lowering(name) {
        result.number += sum.number;
      }
    }

    if !result.number.is_zero() {
      Some(result)
    } else {
      None
    }
  }

  pub(crate) fn abs(&self) -> Self {
    let mut result = self.clone();

    for mut qty in &mut result.inner {
      if qty.number.is_sign_negative() {
        qty.number = qty.number.abs();
      }
    }
    result
  }

  pub fn common(&self, rhs: &Self) -> Option<Uom> {
    let mut result: Option<Uom> = None;

    for left in &self.inner {
      for right in &rhs.inner {
        if let Some(common) = left.common(&right) {
          if let Some(res) = &result {
            // we must write a minimal uom from qty, otherwise cost will not calculate correctly
            if common.depth() < res.depth() {
              result = Some(common);
            }
          } else {
            result = Some(common);
          }
        }
      }
      if result.is_none() {
        return result;
      }
    }

    result
  }

  pub(crate) fn cost(&self, balance: &BalanceForGoods) -> Cost {
    if self.is_zero() || balance.is_zero() {
      Cost::ZERO
    } else {
      if let Some(common) = self.common(&balance.qty) {
        if let Some(lower) = self.lowering(&common) {
          let price = balance.price(&common);
          (lower * price.number()).round_dp(5).into()
        } else {
          Cost::ERROR
        }
      } else {
        Cost::ERROR
      }
    }
  }

  pub(crate) fn price(&self, balance: &BalanceForGoods) -> Option<Price> {
    if self.is_zero() {
      None
    } else {
      if let Some(common) = self.common(&balance.qty) {
        Some(balance.price(&common))
      } else {
        None
      }
    }
  }

  pub(crate) fn is_greater_or_equal(&self, rhs: &Self) -> Result<bool, WHError> {
    if let Some(uom) = self.common(&rhs) {
      let left = self.lowering(&uom).unwrap().number;
      let right = rhs.lowering(&uom).unwrap().number;
      Ok(left >= right)
    } else {
      Err(WHError::new("two Qty don't have common part"))
    }
  }

  pub(crate) fn plus_with_relax(&self, balance: &Self) -> Self {
    let mut result = Qty::new(vec![]);
    let len = self.inner.len();

    for i in 0..len {
      let first = &(self.inner[i]);
      for j in i + 1..len {
        let second = &(self.inner[j]);
        if let Some(common) = first.common(second) {
          if let (Some(low_first), Some(low_second)) =
            (first.lowering(&common), second.lowering(&common))
          {
            // result.inner[i] = first + second;
            let sum = low_first.number + low_second.number;
            result.inner.append(
              &mut Number::new(sum, low_first.uuid(), low_first.named())
                .elevate_to_balance(&balance)
                .inner,
            );
          }
        } else {
          let (first_base, second_base) = (first.base(), second.base());
          if first_base == second_base {
            if let (Some(low_first), Some(low_second)) =
              (first.lowering(&first_base), second.lowering(&second_base))
            {
              // result.inner[i] = low_first + low_second;
              let sum = low_first.number + low_second.number;
              result.inner.append(
                &mut Number::new(sum, low_first.uuid(), low_first.named())
                  .elevate_to_balance(&balance)
                  .inner,
              );
            }
          }
        }
      }
    }

    result
  }
}

impl Mul<Price> for &Qty {
  type Output = Cost;

  fn mul(self, price: Price) -> Self::Output {
    if let Some(lower) = self.lowering(&price.uom()) {
      let number = lower * price.number();
      return Cost::from(number);
    }
    Cost::ERROR
  }
}

impl Add for &Qty {
  type Output = Qty;

  fn add(self, rhs: Self) -> Self::Output {
    if self.is_zero() && rhs.is_zero() {
      return Qty::default();
    } else if self.is_zero() {
      return rhs.clone();
    } else if rhs.is_zero() {
      return self.clone();
    }

    let mut sorted_left = self.clone();
    sorted_left.sort();
    let mut sorted_right = rhs.clone();
    sorted_right.sort();

    let mut left = VecDeque::from(sorted_left.inner);
    let mut right = VecDeque::from(sorted_right.inner);

    'left: while let Some(cur_left) = left.pop_front() {
      let mut tmp_right = right.clone();
      while let Some(cur_right) = tmp_right.pop_front() {
        // log::debug!("ADD: {:?} + {:?}", cur_left, cur_right);
        let l_depth = cur_left.name.depth();
        let r_depth = cur_right.name.depth();
        let skip = if (l_depth > r_depth && cur_left.is_negative())
          || (r_depth > l_depth && cur_right.is_negative())
        {
          true
        } else {
          false
        };

        let common = cur_left.common(&cur_right);

        // if let Some(common) = cur_left.common(&cur_right) {
        if !skip && common.is_some() {
          let common = common.unwrap();
          if let (Some(low_left), Some(low_right)) =
            (cur_left.lowering(&common), cur_right.lowering(&common))
          {
            right.pop_front();

            let result = low_left.number + low_right.number;
            log::debug!("ADD: {} + {} = {}", low_left.number, low_right.number, result);

            if result != Decimal::ZERO {
              let upper_number =
                if cur_left.name.depth() > cur_right.name.depth() { &cur_left } else { &cur_right };

              let upper_qty = Number::new(result, low_left.uuid(), low_left.named())
                .elevate_to_uom(&upper_number.name);

              upper_qty.inner.into_iter().for_each(|n| left.push_back(n));
            }
            break;
          }
        } else if !skip && common.is_none() {
          if tmp_right.is_empty() {
            if left.is_empty() {
              left.push_back(cur_left.clone());
              break 'left;
            } else {
              left.push_back(cur_left.clone());
              continue 'left;
            }
          } else {
            right.pop_front();
            right.push_back(cur_right);
            continue;
          }
        } else {
          if left.is_empty() {
            left.push_back(cur_left.clone());
            right.into_iter().for_each(|n| left.push_back(n));

            let mut result = Qty::new(left.into());
            result.sort();
            return result;
          } else {
            left.push_back(cur_left.clone());
          }
        }
      }
      if right.is_empty() {
        break 'left;
      }
    }

    right.into_iter().for_each(|n| left.push_back(n));

    let mut result = Qty::new(left.into());
    result.sort();
    result
  }
}

impl AddAssign<&Qty> for Qty {
  fn add_assign(&mut self, rhs: &Qty) {
    let result = &self.clone() + rhs;
    *self = result;
  }
}

impl Sub for &Qty {
  type Output = Qty;

  fn sub(self, rhs: Self) -> Self::Output {
    if self.is_zero() && rhs.is_zero() {
      return Qty::default();
    } else if self.is_zero() {
      return -rhs.clone();
    } else if rhs.is_zero() {
      return self.clone();
    }

    let mut sorted_left = self.clone();
    sorted_left.sort();
    let mut sorted_right = rhs.clone();
    sorted_right.sort();

    let mut left = VecDeque::from(sorted_left.inner);
    let mut right = VecDeque::from(sorted_right.inner);

    'left: while let Some(cur_left) = left.pop_front() {
      let mut tmp_right = right.clone();

      while let Some(cur_right) = tmp_right.pop_front() {
        log::debug!("SUB: {:?} - {:?}", cur_left, cur_right);
        let l_depth = cur_left.name.depth();
        let r_depth = cur_right.name.depth();

        let common = cur_left.common(&cur_right);

        let skip = (l_depth > r_depth && cur_left.is_negative() && cur_right.is_negative())
          || (l_depth < r_depth && cur_left.is_positive() && cur_right.is_positive());

        if !skip && common.is_some() {
          let common = common.unwrap();
          if let (Some(low_left), Some(low_right)) =
            (cur_left.lowering(&common), cur_right.lowering(&common))
          {
            right.pop_front();

            let result = low_left.number - low_right.number;
            log::debug!("SUB: {} - {} = {}", low_left.number, low_right.number, result);

            let upper_number =
              if cur_left.name.depth() > cur_right.name.depth() { &cur_left } else { &cur_right };

            let upper_qty = Number::new(result, low_left.uuid(), low_left.named())
              .elevate_to_uom(&upper_number.name);

            if result > Decimal::ZERO {
              upper_qty.inner.into_iter().for_each(|n| left.push_back(n));
            } else if result < Decimal::ZERO {
              upper_qty.inner.into_iter().for_each(|n| right.push_back(-n));
            }
            break;
          }
        } else if !skip && common.is_none() {
          if tmp_right.is_empty() {
            if left.is_empty() {
              left.push_back(cur_left.clone());
              break 'left;
            } else {
              left.push_back(cur_left.clone());
              continue 'left;
            }
          } else {
            right.pop_front();
            right.push_back(cur_right);
            continue;
          }
        } else {
          if left.is_empty() {
            left.push_back(cur_left.clone());
            right.into_iter().for_each(|n| left.push_back(-n));
            let mut result = Qty::new(left.into());
            result.sort();
            return result;
          } else {
            left.push_back(cur_left.clone());
          }
        }
      }
      if right.is_empty() {
        break 'left;
      }
    }

    right.into_iter().for_each(|n| left.push_back(-n));

    let mut result = Qty::new(left.into());
    result.sort();
    result
  }
}

impl SubAssign<&Qty> for Qty {
  fn sub_assign(&mut self, rhs: &Qty) {
    let result = &self.clone() - rhs;
    *self = result;
  }
}

impl Neg for Qty {
  type Output = Self;

  fn neg(self) -> Self::Output {
    let mut result = self;

    for mut qty in &mut result.inner {
      qty.number = -qty.number;
    }

    result
  }
}

impl Into<JsonValue> for &Qty {
  fn into(self) -> JsonValue {
    let mut result = JsonValue::new_array();
    for qty in &self.inner {
      let q: JsonValue = qty.into();
      result.push(q).unwrap_or_default();
    }
    result
  }
}

impl TryInto<Qty> for JsonValue {
  type Error = WHError;

  fn try_into(self) -> Result<Qty, Self::Error> {
    let mut result = Qty { inner: Vec::new() };

    if self.is_array() {
      for qty in self.members() {
        let q: Number<Uom> = qty.clone().try_into()?;
        result.inner.push(q);
      }
    } else if self.is_object() {
      let q: Number<Uom> = self.clone().try_into()?;
      result.inner.push(q);
    } else {
      return Err(WHError::new("JsonValue is not an object/array"));
    }
    Ok(result)
  }
}

#[cfg(test)]
mod tests {
  use crate::elements::ToJson;
  use crate::error::WHError;
  use crate::qty::{Number, Qty, Uom};
  use json::{array, object, JsonValue};
  use rust_decimal::Decimal;
  use uuid::Uuid;

  #[test]
  fn create() {
    let u0 = Uuid::new_v4();
    let u1 = Uuid::new_v4();
    let u2 = Uuid::new_v4();

    let data0 = object! {
      "number": 1,
      "uom": u0.to_json(),
    };

    let qty0: Qty = data0.try_into().unwrap();
    // println!("{qty0:?}");

    assert_eq!(qty0.inner.len(), 1);
    assert_eq!(qty0.inner[0].number, Decimal::from(1));
    assert_eq!(qty0.inner[0].name, Uom::In(u0, None));

    let data1 = object! {
      "number": 1,
      "uom": object! {
        "number": 2,
        "uom": u1.to_json(),
        "in": u0.to_json(),
      },
    };

    let qty1: Qty = data1.try_into().unwrap();
    // println!("{qty1:?}");

    assert_eq!(qty1.inner.len(), 1);
    assert_eq!(qty1.inner[0].number, Decimal::from(1));
    assert_eq!(
      qty1.inner[0].name,
      Uom::In(u0, Some(Box::new(Number { number: Decimal::from(2), name: Uom::In(u1, None) })))
    );

    let data2 = object! {
      "number": 2,
      "uom": object! {
        "number": 10,
        "uom": object! {
          "number": 100,
          "uom": u2.to_json(),
          "in": u1.to_json(),
        },
        "in": u0.to_json(),
      },
    };

    let qty2: Qty = data2.try_into().unwrap();
    // println!("{qty2:?}");

    assert_eq!(qty2.inner.len(), 1);
    assert_eq!(qty2.inner[0].number, Decimal::from(2));
    assert_eq!(
      qty2.inner[0].name,
      Uom::In(
        u0,
        Some(Box::new(Number {
          number: Decimal::from(10),
          name: Uom::In(
            u1,
            Some(Box::new(Number { number: Decimal::from(100), name: Uom::In(u2, None) }))
          ),
        }))
      )
    );
  }

  #[test]
  fn sort() {
    let uom0 = Uuid::new_v4();
    let uom1 = Uuid::new_v4();
    let uom2 = Uuid::new_v4();

    let mut data0 = Qty::new(vec![
      Number::new(
        Decimal::from(10),
        uom0,
        Some(Box::new(Number::new(Decimal::from(10), uom1, None))),
      ),
      Number::new(Decimal::from(3), uom1, None),
      Number::new(
        Decimal::from(-2),
        uom0,
        Some(Box::new(Number::new(Decimal::from(10), uom1, None))),
      ),
    ]);

    data0.sort();

    println!("{data0:?}");
  }

  #[test]
  fn to_json() {
    let u0 = Uuid::new_v4();
    let u1 = Uuid::new_v4();
    let u2 = Uuid::new_v4();

    // check 'enrich' data
    let data00 = array![object! {
      "number": 1,
      "uom": object! {
        "number": 3,
        "uom": object! { "_uuid": u1.to_json() },
        "in": object! { "_uuid": u0.to_json() },
      },
    }];

    let qty00: Qty = data00.clone().try_into().unwrap();
    println!("qty00 {qty00:?}");

    let json00: JsonValue = (&qty00).into();

    let compare00 = array![object! {
      "number": "1",
      "uom": object! {
        "number": "3",
        "uom": u1.to_json(),
        "in": u0.to_json(),
      },
    }];

    assert_eq!(compare00, json00);

    let data0 = array![object! {
      "number": Decimal::from(1).to_json(),
      "uom": u0.to_json(),
    }];

    let qty0: Qty = data0.clone().try_into().unwrap();

    let mut json0: JsonValue = (&qty0).into();
    // println!("json0 {json0}");

    assert_eq!(data0, json0);

    let data2 = array![object! {
      "number": Decimal::from(2).to_json(),
      "uom": object! {
        "number": Decimal::from(10).to_json(),
        "uom": object! {
          "number": Decimal::from(100).to_json(),
          "uom": u2.to_json(),
          "in": u1.to_json(),
        },
        "in": u0.to_json(),
      },
    }];

    let qty2: Qty = data2.clone().try_into().unwrap();
    // println!("qty2 {qty2:?}");

    let mut json2: JsonValue = (&qty2).into();
    // println!("json2 {json2}");

    assert_eq!(data2, json2);

    // incomplete data must return an error
    let data3 = array![object! {
      "number": Decimal::from(1).to_json(),
      // "uom": u0.to_json(),
    }];

    let qty3: Result<Qty, WHError> = data3.clone().try_into();
    assert_eq!(qty3.is_err(), true);
  }

  #[test]
  fn into_vec() {
    let u0 = Uuid::new_v4();
    let u1 = Uuid::new_v4();
    let u2 = Uuid::new_v4();

    let data0 = object! {
      "number": 2,
      "uom": object! {
        "number": 10,
        "uom": object! {
          "number": 100,
          "uom": u2.to_json(),
          "in": u1.to_json(),
        },
        "in": u0.to_json(),
      },
    };

    let qty0: Qty = data0.try_into().unwrap();
    assert_eq!(qty0.inner.len(), 1);

    let result = qty0.inner[0].clone().to_vec();
    // println!("{result:?}");

    assert_eq!(result.len(), 3);
    assert_eq!(result[0].0, Decimal::from(100));
    assert_eq!(result[1].0, Decimal::from(10));
    assert_eq!(result[2].0, Decimal::from(2));
  }

  #[test]
  fn simplify() {
    let u0 = Uuid::new_v4();
    let u1 = Uuid::new_v4();
    let u2 = Uuid::new_v4();

    let data2 = object! {
      "number": 2,
      "uom": object! {
        "number": 10,
        "uom": object! {
          "number": 100,
          "uom": u2.to_json(),
          "in": u1.to_json(),
        },
        "in": u0.to_json(),
      },
    };

    let qty2: Qty = data2.try_into().unwrap();
    assert_eq!(qty2.inner.len(), 1);

    let mut vector = qty2.inner[0].clone().to_vec();
    assert_eq!(vector.len(), 3);
    // println!("{vector:?}");

    Number::simplify(&mut vector, 1);

    assert_eq!(vector.len(), 2);
    assert_eq!(vector[0].0, Decimal::from(100));
    assert_eq!(vector[1].0, Decimal::from(20));

    // println!("{vector:?}");
  }

  #[test]
  fn number_lowering() {
    let uom0 = Uuid::new_v4();
    let uom1 = Uuid::new_v4();
    let uom2 = Uuid::new_v4();

    let data0 = Number::new(
      Decimal::from(2),
      uom0,
      Some(Box::new(Number::new(
        Decimal::from(10),
        uom1,
        Some(Box::new(Number::new(Decimal::from(100), uom2, None))),
      ))),
    );

    let name = Uom::In(uom2, None);

    let lower = data0.lowering(&name).unwrap();

    assert_eq!(lower.number, Decimal::from(2000));
    assert_eq!(lower.name, name);
  }

  #[test]
  fn qty_lowering() {
    let u0 = Uuid::new_v4();
    let u1 = Uuid::new_v4();
    let u2 = Uuid::new_v4();

    let data0 = object! {
      "number": 2,
      "uom": object! {
        "number": 10,
        "uom": object! {
          "number": 100,
          "uom": u2.to_json(),
          "in": u1.to_json(),
        },
        "in": u0.to_json(),
      },
    };

    let qty0: Qty = data0.try_into().unwrap();

    let data1 = object! {
      "number": 20,
      "uom": object! {
        "number": 100,
        "uom": u2.to_json(),
        "in": u1.to_json(),
      },
    };

    let qty1: Qty = data1.try_into().unwrap();

    // u1 x 100 u2
    let uom = qty0.inner[0].name.named().unwrap().name;

    let lower0 = qty0.lowering(&uom).unwrap();
    // println!("lower0 {lower0:?}");

    assert_eq!(qty1.inner[0], lower0);

    let data2 = object! {
        "number": 3,
        "uom": u0.to_json(),
    };

    let qty2: Qty = data2.try_into().unwrap();

    let data3 = object! {
      "number": 20,
      "uom": object! {
        "number": 100,
        "uom": u2.to_json(),
        "in": u1.to_json(),
      },
    };

    let qty3: Qty = data3.try_into().unwrap();

    // u0
    let uom = qty2.inner[0].clone().name;

    let lower1 = qty3.lowering(&uom);
    // println!("lower1 {lower1:?}");

    assert_eq!(lower1.is_none(), true);

    // u1 x 100 u2
    let uom = qty3.inner[0].clone().name;
    // println!("uom {uom:?}");

    let lower2 = qty2.lowering(&uom);
    // println!("lower2 {lower2:?}");

    assert_eq!(lower2.is_none(), true);

    let data4 = object! {
      "number": 10,
      "uom": object! {
        "number": 99,
        "uom": u2.to_json(),
        "in": u1.to_json(),
        },
    };

    let qty4: Qty = data4.try_into().unwrap();

    // u0
    let uom = qty4.inner[0].clone().name;

    let lower3 = qty4.lowering(&uom);
    // println!("lower3 {lower3:?}");

    assert_eq!(qty4.inner[0], lower3.unwrap());
  }

  #[test]
  fn common() {
    let u0 = Uuid::new_v4();
    let u1 = Uuid::new_v4();
    let u2 = Uuid::new_v4();
    let u3 = Uuid::new_v4();

    let qty00 = Qty::new(vec![Number::new(
      Decimal::from(1),
      u0,
      Some(Box::new(Number::new(Decimal::from(11), u1, None))),
    )]);

    let common00 = qty00.common(&qty00).unwrap();
    // println!("common00 {common00:?}");
    assert_eq!(common00.uuid(), u0);
    assert_eq!(common00.named().unwrap().uuid(), u1);
    assert_eq!(common00.named().unwrap().number, Decimal::from(11));

    // let qty01 = Qty::new(vec![Number::new(
    //   Decimal::from(1),
    //   u2,
    //   Some(Box::new(Number::new(Decimal::from(30), u1, None))),
    // )]);
    //
    // // same uom only in the end
    // let common01 = qty00.common(&qty01).unwrap();
    // // println!("common01 {common01:?}");
    // assert_eq!(common01.uuid(), u1);

    // two structs with depth == 0
    let data0 = object! {
      "number": 1,
      "uom": u2.to_json(),
    };

    let data01 = object! {
      "number": 1,
      "uom": u1.to_json(),
    };

    let qty0: Qty = data0.try_into().unwrap();
    let qty01: Qty = data01.try_into().unwrap();

    let common01 = qty0.common(&qty01);
    assert_eq!(common01.is_none(), true);

    let data1 = object! {
      "number": 1,
      "uom": u2.to_json(),
    };

    let qty1: Qty = data1.try_into().unwrap();

    let common0 = qty0.common(&qty1).unwrap();
    // println!("common0 {common0:?}");

    assert_eq!(common0, qty0.inner[0].name);

    // one struct with depth == 0 and one with depth > 0
    let data2 = object! {
      "number": 1,
      "uom": u2.to_json(),
    };

    let qty2: Qty = data2.try_into().unwrap();

    let data3 = object! {
      "number": 2,
      "uom": object! {
        "number": 10,
        "uom": object! {
          "number": 100,
          "uom": u2.to_json(),
          "in": u1.to_json(),
        },
        "in": u0.to_json(),
      },
    };

    let qty3: Qty = data3.try_into().unwrap();

    let common1 = qty2.common(&qty3).unwrap();
    // println!("common1 {common1:?}");

    assert_eq!(common1, qty2.inner[0].name);

    // two structs with depth > 0
    let data4 = object! {
      "number": 3,
      "uom": object! {
        "number": 15,
        "uom": object! {
          "number": 100,
          "uom": u2.to_json(),
          "in": u1.to_json(),
        },
        "in": u3.to_json(),
      },
    };

    let qty4: Qty = data4.try_into().unwrap();

    let data5 = object! {
      "number": 50,
      "uom": object! {
        "number": 100,
        "uom": u2.to_json(),
        "in": u1.to_json(),
      },
    };

    let qty5: Qty = data5.try_into().unwrap();

    // // different top uoms
    // let common2_0 = qty4.common(&qty3).unwrap();
    // println!("common2_0 {common2_0:?}");
    // assert_eq!(common2_0, qty5.inner[0].name);

    let common2 = qty4.common(&qty5).unwrap();
    // println!("common2 {common2:?}");

    assert_eq!(common2, qty5.inner[0].name);

    let data6 = object! {
      "number": 1,
      "uom": object! {
        "number": 4,
        "uom": u2.to_json(),
        "in": u1.to_json(),
      },
    };
    let qty6: Qty = data6.try_into().unwrap();

    let data7 = object! {
      "number": 1,
      "uom": object! {
        "number": 3,
        "uom": u2.to_json(),
        "in": u1.to_json(),
      },
    };
    let qty7: Qty = data7.try_into().unwrap();

    let common3 = qty6.common(&qty7).unwrap();
    // println!("common3 {common3:?}");

    assert_eq!(common3, Uom::In(u2, None));
  }

  #[test]
  fn depth() {
    let u0 = Uuid::new_v4();
    let u1 = Uuid::new_v4();
    let u2 = Uuid::new_v4();

    let data0 = object! {
      "number": 1,
      "uom": u0.to_json(),
    };

    let qty0: Qty = data0.try_into().unwrap();

    let depth0 = qty0.inner[0].name.depth();
    // println!("depth0 {depth0}");

    assert_eq!(depth0, 0);

    let data1 = object! {
      "number": 20,
      "uom": object! {
        "number": 100,
        "uom": u2.to_json(),
        "in": u1.to_json(),
      },
    };

    let qty1: Qty = data1.try_into().unwrap();

    let depth1 = qty1.inner[0].name.depth();
    // println!("depth1 {depth1}");

    assert_eq!(depth1, 1);

    let data2 = object! {
      "number": 2,
      "uom": object! {
        "number": 10,
        "uom": object! {
          "number": 100,
          "uom": u2.to_json(),
          "in": u1.to_json(),
        },
        "in": u0.to_json(),
      },
    };

    let qty2: Qty = data2.try_into().unwrap();

    let depth2 = qty2.inner[0].name.depth();
    // println!("depth2 {depth2}");

    assert_eq!(depth2, 2);
  }

  #[test]
  fn mul() {
    let u0 = Uuid::new_v4();
    let u1 = Uuid::new_v4();
    let u2 = Uuid::new_v4();

    let data0 = object! {
      "number": 2,
      "uom": object! {
        "number": 10,
        "uom": object! {
          "number": 100,
          "uom": u2.to_json(),
          "in": u1.to_json(),
        },
        "in": u0.to_json(),
      },
    };

    let qty0: Qty = data0.try_into().unwrap();

    let res = qty0.inner[0].clone() * Decimal::from(2);
    // println!("mul {res:?}");

    assert_eq!(res, Decimal::from(4000));
  }

  #[test]
  fn div() {
    let u0 = Uuid::new_v4();
    let u1 = Uuid::new_v4();
    let u2 = Uuid::new_v4();

    let data0 = object! {
      "number": 1,
      "uom": object! {
        "number": 10,
        "uom": object! {
          "number": 100,
          "uom": u2.to_json(),
          "in": u1.to_json(),
        },
        "in": u0.to_json(),
      },
    };

    let qty0: Qty = data0.try_into().unwrap();

    let res = Decimal::from(2000) / qty0.inner[0].clone();
    // println!("div {res:?}");

    assert_eq!(res, Decimal::from(2));
  }

  #[test]
  fn base() {
    let uom0 = Uuid::new_v4();
    let uom1 = Uuid::new_v4();
    let uom2 = Uuid::new_v4();

    let data0 = Number::new(Decimal::from(100), uom2, None);

    let base0 = data0.base();
    // println!("base0 {base0:?}");

    assert_eq!(base0.uuid(), uom2);
    assert_eq!(base0.named(), None);

    let data1 = Number::new(
      Decimal::from(2),
      uom0,
      Some(Box::new(Number::new(Decimal::from(10), uom1, Some(Box::new(data0.clone()))))),
    );

    let base1 = data1.base();
    // println!("base1 {base1:?}");

    assert_eq!(base1.uuid(), uom2);
    assert_eq!(base1.named(), None);
  }

  #[test]
  fn elevate_to_balance() {
    let uom0 = Uuid::new_v4();
    let uom1 = Uuid::new_v4();
    let uom2 = Uuid::new_v4();

    let data0 = Qty::new(vec![Number::new(
      Decimal::from(2),
      uom0,
      Some(Box::new(Number::new(
        Decimal::from(10),
        uom1,
        Some(Box::new(Number::new(Decimal::from(100), uom2, None))),
      ))),
    )]);

    let data1 = Number::new(Decimal::from(1000), uom2, None);

    let elevate0 = data1.elevate_to_balance(&data0);
    // println!("elevate0 {elevate0:?}");

    let check0 = Qty::new(vec![Number::new(
      Decimal::from(1),
      uom0,
      Some(Box::new(Number::new(
        Decimal::from(10),
        uom1,
        Some(Box::new(Number::new(Decimal::from(100), uom2, None))),
      ))),
    )]);

    assert_eq!(elevate0, check0);
  }

  #[test]
  fn elevate_to_uom() {
    let uom0 = Uuid::new_v4();
    let uom1 = Uuid::new_v4();
    let uom2 = Uuid::new_v4();

    let data0 = Uom::In(
      uom0,
      Some(Box::new(Number::new(
        Decimal::from(10),
        uom1,
        Some(Box::new(Number::new(Decimal::from(100), uom2, None))),
      ))),
    );

    let data1 = Number::new(
      Decimal::from(150),
      uom1,
      Some(Box::new(Number::new(Decimal::from(100), uom2, None))),
    );

    let result0 = data1.elevate_to_uom(&data0);
    // println!("result0 {result0:?}");

    let compare0 = Qty::new(vec![Number::new(
      Decimal::from(15),
      uom0,
      Some(Box::new(Number::new(
        Decimal::from(10),
        uom1,
        Some(Box::new(Number::new(Decimal::from(100), uom2, None))),
      ))),
    )]);

    assert_eq!(result0.inner.len(), 1);
    assert_eq!(result0, compare0);

    let data2 = Number::new(Decimal::from(1200), uom2, None);

    let result1 = data2.elevate_to_uom(&data0);
    println!("result1 {result1:?}");

    let compare1 = Qty::new(vec![
      Number::new(
        Decimal::from(1),
        uom0,
        Some(Box::new(Number::new(
          Decimal::from(10),
          uom1,
          Some(Box::new(Number::new(Decimal::from(100), uom2, None))),
        ))),
      ),
      Number::new(
        Decimal::from(2),
        uom1,
        Some(Box::new(Number::new(Decimal::from(100), uom2, None))),
      ),
    ]);

    assert_eq!(result1.inner.len(), 2);
    assert_eq!(result1, compare1);

    // negative number

    let data3 = Uom::In(uom1, Some(Box::new(Number::new(Decimal::from(10), uom2, None))));

    let data4 = Number::new(Decimal::from(-11), uom2, None);

    let result2 = data4.elevate_to_uom(&data3);
    // println!("result2 {result2:?}");

    let compare2 = Qty::new(vec![
      Number::new(
        Decimal::from(-1),
        uom1,
        Some(Box::new(Number::new(Decimal::from(10), uom2, None))),
      ),
      Number::new(Decimal::from(-1), uom2, None),
    ]);

    assert_eq!(result2.inner.len(), 2);
    assert_eq!(result2, compare2);
  }

  fn check_add(left: Qty, right: Qty, check: Qty) {
    let result = &left + &right;
    println!("check_add {result:?}");

    assert_eq!(result.inner.len(), check.inner.len());
    for i in 0..check.inner.len() {
      assert_eq!(result.inner[i].number, check.inner[i].number);
      assert_eq!(result.inner[i].name, check.inner[i].name);
    }
  }
  #[test]
  fn add() {
    let uom0 = Uuid::new_v4();
    let uom1 = Uuid::new_v4();
    let uom2 = Uuid::new_v4();

    // [1 of 4, -1 of 3] + (1 of 3) = (1 of 4)
    check_add(
      Qty::new(vec![
        Number::new(
          Decimal::from(1),
          uom0,
          Some(Box::new(Number::new(Decimal::from(4), uom1, None))),
        ),
        Number::new(
          Decimal::from(-1),
          uom0,
          Some(Box::new(Number::new(Decimal::from(3), uom1, None))),
        ),
      ]),
      Qty::new(vec![Number::new(
        Decimal::from(1),
        uom0,
        Some(Box::new(Number::new(Decimal::from(3), uom1, None))),
      )]),
      Qty::new(vec![Number::new(
        Decimal::from(1),
        uom0,
        Some(Box::new(Number::new(Decimal::from(4), uom1, None))),
      )]),
    );

    // (1 of 3) + [1 of 4, -1 of 3] = (1 of 4)
    check_add(
      Qty::new(vec![Number::new(
        Decimal::from(1),
        uom0,
        Some(Box::new(Number::new(Decimal::from(3), uom1, None))),
      )]),
      Qty::new(vec![
        Number::new(
          Decimal::from(1),
          uom0,
          Some(Box::new(Number::new(Decimal::from(4), uom1, None))),
        ),
        Number::new(
          Decimal::from(-1),
          uom0,
          Some(Box::new(Number::new(Decimal::from(3), uom1, None))),
        ),
      ]),
      Qty::new(vec![Number::new(
        Decimal::from(1),
        uom0,
        Some(Box::new(Number::new(Decimal::from(4), uom1, None))),
      )]),
    );

    // (-10 of 10) + (6) = [-10 of 10, 6]
    check_add(
      Qty::new(vec![Number::new(
        Decimal::from(-10),
        uom0,
        Some(Box::new(Number::new(Decimal::from(10), uom1, None))),
      )]),
      Qty::new(vec![Number::new(Decimal::from(6), uom1, None)]),
      Qty::new(vec![
        Number::new(
          Decimal::from(-10),
          uom0,
          Some(Box::new(Number::new(Decimal::from(10), uom1, None))),
        ),
        Number::new(Decimal::from(6), uom1, None),
      ]),
    );

    // (-6) + (10 of 10) = [9 of 10, 4]
    check_add(
      Qty::new(vec![Number::new(Decimal::from(-6), uom1, None)]),
      Qty::new(vec![Number::new(
        Decimal::from(10),
        uom0,
        Some(Box::new(Number::new(Decimal::from(10), uom1, None))),
      )]),
      Qty::new(vec![
        Number::new(
          Decimal::from(9),
          uom0,
          Some(Box::new(Number::new(Decimal::from(10), uom1, None))),
        ),
        Number::new(Decimal::from(4), uom1, None),
      ]),
    );

    // (10 of 10) + (-6) = [9 of 10, 4]
    check_add(
      Qty::new(vec![Number::new(
        Decimal::from(10),
        uom0,
        Some(Box::new(Number::new(Decimal::from(10), uom1, None))),
      )]),
      Qty::new(vec![Number::new(Decimal::from(-6), uom1, None)]),
      Qty::new(vec![
        Number::new(
          Decimal::from(9),
          uom0,
          Some(Box::new(Number::new(Decimal::from(10), uom1, None))),
        ),
        Number::new(Decimal::from(4), uom1, None),
      ]),
    );

    // (6) + (-10 of 10) = [-10 of 10, 6]
    check_add(
      Qty::new(vec![Number::new(Decimal::from(6), uom1, None)]),
      Qty::new(vec![Number::new(
        Decimal::from(-10),
        uom0,
        Some(Box::new(Number::new(Decimal::from(10), uom1, None))),
      )]),
      Qty::new(vec![
        Number::new(
          Decimal::from(-10),
          uom0,
          Some(Box::new(Number::new(Decimal::from(10), uom1, None))),
        ),
        Number::new(Decimal::from(6), uom1, None),
      ]),
    );

    // (-10 of 10) + (-6) = [-10 of 10, -6]
    check_add(
      Qty::new(vec![Number::new(
        Decimal::from(-10),
        uom0,
        Some(Box::new(Number::new(Decimal::from(10), uom1, None))),
      )]),
      Qty::new(vec![Number::new(Decimal::from(-6), uom1, None)]),
      Qty::new(vec![
        Number::new(
          Decimal::from(-10),
          uom0,
          Some(Box::new(Number::new(Decimal::from(10), uom1, None))),
        ),
        Number::new(Decimal::from(-6), uom1, None),
      ]),
    );

    // (6) + (10 of 10) = [10 of 10, 6]
    check_add(
      Qty::new(vec![Number::new(Decimal::from(6), uom1, None)]),
      Qty::new(vec![Number::new(
        Decimal::from(10),
        uom0,
        Some(Box::new(Number::new(Decimal::from(10), uom1, None))),
      )]),
      Qty::new(vec![
        Number::new(
          Decimal::from(10),
          uom0,
          Some(Box::new(Number::new(Decimal::from(10), uom1, None))),
        ),
        Number::new(Decimal::from(6), uom1, None),
      ]),
    );

    // (-6) + (-10 of 10) =  [-10 of 10, -6]
    check_add(
      Qty::new(vec![Number::new(Decimal::from(-6), uom1, None)]),
      Qty::new(vec![Number::new(
        Decimal::from(-10),
        uom0,
        Some(Box::new(Number::new(Decimal::from(10), uom1, None))),
      )]),
      Qty::new(vec![
        Number::new(
          Decimal::from(-10),
          uom0,
          Some(Box::new(Number::new(Decimal::from(10), uom1, None))),
        ),
        Number::new(Decimal::from(-6), uom1, None),
      ]),
    );

    // (10 of 10) + (6) = [10 of 10, 6]
    check_add(
      Qty::new(vec![Number::new(
        Decimal::from(10),
        uom0,
        Some(Box::new(Number::new(Decimal::from(10), uom1, None))),
      )]),
      Qty::new(vec![Number::new(Decimal::from(6), uom1, None)]),
      Qty::new(vec![
        Number::new(
          Decimal::from(10),
          uom0,
          Some(Box::new(Number::new(Decimal::from(10), uom1, None))),
        ),
        Number::new(Decimal::from(6), uom1, None),
      ]),
    );

    // *******************

    // 1 of 10 + (-2 of 10) = -1 of 10
    check_add(
      Qty::new(vec![Number::new(
        Decimal::from(1),
        uom0,
        Some(Box::new(Number::new(Decimal::from(10), uom1, None))),
      )]),
      Qty::new(vec![Number::new(
        Decimal::from(-2),
        uom0,
        Some(Box::new(Number::new(Decimal::from(10), uom1, None))),
      )]),
      Qty::new(vec![Number::new(
        Decimal::from(-1),
        uom0,
        Some(Box::new(Number::new(Decimal::from(10), uom1, None))),
      )]),
    );

    // 6 of 10 + [-2 of 10, -5] = [3 of 10, 5]
    check_add(
      Qty::new(vec![Number::new(
        Decimal::from(6),
        uom0,
        Some(Box::new(Number::new(Decimal::from(10), uom1, None))),
      )]),
      Qty::new(vec![
        Number::new(
          Decimal::from(-2),
          uom0,
          Some(Box::new(Number::new(Decimal::from(10), uom1, None))),
        ),
        Number::new(Decimal::from(-5), uom1, None),
      ]),
      Qty::new(vec![
        Number::new(
          Decimal::from(3),
          uom0,
          Some(Box::new(Number::new(Decimal::from(10), uom1, None))),
        ),
        Number::new(Decimal::from(5), uom1, None),
      ]),
    );

    // 1 uom0 of 10 uom2 + 1 uom1 of 10 uom2 = [1 uom0 of 10 uom2, 1 uom1 of 10 uom2]
    check_add(
      Qty::new(vec![Number::new(
        Decimal::from(1),
        uom0,
        Some(Box::new(Number::new(Decimal::from(10), uom2, None))),
      )]),
      Qty::new(vec![Number::new(
        Decimal::from(1),
        uom1,
        Some(Box::new(Number::new(Decimal::from(10), uom2, None))),
      )]),
      Qty::new(vec![
        Number::new(
          Decimal::from(1),
          uom0,
          Some(Box::new(Number::new(Decimal::from(10), uom2, None))),
        ),
        Number::new(
          Decimal::from(1),
          uom1,
          Some(Box::new(Number::new(Decimal::from(10), uom2, None))),
        ),
      ]),
    );

    // 1 uom0 + 1 uom1 = [1 uom0, 1 uom1]
    check_add(
      Qty::new(vec![Number::new(Decimal::from(1), uom0, None)]),
      Qty::new(vec![Number::new(Decimal::from(1), uom1, None)]),
      Qty::new(vec![
        Number::new(Decimal::from(1), uom0, None),
        Number::new(Decimal::from(1), uom1, None),
      ]),
    );

    // 1 uom0 + (1 uom1 of 10 uom2) = [1 of 10, 1 uom0]
    check_add(
      Qty::new(vec![Number::new(Decimal::from(1), uom0, None)]),
      Qty::new(vec![Number::new(
        Decimal::from(1),
        uom1,
        Some(Box::new(Number::new(Decimal::from(10), uom2, None))),
      )]),
      Qty::new(vec![
        Number::new(
          Decimal::from(1),
          uom1,
          Some(Box::new(Number::new(Decimal::from(10), uom2, None))),
        ),
        Number::new(Decimal::from(1), uom0, None),
      ]),
    );

    // 1 of 10 + 1 of 11 = [1 of 10, 1 of 11]
    check_add(
      Qty::new(vec![Number::new(
        Decimal::from(1),
        uom0,
        Some(Box::new(Number::new(Decimal::from(10), uom1, None))),
      )]),
      Qty::new(vec![Number::new(
        Decimal::from(1),
        uom0,
        Some(Box::new(Number::new(Decimal::from(11), uom1, None))),
      )]),
      Qty::new(vec![
        Number::new(
          Decimal::from(1),
          uom0,
          Some(Box::new(Number::new(Decimal::from(10), uom1, None))),
        ),
        Number::new(
          Decimal::from(1),
          uom0,
          Some(Box::new(Number::new(Decimal::from(11), uom1, None))),
        ),
      ]),
    );

    // 2 of 10 of 100 + 2 of 10 of 99 = [2 of 10 of 100, 2 of 10 of 99]
    check_add(
      Qty::new(vec![Number::new(
        Decimal::from(2),
        uom0,
        Some(Box::new(Number::new(
          Decimal::from(10),
          uom1,
          Some(Box::new(Number::new(Decimal::from(100), uom2, None))),
        ))),
      )]),
      Qty::new(vec![Number::new(
        Decimal::from(2),
        uom0,
        Some(Box::new(Number::new(
          Decimal::from(10),
          uom1,
          Some(Box::new(Number::new(Decimal::from(99), uom2, None))),
        ))),
      )]),
      Qty::new(vec![
        Number::new(
          Decimal::from(2),
          uom0,
          Some(Box::new(Number::new(
            Decimal::from(10),
            uom1,
            Some(Box::new(Number::new(Decimal::from(100), uom2, None))),
          ))),
        ),
        Number::new(
          Decimal::from(2),
          uom0,
          Some(Box::new(Number::new(
            Decimal::from(10),
            uom1,
            Some(Box::new(Number::new(Decimal::from(99), uom2, None))),
          ))),
        ),
      ]),
    );

    // 2 + 3 = 5
    check_add(
      Qty::new(vec![Number::new(Decimal::from(2), uom0, None)]),
      Qty::new(vec![Number::new(Decimal::from(3), uom0, None)]),
      Qty::new(vec![Number::new(Decimal::from(5), uom0, None)]),
    );

    // 1 + 0 = 1
    check_add(
      Qty::new(vec![Number::new(Decimal::from(1), uom0, None)]),
      Qty::new(vec![Number::new(Decimal::from(0), uom0, None)]),
      Qty::new(vec![Number::new(Decimal::from(1), uom0, None)]),
    );

    // 1 + "void" = 1
    check_add(
      Qty::new(vec![Number::new(Decimal::from(1), uom0, None)]),
      Qty::default(),
      Qty::new(vec![Number::new(Decimal::from(1), uom0, None)]),
    );

    // (-1) + (-1) = -2
    check_add(
      Qty::new(vec![Number::new(Decimal::from(-1), uom0, None)]),
      Qty::new(vec![Number::new(Decimal::from(-1), uom0, None)]),
      Qty::new(vec![Number::new(Decimal::from(-2), uom0, None)]),
    );

    // (-1) + (2) = 1
    check_add(
      Qty::new(vec![Number::new(Decimal::from(-1), uom0, None)]),
      Qty::new(vec![Number::new(Decimal::from(2), uom0, None)]),
      Qty::new(vec![Number::new(Decimal::from(1), uom0, None)]),
    );

    // 1 of 10 + 1 of 10 = 2 of 10
    check_add(
      Qty::new(vec![Number::new(
        Decimal::from(1),
        uom0,
        Some(Box::new(Number::new(Decimal::from(10), uom1, None))),
      )]),
      Qty::new(vec![Number::new(
        Decimal::from(1),
        uom0,
        Some(Box::new(Number::new(Decimal::from(10), uom1, None))),
      )]),
      Qty::new(vec![Number::new(
        Decimal::from(2),
        uom0,
        Some(Box::new(Number::new(Decimal::from(10), uom1, None))),
      )]),
    );

    // (1 of 10) + (-1 of 10) = 0
    check_add(
      Qty::new(vec![Number::new(
        Decimal::from(1),
        uom0,
        Some(Box::new(Number::new(Decimal::from(10), uom1, None))),
      )]),
      Qty::new(vec![Number::new(
        Decimal::from(-1),
        uom0,
        Some(Box::new(Number::new(Decimal::from(10), uom1, None))),
      )]),
      Qty::default(),
    );

    // (-1 of 10) + (1 of 10) = 0
    check_add(
      Qty::new(vec![Number::new(
        Decimal::from(-1),
        uom0,
        Some(Box::new(Number::new(Decimal::from(10), uom1, None))),
      )]),
      Qty::new(vec![Number::new(
        Decimal::from(1),
        uom0,
        Some(Box::new(Number::new(Decimal::from(10), uom1, None))),
      )]),
      Qty::default(),
    );

    // (-1 of 10) + (-1 of 10) = (-2 of 10)
    check_add(
      Qty::new(vec![Number::new(
        Decimal::from(-1),
        uom0,
        Some(Box::new(Number::new(Decimal::from(10), uom1, None))),
      )]),
      Qty::new(vec![Number::new(
        Decimal::from(-1),
        uom0,
        Some(Box::new(Number::new(Decimal::from(10), uom1, None))),
      )]),
      Qty::new(vec![Number::new(
        Decimal::from(-2),
        uom0,
        Some(Box::new(Number::new(Decimal::from(10), uom1, None))),
      )]),
    );

    // (1 of 10) + (-2) = 8
    check_add(
      Qty::new(vec![Number::new(
        Decimal::from(1),
        uom0,
        Some(Box::new(Number::new(Decimal::from(10), uom1, None))),
      )]),
      Qty::new(vec![Number::new(Decimal::from(-2), uom1, None)]),
      Qty::new(vec![Number::new(Decimal::from(8), uom1, None)]),
    );

    // (-1 of 10) + (-2) = [-1 of 10, -2]
    check_add(
      Qty::new(vec![Number::new(
        Decimal::from(-1),
        uom0,
        Some(Box::new(Number::new(Decimal::from(10), uom1, None))),
      )]),
      Qty::new(vec![Number::new(Decimal::from(-2), uom1, None)]),
      Qty::new(vec![
        Number::new(
          Decimal::from(-1),
          uom0,
          Some(Box::new(Number::new(Decimal::from(10), uom1, None))),
        ),
        Number::new(Decimal::from(-2), uom1, None),
      ]),
    );

    // [-10 of 10, -5] + [2 of 10, 5] = [-8 of 10]
    check_add(
      Qty::new(vec![
        Number::new(
          Decimal::from(-10),
          uom0,
          Some(Box::new(Number::new(Decimal::from(10), uom1, None))),
        ),
        Number::new(Decimal::from(-5), uom1, None),
      ]),
      Qty::new(vec![
        Number::new(
          Decimal::from(2),
          uom0,
          Some(Box::new(Number::new(Decimal::from(10), uom1, None))),
        ),
        Number::new(Decimal::from(5), uom1, None),
      ]),
      Qty::new(vec![
        Number::new(
          Decimal::from(-8),
          uom0,
          Some(Box::new(Number::new(Decimal::from(10), uom1, None))),
        ),
        // Number::new(Decimal::from(5), uom1, None),
      ]),
    );

    // (-1 of 10) + (5) = -5 // this is wrong
    // (-1 of 10) + (5) = [-1 of 10, 5]
    check_add(
      Qty::new(vec![Number::new(
        Decimal::from(-1),
        uom0,
        Some(Box::new(Number::new(Decimal::from(10), uom1, None))),
      )]),
      Qty::new(vec![Number::new(Decimal::from(5), uom1, None)]),
      Qty::new(vec![
        Number::new(
          Decimal::from(-1),
          uom0,
          Some(Box::new(Number::new(Decimal::from(10), uom1, None))),
        ),
        Number::new(Decimal::from(5), uom1, None),
      ]),
    );

    // (5) + (-1 of 10) = [-1 of 10, 5]
    check_add(
      Qty::new(vec![Number::new(Decimal::from(5), uom1, None)]),
      Qty::new(vec![Number::new(
        Decimal::from(-1),
        uom0,
        Some(Box::new(Number::new(Decimal::from(10), uom1, None))),
      )]),
      Qty::new(vec![
        Number::new(
          Decimal::from(-1),
          uom0,
          Some(Box::new(Number::new(Decimal::from(10), uom1, None))),
        ),
        Number::new(Decimal::from(5), uom1, None),
      ]),
    );

    // [2 of 10, 5] - [10 of 10] = [-8 of 10, 5] the same as below
    // [-10 of 10] + [2 of 10, 5] = [-8 of 10, 5]
    check_add(
      Qty::new(vec![Number::new(
        Decimal::from(-10),
        uom0,
        Some(Box::new(Number::new(Decimal::from(10), uom1, None))),
      )]),
      Qty::new(vec![
        Number::new(
          Decimal::from(2),
          uom0,
          Some(Box::new(Number::new(Decimal::from(10), uom1, None))),
        ),
        Number::new(Decimal::from(5), uom1, None),
      ]),
      Qty::new(vec![
        Number::new(
          Decimal::from(-8),
          uom0,
          Some(Box::new(Number::new(Decimal::from(10), uom1, None))),
        ),
        Number::new(Decimal::from(5), uom1, None),
      ]),
    );

    // [2 of 10, 5] + [7 of 10, 5] = [9 of 10, 10]
    check_add(
      Qty::new(vec![
        Number::new(
          Decimal::from(2),
          uom0,
          Some(Box::new(Number::new(Decimal::from(10), uom1, None))),
        ),
        Number::new(Decimal::from(5), uom1, None),
      ]),
      Qty::new(vec![
        Number::new(
          Decimal::from(7),
          uom0,
          Some(Box::new(Number::new(Decimal::from(10), uom1, None))),
        ),
        Number::new(Decimal::from(5), uom1, None),
      ]),
      Qty::new(vec![
        Number::new(
          Decimal::from(9),
          uom0,
          Some(Box::new(Number::new(Decimal::from(10), uom1, None))),
        ),
        Number::new(Decimal::from(10), uom1, None),
      ]),
    );

    // [-2 of 10, -5] + [-7 of 10, -5] = [-9 of 10, -10]
    check_add(
      Qty::new(vec![
        Number::new(
          Decimal::from(-2),
          uom0,
          Some(Box::new(Number::new(Decimal::from(10), uom1, None))),
        ),
        Number::new(Decimal::from(-5), uom1, None),
      ]),
      Qty::new(vec![
        Number::new(
          Decimal::from(-7),
          uom0,
          Some(Box::new(Number::new(Decimal::from(10), uom1, None))),
        ),
        Number::new(Decimal::from(-5), uom1, None),
      ]),
      Qty::new(vec![
        Number::new(
          Decimal::from(-9),
          uom0,
          Some(Box::new(Number::new(Decimal::from(10), uom1, None))),
        ),
        Number::new(Decimal::from(-10), uom1, None),
      ]),
    );

    // [-2 of 10, -5] + [2 of 10, 5] = []
    check_add(
      Qty::new(vec![
        Number::new(
          Decimal::from(-2),
          uom0,
          Some(Box::new(Number::new(Decimal::from(10), uom1, None))),
        ),
        Number::new(Decimal::from(-5), uom1, None),
      ]),
      Qty::new(vec![
        Number::new(
          Decimal::from(2),
          uom0,
          Some(Box::new(Number::new(Decimal::from(10), uom1, None))),
        ),
        Number::new(Decimal::from(5), uom1, None),
      ]),
      Qty::default(),
    );
  }

  fn check_sub(left: Qty, right: Qty, check: Qty) {
    let result = &left - &right;
    println!("check_sub {result:?}");

    assert_eq!(result.inner.len(), check.inner.len());
    for i in 0..check.inner.len() {
      assert_eq!(result.inner[i].number, check.inner[i].number);
      assert_eq!(result.inner[i].name, check.inner[i].name);
    }
  }

  #[test]
  fn sub() {
    let uom0 = Uuid::new_v4();
    let uom1 = Uuid::new_v4();
    let uom2 = Uuid::new_v4();

    // [1 of 4, -1 of 3] - (-1 of 3) = (1 of 4)
    check_sub(
      Qty::new(vec![
        Number::new(
          Decimal::from(1),
          uom0,
          Some(Box::new(Number::new(Decimal::from(4), uom1, None))),
        ),
        Number::new(
          Decimal::from(-1),
          uom0,
          Some(Box::new(Number::new(Decimal::from(3), uom1, None))),
        ),
      ]),
      Qty::new(vec![Number::new(
        Decimal::from(-1),
        uom0,
        Some(Box::new(Number::new(Decimal::from(3), uom1, None))),
      )]),
      Qty::new(vec![Number::new(
        Decimal::from(1),
        uom0,
        Some(Box::new(Number::new(Decimal::from(4), uom1, None))),
      )]),
    );

    // (1 of 3) - [-1 of 4, 1 of 3] = (1 of 4)
    check_sub(
      Qty::new(vec![Number::new(
        Decimal::from(1),
        uom0,
        Some(Box::new(Number::new(Decimal::from(3), uom1, None))),
      )]),
      Qty::new(vec![
        Number::new(
          Decimal::from(-1),
          uom0,
          Some(Box::new(Number::new(Decimal::from(4), uom1, None))),
        ),
        Number::new(
          Decimal::from(1),
          uom0,
          Some(Box::new(Number::new(Decimal::from(3), uom1, None))),
        ),
      ]),
      Qty::new(vec![Number::new(
        Decimal::from(1),
        uom0,
        Some(Box::new(Number::new(Decimal::from(4), uom1, None))),
      )]),
    );

    // (-10 of 10) - (6) = [-10 of 10, -6]
    check_sub(
      Qty::new(vec![Number::new(
        Decimal::from(-10),
        uom0,
        Some(Box::new(Number::new(Decimal::from(10), uom1, None))),
      )]),
      Qty::new(vec![Number::new(Decimal::from(6), uom1, None)]),
      Qty::new(vec![
        Number::new(
          Decimal::from(-10),
          uom0,
          Some(Box::new(Number::new(Decimal::from(10), uom1, None))),
        ),
        Number::new(Decimal::from(-6), uom1, None),
      ]),
    );

    // (-6) - (10 of 10) = [-10 of 10, -6]
    check_sub(
      Qty::new(vec![Number::new(Decimal::from(-6), uom1, None)]),
      Qty::new(vec![Number::new(
        Decimal::from(10),
        uom0,
        Some(Box::new(Number::new(Decimal::from(10), uom1, None))),
      )]),
      Qty::new(vec![
        Number::new(
          Decimal::from(-10),
          uom0,
          Some(Box::new(Number::new(Decimal::from(10), uom1, None))),
        ),
        Number::new(Decimal::from(-6), uom1, None),
      ]),
    );

    // (10 of 10) - (-6) = [10 of 10, 6]
    check_sub(
      Qty::new(vec![Number::new(
        Decimal::from(10),
        uom0,
        Some(Box::new(Number::new(Decimal::from(10), uom1, None))),
      )]),
      Qty::new(vec![Number::new(Decimal::from(-6), uom1, None)]),
      Qty::new(vec![
        Number::new(
          Decimal::from(10),
          uom0,
          Some(Box::new(Number::new(Decimal::from(10), uom1, None))),
        ),
        Number::new(Decimal::from(6), uom1, None),
      ]),
    );

    // (6) - (-10 of 10) = [10 of 10, 6]
    check_sub(
      Qty::new(vec![Number::new(Decimal::from(6), uom1, None)]),
      Qty::new(vec![Number::new(
        Decimal::from(-10),
        uom0,
        Some(Box::new(Number::new(Decimal::from(10), uom1, None))),
      )]),
      Qty::new(vec![
        Number::new(
          Decimal::from(10),
          uom0,
          Some(Box::new(Number::new(Decimal::from(10), uom1, None))),
        ),
        Number::new(Decimal::from(6), uom1, None),
      ]),
    );

    // (-10 of 10) - (-6) = [-10 of 10, 6]
    check_sub(
      Qty::new(vec![Number::new(
        Decimal::from(-10),
        uom0,
        Some(Box::new(Number::new(Decimal::from(10), uom1, None))),
      )]),
      Qty::new(vec![Number::new(Decimal::from(-6), uom1, None)]),
      Qty::new(vec![
        Number::new(
          Decimal::from(-10),
          uom0,
          Some(Box::new(Number::new(Decimal::from(10), uom1, None))),
        ),
        Number::new(Decimal::from(6), uom1, None),
      ]),
    );

    // (6) - (10 of 10) = [-10 of 10, 6]
    check_sub(
      Qty::new(vec![Number::new(Decimal::from(6), uom1, None)]),
      Qty::new(vec![Number::new(
        Decimal::from(10),
        uom0,
        Some(Box::new(Number::new(Decimal::from(10), uom1, None))),
      )]),
      Qty::new(vec![
        Number::new(
          Decimal::from(-10),
          uom0,
          Some(Box::new(Number::new(Decimal::from(10), uom1, None))),
        ),
        Number::new(Decimal::from(6), uom1, None),
      ]),
    );

    // (-6) - (-10 of 10) =  [9 of 10, 4]
    check_sub(
      Qty::new(vec![Number::new(Decimal::from(-6), uom1, None)]),
      Qty::new(vec![Number::new(
        Decimal::from(-10),
        uom0,
        Some(Box::new(Number::new(Decimal::from(10), uom1, None))),
      )]),
      Qty::new(vec![
        Number::new(
          Decimal::from(9),
          uom0,
          Some(Box::new(Number::new(Decimal::from(10), uom1, None))),
        ),
        Number::new(Decimal::from(4), uom1, None),
      ]),
    );
    // (10 of 10) - (6) = [9 of 10, 4]
    check_sub(
      Qty::new(vec![Number::new(
        Decimal::from(10),
        uom0,
        Some(Box::new(Number::new(Decimal::from(10), uom1, None))),
      )]),
      Qty::new(vec![Number::new(Decimal::from(6), uom1, None)]),
      Qty::new(vec![
        Number::new(
          Decimal::from(9),
          uom0,
          Some(Box::new(Number::new(Decimal::from(10), uom1, None))),
        ),
        Number::new(Decimal::from(4), uom1, None),
      ]),
    );

    // *******************

    // [2 of 10, 5] - [10 of 10] = [-8 of 10, 5]
    check_sub(
      Qty::new(vec![
        Number::new(
          Decimal::from(2),
          uom0,
          Some(Box::new(Number::new(Decimal::from(10), uom1, None))),
        ),
        Number::new(Decimal::from(5), uom1, None),
      ]),
      Qty::new(vec![Number::new(
        Decimal::from(10),
        uom0,
        Some(Box::new(Number::new(Decimal::from(10), uom1, None))),
      )]),
      Qty::new(vec![
        Number::new(
          Decimal::from(-8),
          uom0,
          Some(Box::new(Number::new(Decimal::from(10), uom1, None))),
        ),
        Number::new(Decimal::from(5), uom1, None),
      ]),
    );

    // 1 of 10 - 2 of 10 = (-1 of 10)
    check_sub(
      Qty::new(vec![Number::new(
        Decimal::from(1),
        uom0,
        Some(Box::new(Number::new(Decimal::from(10), uom1, None))),
      )]),
      Qty::new(vec![Number::new(
        Decimal::from(2),
        uom0,
        Some(Box::new(Number::new(Decimal::from(10), uom1, None))),
      )]),
      Qty::new(vec![Number::new(
        Decimal::from(-1),
        uom0,
        Some(Box::new(Number::new(Decimal::from(10), uom1, None))),
      )]),
    );

    // [6 of 10] - [2 of 10, 5] = [3 of 10, 5]
    check_sub(
      Qty::new(vec![Number::new(
        Decimal::from(6),
        uom0,
        Some(Box::new(Number::new(Decimal::from(10), uom1, None))),
      )]),
      Qty::new(vec![
        Number::new(
          Decimal::from(2),
          uom0,
          Some(Box::new(Number::new(Decimal::from(10), uom1, None))),
        ),
        Number::new(Decimal::from(5), uom1, None),
      ]),
      Qty::new(vec![
        Number::new(
          Decimal::from(3),
          uom0,
          Some(Box::new(Number::new(Decimal::from(10), uom1, None))),
        ),
        Number::new(Decimal::from(5), uom1, None),
      ]),
    );

    // [7 of 10, 5] - [2 of 10, 5] = [5 of 10]
    check_sub(
      Qty::new(vec![
        Number::new(
          Decimal::from(7),
          uom0,
          Some(Box::new(Number::new(Decimal::from(10), uom1, None))),
        ),
        Number::new(Decimal::from(5), uom1, None),
      ]),
      Qty::new(vec![
        Number::new(
          Decimal::from(2),
          uom0,
          Some(Box::new(Number::new(Decimal::from(10), uom1, None))),
        ),
        Number::new(Decimal::from(5), uom1, None),
      ]),
      Qty::new(vec![Number::new(
        Decimal::from(5),
        uom0,
        Some(Box::new(Number::new(Decimal::from(10), uom1, None))),
      )]),
    );

    // 2 of 10 - 5 = [1 of 10, 5]
    check_sub(
      Qty::new(vec![Number::new(
        Decimal::from(2),
        uom0,
        Some(Box::new(Number::new(Decimal::from(10), uom1, None))),
      )]),
      Qty::new(vec![Number::new(Decimal::from(5), uom1, None)]),
      Qty::new(vec![
        Number::new(
          Decimal::from(1),
          uom0,
          Some(Box::new(Number::new(Decimal::from(10), uom1, None))),
        ),
        Number::new(Decimal::from(5), uom1, None),
      ]),
    );

    // 1 of 10 - 20 = (-1 of 10)
    check_sub(
      Qty::new(vec![Number::new(
        Decimal::from(1),
        uom0,
        Some(Box::new(Number::new(Decimal::from(10), uom1, None))),
      )]),
      Qty::new(vec![Number::new(Decimal::from(20), uom1, None)]),
      Qty::new(vec![Number::new(
        Decimal::from(-1),
        uom0,
        Some(Box::new(Number::new(Decimal::from(10), uom1, None))),
      )]),
    );

    // 3 - 1 = 2
    check_sub(
      Qty::new(vec![Number::new(Decimal::from(3), uom0, None)]),
      Qty::new(vec![Number::new(Decimal::from(1), uom0, None)]),
      Qty::new(vec![Number::new(Decimal::from(2), uom0, None)]),
    );

    // 1 - 1 = 0
    check_sub(
      Qty::new(vec![Number::new(Decimal::from(1), uom0, None)]),
      Qty::new(vec![Number::new(Decimal::from(1), uom0, None)]),
      Qty::default(),
    );

    // 2 - 3 = -1
    check_sub(
      Qty::new(vec![Number::new(Decimal::from(2), uom0, None)]),
      Qty::new(vec![Number::new(Decimal::from(3), uom0, None)]),
      Qty::new(vec![Number::new(Decimal::from(-1), uom0, None)]),
    );

    // 1 - 0 = 1
    check_sub(
      Qty::new(vec![Number::new(Decimal::from(1), uom0, None)]),
      Qty::new(vec![Number::new(Decimal::from(0), uom0, None)]),
      Qty::new(vec![Number::new(Decimal::from(1), uom0, None)]),
    );

    // 1 - "void" = 1
    check_sub(
      Qty::new(vec![Number::new(Decimal::from(1), uom0, None)]),
      Qty::default(),
      Qty::new(vec![Number::new(Decimal::from(1), uom0, None)]),
    );

    // 1 of 10 - 2 = 8
    check_sub(
      Qty::new(vec![Number::new(
        Decimal::from(1),
        uom0,
        Some(Box::new(Number::new(Decimal::from(10), uom1, None))),
      )]),
      Qty::new(vec![Number::new(Decimal::from(2), uom1, None)]),
      Qty::new(vec![Number::new(Decimal::from(8), uom1, None)]),
    );

    // 2 of 10 of 100 - 6 of 100 = [1 of 10 of 100, 4 of 100]
    check_sub(
      Qty::new(vec![Number::new(
        Decimal::from(2),
        uom0,
        Some(Box::new(Number::new(
          Decimal::from(10),
          uom1,
          Some(Box::new(Number::new(Decimal::from(100), uom2, None))),
        ))),
      )]),
      Qty::new(vec![Number::new(
        Decimal::from(6),
        uom1,
        Some(Box::new(Number::new(Decimal::from(100), uom2, None))),
      )]),
      Qty::new(vec![
        Number::new(
          Decimal::from(1),
          uom0,
          Some(Box::new(Number::new(
            Decimal::from(10),
            uom1,
            Some(Box::new(Number::new(Decimal::from(100), uom2, None))),
          ))),
        ),
        Number::new(
          Decimal::from(4),
          uom1,
          Some(Box::new(Number::new(Decimal::from(100), uom2, None))),
        ),
      ]),
    );

    // 1 uom0 - 1 uom1 = [1 uom0, -1 uom1]
    check_sub(
      Qty::new(vec![Number::new(Decimal::from(1), uom0, None)]),
      Qty::new(vec![Number::new(Decimal::from(1), uom1, None)]),
      Qty::new(vec![
        Number::new(Decimal::from(1), uom0, None),
        Number::new(Decimal::from(-1), uom1, None),
      ]),
    );

    // 2 of 10 of 100 - 5 of 99 = [2 of 10 of 100, -5 of 99]
    check_sub(
      Qty::new(vec![Number::new(
        Decimal::from(2),
        uom0,
        Some(Box::new(Number::new(
          Decimal::from(10),
          uom1,
          Some(Box::new(Number::new(Decimal::from(100), uom2, None))),
        ))),
      )]),
      Qty::new(vec![Number::new(
        Decimal::from(5),
        uom1,
        Some(Box::new(Number::new(Decimal::from(99), uom2, None))),
      )]),
      Qty::new(vec![
        Number::new(
          Decimal::from(2),
          uom0,
          Some(Box::new(Number::new(
            Decimal::from(10),
            uom1,
            Some(Box::new(Number::new(Decimal::from(100), uom2, None))),
          ))),
        ),
        Number::new(
          Decimal::from(-5),
          uom1,
          Some(Box::new(Number::new(Decimal::from(99), uom2, None))),
        ),
      ]),
    );

    // (-1) - (-1) = 0
    check_sub(
      Qty::new(vec![Number::new(Decimal::from(-1), uom0, None)]),
      Qty::new(vec![Number::new(Decimal::from(-1), uom0, None)]),
      Qty::default(),
    );

    // (1) - (-1) = 2
    check_sub(
      Qty::new(vec![Number::new(Decimal::from(1), uom0, None)]),
      Qty::new(vec![Number::new(Decimal::from(-1), uom0, None)]),
      Qty::new(vec![Number::new(Decimal::from(2), uom0, None)]),
    );

    // (-1) - (1) = -2
    check_sub(
      Qty::new(vec![Number::new(Decimal::from(-1), uom0, None)]),
      Qty::new(vec![Number::new(Decimal::from(1), uom0, None)]),
      Qty::new(vec![Number::new(Decimal::from(-2), uom0, None)]),
    );

    // (-1 of 10) - (-1 of 10) = 0
    check_sub(
      Qty::new(vec![Number::new(
        Decimal::from(-1),
        uom0,
        Some(Box::new(Number::new(Decimal::from(10), uom1, None))),
      )]),
      Qty::new(vec![Number::new(
        Decimal::from(-1),
        uom0,
        Some(Box::new(Number::new(Decimal::from(10), uom1, None))),
      )]),
      Qty::default(),
    );

    // (1 of 10) - (-1 of 10) = 2 of 10
    check_sub(
      Qty::new(vec![Number::new(
        Decimal::from(1),
        uom0,
        Some(Box::new(Number::new(Decimal::from(10), uom1, None))),
      )]),
      Qty::new(vec![Number::new(
        Decimal::from(-1),
        uom0,
        Some(Box::new(Number::new(Decimal::from(10), uom1, None))),
      )]),
      Qty::new(vec![Number::new(
        Decimal::from(2),
        uom0,
        Some(Box::new(Number::new(Decimal::from(10), uom1, None))),
      )]),
    );

    // (-1 of 10) - (1 of 10) = -2 of 10
    check_sub(
      Qty::new(vec![Number::new(
        Decimal::from(-1),
        uom0,
        Some(Box::new(Number::new(Decimal::from(10), uom1, None))),
      )]),
      Qty::new(vec![Number::new(
        Decimal::from(1),
        uom0,
        Some(Box::new(Number::new(Decimal::from(10), uom1, None))),
      )]),
      Qty::new(vec![Number::new(
        Decimal::from(-2),
        uom0,
        Some(Box::new(Number::new(Decimal::from(10), uom1, None))),
      )]),
    );

    // (1 of 10) - (1 of 10) = 0
    check_sub(
      Qty::new(vec![Number::new(
        Decimal::from(1),
        uom0,
        Some(Box::new(Number::new(Decimal::from(10), uom1, None))),
      )]),
      Qty::new(vec![Number::new(
        Decimal::from(1),
        uom0,
        Some(Box::new(Number::new(Decimal::from(10), uom1, None))),
      )]),
      Qty::default(),
    );

    // [3, 4] - 5 = 2
    check_sub(
      Qty::new(vec![
        Number::new(Decimal::from(3), uom0, None),
        Number::new(Decimal::from(4), uom0, None),
      ]),
      Qty::new(vec![Number::new(Decimal::from(5), uom0, None)]),
      Qty::new(vec![Number::new(Decimal::from(2), uom0, None)]),
    );
  }

  #[test]
  fn relax() {
    let uom0 = Uuid::new_v4();
    let uom1 = Uuid::new_v4();
    let uom2 = Uuid::new_v4();

    let data0 = Qty::new(vec![
      Number::new(Decimal::from(1), uom1, Some(Box::new(Number::new(Decimal::from(4), uom2, None)))),
      Number::new(
        Decimal::from(-1),
        uom1,
        Some(Box::new(Number::new(Decimal::from(3), uom2, None))),
      ),
    ]);

    let relax0 = data0.plus_with_relax(&Qty::new(vec![]));
    println!("relax0 {relax0:?}");

    let check0 = Qty::new(vec![Number::new(Decimal::ONE, uom2, None)]);

    assert_eq!(relax0, check0);
  }
}
