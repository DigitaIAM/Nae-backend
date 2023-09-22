use crate::balance::{BalanceForGoods, Cost, Price};
use crate::elements::{ToJson, UUID_NIL};
use crate::error::WHError;
use actix_web::body::MessageBody;
use json::{object, JsonValue};
use rust_decimal::prelude::{ToPrimitive, Zero};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use service::utils::json::JsonParams;
use std::ops::{Add, AddAssign, Deref, Div, Mul, Neg, Sub, SubAssign};
use uuid::Uuid;

#[derive(Clone, Debug, PartialOrd, Eq, Hash, Serialize, Deserialize)]
pub enum Uom {
  In(Uuid, Option<Box<Number<Uom>>>),
}

#[derive(Clone, Debug, Default, PartialEq, PartialOrd, Eq, Hash, Serialize, Deserialize)]
pub struct Number<N> {
  number: Decimal,
  name: N,
}

#[derive(Clone, Debug, Default, PartialEq, PartialOrd, Eq, Hash, Serialize, Deserialize)]
pub struct Qty {
  inner: Vec<Number<Uom>>,
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

  fn depth(&self) -> usize {
    let mut result = 0;
    let mut data = self.named().clone();

    while let Some(qty) = data.as_ref() {
      result += 1;
      data = qty.name.named();
    }

    result
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
      if uom.is_object() {
        tmp.number = uom["number"].number();
        tmp.name = Uom::In(UUID_NIL, None);
        head.name = Uom::In(uom["in"].uuid().unwrap_or_default(), Some(Box::new(tmp)));
        head = match &mut head.name {
          Uom::In(_, ref mut qty) => qty.as_mut().unwrap(),
          _ => unreachable!(),
        };
      } else {
        head.name = Uom::In(uom.uuid().unwrap_or_default(), None);
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

  pub(crate) fn number(&self) -> Decimal {
    self.number
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

  fn subtract(
    full: Vec<(Decimal, Uuid)>,
    mut left: Vec<(Decimal, Uuid)>,
    right: Vec<(Decimal, Uuid)>,
  ) -> Result<Vec<Self>, WHError> {
    if let (Some(l_last), Some(r_last)) = (left.last_mut(), right.last()) {
      let (last_number, last_uuid) = *l_last;
      let number = last_number - r_last.0;
      if number == Decimal::ZERO {
        return Ok(Vec::new());
      }
      *l_last = (number, last_uuid);
    }

    left.convert(full)
  }

  pub fn is_zero(&self) -> bool {
    if self.number.is_zero() {
      true
    } else {
      false
    } // TODO is this correct?
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
          if left.name == right.name {
            return Some(left.name);
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
}

impl Neg for Number<Uom> {
  type Output = Self;

  fn neg(self) -> Self::Output {
    Number { number: -self.number, name: self.name }
  }
}

impl Sub for Number<Uom> {
  type Output = Qty;

  fn sub(self, rhs: Self) -> Self::Output {
    let mut left = self.to_vec();
    let mut right = rhs.to_vec();
    let mut index = 0;

    for (l, r) in left.iter().zip(right.iter()) {
      // compare name and number parts if it's not the last value
      if index + 1 < left.len() && index + 1 < right.len() {
        if l != r {
          return Qty { inner: vec![self, -rhs] };
        }
        index += 1;
      } else {
        // compare only name part if it's the last value
        let (_, left_id) = l;
        let (_, right_id) = r;
        if left_id != right_id {
          return Qty { inner: vec![self, -rhs] };
        }
      }
    }

    let full = left.clone();

    if left.len() > index + 1 {
      Number::simplify(&mut left, index);
    } else if right.len() > index + 1 {
      Number::simplify(&mut right, index);
    }

    let mut result = Qty { inner: Vec::new() };

    if left.len() == right.len() {
      result.inner = Number::subtract(full, left, right).unwrap_or_default();
    }

    result
  }
}

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
      false
    }
  }

  pub(crate) fn lowering(&self, name: &Uom) -> Option<Number<Uom>> {
    let mut result = Number::new(Decimal::ZERO, name.uuid(), name.named());

    'outer: for qty in &self.inner {
      if name.depth() > qty.name.depth() {
        return None;
      }

      if &qty.name == name {
        result.number += qty.number;
        continue;
      }
      let mut tmp = qty.clone();
      while let Some(mut inner_qty) = tmp.named() {
        inner_qty.number *= tmp.number;

        if &inner_qty.name == name {
          result.number += inner_qty.number;
          continue 'outer;
        }

        if inner_qty.named().is_none() {
          return None;
        }

        tmp = *inner_qty;
      }
    }

    Some(result)
  }

  pub(crate) fn abs(&self) -> Self {
    let mut result = self.clone();

    for mut qty in &mut result.inner {
      if qty.number.is_sign_negative() {
        qty.number = -qty.number;
      }
    }
    result
  }

  pub fn common(&self, rhs: &Self) -> Option<Uom> {
    let mut result: Option<Uom> = None;

    for left in &self.inner {
      for right in &rhs.inner {
        if let Some(common) = left.common(&right) {
          // TODO replace unwrap?
          if result.is_none() || common.depth() >= result.clone().unwrap().depth() {
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
    if self.is_zero() {
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
    let common = self.common(&rhs);

    if let Some(uom) = common {
      let left = self.lowering(&uom).unwrap().number;
      let right = rhs.lowering(&uom).unwrap().number;
      Ok(left >= right)
    } else {
      Err(WHError::new("two Qty don't have common part"))
    }
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
    let mut vector = Qty { inner: Vec::new() };

    let mut rhs = rhs.inner.clone();

    for left in &self.inner {
      let mut index = usize::MAX;
      for (i, right) in rhs.iter().enumerate() {
        if left.name == right.name {
          let mut result = left.clone();
          result.number += right.number;
          vector.inner.push(result);
          index = i;
          break;
        }
      }
      if index != usize::MAX {
        rhs.remove(index);
      } else {
        vector.inner.push(left.clone());
      }
    }

    for right in rhs {
      vector.inner.push(right);
    }
    vector
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
    let mut result = Qty { inner: Vec::new() };
    let mut vec_left = self.inner.clone();
    let mut vec_right = rhs.inner.clone();
    let mut untouched_left: Option<Number<Uom>> = None;

    let mut find_difference = |index: &mut usize,
                               i,
                               left: &Number<Uom>,
                               right: &mut Number<Uom>,
                               inner: &mut Vec<Number<Uom>>| {
      *index = i;
      let mut diff = left.clone() - right.clone();
      if !left.is_negative() && diff.is_negative() {
        for qty in diff.inner {
          if qty.is_negative() {
            *right = qty;
            return false;
          }
        }
      } else {
        inner.append(&mut diff.inner);
      }
      true
    };

    for right in vec_right {
      let mut index = usize::MAX;
      let mut right = right.clone();
      for (i, left) in vec_left.iter().enumerate() {
        if i == vec_left.len() - 1 && right.is_negative() {
          right = -right
        }
        if left.name == right.name {
          if find_difference(&mut index, i, left, &mut right, &mut result.inner) {
            break;
          } else {
            continue;
          }
        } else {
          untouched_left = Some(left.clone());
          if let Some(left_inner) = left.named() {
            if left_inner.name == right.name {
              if find_difference(&mut index, i, left, &mut right, &mut result.inner) {
                break;
              } else {
                continue;
              }
            }
          } else if let Some(right_inner) = right.named() {
            if left.name == right_inner.name {
              if find_difference(&mut index, i, left, &mut right, &mut result.inner) {
                break;
              } else {
                continue;
              }
            }
          }
        }
      }

      if index != usize::MAX {
        vec_left.remove(index);
        if right.is_negative() {
          if vec_left.is_empty() {
            result.inner.push(right.clone());
          } else {
            result.inner.push(-right.clone());
          }
        }
      } else {
        if let Some(untouched) = &untouched_left {
          result.inner.push(untouched.clone());
        }
        result.inner.push(-right.clone());
      }
    }
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
  use crate::elements::{ToJson, UUID_MAX, UUID_NIL};
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
  fn to_json() {
    let u0 = Uuid::new_v4();
    let u1 = Uuid::new_v4();
    let u2 = Uuid::new_v4();

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
  fn add_success() {
    let data0 = object! {
      "number": 2,
      "uom": UUID_NIL.to_json(),
    };

    let qty0: Qty = data0.try_into().unwrap();
    // println!("{qty0:?}");

    let data1 = object! {
      "number": 3,
      "uom": UUID_NIL.to_json(),
    };

    let qty1: Qty = data1.try_into().unwrap();
    // println!("{qty1:?}");

    let res0 = &qty0 + &qty1;
    // println!("res0= {res0:?}");

    assert_eq!(res0.inner.len(), 1);
    assert_eq!(res0.inner[0].number, Decimal::from(5));

    let data2 = object! {
      "number": 1,
      "uom": object! {
        "number": 10,
        "uom": UUID_NIL.to_json(),
        "in": UUID_MAX.to_json(),
      },
    };

    let qty2: Qty = data2.try_into().unwrap();
    // println!("{qty2:?}");

    let data3 = object! {
      "number": 1,
      "uom": object! {
        "number": 10,
        "uom": UUID_NIL.to_json(),
        "in": UUID_MAX.to_json(),
      },
    };

    let qty3: Qty = data3.try_into().unwrap();
    // println!("{qty3:?}");

    let res1 = &qty2 + &qty3;
    // println!("res1= {res1:?}");

    assert_eq!(res1.inner.len(), 1);
    assert_eq!(res1.inner[0].number, Decimal::from(2));
    assert_eq!(res1.inner[0].name.named().unwrap().number, Decimal::from(10));
  }

  #[test]
  fn add_failure() {
    let u0 = Uuid::new_v4();
    let u1 = Uuid::new_v4();
    let u2 = Uuid::new_v4();

    // different uuids
    let data0 = object! {
      "number": 2,
      "uom": u0.to_json(),
    };

    let qty0: Qty = data0.try_into().unwrap();
    // println!("{qty0:?}");

    let data1 = object! {
      "number": 2,
      "uom": u1.to_json(),
    };

    let qty1: Qty = data1.try_into().unwrap();
    // println!("{qty1:?}");

    let res0 = &qty0 + &qty1;
    // println!("res= {res0:?}");

    assert_eq!(res0.inner.len(), 2);
    assert_eq!(res0.inner[0].number, Decimal::from(2));
    assert_eq!(res0.inner[1].number, Decimal::from(2));

    // different numbers
    let data2 = object! {
      "number": 1,
      "uom": object! {
        "number": 10,
        "uom": u1.to_json(),
        "in": u0.to_json(),
      },
    };

    let qty2: Qty = data2.try_into().unwrap();
    // println!("{qty2:?}");

    let data3 = object! {
      "number": 1,
      "uom": object! {
        "number": 11,
        "uom": u1.to_json(),
        "in": u0.to_json(),
      },
    };

    let qty3: Qty = data3.try_into().unwrap();
    // println!("{qty3:?}");

    let res1 = &qty2 + &qty3;

    // println!("res= {res1:?}");

    assert_eq!(res1.inner.len(), 2);
    assert_eq!(res1.inner[0].number, Decimal::from(1));
    assert_eq!(res1.inner[1].number, Decimal::from(1));

    // different numbers
    let data4 = object! {
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

    let qty4: Qty = data4.try_into().unwrap();
    // println!("{qty4:?}");

    let data5 = object! {
      "number": 2,
      "uom": object! {
        "number": 10,
        "uom": object! {
          "number": 99,
          "uom": u2.to_json(),
          "in": u1.to_json(),
        },
        "in": u0.to_json(),
      },
    };

    let qty5: Qty = data5.try_into().unwrap();
    // println!("{qty5:?}");

    let res2 = &qty4 + &qty5;
    // println!("res= {res2:?}");

    assert_eq!(res2.inner.len(), 2);
    assert_eq!(res2.inner[0].number, Decimal::from(2));
    assert_eq!(res2.inner[1].number, Decimal::from(2));
  }

  #[test]
  fn sub_success() {
    let u0 = Uuid::new_v4();
    let u1 = Uuid::new_v4();
    let u2 = Uuid::new_v4();

    let data0 = object! {
      "number": 1,
      "uom": object! {
        "number": 10,
        "uom": u0.to_json(),
        "in": u1.to_json(),
      },
    };

    let qty0: Qty = data0.try_into().unwrap();
    // println!("{qty0:?}");

    let data1 = object! {
      "number": 2,
      "uom": u0.to_json(),
    };

    let qty1: Qty = data1.try_into().unwrap();
    // println!("{qty1:?}");

    let res0 = &qty0 - &qty1;
    // println!("res= {res0:?}");

    assert_eq!(res0.inner.len(), 1);
    assert_eq!(res0.inner[0].number, Decimal::from(8));

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

    let data3 = object! {
      "number": 5,
      "uom": object! {
        "number": 100,
        "uom": u2.to_json(),
        "in": u1.to_json(),
      },
    };

    let qty3: Qty = data3.try_into().unwrap();
    // println!("{qty3:?}");

    let res1 = &qty2 - &qty3;
    // println!("res= {res1:?}");

    assert_eq!(res1.inner.len(), 2);
    assert_eq!(res1.inner[0].number, Decimal::from(5));

    let qty3 = res1.inner[1].clone();
    assert_eq!(qty3.number, Decimal::from(1));

    let qty3_inner = qty3.name.named().unwrap();
    assert_eq!(qty3_inner.number, Decimal::from(10));

    let qty3_inner_inner = qty3_inner.name.named().unwrap();
    assert_eq!(qty3_inner_inner.number, Decimal::from(100));
  }

  #[test]
  fn sub_failure() {
    let u0 = Uuid::new_v4();
    let u1 = Uuid::new_v4();
    let u2 = Uuid::new_v4();

    let data0 = object! {
      "number": 3,
      "uom": u0.to_json(),
    };

    let qty0: Qty = data0.try_into().unwrap();
    // println!("{qty0:?}");

    let data1 = object! {
      "number": 2,
      "uom": u1.to_json(),
    };

    let qty1: Qty = data1.try_into().unwrap();
    // println!("{qty1:?}");

    let res0 = &qty0 - &qty1;
    // println!("res= {res0:?}");

    assert_eq!(res0.inner.len(), 2);
    assert_eq!(res0.inner[0].number, Decimal::from(3));
    assert_eq!(res0.inner[1].number, Decimal::from(-2));

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

    let data3 = object! {
      "number": 5,
      "uom": object! {
        "number": 99,
        "uom": u2.to_json(),
        "in": u1.to_json(),
      },
    };

    let qty3: Qty = data3.try_into().unwrap();
    // println!("{qty3:?}");

    let res1 = &qty2 - &qty3;
    // println!("res= {res1:?}");

    assert_eq!(res1.inner.len(), 2);
    assert_eq!(res1.inner[0].number, Decimal::from(2));
    assert_eq!(res1.inner[1].number, Decimal::from(-5));
  }

  #[test]
  fn sub_neg_result() {
    let u0 = Uuid::new_v4();
    let u1 = Uuid::new_v4();
    let u2 = Uuid::new_v4();

    let data0 = object! {
      "number": 2,
      "uom": u0.to_json(),
    };

    let qty0: Qty = data0.try_into().unwrap();
    // println!("{qty0:?}");

    let data1 = object! {
      "number": 3,
      "uom": u0.to_json(),
    };

    let qty1: Qty = data1.try_into().unwrap();
    // println!("{qty1:?}");

    let res0 = &qty0 - &qty1;
    // println!("res0= {res0:?}");

    assert_eq!(res0.inner.len(), 1);
    assert_eq!(res0.inner[0].number, Decimal::from(-1));

    let data2 = array![
      object! {
        "number": 3,
        "uom": u0.to_json(),
      },
      object! {
        "number": 4,
        "uom": u0.to_json(),
      },
    ];

    let qty2: Qty = data2.try_into().unwrap();
    // println!("{qty0:?}");

    let data3 = object! {
      "number": 5,
      "uom": u0.to_json(),
    };

    let qty3: Qty = data3.try_into().unwrap();
    // println!("{qty1:?}");

    let res1 = &qty2 - &qty3;
    // println!("res1= {res1:?}");

    assert_eq!(res1.inner.len(), 1);
    assert_eq!(res1.inner[0].number, Decimal::from(2));
  }

  #[test]
  fn lowering() {
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

    // two structs with depth == 0
    let data0 = object! {
      "number": 1,
      "uom": u2.to_json(),
    };

    let qty0: Qty = data0.try_into().unwrap();

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

    let qty4: Qty = data4.try_into().unwrap();

    let data5 = object! {
      "number": 20,
      "uom": object! {
        "number": 100,
        "uom": u2.to_json(),
        "in": u1.to_json(),
      },

    };

    let qty5: Qty = data5.try_into().unwrap();

    let common2 = qty4.common(&qty5).unwrap();
    // println!("common2 {common2:?}");

    assert_eq!(common2, qty5.inner[0].name);
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
}
