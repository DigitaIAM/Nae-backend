use crate::elements::UUID_NIL;
use crate::error::WHError;
use actix_web::body::MessageBody;
use json::JsonValue;
use rust_decimal::prelude::ToPrimitive;
use rust_decimal::Decimal;
use service::utils::json::JsonParams;
use std::ops::{Add, AddAssign, Deref, Neg, Sub, SubAssign};
use uuid::Uuid;

#[derive(Clone, Debug)]
enum Uom {
  In(Uuid, Option<Box<Qty>>),
}

#[derive(Clone, Debug, PartialEq)]
pub struct Qty {
  number: Decimal,
  name: Uom,
}

trait Convert {
  type Into;
  fn convert(&mut self, into: Self) -> Result<Vec<Self::Into>, WHError>;
}

impl Convert for Vec<(Decimal, Uuid)> {
  type Into = Qty;

  fn convert(&mut self, into: Self) -> Result<Vec<Self::Into>, WHError> {
    let mut result: Vec<Qty> = Vec::new();

    if self.len() >= into.len() {
      result.push(Qty::from_vec(self.clone())?);
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
              result.push(Qty::from_vec(tmp)?);
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

    result.push(Qty::from_vec(from.clone())?);
    Ok(result)
  }
}

impl Uom {
  fn uuid(&self) -> Uuid {
    match self {
      Uom::In(uuid, _) => uuid.clone(),
    }
  }

  fn qty(&self) -> Option<Box<Qty>> {
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

impl Qty {
  fn from_json(data: &JsonValue) -> Result<Self, WHError> {
    let mut data = data;

    let mut root = Qty { number: data["number"].number(), name: Uom::In(UUID_NIL, None) };

    let mut head = &mut root;

    while data.is_object() {
      let uom = &data["uom"];
      let mut tmp = Qty { number: Decimal::ZERO, name: Uom::In(UUID_NIL, None) };
      if uom.is_object() {
        tmp.number = uom["number"].number();
        tmp.name = Uom::In(UUID_NIL, None);
        head.name = Uom::In(uom["in"].uuid()?, Some(Box::new(tmp)));
        head = match &mut head.name {
          Uom::In(_, ref mut qty) => qty.as_mut().unwrap(),
          _ => unreachable!(),
        };
      } else {
        head.name = Uom::In(uom.uuid()?, None);
      }
      data = uom;
    }

    Ok(root)
  }

  fn from_vec(data: Vec<(Decimal, Uuid)>) -> Result<Self, WHError> {
    if data.len() < 1 {
      return Err(WHError::new("empty vector"));
    }
    let mut data = data.clone();
    data.reverse();

    let mut root = Qty { number: data[0].0, name: Uom::In(data[0].1, None) };

    let mut head = &mut root;

    for (num, id) in data[1..].iter() {
      let tmp = Qty { number: *num, name: Uom::In(*id, None) };
      head.name = Uom::In(head.name.uuid(), Some(Box::new(tmp)));
      head = match &mut head.name {
        Uom::In(_, ref mut qty) => qty.as_mut().unwrap(),
      };
    }

    Ok(root)
  }

  fn qty(&self) -> Option<Box<Qty>> {
    match &self.name {
      Uom::In(_, qty) => qty.clone(),
    }
  }

  fn depth(&self) -> Vec<(Decimal, Uuid)> {
    let mut result = Vec::new();

    result.insert(0, (self.number, self.name.uuid()));

    let mut current = self.clone();

    while let Some(qty) = current.qty() {
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
}

impl Add for Qty {
  type Output = Vec<Self>;

  fn add(self, rhs: Self) -> Self::Output {
    let mut vector: Vec<Self> = Vec::new();

    let mut is_equal = true;

    if self.name != rhs.name {
      is_equal = false;
    } else {
      let mut left = Uom::In(self.name.uuid(), Some(Box::new(self.clone())));
      let mut right = Uom::In(self.name.uuid(), Some(Box::new(rhs.clone())));

      while left.is_some() && right.is_some() {
        left = match left {
          Uom::In(uuid, qty) => {
            if let Some(q) = qty {
              match q.name {
                Uom::In(inner_uuid, inner_qty) => Uom::In(inner_uuid, inner_qty),
              }
            } else {
              Uom::In(uuid, None)
            }
          },
        };

        right = match right {
          Uom::In(uuid, qty) => {
            if let Some(q) = qty {
              match q.name {
                Uom::In(inner_uuid, inner_qty) => Uom::In(inner_uuid, inner_qty),
              }
            } else {
              Uom::In(uuid, None)
            }
          },
        };

        if left != right {
          is_equal = false;
          break;
        }
      }
    }

    if is_equal {
      let mut result = self.clone();
      result.number += rhs.number;
      vector.push(result);
    } else {
      vector.push(self);
      vector.push(rhs);
    }

    vector
  }
}

impl Neg for Qty {
  type Output = Self;

  fn neg(self) -> Self::Output {
    Qty { number: -self.number, name: self.name }
  }
}

impl Sub for Qty {
  type Output = Result<Vec<Self>, WHError>;

  fn sub(self, rhs: Self) -> Self::Output {
    let mut left = self.depth();
    let mut right = rhs.depth();
    let mut index = 0;

    for (l, r) in left.iter().zip(right.iter()) {
      // compare name and number parts if it's not the last value
      if index + 1 < left.len() && index + 1 < right.len() {
        if l != r {
          return Ok(vec![self, -rhs]);
        }
        index += 1;
      } else {
        // compare only name part if it's the last value
        let (_, left_id) = l;
        let (_, right_id) = r;
        if left_id != right_id {
          return Ok(vec![self, -rhs]);
        }
      }
    }

    // println!("index= {index}");

    let full = left.clone();

    if left.len() > index + 1 {
      Qty::simplify(&mut left, index);
    } else if right.len() > index + 1 {
      Qty::simplify(&mut right, index);
    }

    let mut result = Vec::new();

    if left.len() == right.len() {
      result = Qty::subtract(full, left, right)?;
    }

    Ok(result)
  }
}

#[cfg(test)]
mod tests {
  use crate::elements::{ToJson, UUID_MAX};
  use crate::qty::UUID_NIL;
  use crate::qty::{Qty, Uom};
  use json::object;
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

    let qty0 = Qty::from_json(&data0).unwrap();
    // println!("{qty0:?}");

    assert_eq!(qty0.number, Decimal::from(1));
    assert_eq!(qty0.name, Uom::In(u0, None));

    let data1 = object! {
      "number": 1,
      "uom": object! {
        "number": 2,
        "uom": u1.to_json(),
        "in": u0.to_json(),
      },
    };

    let qty1 = Qty::from_json(&data1).unwrap();
    // println!("{qty1:?}");

    assert_eq!(qty1.number, Decimal::from(1));
    assert_eq!(
      qty1.name,
      Uom::In(u0, Some(Box::new(Qty { number: Decimal::from(2), name: Uom::In(u1, None) })))
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

    let qty2 = Qty::from_json(&data2).unwrap();
    // println!("{qty2:?}");

    assert_eq!(qty2.number, Decimal::from(2));
    assert_eq!(
      qty2.name,
      Uom::In(
        u0,
        Some(Box::new(Qty {
          number: Decimal::from(10),
          name: Uom::In(
            u1,
            Some(Box::new(Qty { number: Decimal::from(100), name: Uom::In(u2, None) }))
          ),
        }))
      )
    );
  }

  #[test]
  fn find_depth() {
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

    let qty0 = Qty::from_json(&data0).unwrap();

    let result = qty0.depth();
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

    let qty2 = Qty::from_json(&data2).unwrap();
    let mut vector = qty2.depth();
    assert_eq!(vector.len(), 3);
    // println!("{vector:?}");

    Qty::simplify(&mut vector, 1);

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

    let qty0 = Qty::from_json(&data0).unwrap();
    // println!("{qty0:?}");

    let data1 = object! {
      "number": 3,
      "uom": UUID_NIL.to_json(),
    };

    let qty1 = Qty::from_json(&data1).unwrap();
    // println!("{qty1:?}");

    let res0 = qty0 + qty1;
    // println!("res= {res0:?}");

    assert_eq!(res0[0].number, Decimal::from(5));

    let data2 = object! {
      "number": 1,
      "uom": object! {
        "number": 10,
        "uom": UUID_NIL.to_json(),
        "in": UUID_MAX.to_json(),
      },
    };

    let qty2 = Qty::from_json(&data2).unwrap();
    // println!("{qty2:?}");

    let data3 = object! {
      "number": 1,
      "uom": object! {
        "number": 10,
        "uom": UUID_NIL.to_json(),
        "in": UUID_MAX.to_json(),
      },
    };

    let qty3 = Qty::from_json(&data3).unwrap();
    // println!("{qty3:?}");

    let res1 = qty2 + qty3;
    // println!("res= {res1:?}");

    assert_eq!(res1[0].number, Decimal::from(2));
  }

  #[test]
  fn add_failure() {
    // different uuids
    let data0 = object! {
      "number": 2,
      "uom": UUID_NIL.to_json(),
    };

    let qty0 = Qty::from_json(&data0).unwrap();
    // println!("{qty0:?}");

    let data1 = object! {
      "number": 2,
      "uom": UUID_MAX.to_json(),
    };

    let qty1 = Qty::from_json(&data1).unwrap();
    // println!("{qty1:?}");

    let res0 = qty0 + qty1;
    // println!("res= {res0:?}");

    assert_eq!(res0[0].number, Decimal::from(2));
    assert_eq!(res0[1].number, Decimal::from(2));

    // different numbers
    let data2 = object! {
      "number": 1,
      "uom": object! {
        "number": 10,
        "uom": UUID_NIL.to_json(),
        "in": UUID_MAX.to_json(),
      },
    };

    let qty2 = Qty::from_json(&data2).unwrap();
    // println!("{qty2:?}");

    let data3 = object! {
      "number": 1,
      "uom": object! {
        "number": 11,
        "uom": UUID_NIL.to_json(),
        "in": UUID_MAX.to_json(),
      },
    };

    let qty3 = Qty::from_json(&data3).unwrap();
    // println!("{qty3:?}");

    let res1 = qty2 + qty3;

    // println!("res= {res1:?}");

    assert_eq!(res1[0].number, Decimal::from(1));
    assert_eq!(res1[1].number, Decimal::from(1));

    // different numbers
    let data4 = object! {
      "number": 2,
      "uom": object! {
        "number": 10,
        "uom": object! {
          "number": 100,
          "uom": UUID_NIL.to_json(),
          "in": UUID_NIL.to_json(),
        },
        "in": UUID_MAX.to_json(),
      },
    };

    let qty4 = Qty::from_json(&data4).unwrap();
    // println!("{qty4:?}");

    let data5 = object! {
      "number": 2,
      "uom": object! {
        "number": 10,
        "uom": object! {
          "number": 99,
          "uom": UUID_NIL.to_json(),
          "in": UUID_NIL.to_json(),
        },
        "in": UUID_MAX.to_json(),
      },
    };

    let qty5 = Qty::from_json(&data5).unwrap();
    // println!("{qty5:?}");

    let res2 = qty4 + qty5;
    // println!("res= {res2:?}");

    assert_eq!(res2[0].number, Decimal::from(2));
    assert_eq!(res2[1].number, Decimal::from(2));
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

    let qty0 = Qty::from_json(&data0).unwrap();
    // println!("{qty0:?}");

    let data1 = object! {
      "number": 2,
      "uom": u0.to_json(),
    };

    let qty1 = Qty::from_json(&data1).unwrap();
    // println!("{qty1:?}");

    let res0 = (qty0 - qty1).unwrap();
    // println!("res= {res0:?}");

    assert_eq!(res0.len(), 1);
    assert_eq!(res0[0].number, Decimal::from(8));

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

    let qty2 = Qty::from_json(&data2).unwrap();
    // println!("{qty2:?}");

    let data3 = object! {
      "number": 5,
      "uom": object! {
        "number": 100,
        "uom": u2.to_json(),
        "in": u1.to_json(),
      },
    };

    let qty3 = Qty::from_json(&data3).unwrap();
    // println!("{qty3:?}");

    let res1 = (qty2 - qty3).unwrap();
    // println!("res= {res1:?}");

    assert_eq!(res1.len(), 2);
    assert_eq!(res1[0].number, Decimal::from(5));

    let qty3 = res1[1].clone();
    assert_eq!(qty3.number, Decimal::from(1));

    let qty3_inner = qty3.name.qty().unwrap();
    assert_eq!(qty3_inner.number, Decimal::from(10));

    let qty3_inner_inner = qty3_inner.name.qty().unwrap();
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

    let qty0 = Qty::from_json(&data0).unwrap();
    // println!("{qty0:?}");

    let data1 = object! {
      "number": 2,
      "uom": u1.to_json(),
    };

    let qty1 = Qty::from_json(&data1).unwrap();
    // println!("{qty1:?}");

    let res0 = (qty0 - qty1).unwrap();
    // println!("res= {res0:?}");

    assert_eq!(res0.len(), 2);
    assert_eq!(res0[0].number, Decimal::from(3));
    assert_eq!(res0[1].number, Decimal::from(-2));

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

    let qty2 = Qty::from_json(&data2).unwrap();
    // println!("{qty2:?}");

    let data3 = object! {
      "number": 5,
      "uom": object! {
        "number": 99,
        "uom": u2.to_json(),
        "in": u1.to_json(),
      },
    };

    let qty3 = Qty::from_json(&data3).unwrap();
    // println!("{qty3:?}");

    let res1 = (qty2 - qty3).unwrap();
    // println!("res= {res1:?}");

    assert_eq!(res1.len(), 2);
    assert_eq!(res1[0].number, Decimal::from(2));
    assert_eq!(res1[1].number, Decimal::from(-5));
  }
}
