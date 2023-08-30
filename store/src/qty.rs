use crate::elements::UUID_NIL;
use crate::error::WHError;
use actix_web::body::MessageBody;
use json::JsonValue;
use rust_decimal::Decimal;
use service::utils::json::JsonParams;
use std::ops::{Add, AddAssign, Sub, SubAssign};
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
  fn convert(&self, into: Uom) -> Decimal;
}

impl Uom {
  fn uuid(&self) -> Uuid {
    match self {
      Uom::In(uuid, _) => uuid.clone(),
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
          } else {
            left_id == right_id
          }
        },
      },
    }
  }

  fn ne(&self, other: &Self) -> bool {
    match self {
      Uom::In(left_id, left_qty) => match other {
        Uom::In(right_id, right_qty) => {
          if let (Some(l), Some(r)) = (left_qty, right_qty) {
            if left_id == right_id && l.name == r.name {
              false
            } else {
              true
            }
          } else {
            left_id != right_id
          }
        },
      },
    }
  }
}

impl Qty {
  fn from(data: &JsonValue) -> Result<Self, WHError> {
    let mut data = data.clone();

    let mut root = Qty { number: data["number"].number(), name: Uom::In(UUID_NIL, None) };

    let mut head = &mut root;

    while data.is_object() {
      let uom = data["uom"].clone();
      let mut tmp = Qty { number: Decimal::ZERO, name: Uom::In(UUID_NIL, None) };
      if uom.is_object() {
        tmp.number = uom["number"].number();
        tmp.name = Uom::In(UUID_NIL, None);
        head.name = Uom::In(uom["in"].uuid()?, Some(Box::new(tmp.clone())));
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
}

impl Add for Qty {
  type Output = Vec<Self>;

  fn add(self, rhs: Self) -> Self::Output {
    let mut result = self.clone();
    let mut vector: Vec<Self> = Vec::new();

    let mut left = Uom::In(self.name.uuid(), Some(Box::new(self.clone())));
    let mut right = Uom::In(self.name.uuid(), Some(Box::new(rhs.clone())));

    let mut is_equal = true;

    while left.is_some() && right.is_some() {
      if left != right {
        is_equal = false;
        break;
      }

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
    }

    if is_equal {
      result.number += rhs.number;
      vector.push(result);
    } else {
      vector.push(self);
      vector.push(rhs);
    }

    vector
  }
}

impl AddAssign for Qty {
  fn add_assign(&mut self, rhs: Self) {
    todo!()
  }
}

impl Sub for Qty {
  type Output = Self;

  fn sub(self, rhs: Self) -> Self::Output {
    todo!()
  }
}

impl SubAssign for Qty {
  fn sub_assign(&mut self, rhs: Self) {
    todo!()
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

    let qty0 = Qty::from(&data0).unwrap();
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

    let qty1 = Qty::from(&data1).unwrap();
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

    let qty2 = Qty::from(&data2).unwrap();
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
  fn add_equals() {
    let data0 = object! {
      "number": 2,
      "uom": UUID_NIL.to_json(),
    };

    let qty0 = Qty::from(&data0).unwrap();
    // println!("{qty0:?}");

    let data1 = object! {
      "number": 3,
      "uom": UUID_NIL.to_json(),
    };

    let qty1 = Qty::from(&data1).unwrap();
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

    let qty2 = Qty::from(&data2).unwrap();
    // println!("{qty2:?}");

    let data3 = object! {
      "number": 1,
      "uom": object! {
        "number": 10,
        "uom": UUID_NIL.to_json(),
        "in": UUID_MAX.to_json(),
      },
    };

    let qty3 = Qty::from(&data3).unwrap();
    // println!("{qty3:?}");

    let res1 = qty2 + qty3;
    // println!("res= {res1:?}");

    assert_eq!(res1[0].number, Decimal::from(2));
  }

  #[test]
  fn add_not_equals() {
    // different uuids
    let data0 = object! {
      "number": 2,
      "uom": UUID_NIL.to_json(),
    };

    let qty0 = Qty::from(&data0).unwrap();
    // println!("{qty0:?}");

    let data1 = object! {
      "number": 2,
      "uom": UUID_MAX.to_json(),
    };

    let qty1 = Qty::from(&data1).unwrap();
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

    let qty2 = Qty::from(&data2).unwrap();
    // println!("{qty2:?}");

    let data3 = object! {
      "number": 1,
      "uom": object! {
        "number": 11,
        "uom": UUID_NIL.to_json(),
        "in": UUID_MAX.to_json(),
      },
    };

    let qty3 = Qty::from(&data3).unwrap();
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

    let qty4 = Qty::from(&data4).unwrap();
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

    let qty5 = Qty::from(&data5).unwrap();
    // println!("{qty5:?}");

    let res2 = qty4 + qty5;
    // println!("res= {res2:?}");

    assert_eq!(res2[0].number, Decimal::from(2));
    assert_eq!(res2[1].number, Decimal::from(2));
  }
}
