use bytecheck::CheckBytes;
use rkyv::{AlignedVec, Archive, Deserialize, Serialize};

use json::JsonValue;
use std::convert::TryFrom;
use std::path::PathBuf;

use crate::animo::db::{FromBytes, ToBytes};
use crate::animo::error::DBError;
use crate::animo::Time;
use crate::warehouse::primitive_types::Decimal;
use values::ID;

// #[derive(Debug, Clone, Hash, Serialize, Deserialize, Eq, PartialEq)]
#[derive(
  Clone, serde::Serialize, serde::Deserialize, Archive, Deserialize, Serialize, Debug, PartialEq,
)]
// This will generate a PartialEq impl between our unarchived and archived types
// #[archive(compare(PartialEq))]
// To use the safe API, you have to derive CheckBytes for the archived type
#[archive_attr(derive(CheckBytes, Debug))]
pub struct IDs(pub Vec<ID>);

// Options: singularity, magnitude
// #[derive(Debug, Clone, Serialize, Deserialize, Eq, PartialEq)]
#[derive(
  Clone, serde::Serialize, serde::Deserialize, Archive, Deserialize, Serialize, Debug, PartialEq,
)]
// This will generate a PartialEq impl between our unarchived and archived types
// #[archive(compare(PartialEq))]
// To use the safe API, you have to derive CheckBytes for the archived type
// #[archive_attr(derive(CheckBytes, Debug))]
#[archive(bound(serialize = "__S: rkyv::ser::ScratchSpace + rkyv::ser::Serializer"))]
pub enum Value {
  Nothing,
  ID(ID),
  IDs(IDs),
  Pairs(#[omit_bounds] Vec<(ID, Box<Value>)>),
  String(String),
  Number(Decimal),
  U128(u128),
  DateTime(Time),
}

pub type Zone = ID;

// #[derive(Debug, Clone, Hash, Eq, PartialEq, Serialize, Deserialize)]
#[derive(
  Clone,
  Hash,
  Eq,
  serde::Serialize,
  serde::Deserialize,
  Archive,
  Deserialize,
  Serialize,
  Debug,
  PartialEq,
)]
// This will generate a PartialEq impl between our unarchived and archived types
// #[archive(compare(PartialEq))]
// To use the safe API, you have to derive CheckBytes for the archived type
// #[archive_attr(derive(CheckBytes, Debug))]
pub struct Context(pub Vec<ID>);

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct ChangeTransformation {
  pub zone: Zone,
  pub context: Context,
  pub what: ID,
  pub into_before: Value,
  pub into_after: Value,
}

impl ChangeTransformation {
  pub(crate) fn new(zone: ID, context: Context, what: ID, into: Value) -> Self {
    ChangeTransformation { zone, context, what, into_before: Value::Nothing, into_after: into }
  }

  pub(crate) fn create(zone: ID, context: ID, what: &str, into: Value) -> Self {
    ChangeTransformation {
      zone,
      context: Context(vec![context]),
      what: what.into(),
      into_before: Value::Nothing,
      into_after: into,
    }
  }

  pub(crate) fn create_(zone: ID, context: ID, what: ID, into: Value) -> Self {
    ChangeTransformation {
      zone,
      context: Context(vec![context]),
      what,
      into_before: Value::Nothing,
      into_after: into,
    }
  }
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub struct TransformationKey {
  pub context: Context,
  pub what: ID,
}

impl TransformationKey {
  pub(crate) fn simple(context: ID, what: &str) -> Self {
    TransformationKey { context: Context(vec![context]), what: what.into() }
  }
}

// #[derive(Debug, Serialize, Deserialize, Eq, PartialEq)]
#[derive(
  Clone, serde::Serialize, serde::Deserialize, Archive, Deserialize, Serialize, Debug, PartialEq,
)]
// This will generate a PartialEq impl between our unarchived and archived types
// #[archive(compare(PartialEq))]
// To use the safe API, you have to derive CheckBytes for the archived type
// #[archive_attr(derive(CheckBytes, Debug))]
pub struct Transformation {
  pub context: Context,
  pub what: ID,
  pub into: Value,
}

impl Transformation {
  pub fn new(context: &Context, what: ID, into: Value) -> Self {
    Transformation { context: context.clone(), what, into }
  }
}

impl From<Vec<ID>> for IDs {
  fn from(v: Vec<ID>) -> Self {
    IDs(v)
  }
}

impl From<Vec<ID>> for Context {
  fn from(v: Vec<ID>) -> Self {
    Context(v)
  }
}

impl From<ID> for Value {
  fn from(data: ID) -> Self {
    Value::ID(data)
  }
}

impl From<IDs> for Value {
  fn from(data: IDs) -> Self {
    Value::IDs(data)
  }
}

impl From<Vec<(ID, Value)>> for Value {
  fn from(data: Vec<(ID, Value)>) -> Self {
    let data = data.iter().map(|p| (p.0, Box::new(p.1.clone()))).collect();
    Value::Pairs(data)
  }
}

impl From<&str> for Value {
  fn from(data: &str) -> Self {
    Value::String(data.into())
  }
}

impl From<String> for Value {
  fn from(data: String) -> Self {
    Value::String(data)
  }
}

impl From<u32> for Value {
  fn from(data: u32) -> Self {
    Value::Number(data.into())
  }
}

impl From<f64> for Value {
  fn from(data: f64) -> Self {
    // TODO recode, do not panic!
    Value::Number(Decimal::try_from(data).unwrap())
  }
}

impl From<Time> for Value {
  fn from(data: Time) -> Self {
    Value::DateTime(data)
  }
}

impl ToBytes for Value {
  fn to_bytes(&self) -> Result<AlignedVec, DBError> {
    rkyv::to_bytes::<_, 1024>(self).map_err(|e| DBError::from(e.to_string()))
    // serde_json::to_string(self)
    //     .map(|s| s.as_bytes().to_vec())
    //     .map_err(|_| format!("fail to encode value {:?}", self).into())
  }
}

impl FromBytes<Value> for Value {
  fn from_bytes(bs: &[u8]) -> Result<Self, DBError> {
    let archived = unsafe { rkyv::archived_root::<Self>(bs) };
    let value: Self = archived.deserialize(&mut rkyv::Infallible).unwrap();
    Ok(value)

    // match rkyv::check_archived_root::<Self>(bs) {
    //     Ok(archived) => {
    //         let value: Self = archived.deserialize(&mut rkyv::Infallible).unwrap();
    //         Ok(value)
    //     },
    //     Err(e) => Err(DBError::from(e.to_string()))
    // }
    // serde_json::from_slice(bs)
    //     .map_err(|_| "fail to decode value".into())
  }
}

impl Value {
  pub(crate) fn to_json(&self) -> JsonValue {
    match self {
      Value::Nothing => JsonValue::Null,
      Value::ID(_) => todo!(),
      Value::IDs(_) => todo!(),
      Value::Pairs(_) => todo!(),
      Value::String(s) => JsonValue::String(s.into()),
      Value::Number(n) => JsonValue::Number((*n).into()),
      Value::U128(_) => todo!(),
      Value::DateTime(_) => todo!(),
    }
  }

  pub(crate) fn is_nothing(&self) -> bool {
    match self {
      Value::Nothing => true,
      _ => false,
    }
  }

  pub(crate) fn is_string(&self) -> bool {
    match self {
      Value::String(_) => true,
      _ => false,
    }
  }

  pub(crate) fn is_number(&self) -> bool {
    match self {
      Value::Number(_number) => true,
      _ => false,
    }
  }

  pub(crate) fn as_string(&self) -> Option<String> {
    match self {
      Value::String(str) => Some(str.clone()),
      _ => None,
    }
  }

  pub(crate) fn as_number(&self) -> Option<Decimal> {
    match self {
      Value::Number(number) => Some(*number),
      _ => None,
    }
  }

  pub(crate) fn as_id(&self) -> Option<ID> {
    match self {
      Value::ID(id) => Some(*id),
      _ => None,
    }
  }

  pub(crate) fn as_time(&self) -> Option<Time> {
    match self {
      Value::DateTime(time) => Some(time.clone()),
      _ => None,
    }
  }

  pub(crate) fn one_of(&self, ids: &[ID]) -> bool {
    match self {
      Value::ID(id) => ids.contains(id),
      _ => false,
    }
  }
}

impl IDs {
  // pub fn to_bytes(&self) -> Vec<u8> {
  //     let mut bs = Vec::with_capacity(ID_BYTES * self.0.len());
  //     for id in &self.0 {
  //         bs.extend_from_slice(id.0.as_slice());
  //     }
  //     bs
  // }

  // pub fn len(&self) -> usize {
  //     self.0.len()
  // }
  //
  // pub fn to_vec(self) -> Vec<ID> {
  //     self.0
  // }
}

impl Context {
  pub fn len(&self) -> usize {
    self.0.len()
  }

  // pub fn to_vec(self) -> Vec<ID> {
  //     self.0
  // }
}

pub trait Memory {
  fn init(folder: PathBuf) -> Result<Self, DBError>
  where
    Self: Sized;

  fn modify(&self, mutations: Vec<ChangeTransformation>) -> Result<(), DBError>;

  fn value(&self, key: TransformationKey) -> Result<Value, DBError>;
  fn query(&self, keys: Vec<TransformationKey>) -> Result<Vec<Transformation>, DBError>;
}

pub(crate) fn create(zone: ID, primary: ID, pairs: Vec<(ID, Value)>) -> Vec<ChangeTransformation> {
  let mut v = vec![];

  let mut processing = vec![];

  for pair in pairs {
    processing.push((Context(vec![primary]), pair.0, Box::new(pair.1)));
  }

  while !processing.is_empty() {
    let (context, what, into) = processing.pop().unwrap();
    let into = *into;
    match into {
      Value::Pairs(ps) => {
        let mut ctx = context.0;
        ctx.push(what);

        for pair in ps {
          processing.push((Context(ctx.clone()), pair.0, pair.1));
        }
      },
      _ => v.push(ChangeTransformation {
        zone,
        context,
        what,
        into_before: Value::Nothing,
        into_after: into,
      }),
    }
  }
  v
}

#[cfg(test)]
mod tests {
  use super::*;
  use crate::animo::DESC;

  #[test]
  fn test_transformation_json() {
    let trans = Transformation {
      context: vec!["1".into(), "2".into()].into(),
      what: "3".into(),
      into: "4".into(),
    };

    let json = serde_json::to_string(&trans).unwrap();

    assert_eq!(
      json,
      r#"{"context":["YlhR44dubm2kBclawkaHzkuyzdj72EWSePbwzoA-E-4","zXrsRZ-5yf1n2J5rczw5TdBQPfOrPQjoCJTJpKFNCG0"],"what":"eqf4At0etdDARMYF6sXi0LAiQSEDgVQ1jporvtXmYA8","into":{"String":"4"}}"#
    );
  }

  #[test]
  fn test_change_transformation_json() {
    let trans = ChangeTransformation {
      zone: *DESC,
      context: vec!["1".into(), "2".into()].into(),
      what: "3".into(),
      into_before: Value::Nothing,
      into_after: "4".into(),
    };

    let json = serde_json::to_string(&trans).unwrap();

    assert_eq!(
      json,
      r#"{"zone":"hHy5FdUoBcm_4Lba8xu50uZloYS5PWL0Aw-UxX0QW34","context":["YlhR44dubm2kBclawkaHzkuyzdj72EWSePbwzoA-E-4","zXrsRZ-5yf1n2J5rczw5TdBQPfOrPQjoCJTJpKFNCG0"],"what":"eqf4At0etdDARMYF6sXi0LAiQSEDgVQ1jporvtXmYA8","into_before":"Nothing","into_after":{"String":"4"}}"#
    );
  }

  #[test]
  fn test_value_number_json() {
    let value = Value::Number(10.into());

    let json = serde_json::to_string(&value).unwrap();

    assert_eq!(json, "{\"Number\":10.0}");

    let restored = serde_json::from_str(json.as_str()).unwrap();

    assert_eq!(value, restored);
  }

  // #[ignore]
  // #[test]
  // fn test_value_number_bincode() {
  //     let value = Value::Number(10.into());
  //
  //     let bs = bincode::serialize(&value).unwrap();
  //
  //     assert_eq!(bs, vec![4, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 49, 48]);
  //
  //     let restored: Value = bincode::deserialize(&bs).unwrap();
  //
  //     assert_eq!(value, restored);
  // }
}
