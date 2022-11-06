use bytecheck::CheckBytes;
use rkyv::{AlignedVec, Archive, Deserialize, Serialize};

use base64::DecodeError;
use std::array::TryFromSliceError;
use std::cmp::Ordering;
use std::fmt::Formatter;
use std::ops::{Add, Sub};

use crate::animo::db::{FromBytes, ToBytes};
use crate::animo::error::DBError;
use crate::animo::Time;
use crate::{Decimal, Settings};
use blake2::{Blake2s256, Digest};
use json::JsonValue;
use json::JsonValue::Number;

type Hasher = Blake2s256;
pub(crate) const ID_BYTES: usize = 32;

pub const ID_MIN: ID = ID([u8::MIN; ID_BYTES]);
pub const ID_MAX: ID = ID([u8::MAX; ID_BYTES]);

// #[derive(Debug, Clone, Hash, Serialize, Deserialize, Eq, PartialEq, Copy)]
#[derive(Clone, Copy, Hash, Ord, PartialOrd, Eq)] // , serde::Serialize, serde::Deserialize
#[derive(Archive, Deserialize, Serialize, Debug, PartialEq)]
// This will generate a PartialEq impl between our unarchived and archived types
// #[archive(compare(PartialEq))]
// To use the safe API, you have to derive CheckBytes for the archived type
#[archive_attr(derive(CheckBytes, Debug))]
pub struct ID([u8; ID_BYTES]);

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

impl From<&str> for ID {
  fn from(data: &str) -> Self {
    let mut bs = [0; 32];
    bs.copy_from_slice(Hasher::digest(data).as_slice());
    ID(bs)
  }
}

impl From<String> for ID {
  fn from(data: String) -> Self {
    let mut bs = [0; 32];
    bs.copy_from_slice(Hasher::digest(data).as_slice());
    ID(bs)
  }
}

impl TryFrom<&[u8]> for ID {
  type Error = DBError;

  fn try_from(bs: &[u8]) -> Result<Self, DBError> {
    let bs = bs.try_into().map_err(|e: TryFromSliceError| DBError::from(e.to_string()))?;
    Ok(ID(bs))
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
      Value::Number(number) => true,
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

impl std::fmt::Display for ID {
  fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
    write!(f, "{}", self.to_base64())
  }
}

impl serde::Serialize for ID {
  fn serialize<S>(&self, s: S) -> Result<S::Ok, S::Error>
  where
    S: serde::Serializer,
  {
    s.serialize_str(self.to_base64().as_str())
  }
}

impl<'de> serde::Deserialize<'de> for ID {
  fn deserialize<D>(d: D) -> Result<Self, D::Error>
  where
    D: serde::Deserializer<'de>,
  {
    struct CustomVisitor;

    impl<'de> serde::de::Visitor<'de> for CustomVisitor {
      type Value = ID;

      fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(formatter, "base64-encoded ID")
      }

      fn visit_str<E>(self, v: &str) -> Result<Self::Value, E>
      where
        E: serde::de::Error,
      {
        ID::from_base64(v.as_bytes()).map_err(|e| E::custom(e.to_string()))
      }
    }

    d.deserialize_string(CustomVisitor)
  }
}

impl ID {
  pub(crate) fn random() -> Self {
    use rand::{distributions::Alphanumeric, Rng};

    let s: String = rand::thread_rng()
      .sample_iter(&Alphanumeric)
      .take(256)
      .map(char::from)
      .collect();

    ID::from(s)
  }

  pub(crate) fn new(data: &[u8]) -> Result<Self, DBError> {
    if data.len() != ID_BYTES {
      Err(DBError::from(format!("ID require {} bytes, but got {}", ID_BYTES, data.len())))
    } else {
      let mut a = [0; ID_BYTES];
      for i in 0..ID_BYTES {
        a[i] = data[i];
      }
      Ok(ID(a))
    }
  }

  pub(crate) fn from_base64(input: &[u8]) -> Result<ID, DBError> {
    match base64::decode_config(input, base64::URL_SAFE_NO_PAD) {
      Ok(bs) => ID::new(bs.as_slice()),
      Err(msg) => Err(DBError::from(msg.to_string())),
    }
  }

  pub(crate) fn to_base64(&self) -> String {
    base64::encode_config(self.0, base64::URL_SAFE_NO_PAD)
  }

  pub(crate) fn to_clear(&self) -> String {
    base64::encode_config(self.0, base64::URL_SAFE_NO_PAD)
      .replace("_", "")
      .replace("-", "")[..12]
      .to_string()
  }

  // TODO make `const`
  pub(crate) fn for_constant(data: &str) -> Self {
    data.into()
  }

  pub fn as_slice(&self) -> &[u8] {
    self.0.as_slice()
  }

  pub fn bytes(context: &Context, what: &ID) -> Vec<u8> {
    let mut bs = Vec::with_capacity(ID_BYTES * (1 + context.len()));

    for id in &context.0 {
      bs.extend_from_slice(id.0.as_slice());
    }

    bs.extend_from_slice(what.0.as_slice());

    bs
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

pub(crate) trait Memory {
  fn init(folder: &str) -> Result<Self, DBError>
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
  use crate::DESC;

  #[test]
  fn test_transformation_json() {
    let trans = Transformation {
      context: vec!["1".into(), "2".into()].into(),
      what: "3".into(),
      into: "4".into(),
    };

    let json = serde_json::to_string(&trans).unwrap();

    assert_eq!(json, "{\"context\":[[98,88,81,227,135,110,110,109,164,5,201,90,194,70,135,206,75,178,205,216,251,216,69,146,120,246,240,206,128,62,19,238],[205,122,236,69,159,185,201,253,103,216,158,107,115,60,57,77,208,80,61,243,171,61,8,232,8,148,201,164,161,77,8,109]],\"what\":[122,167,248,2,221,30,181,208,192,68,198,5,234,197,226,208,176,34,65,33,3,129,84,53,142,154,43,190,213,230,96,15],\"into\":{\"String\":\"4\"}}");
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

    assert_eq!(json, "{\"context\":[[98,88,81,227,135,110,110,109,164,5,201,90,194,70,135,206,75,178,205,216,251,216,69,146,120,246,240,206,128,62,19,238],[205,122,236,69,159,185,201,253,103,216,158,107,115,60,57,77,208,80,61,243,171,61,8,232,8,148,201,164,161,77,8,109]],\"what\":[122,167,248,2,221,30,181,208,192,68,198,5,234,197,226,208,176,34,65,33,3,129,84,53,142,154,43,190,213,230,96,15],\"into_before\":\"Nothing\",\"into_after\":{\"String\":\"4\"}}");
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
