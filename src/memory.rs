use std::array::TryFromSliceError;
use serde::{Deserialize, Serialize};
use blake2::{Digest, Blake2s256};
use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use chrono::serde::ts_milliseconds;
use crate::error::DBError;
use crate::rocksdb::{FromBytes, ToBytes};

pub type Time = DateTime<Utc>;

type Hasher = Blake2s256;
pub(crate) const ID_BYTES: usize = 32;

pub const ID_MIN: ID = ID([u8::MIN;ID_BYTES]);
pub const ID_MAX: ID = ID([u8::MAX;ID_BYTES]);

#[derive(Debug, Clone, Hash, Serialize, Deserialize, Eq, PartialEq, Copy)]
pub struct ID([u8; ID_BYTES]);

#[derive(Debug, Clone, Hash, Serialize, Deserialize, Eq, PartialEq)]
pub struct IDs(pub Vec<ID>);

#[derive(Debug, Clone, Serialize, Deserialize, Eq, PartialEq)]
pub enum Value {
    Nothing,
    ID(ID),
    IDs(IDs),
    String(String),
    Number(Decimal),
    #[serde(with = "ts_milliseconds")]
    DateTime(Time)
}

#[derive(Debug, Clone, Hash, Eq, PartialEq, Serialize, Deserialize)]
pub struct Context(pub Vec<ID>);

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChangeTransformation {
    pub context: Context,
    pub what: ID,
    pub into_before: Value,
    pub into_after: Value
}

#[derive(Debug, Serialize, Deserialize)]
pub struct TransformationKey {
    pub context: Context,
    pub what: ID,
}

#[derive(Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct Transformation {
    pub context: Context,
    pub what: ID,
    pub into: Value,
}

impl Transformation {
    pub fn new(context: &Context, what: ID, into: Value) -> Self {
        Transformation {
            context: context.clone(),
            what,
            into,
        }
    }
}

impl From<&str> for ID {
    fn from(data: &str) -> Self {
        let mut bs = [0; 32];
        bs.copy_from_slice(Hasher::digest(data).as_slice());
        ID(bs)
    }
}

impl TryFrom<&[u8]> for ID {
    type Error = DBError;

    fn try_from(bs: &[u8]) -> Result<Self,DBError> {
        let bs = bs.try_into()
            .map_err(|e: TryFromSliceError| DBError::from(e.to_string()))?;
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

impl From<&str> for Value {
    fn from(data: &str) -> Self {
        Value::String(data.into())
    }
}

impl From<u32> for Value {
    fn from(data: u32) -> Self {
        Value::Number(data.into())
    }
}

impl From<Time> for Value {
    fn from(data: Time) -> Self {
        Value::DateTime(data)
    }
}

impl ToBytes for Value {
    fn to_bytes(&self) -> Result<Vec<u8>, DBError> {
        serde_json::to_string(self)
            .map(|s| s.as_bytes().to_vec())
            .map_err(|_| format!("fail to encode value {:?}", self).into())
    }
}

impl FromBytes<Value> for Value {
    fn from_bytes(bs: &[u8]) -> Result<Self, DBError> {
        serde_json::from_slice(bs)
            .map_err(|_| "fail to decode value".into())
    }
}

impl Value {
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
            Value::DateTime(time) => Some(*time),
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

impl ID {
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
    fn init(path: &str) -> Result<Self, DBError> where Self: Sized;

    fn modify(&self, mutations: Vec<ChangeTransformation>) -> Result<(), DBError>;

    fn query(&self, keys: Vec<TransformationKey>) -> Result<Vec<Transformation>, DBError>;
}

#[cfg(test)]
mod tests {
    use super::*;

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

        assert_eq!(json, "{\"Number\":\"10\"}");

        let restored = serde_json::from_str(json.as_str()).unwrap();

        assert_eq!(value, restored);
    }

    #[ignore]
    #[test]
    fn test_value_number_bincode() {
        let value = Value::Number(10.into());

        let bs = bincode::serialize(&value).unwrap();

        assert_eq!(bs, vec![4, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 49, 48]);

        let restored: Value = bincode::deserialize(&bs).unwrap();

        assert_eq!(value, restored);
    }
}
