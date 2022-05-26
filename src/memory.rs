use serde::{Deserialize, Serialize};
use blake2::{Digest, Blake2s256};
use rust_decimal::Decimal;
use crate::error::DBError;

type HASHER = Blake2s256;
const ID_BYTES: usize = 32;

#[derive(Debug, Clone, Serialize, Deserialize, Eq, PartialEq)]
pub struct ID([u8; ID_BYTES]);

#[derive(Debug, Clone, Serialize, Deserialize, Eq, PartialEq)]
pub struct IDS(pub Vec<ID>);

#[derive(Debug, Serialize, Deserialize, Eq, PartialEq)]
pub enum Value {
    Nothing,
    ID(ID),
    IDS(IDS),
    String(String),
    Number(Decimal) // Number // TODO BigDecimal ?
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ChangeTransformation {
    pub context: IDS,
    pub what: ID,
    pub into_before: Value,
    pub into_after: Value
}

#[derive(Debug, Serialize, Deserialize)]
pub struct TransformationKey {
    pub context: IDS,
    pub what: ID,
}

#[derive(Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct Transformation {
    pub context: IDS,
    pub what: ID,
    pub into: Value,
}

impl From<&str> for ID {
    fn from(data: &str) -> Self {
        let mut bs = [0; 32];
        bs.copy_from_slice(HASHER::digest(data).as_slice());
        ID(bs)
    }
}

impl From<Vec<ID>> for IDS {
    fn from(v: Vec<ID>) -> Self {
        IDS(v)
    }
}

impl From<&str> for Value {
    fn from(data: &str) -> Self {
        Value::String(data.into())
    }
}


impl Value {
    pub(crate) fn to_bytes(&self) -> Result<Vec<u8>, DBError> {
        bincode::serialize(self)
            .map_err(|_| "fail to encode value".into())
    }

    pub(crate) fn from_bytes(bs: Option<Vec<u8>>) -> Result<Self, DBError> {
        match bs {
            Some(bs) => bincode::deserialize(&bs)
                .map_err(|_| "fail to decode value".into()),
            None => Ok(Value::Nothing)
        }
    }

    pub(crate) fn number_or_err(self) -> Result<Decimal, DBError> {
        match self {
            Value::Number(number) => Ok(number),
            _ => Err("value is not number".into())
        }
    }
}

impl TransformationKey {
    pub fn to_bytes(&self) -> Vec<u8> {
        ID::bytes(&self.context, &self.what)
    }
}

impl ID {
    pub fn bytes(context: &IDS, what: &ID) -> Vec<u8> {
        let mut bs = Vec::with_capacity(ID_BYTES * (1 + context.len()));

        for id in &context.0 {
            bs.extend_from_slice(id.0.as_slice());
        }

        bs.extend_from_slice(what.0.as_slice());

        bs
    }
}

impl IDS {
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut bs = Vec::with_capacity(ID_BYTES * self.0.len());
        for id in &self.0 {
            bs.extend_from_slice(id.0.as_slice());
        }
        bs
    }

    pub fn len(&self) -> usize {
        self.0.len()
    }
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
}
