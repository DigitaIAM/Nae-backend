use serde::{Deserialize, Serialize};
use blake2::{Digest, Blake2s256};
use serde_json::Number;
use crate::error::DBError;

type HASHER = Blake2s256;
const ID_BYTES: usize = 32;

#[derive(Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct ID([u8; ID_BYTES]);

#[derive(Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct IDS(Vec<ID>);

#[derive(Debug, Serialize, Deserialize, Eq, PartialEq)]
pub enum Value {
    Nothing,
    ID(ID),
    IDS(IDS),
    String(String),
    Number(Number) // TODO BigDecimal ?
}

#[derive(Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct Record {
    pub key: IDS,
    pub value: Value,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Change {
    pub primary: ID,
    pub relation: IDS,
    pub before: Value,
    pub after: Value,
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
}

impl ID {
    pub(crate) fn bytes(primary: &ID, relation: &IDS) -> Vec<u8> {
        let mut bs = Vec::with_capacity(ID_BYTES * (1 + relation.0.len()));
        bs.extend_from_slice(primary.0.as_slice());

        for id in &relation.0 {
            bs.extend_from_slice(id.0.as_slice());
        }

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

}


pub(crate) trait Memory {
    fn init(path: &str) -> Result<Self, DBError> where Self: Sized;

    fn modify(&self, mutations: Vec<Change>) -> Result<(), DBError>;

    fn query(&self, keys: Vec<IDS>) -> Result<Vec<Record>, DBError>;
}