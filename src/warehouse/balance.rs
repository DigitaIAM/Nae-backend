use serde::{Deserialize, Serialize};
use crate::animo::Object;
use crate::error::DBError;
use crate::rocksdb::{FromBytes, ToBytes};
use crate::warehouse::balance_operation::BalanceOperation;
use crate::warehouse::primitives::{Money, Qty};

#[derive(Debug, Clone, Eq, PartialEq, Default, Serialize, Deserialize)]
pub struct Balance(pub Qty, pub Money);

impl Object<BalanceOperation> for Balance {
    // fn apply_delta(&self, other: &Balance) -> Self {
    //     self + other
    // }

    fn apply(&self, op: &BalanceOperation) -> Result<Self,DBError> {
        let (qty, cost) = match op {
            BalanceOperation::In(qty, cost) => (&self.0 + qty, &self.1 + cost),
            BalanceOperation::Out(qty, cost) => (&self.0 - qty, &self.1 - cost),
        };
        debug!("apply {:?} to {:?}", op, self);

        Ok(Balance(qty, cost))
    }
}

impl ToBytes for Balance {
    fn to_bytes(&self) -> Result<Vec<u8>, DBError> {
        serde_json::to_vec(self)
            .map_err(|e| e.to_string().into())
    }
}

impl FromBytes<Balance> for Balance {
    fn from_bytes(bs: &[u8]) -> Result<Balance, DBError> {
        serde_json::from_slice(bs)
            .map_err(|e| e.to_string().into())
    }
}

impl<'a, 'b> std::ops::Add<&'b Balance> for &'a Balance {
    type Output = Balance;

    fn add(self, other: &'b Balance) -> Balance {
        Balance(&self.0 + &other.0, &self.1 + &other.1)
    }
}