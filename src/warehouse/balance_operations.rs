use serde::{Deserialize, Serialize};
use crate::animo::{AObject, AOperation};
use crate::error::DBError;
use crate::rocksdb::{FromBytes, ToBytes};
use crate::warehouse::balance::Balance;
use crate::warehouse::balance_operation::BalanceOperation;
use crate::warehouse::primitives::{Money, Qty};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BalanceOps {
    incoming: (Qty, Money),
    outgoing: (Qty, Money),
}

impl AOperation<Balance> for BalanceOps {
    fn to_value(&self) -> Balance {
        Balance(
            &self.incoming.0 - &self.outgoing.0,
            &self.incoming.1 - &self.outgoing.1
        )
    }
}

impl AObject<BalanceOps> for Balance {
    fn apply_aggregation(&self, op: &BalanceOps) -> Result<Self,DBError> {
        let qty = &(&self.0 + &op.incoming.0) - &op.outgoing.0;
        let cost = &(&self.1 + &op.incoming.1) - &op.outgoing.1;

        debug!("apply aggregation {:?} to {:?}", op, self);

        Ok(Balance(qty, cost))
    }
}

impl From<BalanceOperation> for BalanceOps {
    fn from(op: BalanceOperation) -> Self {
        match op {
            BalanceOperation::In(qty, cost) => {
                BalanceOps {
                    incoming: (qty, cost),
                    outgoing: (Qty::default(), Money::default()),
                }
            }
            BalanceOperation::Out(qty, cost) => {
                BalanceOps {
                    incoming: (Qty::default(), Money::default()),
                    outgoing: (qty, cost),
                }
            }
        }
    }
}

impl FromBytes<Self> for BalanceOps {
    fn from_bytes(bs: &[u8]) -> Result<Self, DBError> {
        serde_json::from_slice(bs)
            .map_err(|_| "fail to decode BalanceOperations".into())
    }
}

impl ToBytes for BalanceOps {
    fn to_bytes(&self) -> Result<Vec<u8>, DBError> {
        serde_json::to_string(self)
            .map(|s| s.as_bytes().to_vec())
            .map_err(|_| format!("fail to encode BalanceOperations {:?}", self).into())
    }
}