use rkyv::{AlignedVec, Archive, Deserialize, Serialize};
use bytecheck::CheckBytes;
use rust_decimal::prelude::Zero;

use crate::animo::{AObject, AOperation};
use crate::animo::error::DBError;
use crate::animo::db::{FromBytes, ToBytes};
use crate::warehouse::balance::WHBalance;
use crate::warehouse::balance_operation::BalanceOperation;
use crate::warehouse::primitives::{Money, MoneyOps, Qty};

// #[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
#[derive(Clone)]
#[derive(Archive, Deserialize, Serialize, Debug, PartialEq)]
// This will generate a PartialEq impl between our unarchived and archived types
// #[archive(compare(PartialEq))]
// To use the safe API, you have to derive CheckBytes for the archived type
#[archive_attr(derive(CheckBytes, Debug))]
pub struct BalanceOps {
    pub(crate) incoming: (Qty, Money),
    pub(crate) outgoing: (Qty, Money),
}

impl std::ops::Add<BalanceOps> for BalanceOps {
    type Output = BalanceOps;

    fn add(self, rhs: BalanceOps) -> Self::Output {
        BalanceOps {
            incoming: (self.incoming.0 + rhs.incoming.0, self.incoming.1 + rhs.incoming.1),
            outgoing: (self.outgoing.0 + rhs.outgoing.0, self.outgoing.1 + rhs.outgoing.1)
        }
    }
}

impl std::ops::Sub<BalanceOps> for BalanceOps {
    type Output = BalanceOps;

    fn sub(self, rhs: BalanceOps) -> Self::Output {
        BalanceOps {
            incoming: (self.incoming.0 - rhs.incoming.0, self.incoming.1 - rhs.incoming.1),
            outgoing: (self.outgoing.0 - rhs.outgoing.0, self.outgoing.1 - rhs.outgoing.1)
        }
    }
}

impl std::ops::Neg for BalanceOps {
    type Output = BalanceOps;

    fn neg(self) -> Self::Output {
        BalanceOps {
            incoming: (-self.incoming.0, -self.incoming.1),
            outgoing: (-self.outgoing.0, -self.outgoing.1)
        }
    }
}

impl From<BalanceOps> for MoneyOps {
    fn from(f: BalanceOps) -> Self {
        MoneyOps { incoming: f.incoming.1, outgoing: f.outgoing.1 }
    }
}

impl AOperation<WHBalance> for BalanceOps {
    fn to_value(&self) -> WHBalance {
        WHBalance(
            &self.incoming.0 - &self.outgoing.0,
            &self.incoming.1 - &self.outgoing.1
        )
    }
}

impl AObject<BalanceOps> for WHBalance {
    fn is_zero(&self) -> bool {
        self.0.0.is_zero() && self.1.0.is_zero()
    }

    fn apply_aggregation(&self, op: &BalanceOps) -> Result<Self,DBError> {
        let qty = &(&self.0 + &op.incoming.0) - &op.outgoing.0;
        let cost = &(&self.1 + &op.incoming.1) - &op.outgoing.1;

        log::debug!("apply aggregation {:?} to {:?}", op, self);

        Ok(WHBalance(qty, cost))
    }
}

impl From<&BalanceOperation> for BalanceOps {
    fn from(op: &BalanceOperation) -> Self {
        match op {
            BalanceOperation::In(qty, cost) => {
                BalanceOps {
                    incoming: (qty.clone(), cost.clone()),
                    outgoing: (Qty::default(), Money::default()),
                }
            }
            BalanceOperation::Out(qty, cost) => {
                BalanceOps {
                    incoming: (Qty::default(), Money::default()),
                    outgoing: (qty.clone(), cost.clone()),
                }
            }
        }
    }
}

impl FromBytes<Self> for BalanceOps {
    fn from_bytes(bs: &[u8]) -> Result<Self, DBError> {
        todo!()
        // serde_json::from_slice(bs)
        //     .map_err(|_| "fail to decode BalanceOperations".into())
    }
}

impl ToBytes for BalanceOps {
    fn to_bytes(&self) -> Result<AlignedVec, DBError> {
        todo!()
        // serde_json::to_string(self)
        //     .map(|s| s.as_bytes().to_vec())
        //     .map_err(|_| format!("fail to encode BalanceOperations {:?}", self).into())
    }
}