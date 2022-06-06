use serde::{Deserialize, Serialize};
use crate::animo::Operation;
use crate::error::DBError;
use crate::memory::ID;
use crate::rocksdb::{FromBytes, ToBytes};
use crate::shared::*;
use crate::warehouse::balance::Balance;
use crate::warehouse::primitives::{Money, Qty};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum BalanceOperation {
    In(Qty, Money),
    Out(Qty, Money),
}

impl BalanceOperation {
    pub(crate) fn resolve(instance_of: ID, qty: Qty, cost: Money) -> Option<BalanceOperation> {
        if instance_of == *GOODS_RECEIVE {
            Some(BalanceOperation::In(qty, cost))
        } else if instance_of == *GOODS_ISSUE {
            Some(BalanceOperation::Out(qty, cost))
        } else {
            None
        }
    }
}

impl ToBytes for BalanceOperation {
    fn to_bytes(&self) -> Result<Vec<u8>, DBError> {
        serde_json::to_vec(self)
            .map_err(|e| e.to_string().into())
    }
}

impl FromBytes<BalanceOperation> for BalanceOperation {
    fn from_bytes(bs: &[u8]) -> Result<BalanceOperation, DBError> {
        serde_json::from_slice(bs)
            .map_err(|e| e.to_string().into())
    }
}

impl Operation<Balance> for BalanceOperation {
    fn delta_between(&self, other: &Self) -> BalanceOperation {
        match self {
            BalanceOperation::In(l_qty, l_cost) => {
                match other {
                    BalanceOperation::In(r_qty, r_cost) => {
                        // 10 > 8 = -2 (8-10)
                        // 10 > 12 = 2 (12-10)
                        BalanceOperation::In(r_qty - l_qty, r_cost - l_cost)
                    }
                    BalanceOperation::Out(r_qty, r_cost) => {
                        // 10 > -8 = -18 (-10-8)
                        BalanceOperation::In(-(l_qty + r_qty), -(l_cost + r_cost))
                    }
                }
            }
            BalanceOperation::Out(l_qty, l_cost) => {
                match other {
                    BalanceOperation::In(r_qty, r_cost) => {
                        // -10 > 8 = 18 (10+8)
                        BalanceOperation::In(l_qty + r_qty, l_cost + r_cost)
                    }
                    BalanceOperation::Out(r_qty, r_cost) => {
                        // -10 > -8 = +2 (10-8)
                        // -10 > -12 = -2 (10-12)
                        BalanceOperation::In(l_qty - r_qty, l_cost + r_cost)
                    }
                }
            }
        }
    }

    fn to_value(&self) -> Balance {
        match self {
            BalanceOperation::In(qty, cost) => Balance(qty.clone(), cost.clone()),
            BalanceOperation::Out(qty, cost) => Balance(-qty.clone(), -cost.clone()),
        }
    }
}