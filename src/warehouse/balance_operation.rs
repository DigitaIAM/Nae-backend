use bytecheck::CheckBytes;
use rkyv::{AlignedVec, Archive, Deserialize, Serialize};

use crate::animo::db::{FromBytes, ToBytes};
use crate::animo::error::DBError;
use crate::animo::memory::ID;
use crate::animo::shared::*;
use crate::animo::Operation;
use crate::warehouse::balance::WHBalance;
use crate::warehouse::primitives::{Money, MoneyOp, Qty};

// #[derive(Debug, Clone, Serialize, Deserialize)]
#[derive(Clone, Archive, Deserialize, Serialize, Debug, PartialEq)]
// This will generate a PartialEq impl between our unarchived and archived types
// #[archive(compare(PartialEq))]
// To use the safe API, you have to derive CheckBytes for the archived type
#[archive_attr(derive(CheckBytes, Debug))]
pub enum BalanceOperation {
  In(Qty, Money),
  Out(Qty, Money),
}

impl From<BalanceOperation> for MoneyOp {
  fn from(op: BalanceOperation) -> Self {
    match op {
      BalanceOperation::In(_, cost) => MoneyOp::Incoming(cost),
      BalanceOperation::Out(_, cost) => MoneyOp::Outgoing(cost),
    }
  }
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
  fn to_bytes(&self) -> Result<AlignedVec, DBError> {
    rkyv::to_bytes::<_, 1024>(self).map_err(|e| DBError::from(e.to_string()))
    // serde_json::to_vec(self)
    //     .map_err(|e| e.to_string().into())
  }
}

impl FromBytes<BalanceOperation> for BalanceOperation {
  fn from_bytes(_bs: &[u8]) -> Result<Self, DBError> {
    todo!()
    // match rkyv::check_archived_root::<Self>(bs) {
    //     Ok(archived) => {
    //         let value: Self = archived.deserialize(&mut rkyv::Infallible).unwrap();
    //         Ok(value)
    //     },
    //     Err(e) => Err(DBError::from(e.to_string()))
    // }
  }
}

impl Operation<WHBalance> for BalanceOperation {
  fn delta_between(&self, other: &Self) -> BalanceOperation {
    match self {
      BalanceOperation::In(l_qty, l_cost) => {
        match other {
          BalanceOperation::In(r_qty, r_cost) => {
            // 10 > 8 = -2 (8-10)
            // 10 > 12 = 2 (12-10)
            BalanceOperation::In(r_qty - l_qty, r_cost - l_cost)
          },
          BalanceOperation::Out(r_qty, r_cost) => {
            // 10 > -8 = -18 (-10-8)
            BalanceOperation::In(-(l_qty + r_qty), -(l_cost + r_cost))
          },
        }
      },
      BalanceOperation::Out(l_qty, l_cost) => {
        match other {
          BalanceOperation::In(r_qty, r_cost) => {
            // -10 > 8 = 18 (10+8)
            BalanceOperation::In(l_qty + r_qty, l_cost + r_cost)
          },
          BalanceOperation::Out(r_qty, r_cost) => {
            // -10 > -8 = +2 (10-8)
            // -10 > -12 = -2 (10-12)
            BalanceOperation::In(l_qty - r_qty, l_cost + r_cost)
          },
        }
      },
    }
  }

  fn to_value(&self) -> WHBalance {
    match self {
      BalanceOperation::In(qty, cost) => WHBalance(qty.clone(), cost.clone()),
      BalanceOperation::Out(qty, cost) => WHBalance(-qty.clone(), -cost.clone()),
    }
  }
}
