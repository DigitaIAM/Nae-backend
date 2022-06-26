use rkyv::{AlignedVec, Archive, Deserialize, Serialize};
use bytecheck::CheckBytes;

use crate::animo::Object;
use crate::animo::error::DBError;
use crate::animo::db::{FromBytes, ToBytes};
use crate::warehouse::balance_operation::BalanceOperation;
use crate::warehouse::primitives::{Money, Qty};

// #[derive(Debug, Clone, Eq, PartialEq, Default, Serialize, Deserialize, ImplBytes)]
#[derive(Clone, Default)]
#[derive(Archive, Deserialize, Serialize, Debug, PartialEq)]
// This will generate a PartialEq impl between our unarchived and archived types
// #[archive(compare(PartialEq))]
// To use the safe API, you have to derive CheckBytes for the archived type
#[archive_attr(derive(CheckBytes, Debug))]
pub struct WHBalance(pub Qty, pub Money);

impl FromBytes<WHBalance> for WHBalance {
    fn from_bytes(bs: &[u8]) -> Result<Self, DBError> {
        todo!()
    }
}

impl ToBytes for WHBalance {
    fn to_bytes(&self) -> Result<AlignedVec, DBError> {
        todo!()
    }
}

impl std::ops::Add<WHBalance> for WHBalance {
    type Output = WHBalance;

    fn add(self, rhs: Self) -> Self::Output {
        WHBalance(self.0 + rhs.0, self.1 + rhs.1)
    }
}

impl<'a, 'b> std::ops::Add<&'b WHBalance> for &'a WHBalance {
    type Output = WHBalance;

    fn add(self, other: &'b WHBalance) -> WHBalance {
        WHBalance(&self.0 + &other.0, &self.1 + &other.1)
    }
}

impl std::ops::Sub<WHBalance> for WHBalance {
    type Output = WHBalance;

    fn sub(self, rhs: Self) -> Self::Output {
        WHBalance(self.0 - rhs.0, self.1 - rhs.1)
    }
}

impl std::ops::Neg for WHBalance {
    type Output = WHBalance;

    fn neg(self) -> Self::Output {
        WHBalance(-self.0, -self.1)
    }
}

impl From<WHBalance> for Money {
    fn from(f: WHBalance) -> Self {
        f.1
    }
}

impl Object<BalanceOperation> for WHBalance {
    // fn apply_delta(&self, other: &Balance) -> Self {
    //     self + other
    // }

    fn apply(&self, op: &BalanceOperation) -> Result<Self,DBError> {
        let (qty, cost) = match op {
            BalanceOperation::In(qty, cost) => (&self.0 + qty, &self.1 + cost),
            BalanceOperation::Out(qty, cost) => (&self.0 - qty, &self.1 - cost),
        };
        log::debug!("apply {:?} to {:?}", op, self);

        Ok(WHBalance(qty, cost))
    }
}