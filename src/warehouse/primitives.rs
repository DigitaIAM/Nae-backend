use rkyv::{Archive, Deserialize, Serialize};
use bytecheck::CheckBytes;
use Decimal;


// #[derive(Debug, Clone, Hash, Eq, PartialEq, Default, Serialize, Deserialize)]
#[derive(Clone, Default)]
#[derive(Archive, Deserialize, Serialize, Debug, PartialEq)]
// This will generate a PartialEq impl between our unarchived and archived types
// #[archive(compare(PartialEq))]
// To use the safe API, you have to derive CheckBytes for the archived type
#[archive_attr(derive(CheckBytes, Debug))]
pub struct Qty(pub Decimal); // TODO UOM,

impl std::ops::Add<Qty> for Qty {
    type Output = Qty;

    fn add(self, other: Qty) -> Qty {
        Qty(self.0 + other.0)
    }
}

impl std::ops::Sub<Qty> for Qty {
    type Output = Qty;

    fn sub(self, other: Qty) -> Qty {
        Qty(self.0 - other.0)
    }
}

impl<'a, 'b> std::ops::Add<&'b Qty> for &'a Qty {
    type Output = Qty;

    fn add(self, other: &'b Qty) -> Qty {
        Qty(self.0 + other.0)
    }
}

impl<'a, 'b> std::ops::Sub<&'b Qty> for &'a Qty {
    type Output = Qty;

    fn sub(self, other: &'b Qty) -> Qty {
        Qty(self.0 - other.0)
    }
}

impl std::ops::Neg for Qty {
    type Output = Qty;

    fn neg(self) -> Self::Output {
        Qty(-self.0)
    }
}

// #[derive(Debug, Default, Clone, Hash, Eq, PartialEq, Serialize, Deserialize)]
#[derive(Clone, Default)]
#[derive(Archive, Deserialize, Serialize, Debug, PartialEq)]
// This will generate a PartialEq impl between our unarchived and archived types
// #[archive(compare(PartialEq))]
// To use the safe API, you have to derive CheckBytes for the archived type
#[archive_attr(derive(CheckBytes, Debug))]
pub struct Money(pub Decimal); // TODO Currency,

impl From<MoneyOp> for Money {
    fn from(f: MoneyOp) -> Self {
        match f {
            MoneyOp::Incoming(number) => number,
            MoneyOp::Outgoing(number) => -number,
        }
    }
}

impl std::ops::Add<Money> for Money {
    type Output = Money;

    fn add(self, other: Money) -> Money {
        Money(self.0 + other.0)
    }
}

impl std::ops::AddAssign<Money> for Money {
    fn add_assign(&mut self, rhs: Money) {
        self.0 += rhs.0;
    }
}

impl std::ops::Sub<Money> for Money {
    type Output = Money;

    fn sub(self, other: Money) -> Money {
        Money(self.0 - other.0)
    }
}

impl std::ops::SubAssign<Money> for Money {
    fn sub_assign(&mut self, rhs: Money) {
        self.0 -= rhs.0;
    }
}

impl<'a, 'b> std::ops::Add<&'b Money> for &'a Money {
    type Output = Money;

    fn add(self, other: &'b Money) -> Money {
        Money(self.0 + other.0)
    }
}

impl<'a, 'b> std::ops::Sub<&'b Money> for &'b Money {
    type Output = Money;

    fn sub(self, other: &'b Money) -> Money {
        Money(self.0 - other.0)
    }
}

impl std::ops::Neg for Money {
    type Output = Money;

    fn neg(self) -> Self::Output {
        Money(-self.0)
    }
}

#[derive(Debug, Clone)]
pub(crate) enum MoneyOp {
    Incoming(Money),
    Outgoing(Money),
}

#[derive(Debug, Default, Clone, PartialEq)]
pub(crate) struct MoneyOps {
    pub(crate) incoming: Money,
    pub(crate) outgoing: Money,
}

impl std::ops::AddAssign<MoneyOps> for MoneyOps {
    fn add_assign(&mut self, rhs: MoneyOps) {
        self.incoming += rhs.incoming;
        self.outgoing += rhs.outgoing;
    }
}

impl std::ops::AddAssign<MoneyOp> for MoneyOps {
    fn add_assign(&mut self, rhs: MoneyOp) {
        match rhs {
            MoneyOp::Incoming(number) => self.incoming += number,
            MoneyOp::Outgoing(number) => self.outgoing += number,
        }
    }
}

impl std::ops::SubAssign<MoneyOps> for MoneyOps {
    fn sub_assign(&mut self, rhs: MoneyOps) {
        self.incoming -= rhs.incoming;
        self.outgoing -= rhs.outgoing;
    }
}

impl std::ops::SubAssign<MoneyOp> for MoneyOps {
    fn sub_assign(&mut self, rhs: MoneyOp) {
        match rhs {
            MoneyOp::Incoming(number) => self.incoming -= number,
            MoneyOp::Outgoing(number) => self.outgoing -= number,
        }
    }
}