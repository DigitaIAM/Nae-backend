use serde::{Deserialize, Serialize};
use rust_decimal::Decimal;

#[derive(Debug, Clone, Hash, Eq, PartialEq, Default, Serialize, Deserialize)]
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

#[derive(Debug, Clone, Hash, Eq, PartialEq, Default, Serialize, Deserialize)]
pub struct Money(pub Decimal); // TODO Currency,

impl std::ops::Add<Money> for Money {
    type Output = Money;

    fn add(self, other: Money) -> Money {
        Money(self.0 + other.0)
    }
}

impl std::ops::Sub<Money> for Money {
    type Output = Money;

    fn sub(self, other: Money) -> Money {
        Money(self.0 - other.0)
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
