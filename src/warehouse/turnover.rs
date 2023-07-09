use rkyv::{Archive, Deserialize, Serialize};

use derives::ImplID;
use values::ID;

use crate::animo::memory::Value;

// #[derive(Debug, Copy, Clone, Hash, Eq, PartialEq, Serialize, Deserialize, ImplID)]
#[derive(Clone, Copy, Eq, Hash, ImplID, Archive, Deserialize, Serialize, Debug, PartialEq)]
// This will generate a PartialEq impl between our unarchived and archived types
// #[archive(compare(PartialEq))]
// To use the safe API, you have to derive CheckBytes for the archived type
// #[archive_attr(derive(CheckBytes, Debug))]
pub struct Organization(ID);

// #[derive(Debug, Copy, Clone, Hash, Eq, PartialEq, Serialize, Deserialize, ImplID)]
#[derive(Clone, Copy, Eq, Hash, ImplID, Archive, Deserialize, Serialize, Debug, PartialEq)]
// This will generate a PartialEq impl between our unarchived and archived types
// #[archive(compare(PartialEq))]
// To use the safe API, you have to derive CheckBytes for the archived type
// #[archive_attr(derive(CheckBytes, Debug))]
pub struct Store(ID);

// #[derive(Debug, Copy, Clone, Hash, Eq, PartialEq, Serialize, Deserialize, ImplID)]
#[derive(Clone, Copy, Eq, Hash, ImplID, Archive, Deserialize, Serialize, Debug, PartialEq)]
// This will generate a PartialEq impl between our unarchived and archived types
// #[archive(compare(PartialEq))]
// To use the safe API, you have to derive CheckBytes for the archived type
// #[archive_attr(derive(CheckBytes, Debug))]
pub struct Goods(ID);

// #[derive(Debug, Default, Clone, Eq, PartialEq, Serialize, Deserialize)]
#[derive(Clone, Default, Eq, Archive, Deserialize, Serialize, Debug, PartialEq)]
// This will generate a PartialEq impl between our unarchived and archived types
// #[archive(compare(PartialEq))]
// To use the safe API, you have to derive CheckBytes for the archived type
// #[archive_attr(derive(CheckBytes, Debug))]
pub struct Turnover<V, O> {
  pub open: V,
  pub ops: O,
  pub close: V,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct NamedValue<N, V> {
  pub value: V,
  pub name: N,
}

impl<N, V> NamedValue<N, V> {
  pub(crate) fn new(name: N, value: V) -> Self {
    NamedValue { name, value }
  }
}
