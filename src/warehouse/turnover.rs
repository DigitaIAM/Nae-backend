use rkyv::{Archive, Deserialize, Serialize};


use derives::ImplID;

use crate::animo::memory::{ID, Value};

// #[derive(Debug, Copy, Clone, Hash, Eq, PartialEq, Serialize, Deserialize, ImplID)]
#[derive(Clone, Copy, Eq, Hash, ImplID)]
#[derive(Archive, Deserialize, Serialize, Debug, PartialEq)]
// This will generate a PartialEq impl between our unarchived and archived types
// #[archive(compare(PartialEq))]
// To use the safe API, you have to derive CheckBytes for the archived type
// #[archive_attr(derive(CheckBytes, Debug))]
pub struct Organization(ID);

// #[derive(Debug, Copy, Clone, Hash, Eq, PartialEq, Serialize, Deserialize, ImplID)]
#[derive(Clone, Copy, Eq, Hash, ImplID)]
#[derive(Archive, Deserialize, Serialize, Debug, PartialEq)]
// This will generate a PartialEq impl between our unarchived and archived types
// #[archive(compare(PartialEq))]
// To use the safe API, you have to derive CheckBytes for the archived type
// #[archive_attr(derive(CheckBytes, Debug))]
pub struct Store(ID);

// #[derive(Debug, Copy, Clone, Hash, Eq, PartialEq, Serialize, Deserialize, ImplID)]
#[derive(Clone, Copy, Eq, Hash, ImplID)]
#[derive(Archive, Deserialize, Serialize, Debug, PartialEq)]
// This will generate a PartialEq impl between our unarchived and archived types
// #[archive(compare(PartialEq))]
// To use the safe API, you have to derive CheckBytes for the archived type
// #[archive_attr(derive(CheckBytes, Debug))]
pub struct Goods(ID);

// #[derive(Debug, Default, Clone, Eq, PartialEq, Serialize, Deserialize)]
#[derive(Clone, Default, Eq)]
#[derive(Archive, Deserialize, Serialize, Debug, PartialEq)]
// This will generate a PartialEq impl between our unarchived and archived types
// #[archive(compare(PartialEq))]
// To use the safe API, you have to derive CheckBytes for the archived type
// #[archive_attr(derive(CheckBytes, Debug))]
pub(crate) struct Turnover<V,O> {
    pub(crate) open: V,
    pub(crate) ops: O,
    pub(crate) close: V,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub(crate) struct NamedValue<N,V> {
    pub(crate) value: V,
    pub(crate) name: N,
}

impl<N,V> NamedValue<N,V> {
    pub(crate) fn new(name: N, value: V) -> Self {
        NamedValue { name, value }
    }
}