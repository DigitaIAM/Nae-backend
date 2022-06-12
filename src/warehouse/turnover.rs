use derives::ImplID;
use serde::{Deserialize, Serialize};
use crate::animo::memory::{ID, Value};

#[derive(Debug, Copy, Clone, Hash, Eq, PartialEq, Serialize, Deserialize, ImplID)]
pub(crate) struct Organization(ID);

#[derive(Debug, Copy, Clone, Hash, Eq, PartialEq, Serialize, Deserialize, ImplID)]
pub(crate) struct Store(ID);

#[derive(Debug, Copy, Clone, Hash, Eq, PartialEq, Serialize, Deserialize, ImplID)]
pub(crate) struct Goods(ID);

#[derive(Debug, Default, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub(crate) struct Turnover<V,O> {
    pub(crate) open: V,
    pub(crate) ops: O,
    pub(crate) close: V,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub(crate) struct NamedValue<N,V> {
    pub(crate) name: N,
    pub(crate) value: V,
}

impl<N,V> NamedValue<N,V> {
    pub(crate) fn new(name: N, value: V) -> Self {
        NamedValue { name, value }
    }
}