use crate::memory::ID;

// TODO pub(crate) static STORE: ID = ID::for_constant("store"); ...

lazy_static! {
    pub(crate) static ref WH_TOPOLOGY: ID = ID::for_constant("warehouse_topology");

    pub(crate) static ref SPECIFIC_OF: ID = ID::for_constant("specific-of");
    pub(crate) static ref GOODS_RECEIVE: ID = ID::for_constant("GoodsReceive");
    pub(crate) static ref GOODS_ISSUE: ID = ID::for_constant("GoodsIssue");

    pub(crate) static ref STORE: ID = ID::for_constant("store");
    pub(crate) static ref GOODS: ID = ID::for_constant("goods");
    pub(crate) static ref DATE: ID = ID::for_constant("date");
    pub(crate) static ref QTY: ID = ID::for_constant("qty");
    pub(crate) static ref COST: ID = ID::for_constant("cost");
}
