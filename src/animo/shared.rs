use crate::animo::memory::ID;

// TODO pub(crate) static STORE: ID = ID::for_constant("store"); ...

lazy_static::lazy_static! {
    pub(crate) static ref DESC: ID = ID::for_constant("description");

    pub(crate) static ref LABEL: ID = ID::for_constant("label");
    pub(crate) static ref REFERENCE: ID = ID::for_constant("reference");

    pub(crate) static ref CAN_BUY_FROM: ID = ID::for_constant("can-buy-from");
    pub(crate) static ref FROM: ID = ID::for_constant("from");
    pub(crate) static ref MINIMUM_ORDER_QTY: ID = ID::for_constant("minimum-order-qty");

    pub(crate) static ref NUMBER: ID = ID::for_constant("number");
    pub(crate) static ref UOM: ID = ID::for_constant("unit-of-measurement");

    pub(crate) static ref UOM_PIECE: ID = ID::for_constant("currency-piece");
    pub(crate) static ref UOM_METER: ID = ID::for_constant("currency-meter");

    pub(crate) static ref CURRENCY: ID = ID::for_constant("currency");

    pub(crate) static ref EUR: ID = ID::for_constant("currency-eur");
    pub(crate) static ref USD: ID = ID::for_constant("currency-usd");

    // контрагент
    pub(crate) static ref COUNTERPARTY: ID = ID::for_constant("counterparty");
    // поставщик
    pub(crate) static ref SUPPLIER: ID = ID::for_constant("supplier");
    // покупатель
    pub(crate) static ref CUSTOMER: ID = ID::for_constant("customer");
    

    pub(crate) static ref WH_BASE_TOPOLOGY: ID = ID::for_constant("warehouse_base_topology");
    pub(crate) static ref WH_STOCK_TOPOLOGY: ID = ID::for_constant("warehouse_stock_topology");
    pub(crate) static ref WH_STORE_TOPOLOGY: ID = ID::for_constant("warehouse_store_topology");
    pub(crate) static ref WH_AGGREGATION_TOPOLOGY: ID = ID::for_constant("warehouse_aggregation_topology");
    pub(crate) static ref WH_AGGREGATION_CHECKPOINTS: ID = ID::for_constant("warehouse_aggregation_checkpoints");

    pub(crate) static ref SPECIFIC_OF: ID = ID::for_constant("specific-of");
    pub(crate) static ref GOODS_RECEIVE: ID = ID::for_constant("GoodsReceive");
    pub(crate) static ref GOODS_ISSUE: ID = ID::for_constant("GoodsIssue");
    pub(crate) static ref GOODS_TRANSFER: ID = ID::for_constant("GoodsTransfer");

    pub(crate) static ref STORE: ID = ID::for_constant("store");
    pub(crate) static ref GOODS: ID = ID::for_constant("goods");
    pub(crate) static ref DATE: ID = ID::for_constant("date");
    pub(crate) static ref QTY: ID = ID::for_constant("qty");
    pub(crate) static ref PRICE: ID = ID::for_constant("price");
    pub(crate) static ref COST: ID = ID::for_constant("cost");
}
