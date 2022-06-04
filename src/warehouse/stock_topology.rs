use chrono::{Datelike, Timelike, TimeZone, Utc};
use serde::{Deserialize, Serialize};
use crate::animo::{AObject, AObjectInTopology, AOperation, AOperationInTopology, AggregationTopology, Memo, Txn};
use crate::error::DBError;
use crate::memory::{ID, ID_BYTES, ID_MAX, ID_MIN, Time};
use crate::rocksdb::{FromKVBytes, ToKVBytes};
use crate::shared::WH_STOCK_TOPOLOGY;
use crate::warehouse::balance::Balance;
use crate::warehouse::balance_operation::BalanceOperation;
use crate::warehouse::balance_operations::BalanceOps;
use crate::warehouse::base_topology::{WarehouseBalance, WarehouseMovement};
use crate::warehouse::{time_to_u64, ts_to_bytes, WarehouseTopology};

#[derive(Debug, Default, Hash, Eq, PartialEq)]
pub struct WarehouseStockTopology();

impl AggregationTopology for WarehouseStockTopology {
    type DependantOn = WarehouseTopology;

    type InObj = Balance;
    type InOp = BalanceOperation;

    type InTObj = WarehouseBalance;
    type InTOp = WarehouseMovement;

    fn depends_on(&self) -> Self::DependantOn {
        todo!()
    }

    fn on_operation(&self, tx: &mut Txn, ops: &Vec<Self::InTOp>) -> Result<(), DBError> {
        // topology
        // [store + time] + goods = Balance,

        for op in ops {
            let delta = WarehouseStockDelta::from(op);

            tx.ops_manager().write_aggregation_delta(tx, delta)?;
        }

        Ok(())
    }
}

// two solutions:
//  - helper topology of goods existed at point in time (aka balance at time)
//    (point of trust because of force to keep list of all goods with balance)
//
//  - operations topology: store, time, goods = op (untrusted list of goods for given time)

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct WarehouseStock {
    // [stock + time] + () + goods..
    balance: Balance,

    goods: ID,

    date: Time,
    store: ID,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WarehouseStockDelta {
    pub(crate) op: BalanceOps,

    pub(crate) from: Time,
    pub(crate) till: Time,

    pub(crate) store: ID,
    pub(crate) goods: ID,
}

impl From<&WarehouseMovement> for WarehouseStockDelta {
    fn from(op: &WarehouseMovement) -> Self {
        WarehouseStockDelta {
            store: op.store,
            goods: op.goods,

            from: op.date,
            till: op.date,

            op: BalanceOps::from(&op.op),
        }
    }
}

impl AOperationInTopology<Balance, BalanceOps,WarehouseStock> for WarehouseStockDelta {
    fn position(&self) -> Vec<u8> {
        WarehouseStock::position_of_value(self.store, self.goods, self.till)
    }

    fn position_of_aggregation(&self) -> Result<Vec<u8>,DBError> {
        WarehouseStock::position_of_aggregation(self.store, self.goods, self.till)
    }

    fn operation(&self) -> BalanceOps {
        self.op.clone()
    }

    fn to_value(&self) -> WarehouseStock {
        WarehouseStock {
            store: self.store, goods: self.goods, date: self.till,
            balance: self.operation().to_value()
        }
    }
}

impl ToKVBytes for WarehouseStock {
    fn to_kv_bytes(&self) -> Result<(Vec<u8>, Vec<u8>), DBError> {
        todo!()
    }
}

impl FromKVBytes<Self> for WarehouseStock {
    fn from_kv_bytes(key: &[u8], value: &[u8]) -> Result<Self, DBError> {
        todo!()
    }
}

impl AObjectInTopology<Balance, BalanceOps,WarehouseStockDelta> for WarehouseStock {
    fn position(&self) -> Vec<u8> {
        WarehouseStock::position_of_value(self.store, self.goods, self.date)
    }

    fn value(&self) -> Balance {
        self.balance.clone()
    }

    fn apply(&self, op: &WarehouseStockDelta) -> Result<Self, DBError> {
        // TODO check self.stock == op.stock && self.goods == op.goods && self.date >= op.date
        let balance = self.balance.apply_aggregation(&op.op)?;
        Ok(WarehouseStock { store: self.store, goods: self.goods, date: self.date, balance })
    }
}

impl WarehouseStock {
    pub(crate) fn goods(tx: &mut Txn, store: ID, date: Time) -> Result<Memo<Vec<Memo<WarehouseStock>>>, DBError> {
        debug!("listing memo at {:?} for {:?}", date, store);

        let from = WarehouseStock::position_at_start(store, date);
        let till = WarehouseStock::position_at_start(store, date);

        let mut items = Vec::new();
        for (_,value) in tx.values(from, till) {
            items.push(Memo::new(value))
        }

        Ok(Memo::new(items))
    }

    fn next_checkpoint(time: Time) -> Result<Time, DBError> {
        if time.day() == 1 && time.num_seconds_from_midnight() == 0 && time.nanosecond() == 0 {
            Ok(time)
        } else {
            // beginning of next month
            Utc.ymd_opt(time.year(), time.month() + 1, 1)
                .single()
                .or_else(|| Utc.ymd_opt(time.year() + 1, 1, 1).single())
                .map_or_else(|| None, |d| d.and_hms_milli_opt(0, 0, 0, 0))
                .ok_or_else(|| format!("").into())
        }
    }

    fn position_of_aggregation(store: ID, goods: ID, time: Time) -> Result<Vec<u8>, DBError> {
        let checkpoint = WarehouseStock::next_checkpoint(time)?;
        Ok(WarehouseStock::position_of_value(store, goods, checkpoint))
    }

    fn position_of_value(store: ID, goods: ID, time: Time) -> Vec<u8> {
        WarehouseStock::position(store, goods, time_to_u64(time))
    }

    fn position_at_start(store: ID, time: Time) -> Vec<u8> {
        WarehouseStock::position(store, ID_MIN, time_to_u64(time))
    }

    fn position_at_end(store: ID, time: Time) -> Vec<u8> {
        WarehouseStock::position(store, ID_MAX, time_to_u64(time))
    }

    fn position(store: ID, goods: ID, ts: u64) -> Vec<u8> {
        let mut bs = Vec::with_capacity((ID_BYTES * 3) + 8);

        // operation prefix
        bs.extend_from_slice((*WH_STOCK_TOPOLOGY).as_slice());

        // prefix define calculation context
        bs.extend_from_slice(store.as_slice());

        // define order by time
        bs.extend_from_slice(ts_to_bytes(ts).as_slice());

        // suffix
        bs.extend_from_slice(goods.as_slice());

        bs
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::cmp::Ordering;
    use std::sync::Arc;
    use chrono::DateTime;
    use crate::{Memory, RocksDB};
    use crate::animo::{Animo, Topology};
    use crate::warehouse::primitives::{Money, Qty};
    use crate::warehouse::test_util::{incoming, outgoing};

    fn init() {
        std::env::set_var("RUST_LOG", "actix_web=debug,nae_backend=debug");
        let _ = env_logger::builder().is_test(true).try_init();
    }

    #[test]
    fn test_bytes_order() {
        println!("testing order");
        let mut bs1 = 0_u64.to_ne_bytes();
        for num in 1_u64..100_000_000_u64 {
            if num % 10_000_000_u64 == 0 {
                print!(".");
            }
            let bs2 = num.to_be_bytes();
            assert_eq!(Ordering::Less, bs1.as_slice().cmp(bs2.as_slice()));
            bs1 = bs2;
        }
    }

    #[test]
    fn test_warehouse_stock() {
        init();

        let tmp_dir = tempfile::tempdir().unwrap();
        let tmp_path = tmp_dir.path().to_str().unwrap();
        let mut db: RocksDB = Memory::init(tmp_path).unwrap();
        let mut animo = Animo::default();
        animo.register_topology(Topology::Warehouse(Arc::new(WarehouseTopology())));
        animo.register_topology(Topology::WarehouseStock(Arc::new(WarehouseStockTopology())));
        db.register_dispatcher(Arc::new(animo)).unwrap();

        let time = |dt: &str| -> Time {
            DateTime::parse_from_rfc3339(format!("{}T00:00:00Z", dt).as_str()).unwrap().into()
        };

        let wh1: ID = "wh1".into();
        let g1: ID = "g1".into();

        debug!("MODIFY A");
        db.modify(incoming("A", "2022-05-27", wh1, g1, 10, Some(50))).expect("Ok");
        debug!("MODIFY B");
        db.modify(incoming("B", "2022-05-30", wh1, g1, 2, Some(10))).expect("Ok");
        debug!("MODIFY C");
        db.modify(outgoing("C", "2022-05-28", wh1, g1, 5, Some(25))).expect("Ok");

        // 2022-05-27	qty	10	cost	50	=	10	50
        // 2022-05-28	qty	-5	cost	-25	=	5	25		< 2022-05-28
        // 2022-05-30	qty	2	cost	10	=	7 	35
        // 													< 2022-05-31

        debug!("READING 2022-05-31");
        let g1_balance = WarehouseTopology::balance(&db, wh1, g1, time("2022-05-31")).expect("Ok");
        assert_eq!(Balance(Qty(7.into()),Money(35.into())), g1_balance.value().into());

        debug!("READING 2022-05-28");
        let g1_balance = WarehouseTopology::balance(&db, wh1, g1, time("2022-05-28")).expect("Ok");
        assert_eq!(Balance(Qty(5.into()),Money(25.into())), g1_balance.value().into());

        debug!("READING 2022-05-31");
        let g1_balance = WarehouseTopology::balance(&db, wh1, g1, time("2022-05-31")).expect("Ok");
        assert_eq!(Balance(Qty(7.into()),Money(35.into())), g1_balance.value().into());

        debug!("MODIFY D");
        db.modify(outgoing("D", "2022-05-31", wh1, g1, 1, Some(5))).expect("Ok");

        debug!("READING 2022-05-31");
        let g1_balance = WarehouseTopology::balance(&db, wh1, g1, time("2022-05-31")).expect("Ok");
        assert_eq!(Balance(Qty(6.into()),Money(30.into())), g1_balance.value().into());
    }
}