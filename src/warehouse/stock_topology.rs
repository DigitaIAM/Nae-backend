use chrono::{Datelike, Timelike, TimeZone, Utc};
use serde::{Deserialize, Serialize};
use crate::animo::{AObject, AggregationObjectInTopology, AOperation, AggregationOperationInTopology, AggregationTopology, Memo, Object, Txn};
use crate::error::DBError;
use crate::memory::{ID, ID_BYTES, Time};
use crate::rocksdb::{FromKVBytes, ToKVBytes};
use crate::warehouse::balance::Balance;
use crate::warehouse::balance_operation::BalanceOperation;
use crate::warehouse::balance_operations::BalanceOps;
use crate::warehouse::base_topology::{WarehouseBalance, WarehouseMovement};
use crate::warehouse::{time_to_bytes, WarehouseTopology};

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
    // [stock + time] + () + goods
    store: ID,
    goods: ID,
    date: Time,

    balance: Balance,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WarehouseStockDelta {
    pub(crate) store: ID,
    pub(crate) goods: ID,

    pub(crate) from: Time,
    pub(crate) till: Time,

    pub(crate) op: BalanceOps,
}

impl ToKVBytes for WarehouseStockDelta {
    fn to_kv_bytes(&self) -> Result<(Vec<u8>, Vec<u8>), DBError> {
        todo!()
    }
}

impl FromKVBytes<WarehouseStockDelta> for WarehouseStockDelta {
    fn from_kv_bytes(key: &[u8], value: &[u8]) -> Result<WarehouseStockDelta, DBError> {
        todo!()
    }
}

impl From<&WarehouseMovement> for WarehouseStockDelta {
    fn from(op: &WarehouseMovement) -> Self {
        WarehouseStockDelta {
            store: op.store,
            goods: op.goods,

            from: op.date,
            till: op.date,

            op: op.op.clone().into(),
        }
    }
}

impl AggregationOperationInTopology<Balance, BalanceOps,WarehouseStock> for WarehouseStockDelta {
    fn position(&self) -> Vec<u8> {
        WarehouseStock::local_topology_position(self.store, self.goods, self.till)
    }

    fn position_of_aggregation(&self) -> Result<Vec<u8>,DBError> {
        WarehouseStock::local_topology_position_of_aggregation(self.store, self.goods, self.till)
    }

    fn operation(&self) -> BalanceOps {
        self.op.clone()
    }

    fn delta_between(&self, other: &Self) -> Self {
        todo!()
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

impl AggregationObjectInTopology<Balance, BalanceOps,WarehouseStockDelta> for WarehouseStock {
    fn apply(&self, op: &WarehouseStockDelta) -> Result<Self, DBError> {
        // TODO check self.stock == op.stock && self.goods == op.goods && self.date >= op.date
        let balance = self.balance.apply_aggregation(&op.op)?;
        Ok(WarehouseStock { store: self.store, goods: self.goods, date: self.date, balance })
    }
}

impl WarehouseStock {
    fn load(k: &Vec<u8>, v: Balance) -> Result<Self, DBError> {
        todo!()
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

    fn local_topology_position_of_aggregation(store: ID, goods: ID, time: Time) -> Result<Vec<u8>, DBError> {
        let checkpoint = WarehouseStock::next_checkpoint(time)?;
        Ok(WarehouseStock::local_topology_position(store, goods, checkpoint))
    }

    fn local_topology_position(store: ID, goods: ID, time: Time) -> Vec<u8> {
        let mut bs = Vec::with_capacity((ID_BYTES * 3) + 8);

        // operation prefix
        bs.extend_from_slice(ID::from("WarehouseStock").as_slice());

        // prefix define calculation context
        bs.extend_from_slice(store.as_slice());

        // define order by time
        bs.extend_from_slice(time_to_bytes(time).as_slice());

        // suffix
        bs.extend_from_slice(goods.as_slice());

        bs
    }

    pub(crate) fn get_memo(tx: &mut Txn, store: ID, goods: ID, date: Time) -> Result<Memo<WarehouseStock>, DBError> {
        // TODO move method to Ops manager
        let ops_manager = tx.s.rf.ops_manager.clone();

        let position = WarehouseStock::local_topology_position(store, goods, date);

        debug!("pining memo at {:?}", position);

        let stock = if let Some((r_position, balance)) = ops_manager.get_closest_memo::<Balance>(tx.s, position.clone())? {
            let mut stock = WarehouseStock::load(&r_position, balance)?;

            debug!("closest memo {:?}", stock);
            if r_position != position {
                debug!("calculate from closest memo");
                for (_,op) in WarehouseTopology::get_ops(tx, stock.store, stock.goods, stock.date, date) {
                    stock.balance = stock.balance.apply(&op)?;
                }

                // store memo
                tx.update_value(&position, &stock.balance)?;
            }
            stock
        } else {
            let balance_memo = WarehouseTopology::balance_tx(tx, store, goods, date)?;
            let balance = balance_memo.value().balance;

            // store memo
            tx.update_value(&position, &balance)?;

            WarehouseStock { store, goods, date, balance }
        };
        Ok(Memo::new(stock))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::shared::*;

    use std::cmp::Ordering;
    use std::sync::Arc;
    use chrono::DateTime;
    use crate::{Memory, RocksDB};
    use crate::animo::{Animo, Topology};
    use crate::memory::{ChangeTransformation, Context, Transformation, Value};
    use crate::warehouse::primitives::{Money, Qty};

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

        let event = |doc: &str, date: &str, class: ID, goods: ID, qty: u32, cost: Option<u32>| {
            let context: Context = vec![doc.into()].into();
            let mut records = vec![
                Transformation::new(&context, *SPECIFIC_OF, class.into()),
                Transformation::new(&context, *DATE, time(date).into()),
                Transformation::new(&context, *STORE, wh1.into()),
                Transformation::new(&context, *GOODS,goods.into()),
                Transformation::new(&context, *QTY,qty.into()),
            ];
            if let Some(cost) = cost {
                records.push(Transformation::new(&context, *COST, cost.into()));
            }
            records.iter().map(|t| ChangeTransformation {
                context: t.context.clone(),
                what: t.what.clone(),
                into_before: Value::Nothing,
                into_after: t.into.clone()
            }).collect::<Vec<_>>()
        };

        debug!("MODIFY A");
        db.modify(event("A", "2022-05-27", *GOODS_RECEIVE, g1, 10, Some(50))).expect("Ok");
        debug!("MODIFY B");
        db.modify(event("B", "2022-05-30", *GOODS_RECEIVE, g1, 2, Some(10))).expect("Ok");
        debug!("MODIFY C");
        db.modify(event("C", "2022-05-28", *GOODS_ISSUE, g1, 5, Some(25))).expect("Ok");

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
        db.modify(event("D", "2022-05-31", *GOODS_ISSUE, g1, 1, Some(5))).expect("Ok");

        debug!("READING 2022-05-31");
        let g1_balance = WarehouseTopology::balance(&db, wh1, g1, time("2022-05-31")).expect("Ok");
        assert_eq!(Balance(Qty(6.into()),Money(30.into())), g1_balance.value().into());
    }
}