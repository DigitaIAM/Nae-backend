use serde::{Deserialize, Serialize};
use derives::ImplBytes;
use crate::animo::db::{FromBytes, FromKVBytes, ToBytes, ToKVBytes};

use std::sync::Arc;
use rkyv::AlignedVec;
use crate::animo::{AggregationTopology, AObject, AOperation, DeltaOp, MemoOfList, Object, Time, TimeInterval, Txn};
use crate::animo::error::DBError;
use crate::animo::memory::ID;
use crate::AnimoDB;
use crate::warehouse::balance::WHBalance;
use crate::warehouse::balance_operation::BalanceOperation;
use crate::warehouse::balance_operations::BalanceOps;
use crate::warehouse::base_topology::{WarehouseBalance, WarehouseMovement};
use crate::warehouse::goods_topology::StockDelta;
use crate::warehouse::{WHGoodsTopology, WHTopology};
use crate::warehouse::primitives::{Money, MoneyOps};
use crate::warehouse::turnover::*;

// [store + from_time + till_time] + goods = (open_balance, operations, close_balance)

#[derive(Debug, Hash, Eq, PartialEq)]
pub struct WHReportTopology(pub Arc<WHTopology>);

impl WHReportTopology {
    // TODO interval, organization = Vec<(store, open_turnover_close)>
    fn report_stores_level(db: &AnimoDB, interval: TimeInterval) -> Result<MemoOfList<NamedValue<Store,Turnover<Money,MoneyOps>>>, DBError> {
        WHGoodsTopology::stores(db, interval)
    }

    // TODO interval, organization+store = Vec<(goods, open_turnover_close)>
    fn report_goods_level(from: Time, till: Time, store: Store) {
        todo!()
    }

    // interval, organization+store+goods = Vec<(event, open_turnover_close)>
    fn report_events_level(from: Time, till: Time, org: ID, store: ID, goods: ID) {
        todo!()
    }

    // interval, prefix = Vec<(event, open_turnover_close)>
    fn report(from: Time, till: Time, prefix: ID) {
        todo!()
    }
}

impl AggregationTopology for WHReportTopology {
    type DependantOn = WHTopology;

    type InObj = WHBalance;
    type InOp = BalanceOperation;

    type InTObj = WarehouseBalance;
    type InTOp = WarehouseMovement;

    fn depends_on(&self) -> Arc<Self::DependantOn> {
        self.0.clone()
    }

    fn on_operation(&self, tx: &mut Txn, ops: &Vec<DeltaOp<Self::InObj,Self::InOp,Self::InTObj,Self::InTOp>>) -> Result<(), DBError> {
        for op in ops {
            let delta = StockDelta::from(op);

            tx.ops_manager().write_aggregation_delta(tx, delta)?;
        }

        Ok(())
    }
}

#[derive(Debug, Clone)] // , Serialize, Deserialize)] // , ImplBytes
struct ReportFigures {
    number_of_ops: i32,
    open: WHBalance,
    ops: BalanceOps,
    close: WHBalance,

    from: Time,
    till: Time,
}

impl ToBytes for ReportFigures {
    fn to_bytes(&self) -> Result<AlignedVec, DBError> {
        todo!()
    }
}

impl FromBytes<Self> for ReportFigures {
    fn from_bytes(bs: &[u8]) -> Result<Self, DBError> {
        todo!()
    }
}

impl AObject<ReportDelta> for ReportFigures {
    fn is_zero(&self) -> bool {
        self.number_of_ops == 0 && self.close.is_zero()
    }

    fn apply_aggregation(&self, op: &ReportDelta) -> Result<Self, DBError> {
        let v = if op.date < self.from {
            ReportFigures {
                number_of_ops: self.number_of_ops,
                open: &self.open + &op.ops.to_value(),
                ops: self.ops.clone(),
                close: &self.close + &op.ops.to_value(),

                from: self.from.clone(),
                till: self.till.clone(),
            }
        } else { // if op.date => self.from {
            if op.date <= self.till {
                ReportFigures {
                    number_of_ops: self.number_of_ops + op.number_of_ops as i32,
                    open: self.open.clone(),
                    ops: self.ops.clone() + op.ops.clone(),
                    close: &self.close + &op.ops.to_value(),

                    from: self.from.clone(),
                    till: self.till.clone(),
                }
            } else { // if op.date > self.till {
                self.clone()
            }
        };
        Ok(v)
    }
}

#[derive(Debug, Clone)] // , Serialize, Deserialize)] // , ImplBytes
pub struct ReportDelta {
    number_of_ops: i8,
    ops: BalanceOps,

    date: Time,
}

impl ToBytes for ReportDelta {
    fn to_bytes(&self) -> Result<AlignedVec, DBError> {
        todo!()
    }
}

impl FromBytes<Self> for ReportDelta {
    fn from_bytes(bs: &[u8]) -> Result<Self, DBError> {
        todo!()
    }
}

impl AOperation<ReportFigures> for ReportDelta {
    fn to_value(&self) -> ReportFigures {
        ReportFigures {
            number_of_ops: self.number_of_ops as i32,
            open: WHBalance::default(),
            ops: self.ops.clone(),
            close: self.ops.to_value(),

            from: self.date.clone(),
            till: self.date.clone(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::cmp::Ordering;
    use crate::Memory;
    use crate::warehouse::test_util::{init, incoming, outgoing, delete};

    #[test]
    fn test_bytes_order_of_u64() {
        let min = 0_u64;
        let mut bs1 = min.to_be_bytes();
        for num in (min+1)..10_000_000_u64 {
            if num % 1_000_000_u64 == 0 {
                print!(".");
            }
            let bs2 = num.to_be_bytes();
            assert_eq!(Ordering::Less, bs1.as_slice().cmp(bs2.as_slice()));
            bs1 = bs2;
        }
    }

    #[ignore]
    #[test]
    fn test_bytes_order_of_i64() {
        let min = -10_000_000_i64;
        let mut bs1 = min.to_be_bytes();
        for num in (min+1)..10_000_000_i64 {
            if num % 1_000_000_i64 == 0 {
                print!(".");
            }
            let bs2 = num.to_be_bytes();
            assert_eq!(
                Ordering::Less,
                bs1.as_slice().cmp(bs2.as_slice()),
                "Number: {}\nprev:{:?}\nnext:{:?}", num, bs1.as_slice(), bs2.as_slice()
            );
            bs1 = bs2;
        }
    }

    #[test]
    fn test_warehouse_stock() {
        let (tmp_dir, settings, db) = init();

        let wh1: Store = ID::from("wh1").into();
        let g1: Goods = ID::from("g1").into();
        let g2: Goods = ID::from("g2").into();

        let d22_05_31 = Time::new("2022-05-31").unwrap();

        log::debug!("MODIFY A");
        db.modify(incoming("A", "2022-05-27", wh1, g1, 10, Some(50))).expect("Ok");
        log::debug!("MODIFY B");
        db.modify(incoming("B", "2022-05-30", wh1, g1, 2, Some(10))).expect("Ok");
        log::debug!("MODIFY C");
        db.modify(outgoing("C", "2022-05-28", wh1, g1, 5, Some(25))).expect("Ok");

        // 2022-05-27	qty	10	cost	50	=	10	50
        // 2022-05-28	qty	-5	cost	-25	=	5	25		< 2022-05-28
        // 2022-05-30	qty	2	cost	10	=	7 	35
        // 													< 2022-05-31

        log::debug!("READING [1] 2022-05-31");
        let whs = WHReportTopology::report_stores_level(
            &db, TimeInterval::new("2022-05-31", "2022-05-31").unwrap()
        ).expect("Ok");
        assert_eq!(1, whs.len());

        log::debug!("MODIFY D");
        db.modify(incoming("D", "2022-05-15", wh1, g2, 7, Some(11))).expect("Ok");

        log::debug!("READING [2] 2022-05-31");
        let goods = WHGoodsTopology::goods(&db, wh1, d22_05_31.clone()).expect("Ok");
        assert_eq!(2, goods.len());

        log::debug!("DELETE D");
        db.modify(delete(incoming("D", "2022-05-15", wh1, g2, 7, Some(11)))).expect("Ok");

        log::debug!("READING [3] 2022-05-31");
        let goods = WHGoodsTopology::goods(&db, wh1, d22_05_31.clone()).expect("Ok");
        assert_eq!(1, goods.len());

        // stop db and delete data folder
        db.close();
        tmp_dir.close().unwrap();
    }
}