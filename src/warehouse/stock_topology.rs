use std::sync::Arc;
use chrono::{Datelike, Timelike, TimeZone, Utc};
use serde::{Deserialize, Serialize};
use crate::animo::{AObject, AObjectInTopology, AOperation, AOperationInTopology, AggregationTopology, Memo, Txn, MemoOfList, PositionInTopology, DeltaOp};
use crate::error::DBError;
use crate::memory::{ID, ID_BYTES, ID_MAX, ID_MIN, Time};
use crate::RocksDB;
use crate::rocksdb::{FromBytes, FromKVBytes, ToBytes, ToKVBytes};
use crate::shared::*;
use crate::warehouse::balance::Balance;
use crate::warehouse::balance_operation::BalanceOperation;
use crate::warehouse::balance_operations::BalanceOps;
use crate::warehouse::base_topology::{WarehouseBalance, WarehouseMovement};
use crate::warehouse::{time_to_u64, ts_to_bytes, WarehouseTopology};

#[derive(Debug, Hash, Eq, PartialEq)]
pub struct WarehouseStockTopology(pub Arc<WarehouseTopology>);

impl AggregationTopology for WarehouseStockTopology {
    type DependantOn = WarehouseTopology;

    type InObj = Balance;
    type InOp = BalanceOperation;

    type InTObj = WarehouseBalance;
    type InTOp = WarehouseMovement;

    fn depends_on(&self) -> Arc<Self::DependantOn> {
        self.0.clone()
    }

    fn on_operation(&self, tx: &mut Txn, ops: &Vec<DeltaOp<Self::InObj,Self::InOp,Self::InTObj,Self::InTOp>>) -> Result<(), DBError> {
        // topology
        // [store + time] + goods = Balance,

        for op in ops {
            let delta = WarehouseStockDelta::from(op);

            tx.ops_manager().write_aggregation_delta(tx, delta)?;
        }

        Ok(())
    }
}

// [stock + time] + () + goods..
impl WarehouseStockTopology {
    pub(crate) fn goods(db: &RocksDB, store: ID, date: Time) -> Result<MemoOfList<WarehouseStock>,DBError> {
        let s = db.snapshot();
        let mut tx = Txn::new(&s);

        let memo = WarehouseStockTopology::goods_tx(&mut tx, store, date)?;

        // TODO: unregister memo if case of error
        tx.commit()?;

        Ok(memo)
    }

    fn goods_tx(tx: &mut Txn, store: ID, date: Time) -> Result<MemoOfList<WarehouseStock>, DBError> {
        debug!("listing memo at {:?} for {:?}", date, store);

        let checkpoint = WarehouseStockTopology::next_checkpoint(date)?;

        debug!("checkpoint {:?} > {:?}", date, checkpoint);

        let from = WarehouseStockTopology::position_at_start(store, checkpoint);
        let till = WarehouseStockTopology::position_at_end(store, checkpoint);

        debug!("goods_tx");
        debug!("from {:?}", from);
        debug!("till {:?}", till);

        let mut items = Vec::new();
        for (_,value) in tx.values(from, till) {
            items.push(Memo::new(value))
        }

        Ok(MemoOfList::new(items))
    }

    // beginning of next month
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
        let checkpoint = WarehouseStockTopology::next_checkpoint(time)?;
        debug!("position_of_aggregation {:?} > {:?}", time, checkpoint);
        Ok(WarehouseStockTopology::position_of_value(store, goods, checkpoint))
    }

    fn position_of_value(store: ID, goods: ID, time: Time) -> Vec<u8> {
        WarehouseStockTopology::position(store, goods, time_to_u64(time))
    }

    fn position_at_start(store: ID, time: Time) -> Vec<u8> {
        WarehouseStockTopology::position(store, ID_MIN, time_to_u64(time))
    }

    fn position_at_end(store: ID, time: Time) -> Vec<u8> {
        WarehouseStockTopology::position(store, ID_MAX, time_to_u64(time))
    }

    fn position_prefix(store: ID) -> Vec<u8> {
        let mut bs = Vec::with_capacity(ID_BYTES * 2);

        // operation prefix
        bs.extend_from_slice((*WH_STOCK_TOPOLOGY).as_slice());

        // prefix define calculation context
        bs.extend_from_slice(store.as_slice());

        bs
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

    fn decode_position_from_bytes(bs: &[u8]) -> Result<(ID,ID,Time), DBError> {
        debug!("decode_position_from_bytes: {:?}",bs);
        let expected = (ID_BYTES * 3) + 8;
        if bs.len() != expected {
            Err(format!("Warehouse stock topology: incorrect number ({}) of bytes, expected {}", bs.len(), expected).into())
        } else {
            let prefix: ID = bs[0..ID_BYTES].try_into()?;
            if prefix != *WH_STOCK_TOPOLOGY {
                Err(format!("incorrect prefix id ({:?}), expected {:?}", prefix, *WH_STOCK_TOPOLOGY).into())
            } else {
                let convert = |bs: &[u8]| -> [u8; 8] {
                    bs.try_into().expect("slice with incorrect length")
                };
                let store = bs[1*ID_BYTES..2*ID_BYTES].try_into()?;
                let ts = u64::from_be_bytes(convert(&bs[2*ID_BYTES..(2*ID_BYTES+8)]));
                let goods = bs[(2*ID_BYTES+8)..(3*ID_BYTES+8)].try_into()?;

                Ok((store, goods, Utc.timestamp_millis(ts as i64)))
            }
        }
    }
}

// two solutions:
//  - helper topology of goods existed at point in time (aka balance at time)
//    (point of trust because of force to keep list of all goods with balance)
//
//  - operations topology: store, time, goods = op (untrusted list of goods for given time)

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct WarehouseStock {
    balance: Balance,

    goods: ID,

    date: Time,
    store: ID,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WarehouseStockDelta {
    number_of_ops: i8,
    pub(crate) op: BalanceOps,

    // TODO avoid serialization & deserialize of prefix & position
    prefix: Vec<u8>,
    position: Vec<u8>,

    pub(crate) date: Time,

    pub(crate) store: ID,
    pub(crate) goods: ID,
}

impl WarehouseStockDelta {
    fn new(number_of_ops: i8, store: ID, goods: ID, date: Time, op: BalanceOps) -> Self {
        let prefix = WarehouseStockTopology::position_prefix(store);
        let position = WarehouseStockTopology::position_of_value(store, goods, date);
        WarehouseStockDelta { number_of_ops, store, goods, date, op, prefix, position }
    }
}

impl From<&DeltaOp<Balance,BalanceOperation,WarehouseBalance,WarehouseMovement>> for WarehouseStockDelta {
    fn from(delta: &DeltaOp<Balance,BalanceOperation,WarehouseBalance,WarehouseMovement>) -> Self {
        if let Some(before) = delta.before.as_ref() {
            if let Some(after) = delta.after.as_ref() {
                WarehouseStockDelta::new(
                    0,
                    after.store, after.goods, after.date,
                    BalanceOps::from(&after.op) - BalanceOps::from(&before.op)
                )
            } else {
                WarehouseStockDelta::new(
                    -1,
                    before.store, before.goods, before.date,
                    -BalanceOps::from(&before.op)
                )
            }
        } else if let Some(after) = delta.after.as_ref() {
            WarehouseStockDelta::new(
                1,
                after.store, after.goods, after.date,
                BalanceOps::from(&after.op)
            )
        } else {
            unreachable!("internal error")
        }
    }
}

impl PositionInTopology for WarehouseStockDelta {
    fn prefix(&self) -> &Vec<u8> {
        &self.prefix
    }

    fn position(&self) -> &Vec<u8> {
        &self.position
    }
}

impl AOperationInTopology<Balance, BalanceOps,WarehouseStock> for WarehouseStockDelta {

    fn position_of_aggregation(&self) -> Result<Vec<u8>,DBError> {
        WarehouseStockTopology::position_of_aggregation(self.store, self.goods, self.date)
    }

    fn operation(&self) -> BalanceOps {
        self.op.clone()
    }

    fn to_value(&self) -> WarehouseStock {
        WarehouseStock {
            store: self.store, goods: self.goods, date: self.date,
            balance: self.operation().to_value()
        }
    }
}

impl ToKVBytes for WarehouseStock {
    fn to_kv_bytes(&self) -> Result<(Vec<u8>, Vec<u8>), DBError> {
        let k = WarehouseStockTopology::position_of_value(self.store, self.goods, self.date);
        let v = self.balance.to_bytes()?;
        Ok((k,v))
    }
}

impl FromKVBytes<Self> for WarehouseStock {
    fn from_kv_bytes(key: &[u8], value: &[u8]) -> Result<WarehouseStock, DBError> {
        let (store, goods, date) = WarehouseStockTopology::decode_position_from_bytes(key)?;
        let balance = Balance::from_bytes(value)?;
        Ok(WarehouseStock { store, goods, date, balance })
    }
}

impl AObjectInTopology<Balance, BalanceOps,WarehouseStockDelta> for WarehouseStock {
    fn position(&self) -> Vec<u8> {
        WarehouseStockTopology::position_of_value(self.store, self.goods, self.date)
    }

    fn value(&self) -> &Balance {
        &self.balance
    }

    fn apply(&self, op: &WarehouseStockDelta) -> Result<Self, DBError> {
        // TODO check self.stock == op.stock && self.goods == op.goods && self.date >= op.date
        let balance = self.balance.apply_aggregation(&op.op)?;
        Ok(WarehouseStock { store: self.store, goods: self.goods, date: self.date, balance })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::cmp::Ordering;
    use crate::Memory;
    use crate::warehouse::test_util::{init, incoming, outgoing, time_end};

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
        let db = init();

        let wh1: ID = "wh1".into();
        let g1: ID = "g1".into();
        let g2: ID = "g2".into();

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
        let goods = WarehouseStockTopology::goods(&db, wh1, time_end("2022-05-31")).expect("Ok");
        assert_eq!(1, goods.len());

        debug!("MODIFY D");
        db.modify(incoming("D", "2022-05-15", wh1, g2, 7, Some(11))).expect("Ok");

        debug!("READING 2022-05-31");
        let goods = WarehouseStockTopology::goods(&db, wh1, time_end("2022-05-31")).expect("Ok");
        assert_eq!(2, goods.len());
    }
}