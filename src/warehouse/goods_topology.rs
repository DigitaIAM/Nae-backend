use std::ops::{Add, Neg, Sub};
use std::sync::Arc;
use chrono::{Datelike, Timelike, TimeZone, Utc};
use serde::{Deserialize, Serialize};
use derives::ImplBytes;
use rkyv::AlignedVec;
use crate::animo::*;
use crate::animo::error::DBError;
use crate::animo::memory::*;
use crate::AnimoDB;
use crate::animo::db::{FromBytes, FromKVBytes, ToBytes, ToKVBytes};
use crate::animo::ops_manager::*;
use crate::animo::shared::*;
use crate::warehouse::balance::WHBalance;
use crate::warehouse::balance_operation::BalanceOperation;
use crate::warehouse::balance_operations::BalanceOps;
use crate::warehouse::base_topology::{WarehouseBalance, WarehouseMovement};
use crate::warehouse::WHTopology;
use crate::warehouse::primitives::*;
use crate::warehouse::turnover::*;

// [store + time] + goods = (number_of_ops, Balance)

#[derive(Debug, Hash, Eq, PartialEq)]
pub struct WHGoodsTopology(pub Arc<WHTopology>);

impl AggregationTopology for WHGoodsTopology {
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

impl WHGoodsTopology {
    pub(crate) fn stores(db: &AnimoDB, interval: TimeInterval) -> Result<MemoOfList<NamedValue<Store,Turnover<Money,MoneyOps>>>, DBError> {
        let s = db.snapshot();
        let mut tx = Txn::new(&s);

        let memo = WHGoodsTopology::stores_tx(&mut tx, interval)?;

        tx.commit()?;

        Ok(memo)
    }

    fn stores_tx(tx: &mut Txn, interval: TimeInterval) -> Result<MemoOfList<NamedValue<Store,Turnover<Money,MoneyOps>>>, DBError> {
        log::debug!("listing stores at {:?}", interval);

        let checkpoint_from = WHGoodsTopology::prev_checkpoint(&interval.from);
        let checkpoint_till = WHGoodsTopology::next_checkpoint(&interval.till);

        log::debug!("checkpoint from {:?} > {:?}", interval.from, checkpoint_from);
        log::debug!("checkpoint till {:?} > {:?}", interval.till, checkpoint_till);

        // get stores in checkpoints interval
        // TODO let stores = WHStockTopology::existence(checkpoint_from, checkpoint_till);


        todo!()
    }


    pub(crate) fn goods(db: &AnimoDB, store: Store, date: Time) -> Result<MemoOfList<WarehouseStock>,DBError> {
        let s = db.snapshot();
        let mut tx = Txn::new(&s);

        let memo = WHGoodsTopology::goods_tx(&mut tx, store, date)?;

        // TODO: unregister memo if case of error
        tx.commit()?;

        Ok(memo)
    }

    fn goods_tx(tx: &mut Txn, store: Store, date: Time) -> Result<MemoOfList<WarehouseStock>, DBError> {
        todo!()
        // log::debug!("listing memo at {:?} for {:?}", date, store);
        //
        // let checkpoint = WHGoodsTopology::next_checkpoint(&date);
        //
        // log::debug!("checkpoint {:?} > {:?}", date, checkpoint);
        //
        // let from = WHGoodsTopology::position_at_start(store, checkpoint.clone());
        // let till = WHGoodsTopology::position_at_end(store, checkpoint);
        //
        // let mut items = Vec::new();
        // for (_,value) in tx.values(from, till) {
        //     items.push(Memo::new(value))
        // }
        //
        // Ok(MemoOfList::new(items))
    }

    fn prev_checkpoint(time: &Time) -> Time {
        time.beginning_of_month()
    }

    fn next_checkpoint(time: &Time) -> Time {
        time.beginning_of_next_month()
    }

    fn position_of_aggregation(store: Store, goods: Goods, time: Time) -> Result<Vec<u8>, DBError> {
        let checkpoint = WHGoodsTopology::next_checkpoint(&time);
        Ok(WHGoodsTopology::position_of_value(store, goods, checkpoint))
    }

    fn position_of_value(store: Store, goods: Goods, time: Time) -> Vec<u8> {
        WHGoodsTopology::position(store.into(), goods.into(), time.end())
    }

    fn position_at_start(store: Store, time: Time) -> Vec<u8> {
        WHGoodsTopology::position(store.into(), ID_MIN, time.start())
    }

    fn position_at_end(store: Store, time: Time) -> Vec<u8> {
        WHGoodsTopology::position(store.into(), ID_MAX, time.end())
    }

    fn position_prefix() -> usize {
        ID_BYTES * 2
    }

    fn position(store: ID, goods: ID, time: Time) -> Vec<u8> {
        let mut bs = Vec::with_capacity((ID_BYTES * 3) + 10);

        // operation prefix
        bs.extend_from_slice((*WH_STOCK_TOPOLOGY).as_slice());

        // prefix define calculation context
        bs.extend_from_slice(store.as_slice());

        // define order by time
        bs.extend_from_slice(time.to_bytes().as_slice());

        // suffix
        bs.extend_from_slice(goods.as_slice());

        bs
    }

    fn decode_position_from_bytes(bs: &[u8]) -> Result<(ID,ID,Time), DBError> {
        let expected = (ID_BYTES * 3) + 10;
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
                let date = Time::from_bytes(bs, 2*ID_BYTES)?;
                let goods = bs[(2*ID_BYTES+10)..(3*ID_BYTES+10)].try_into()?;

                Ok((store, goods, date))
            }
        }
    }
}

// two solutions:
//  - helper topology of goods existed at point in time (aka balance at time)
//    (point of trust because of force to keep list of all goods with balance)
//
//  - operations topology: store, time, goods = op (untrusted list of goods for given time)

#[derive(Debug, Clone, PartialEq)] // , Serialize, Deserialize)] // , ImplBytes
pub(crate) struct BalanceCheckpoint {
    number_of_ops: i16,
    balance: WHBalance,
}

impl ToBytes for BalanceCheckpoint {
    fn to_bytes(&self) -> Result<AlignedVec, DBError> {
        todo!()
    }
}

impl FromBytes<Self> for BalanceCheckpoint {
    fn from_bytes(bs: &[u8]) -> Result<Self, DBError> {
        todo!()
    }
}

impl AObject<CheckpointOp> for BalanceCheckpoint {
    fn is_zero(&self) -> bool {
        self.number_of_ops == 0 && self.balance.is_zero()
    }

    fn apply_aggregation(&self, op: &CheckpointOp) -> Result<Self, DBError> {
        Ok(BalanceCheckpoint {
            number_of_ops: self.number_of_ops + op.number_of_ops as i16,
            balance: self.balance.apply_aggregation(&op.op)?,
        })
    }
}

impl Add<Self> for BalanceCheckpoint {
    type Output = Self;

    fn add(self, rhs: Self) -> Self::Output {
        BalanceCheckpoint {
            number_of_ops: self.number_of_ops + rhs.number_of_ops, // TODO is it correct?
            balance: self.balance + rhs.balance,
        }
    }
}

impl Sub<Self> for BalanceCheckpoint {
    type Output = Self;

    fn sub(self, rhs: Self) -> Self::Output {
        BalanceCheckpoint {
            number_of_ops: self.number_of_ops - rhs.number_of_ops, // TODO is it correct?
            balance: self.balance - rhs.balance,
        }
    }
}

impl Neg for BalanceCheckpoint {
    type Output = Self;

    fn neg(self) -> Self::Output {
        BalanceCheckpoint {
            number_of_ops: self.number_of_ops,
            balance: -self.balance,
        }
    }
}

#[derive(Debug, Clone, PartialEq)] // , Serialize, Deserialize)]
pub(crate) struct CheckpointOp {
    number_of_ops: i8,
    op: BalanceOps,
}

impl AOperation<BalanceCheckpoint> for CheckpointOp {
    fn to_value(&self) -> BalanceCheckpoint {
        BalanceCheckpoint {
            number_of_ops: self.number_of_ops as i16,
            balance: self.op.to_value(),
        }
    }
}

impl ToBytes for CheckpointOp {
    fn to_bytes(&self) -> Result<AlignedVec, DBError> {
        todo!()
        // serde_json::to_vec(self)
        //     .map_err(|e| e.to_string().into())
    }
}

impl FromBytes<Self> for CheckpointOp {
    fn from_bytes(bs: &[u8]) -> Result<Self, DBError> {
        todo!()
        // serde_json::from_slice(bs)
        //     .map_err(|e| e.to_string().into())
    }
}

#[derive(Debug, Clone, PartialEq)] // , Serialize, Deserialize)]
pub struct WarehouseStock {
    value: BalanceCheckpoint,

    goods: Goods,

    date: Time,
    store: Store,
}

#[derive(Debug, Clone)] // , Serialize, Deserialize)]
pub struct StockDelta {
    op: CheckpointOp,

    // TODO avoid serialization & deserialize of prefix & position
    prefix: usize,
    position: Vec<u8>,

    pub(crate) date: Time,

    pub(crate) store: Store,
    pub(crate) goods: Goods,
}

impl StockDelta {
    fn new(number_of_ops: i8, store: Store, goods: Goods, date: Time, op: BalanceOps) -> Self {
        let prefix = WHGoodsTopology::position_prefix();
        let position = WHGoodsTopology::position_of_value(store, goods, date.clone());

        let op = CheckpointOp { number_of_ops, op };
        StockDelta { op, store, goods, date, prefix, position }
    }
}

impl From<&DeltaOp<WHBalance,BalanceOperation,WarehouseBalance,WarehouseMovement>> for StockDelta {
    fn from(delta: &DeltaOp<WHBalance,BalanceOperation,WarehouseBalance,WarehouseMovement>) -> Self {
        if let Some(before) = delta.before.as_ref() {
            if let Some(after) = delta.after.as_ref() {
                StockDelta::new(
                    0,
                    after.store, after.goods, after.date.clone(),
                    BalanceOps::from(&after.op) - BalanceOps::from(&before.op)
                )
            } else {
                StockDelta::new(
                    -1,
                    before.store, before.goods, before.date.clone(),
                    -BalanceOps::from(&before.op)
                )
            }
        } else if let Some(after) = delta.after.as_ref() {
            StockDelta::new(
                1,
                after.store, after.goods, after.date.clone(),
                BalanceOps::from(&after.op)
            )
        } else {
            unreachable!("internal error")
        }
    }
}

impl PositionInTopology for StockDelta {
    fn prefix(&self) -> usize {
        self.prefix
    }

    fn position(&self) -> &Vec<u8> {
        &self.position
    }
}

impl AOperationInTopology<BalanceCheckpoint,CheckpointOp,WarehouseStock> for StockDelta {

    fn position_of_aggregation(&self) -> Result<Vec<u8>,DBError> {
        WHGoodsTopology::position_of_aggregation(self.store, self.goods, self.date.clone())
    }

    fn operation(&self) -> CheckpointOp {
        self.op.clone()
    }

    fn to_value(&self) -> WarehouseStock {
        let op = self.operation();
        WarehouseStock {
            value: op.to_value(),
            store: self.store, goods: self.goods, date: self.date.clone(),
        }
    }
}

impl ToKVBytes for WarehouseStock {
    fn to_kv_bytes(&self) -> Result<(Vec<u8>, AlignedVec), DBError> {
        let k = WHGoodsTopology::position_of_value(self.store, self.goods, self.date.clone());
        let v = self.value.to_bytes()?;
        Ok((k,v))
    }
}

impl FromKVBytes<Self> for WarehouseStock {
    fn from_kv_bytes(k: &[u8], v: &[u8]) -> Result<WarehouseStock, DBError> {
        let (store, goods, date) = WHGoodsTopology::decode_position_from_bytes(k)?;
        let value = BalanceCheckpoint::from_bytes(v)?;
        Ok(WarehouseStock { store: Store::from(store), goods: Goods::from(goods), date, value })
    }
}

impl AObjectInTopology<BalanceCheckpoint,CheckpointOp,StockDelta> for WarehouseStock {
    fn position(&self) -> Vec<u8> {
        WHGoodsTopology::position_of_value(self.store, self.goods, self.date.clone())
    }

    fn value(&self) -> &BalanceCheckpoint {
        &self.value
    }

    fn apply(&self, op: &StockDelta) -> Result<Option<Self>, DBError> {
        todo!()
        // TODO check self.stock == op.stock && self.goods == op.goods && self.date >= op.date
        // let value = self.value.apply_aggregation(&op.op)?;
        // Ok(WarehouseStock { store: self.store, goods: self.goods, date: self.date.clone(), value })
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
        let goods = WHGoodsTopology::goods(&db, wh1, d22_05_31.clone()).expect("Ok");
        assert_eq!(1, goods.len());

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