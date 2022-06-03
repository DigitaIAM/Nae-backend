use std::cmp::Ordering;
use std::collections::HashSet;
use std::sync::Arc;
use actix_web::cookie::time::macros::time;
use chrono::{Datelike, Timelike, TimeZone, Utc};
use serde::{Deserialize, Serialize};
use crate::animo::{AggregationTopology, Txn, Memo, Object, Operation, OperationsTopology, AggregationOperation, OperationInTopology, ObjectInTopology, AggregationOperationInTopology, AggregationObjectInTopology, AggregationObject};
use crate::animo::ops_manager::{BetweenIterator, ItemsIterator};
use crate::animo::primitives::{Qty, Money};
use crate::error::DBError;
use crate::memory::{Context, ID, ID_BYTES, Time};
use crate::RocksDB;
use crate::rocksdb::{FromBytes, FromKVBytes, Snapshot, ToBytes, ToKVBytes};
use crate::shared::*;

fn time_to_u64(time: Time) -> u64 {
    time.timestamp().try_into().unwrap()
}

fn ts_to_bytes(ts: u64) -> [u8; 8] {
    ts.to_be_bytes()
}

fn time_to_bytes(time: Time) -> [u8; 8] {
    ts_to_bytes(time.timestamp().try_into().unwrap())
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
    store: ID,
    goods: ID,

    from: Time,
    till: Time,

    op: BalanceOperationsAggregation,
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

// impl AggregationOperationInTopology<Balance,BalanceOperation,WarehouseStock> for WarehouseStockDelta {
//     fn resolve(env: &Txn, context: &Context) -> Result<Self, DBError> {
//         todo!()
//     }
//
//     fn position(&self) -> Vec<u8> {
//         WarehouseStock::local_topology_position(self.store, self.goods, self.date)
//     }
//
//     fn operation(self) -> BalanceOperation {
//         self.delta
//     }
//
//     fn delta_between(&self, other: &Self) -> Self {
//         todo!()
//     }
//
//     fn to_value(&self) -> WarehouseStock {
//         WarehouseStock {
//             store: self.store, goods: self.goods, date: self.date,
//             balance: self.delta.to_value()
//         }
//     }
// }

// impl ToBytes for WarehouseStock {
//     fn to_bytes(&self) -> Result<Vec<u8>, DBError> {
//         serde_json::to_vec(self)
//             .map_err(|e| e.to_string().into())
//     }
// }
//
// impl FromBytes<WarehouseStock> for WarehouseStock {
//     fn from_bytes(bs: &[u8]) -> Result<WarehouseStock, DBError> {
//         serde_json::from_slice(bs)
//             .map_err(|e| e.to_string().into())
//     }
// }

impl AggregationOperationInTopology<Balance,BalanceOperationsAggregation,WarehouseStock> for WarehouseStockDelta {
    fn position(&self) -> Vec<u8> {
        WarehouseStock::local_topology_position(self.store, self.goods, self.till)
    }

    fn position_of_aggregation(&self) -> Result<Vec<u8>,DBError> {
        WarehouseStock::local_topology_position_of_aggregation(self.store, self.goods, self.till)
    }

    fn operation(&self) -> BalanceOperationsAggregation {
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

impl AggregationObjectInTopology<Balance,BalanceOperationsAggregation,WarehouseStockDelta> for WarehouseStock {
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

        let stock = if let Some((r_position, mut balance)) = ops_manager.get_closest_memo::<Balance>(tx.s, position.clone())? {
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

            // store memo
            tx.update_value(&position, &balance_memo.object.balance)?;

            WarehouseStock { store, goods, date, balance: balance_memo.object.balance }
        };
        Ok(Memo::new(stock))
    }
}

#[derive(Debug, Default, Hash, Eq, PartialEq)]
pub(crate) struct WarehouseStockTopology();

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

struct Movements {
    open: Balance,
    ops: BalanceOperation,
    close: Balance,
}

pub struct WarehouseMovements {
    // store + till + from
    position: Vec<u8>,
    movements: Movements,
}

pub struct WarehouseItemsMovements {
    // store + goods + till + from
    position: Vec<u8>,
    movements: Movements,
}

impl WarehouseMovements {
    pub(crate) fn read(s: &Snapshot, store: ID, from: Time, till: Time) -> Result<Self, DBError> {
        todo!()
    }
}

impl WarehouseItemsMovements {
    pub(crate) fn read(s: &Snapshot, store: ID, goods: ID, from: Time, till: Time) -> Result<Self, DBError> {
        todo!()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum BalanceOperation {
    In(Qty, Money),
    Out(Qty, Money),
}

impl BalanceOperation {
    fn new(instance_of: ID, qty: Qty, cost: Money) -> Result<BalanceOperation, DBError> {
        if instance_of == *GOODS_RECEIVE {
            Ok(BalanceOperation::In(qty, cost))
        } else if instance_of == *GOODS_ISSUE {
            Ok(BalanceOperation::Out(qty, cost))
        } else {
            Err(format!("unknown type {:?}", instance_of).into())
        }
    }
}

impl ToBytes for BalanceOperation {
    fn to_bytes(&self) -> Result<Vec<u8>, DBError> {
        serde_json::to_vec(self)
            .map_err(|e| e.to_string().into())
    }
}

impl FromBytes<BalanceOperation> for BalanceOperation {
    fn from_bytes(bs: &[u8]) -> Result<BalanceOperation, DBError> {
        serde_json::from_slice(bs)
            .map_err(|e| e.to_string().into())
    }
}

impl Operation<Balance> for BalanceOperation {
    fn delta_between(&self, other: &Self) -> BalanceOperation {
        match self {
            BalanceOperation::In(l_qty, l_cost) => {
                match other {
                    BalanceOperation::In(r_qty, r_cost) => {
                        // 10 > 8 = -2 (8-10)
                        // 10 > 12 = 2 (12-10)
                        BalanceOperation::In(r_qty - l_qty, r_cost - l_cost)
                    }
                    BalanceOperation::Out(r_qty, r_cost) => {
                        // 10 > -8 = -18 (-10-8)
                        BalanceOperation::In(-(l_qty + r_qty), -(l_cost + r_cost))
                    }
                }
            }
            BalanceOperation::Out(l_qty, l_cost) => {
                match other {
                    BalanceOperation::In(r_qty, r_cost) => {
                        // -10 > 8 = 18 (10+8)
                        BalanceOperation::In(l_qty + r_qty, l_cost + r_cost)
                    }
                    BalanceOperation::Out(r_qty, r_cost) => {
                        // -10 > -8 = +2 (10-8)
                        // -10 > -12 = -2 (10-12)
                        BalanceOperation::In(l_qty - r_qty, l_cost + r_cost)
                    }
                }
            }
        }
    }

    fn to_value(&self) -> Balance {
        match self {
            BalanceOperation::In(qty, cost) => Balance(qty.clone(), cost.clone()),
            BalanceOperation::Out(qty, cost) => Balance(-qty.clone(), -cost.clone()),
        }
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Default, Serialize, Deserialize)]
pub struct Balance(pub Qty, pub Money);

impl Object<BalanceOperation> for Balance {
    // fn apply_delta(&self, other: &Balance) -> Self {
    //     self + other
    // }

    fn apply(&self, op: &BalanceOperation) -> Result<Self,DBError> {
        let (qty, cost) = match op {
            BalanceOperation::In(qty, cost) => (&self.0 + qty, &self.1 + cost),
            BalanceOperation::Out(qty, cost) => (&self.0 - qty, &self.1 - cost),
        };
        debug!("apply {:?} to {:?}", op, self);

        Ok(Balance(qty, cost))
    }
}

impl AggregationObject<BalanceOperationsAggregation> for Balance {
    fn apply_aggregation(&self, op: &BalanceOperationsAggregation) -> Result<Self,DBError> {
        let qty = &(&self.0 + &op.incoming.0) - &op.outgoing.0;
        let cost = &(&self.1 + &op.incoming.1) - &op.outgoing.1;

        debug!("apply aggregation {:?} to {:?}", op, self);

        Ok(Balance(qty, cost))
    }
}

impl ToBytes for Balance {
    fn to_bytes(&self) -> Result<Vec<u8>, DBError> {
        serde_json::to_vec(self)
            .map_err(|e| e.to_string().into())
    }
}

impl FromBytes<Balance> for Balance {
    fn from_bytes(bs: &[u8]) -> Result<Balance, DBError> {
        serde_json::from_slice(bs)
            .map_err(|e| e.to_string().into())
    }
}

impl<'a, 'b> std::ops::Add<&'b Balance> for &'a Balance {
    type Output = Balance;

    fn add(self, other: &'b Balance) -> Balance {
        Balance(&self.0 + &other.0, &self.1 + &other.1)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct BalanceOperationsAggregation {
    incoming: (Qty, Money),
    outgoing: (Qty, Money),
}

impl AggregationOperation<Balance> for BalanceOperationsAggregation {
    fn to_value(&self) -> Balance {
        Balance(&self.incoming.0 - &self.outgoing.0, &self.incoming.1 - &self.outgoing.1)
    }
}

impl From<BalanceOperation> for BalanceOperationsAggregation {
    fn from(op: BalanceOperation) -> Self {
        match op {
            BalanceOperation::In(qty, cost) => {
                BalanceOperationsAggregation {
                    incoming: (qty, cost),
                    outgoing: (Qty::default(), Money::default()),
                }
            }
            BalanceOperation::Out(qty, cost) => {
                BalanceOperationsAggregation {
                    incoming: (Qty::default(), Money::default()),
                    outgoing: (qty, cost),
                }
            }
        }
    }
}

impl FromBytes<Self> for BalanceOperationsAggregation {
    fn from_bytes(bs: &[u8]) -> Result<Self, DBError> {
        todo!()
    }
}

impl ToBytes for BalanceOperationsAggregation {
    fn to_bytes(&self) -> Result<Vec<u8>, DBError> {
        todo!()
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub(crate) struct WarehouseBalance {
    store: ID,
    goods: ID,
    date: Time,

    balance: Balance,
}

impl ToKVBytes for WarehouseBalance {
    fn to_kv_bytes(&self) -> Result<(Vec<u8>, Vec<u8>), DBError> {
        let k = WarehouseTopology::position_at_end(self.store, self.goods, self.date);
        let v = self.balance.to_bytes()?;
        Ok((k,v))
    }
}

impl FromKVBytes<Self> for WarehouseBalance {
    fn from_kv_bytes(key: &[u8], value: &[u8]) -> Result<WarehouseBalance, DBError> {
        todo!()
    }
}

impl From<WarehouseBalance> for Balance {
    fn from(v: WarehouseBalance) -> Self {
        v.balance
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct WarehouseMovement {
    store: ID,
    goods: ID,
    date: Time,

    op: BalanceOperation,
}

impl ToKVBytes for WarehouseMovement {
    fn to_kv_bytes(&self) -> Result<(Vec<u8>, Vec<u8>), DBError> {
        let k = WarehouseTopology::position_of_operation(self.store, self.goods, self.date, &self.op);
        let v = self.op.to_bytes()?;
        Ok((k,v))
    }
}

impl FromKVBytes<WarehouseMovement> for WarehouseMovement {
    fn from_kv_bytes(key: &[u8], value: &[u8]) -> Result<WarehouseMovement, DBError> {
        todo!()
    }
}

impl OperationInTopology<Balance,BalanceOperation,WarehouseBalance> for WarehouseMovement {
    fn resolve(env: &Txn, context: &Context) -> Result<Self, DBError> {
        let instance_of = env.resolve_as_id(context, *SPECIFIC_OF)?;
        let store = env.resolve_as_id(context, *STORE)?;
        let goods = env.resolve_as_id(context, *GOODS)?;
        let date = env.resolve_as_time(context, *DATE)?;

        let qty = env.resolve_as_number(context, *QTY)?;
        let cost = env.resolve_as_number(context, *COST)?;

        let op = BalanceOperation::new(instance_of, Qty(qty), Money(cost))?;

        Ok(WarehouseMovement { store, goods, date, op })
    }

    fn position(&self) -> Vec<u8> {
        WarehouseTopology::position_of_operation(self.store, self.goods, self.date, &self.op)
    }

    fn operation(&self) -> BalanceOperation {
        self.op.clone()
    }

    fn delta_between(&self, other: &Self) -> WarehouseMovement {
        todo!()
    }

    fn to_value(&self) -> WarehouseBalance {
        WarehouseBalance {
            store: self.store, goods: self.goods, date: self.date,
            balance: self.op.to_value()
        }
    }
}

impl ObjectInTopology<Balance,BalanceOperation,WarehouseMovement> for WarehouseBalance {
    fn apply(&self, op: &WarehouseMovement) -> Result<Self, DBError> {
        // TODO check self.store == op.store && self.goods == op.goods && self.date >= op.date
        let balance = self.balance.apply(&op.op)?;
        Ok(WarehouseBalance {
            store: self.store, goods: self.goods, date: self.date,
            balance
        })
    }
}

#[derive(Debug, Hash, Eq, PartialEq)]
pub(crate) struct WarehouseTopology();

impl WarehouseTopology {
    fn get_ops_till<'a,O>(tx: &'a Txn, store: ID, goods: ID, till: Time) -> BetweenIterator<'a,O> {
        let from_point = WarehouseTopology::position_of_zero(store, goods);
        let till_point = WarehouseTopology::position_at_end(store, goods, till);

        tx.operations(from_point, till_point)
    }

    fn get_ops<'a,O>(tx: &'a Txn, store: ID, goods: ID, from: Time, till: Time) -> BetweenIterator<'a,O> {
        let from_point = WarehouseTopology::position_at_start(store, goods, from);
        let till_point = WarehouseTopology::position_at_end(store, goods, till);

        tx.operations(from_point, till_point)
    }

    fn balance(db: &RocksDB, store: ID, goods: ID, date: Time) -> Result<Memo<WarehouseBalance>,DBError> {
        let s = db.snapshot();
        let mut tx = Txn::new(&s);

        let memo = WarehouseTopology::balance_tx(&mut tx, store, goods, date)?;

        // TODO: unregister memo if case of error
        tx.commit()?;

        Ok(memo)
    }

    fn balance_tx(tx: &mut Txn, store: ID, goods: ID, date: Time) -> Result<Memo<WarehouseBalance>,DBError>{
        // TODO move method to Ops manager
        let ops_manager = tx.s.rf.ops_manager.clone();

        let position = WarehouseTopology::position_at_end(store, goods, date);

        debug!("pining memo at {:?}", position);

        let balance = if let Some((r_position, mut balance)) = ops_manager.get_closest_memo::<Balance>(tx.s, position.clone())? {
            debug!("closest memo {:?} at {:?}", balance, r_position);
            if r_position != position {
                debug!("calculate from closest memo {:?}", r_position);
                // TODO write test for this branch
                // calculate on interval between memo position and requested position
                for (_,op) in tx.operations(r_position, position.clone()) {
                    balance = balance.apply(&op)?;
                }

                // store memo
                tx.update_value(&position, &balance)?;
            }
            WarehouseBalance { store, goods, date, balance }
        } else {
            debug!("calculate from zero position");
            let mut balance = Balance::default();
            for (_,op) in WarehouseTopology::get_ops_till(tx, store, goods, date) {
                balance = balance.apply(&op)?;
            }

            // store memo
            tx.update_value(&position, &balance)?;

            WarehouseBalance { store, goods, date, balance }
        };

        Ok(Memo::new(balance))
    }

    fn position(store: ID, goods: ID, time: u64, op: u8) -> Vec<u8> {
        let mut bs = Vec::with_capacity((ID_BYTES * 2) + 8 + 1);

        // operation prefix
        bs.extend_from_slice((*WH_TOPOLOGY).as_slice());

        // prefix define calculation context
        bs.extend_from_slice(store.as_slice());
        bs.extend_from_slice(goods.as_slice());

        // define order by time
        bs.extend_from_slice(ts_to_bytes(time).as_slice());

        // order by operations
        bs.extend([op].into_iter());

        bs
    }

    fn position_of_operation(store: ID, goods: ID, time: Time, op: &BalanceOperation) -> Vec<u8> {
        let op: u8 = match op {
            BalanceOperation::In(..) => u8::MAX,
            BalanceOperation::Out(..) => u8::MIN,
        };

        WarehouseTopology::position(store, goods, time_to_u64(time), op)
    }

    fn position_of_zero(store: ID, goods: ID) -> Vec<u8> {
        WarehouseTopology::position(store, goods, u64::MIN, u8::MIN)
    }

    fn position_at_start(store: ID, goods: ID, time: Time) -> Vec<u8> {
        WarehouseTopology::position(store, goods, time_to_u64(time), u8::MIN)
    }

    fn position_at_end(store: ID, goods: ID, time: Time) -> Vec<u8> {
        WarehouseTopology::position(store, goods, time_to_u64(time), u8::MAX)
    }
}

impl OperationsTopology for WarehouseTopology {
    type Obj = Balance;
    type Op = BalanceOperation;

    type TObj = WarehouseBalance;
    type TOp = WarehouseMovement;

    fn depends_on(&self) -> Vec<ID> {
        vec![
            *SPECIFIC_OF,
            *STORE, *DATE,
            *GOODS, *QTY, *COST
        ]
    }

    fn on_mutation(&self, tx: &mut Txn, cs: HashSet<Context>) -> Result<Vec<Self::TOp>, DBError> {
        // GoodsReceive, GoodsIssue

        // TODO handle delete case

        // filter contexts by "object type"
        let mut contexts = HashSet::with_capacity(cs.len());
        for c in cs {
            if let Some(instance_of) = tx.resolve(&c, *SPECIFIC_OF)? {
                if instance_of.into.one_of(vec![*GOODS_RECEIVE, *GOODS_ISSUE]) {
                    contexts.insert(c);
                }
            }
        }

        // TODO resolve up-dependent contexts

        let mut ops = Vec::with_capacity(contexts.len());
        for context in contexts {
            ops.push(
                WarehouseMovement::resolve(tx, &context)?
            );
        }
        tx.ops_manager().write_ops(tx, ops.to_vec())?;

        Ok(ops.to_vec())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::shared::*;

    use std::cmp::Ordering;
    use std::collections::HashMap;
    use std::sync::Arc;
    use chrono::DateTime;
    use crate::{Memory, RocksDB};
    use crate::animo::{Animo, Topology};
    use crate::animo::primitives::{Money, Qty};
    use crate::animo::warehouse::Balance;
    use crate::memory::{ChangeTransformation, Transformation, Value};

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
    fn test_store_operations() {
        init();

        let tmp_dir = tempfile::tempdir().unwrap();
        let tmp_path = tmp_dir.path().to_str().unwrap();
        let mut db: RocksDB = Memory::init(tmp_path).unwrap();
        let mut animo = Animo {
            topologies: Vec::default(),
            what_to_topologies: HashMap::new(),
            op_to_topologies: HashMap::new(),
        };
        animo.register_topology(Topology::Warehouse(Arc::new(WarehouseTopology())));
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

    #[test]
    fn test_warehouse_stock() {
        init();

        let tmp_dir = tempfile::tempdir().unwrap();
        let tmp_path = tmp_dir.path().to_str().unwrap();
        let mut db: RocksDB = Memory::init(tmp_path).unwrap();
        let mut animo = Animo {
            topologies: Vec::default(),
            what_to_topologies: HashMap::new(),
            op_to_topologies: HashMap::new(),
        };
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
        let s = db.snapshot();
        let g1_balance = WarehouseTopology::balance(&db, wh1, g1, time("2022-05-31")).expect("Ok");
        assert_eq!(Balance(Qty(7.into()),Money(35.into())), g1_balance.value().into());

        debug!("READING 2022-05-28");
        let s = db.snapshot();
        let g1_balance = WarehouseTopology::balance(&db, wh1, g1, time("2022-05-28")).expect("Ok");
        assert_eq!(Balance(Qty(5.into()),Money(25.into())), g1_balance.value().into());

        debug!("READING 2022-05-31");
        let s = db.snapshot();
        let g1_balance = WarehouseTopology::balance(&db, wh1, g1, time("2022-05-31")).expect("Ok");
        assert_eq!(Balance(Qty(7.into()),Money(35.into())), g1_balance.value().into());

        debug!("MODIFY D");
        db.modify(event("D", "2022-05-31", *GOODS_ISSUE, g1, 1, Some(5))).expect("Ok");

        debug!("READING 2022-05-31");
        let s = db.snapshot();
        let g1_balance = WarehouseTopology::balance(&db, wh1, g1, time("2022-05-31")).expect("Ok");
        assert_eq!(Balance(Qty(6.into()),Money(30.into())), g1_balance.value().into());
    }
}