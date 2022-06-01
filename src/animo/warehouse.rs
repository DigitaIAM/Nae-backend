use std::cmp::Ordering;
use std::collections::HashSet;
use std::sync::Arc;
use actix_web::cookie::time::macros::time;
use chrono::{Datelike, Timelike, TimeZone, Utc};
use serde::{Deserialize, Serialize};
use crate::animo::{Env, Object, Operation, OperationGenerator};
use crate::animo::primitives::{Qty, Money};
use crate::error::DBError;
use crate::memory::{Context, ID, ID_BYTES, Time};
use crate::rocksdb::{FromBytes, Snapshot, ToBytes};
use crate::shared::*;

// two solutions:
//  - helper topology of goods existed at point in time (aka balance at time)
//    (point of trust because of force to keep list of all goods with balance)
//
//  - operations topology: store, time, goods = op (untrusted list of goods for given time)

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct WarehouseStock {
    // stock + time + goods
    position: Vec<u8>,
    balance: Balance,
}

impl WarehouseStock {
    fn load(k: Vec<u8>, v: Vec<u8>) -> Result<Self, DBError> {
        // TODO: check by operation prefix?
        Ok(WarehouseStock { position: k, balance: Balance::from_bytes(&v)? })
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
        bs.extend_from_slice(WarehouseItem::time_to_bytes(time).as_slice());

        // suffix
        bs.extend_from_slice(goods.as_slice());

        bs
    }

    pub(crate) fn get_memo(s: &Snapshot, store: ID, goods: ID, time: Time) -> Result<Balance, DBError> {
        // TODO move method to Ops manager
        let ops_manager = s.rf.ops_manager.clone();

        let position = WarehouseStock::local_topology_position(store, goods, time);

        debug!("pining memo at {:?}", position);

        let balance = if let Some((r_position, mut balance)) = ops_manager.get_closest_memo::<Balance>(s, &position)? {
            debug!("closest memo {:?} at {:?}", balance, r_position);
            if r_position != position {
                debug!("calculate from closest memo {:?}", r_position);
                // TODO write test for this branch
                // calculate on interval between memo position and requested position
                for (_,op) in ops_manager.ops_between(s, &r_position, &position) {
                    balance = balance.apply(&op);
                }

                // store memo
                s.rf.db.put_cf(&s.cf_memos(), &position, balance.to_bytes()?)?;
            }
            balance
        } else {
            debug!("calculate from zero position");
            let zero_position = WarehouseItem::local_topology_position_of_zero(store, goods);
            let mut balance = Balance::default();

            for (k,op) in ops_manager.ops_following::<OpWarehouse>(s, &zero_position)? {
                let ordering = k.cmp(&position);
                if ordering <= Ordering::Equal {
                    balance = balance.apply(&op);
                } else {
                    break;
                }
            }

            // store memo
            s.rf.db.put_cf(&s.cf_memos(), position, balance.to_bytes()?)?;

            balance
        };
        Ok(balance)
    }
}

impl Operation<WarehouseStock> for WarehouseStock {
    fn delta_after_operation(&self) -> WarehouseStock {
        todo!()
    }

    fn delta_between_operations(&self, other: &Self) -> WarehouseStock {
        todo!()
    }
}

#[derive(Debug, Default, Hash, Eq, PartialEq)]
struct WarehouseStockOpGenerator();

impl OperationGenerator for WarehouseStockOpGenerator {
    fn depends_on(&self) -> Vec<ID> {
        // TODO fix it
        WarehouseItemOpGenerator().depends_on()
    }

    fn generate_op(&self, env: &mut Env, cs: HashSet<Context>) -> Result<(), DBError> {
        // topology
        // store + time + goods = Balance,

        // TODO handle delete case

        // filter contexts by "object type"
        let mut contexts = Vec::with_capacity(cs.len());
        for c in cs {
            if let Some(instance_of) = env.resolve(&c, *SPECIFIC_OF)? {
                if instance_of.into.one_of(vec![*GOODS_RECEIVE, *GOODS_ISSUE]) {
                    contexts.push(c);
                }
            }
        }

        // TODO resolve up-dependent contexts

        for context in contexts {
            let instance_of = env.resolve_as_id(&context, *SPECIFIC_OF)?;
            let store = env.resolve_as_id(&context, *STORE)?;
            let goods = env.resolve_as_id(&context, *GOODS)?;
            let date = env.resolve_as_time(&context, *DATE)?;

            let qty = env.resolve_as_number(&context, *QTY)?;
            let cost = env.resolve_as_number(&context, *COST)?;

            let op = OpWarehouse::new(instance_of, Qty(qty), Money(cost));
            let delta = op.delta_after_operation();

            let position = WarehouseStock::local_topology_position(store, goods, date);
            let checkpoint = WarehouseStock::local_topology_position_of_aggregation(store, goods, date)?;

            env.ops_manager().write_aggregation_delta(env, position, checkpoint, delta)?;
        }

        Ok(())
    }
}

struct Movements {
    open: Balance,
    ops: OpWarehouse,
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




#[derive(Debug, Serialize, Deserialize)]
pub enum OpWarehouse {
    In(Qty, Money),
    Out(Qty, Money),
}

impl OpWarehouse {
    fn new(instance_of: ID, qty: Qty, cost: Money) -> OpWarehouse {
        if instance_of == *GOODS_RECEIVE {
            OpWarehouse::In(qty, cost)
        } else { // TODO instance_of == *GOODS_ISSUE
            OpWarehouse::Out(qty, cost)
        }
    }
}

impl ToBytes for OpWarehouse {
    fn to_bytes(&self) -> Result<Vec<u8>, DBError> {
        serde_json::to_vec(self)
            .map_err(|e| e.to_string().into())
    }
}

impl FromBytes<OpWarehouse> for OpWarehouse {
    fn from_bytes(bs: &[u8]) -> Result<OpWarehouse, DBError> {
        serde_json::from_slice(bs)
            .map_err(|e| e.to_string().into())
    }
}

impl Operation<Balance> for OpWarehouse {
    fn delta_after_operation(&self) -> Balance {
        Balance::default().apply(self)
    }

    fn delta_between_operations(&self, other: &Self) -> Balance {
        match self {
            OpWarehouse::In(l_qty, l_cost) => {
                match other {
                    OpWarehouse::In(r_qty, r_cost) => {
                        // 10 > 8 = -2 (8-10)
                        // 10 > 12 = 2 (12-10)
                        Balance(r_qty - l_qty, r_cost - l_cost)
                    }
                    OpWarehouse::Out(r_qty, r_cost) => {
                        // 10 > -8 = -18 (-10-8)
                        Balance(-(l_qty + r_qty), -(l_cost + r_cost))
                    }
                }
            }
            OpWarehouse::Out(l_qty, l_cost) => {
                match other {
                    OpWarehouse::In(r_qty, r_cost) => {
                        // -10 > 8 = 18 (10+8)
                        Balance(l_qty + r_qty, l_cost + r_cost)
                    }
                    OpWarehouse::Out(r_qty, r_cost) => {
                        // -10 > -8 = +2 (10-8)
                        // -10 > -12 = -2 (10-12)
                        Balance(l_qty - r_qty, l_cost + r_cost)
                    }
                }
            }
        }
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Default, Serialize, Deserialize)]
pub struct Balance(pub Qty, pub Money);

impl Object<Balance, OpWarehouse> for Balance {
    fn apply_delta(&self, other: &Balance) -> Self {
        self + other
    }

    fn apply(&self, op: &OpWarehouse) -> Self {
        let (qty, cost) = match op {
            OpWarehouse::In(qty, cost) => (&self.0 + qty, &self.1 + cost),
            OpWarehouse::Out(qty, cost) => (&self.0 - qty, &self.1 - cost),
        };
        debug!("apply {:?} to {:?}", op, self);

        Balance(qty, cost)
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

#[derive(Debug, Clone, Eq, PartialEq, Default, Serialize, Deserialize)]
struct WarehouseItem {
    // store + goods + time + op
    position: Vec<u8>,
    balance: Balance,
}

impl WarehouseItem {
    fn time_to_bytes(time: Time) -> [u8; 8] {
        WarehouseItem::ts_to_bytes(time.timestamp().try_into().unwrap())
    }

    fn ts_to_bytes(ts: u64) -> [u8; 8] {
        ts.to_be_bytes()
    }

    fn local_topology_position_of_zero(store: ID, goods: ID) -> Vec<u8> {
        let mut bs = Vec::with_capacity((ID_BYTES * 2) + 8 + 1);

        // TODO operation prefix

        // prefix define calculation context
        bs.extend_from_slice(store.as_slice());
        bs.extend_from_slice(goods.as_slice());

        // define order by time
        bs.extend_from_slice(WarehouseItem::ts_to_bytes(u64::MIN).as_slice());

        // order by operations
        bs.extend([u8::MIN].into_iter());

        bs
    }

    fn local_topology_position_of_memo(store: ID, goods: ID, time: Time) -> Vec<u8> {
        let mut bs = Vec::with_capacity((ID_BYTES * 2) + 8 + 1);

        // TODO operation prefix

        // prefix define calculation context
        bs.extend_from_slice(store.as_slice());
        bs.extend_from_slice(goods.as_slice());

        // define order by time
        bs.extend_from_slice(WarehouseItem::time_to_bytes(time).as_slice());

        // order by operations
        bs.extend([u8::MAX].into_iter());

        bs
    }

    fn local_topology_position_of_operation(store: ID, goods: ID, time: Time, op: &OpWarehouse) -> Vec<u8> {
        let mut bs = Vec::with_capacity((ID_BYTES * 2) + 8 + 1);

        // TODO operation prefix

        // prefix define calculation context
        bs.extend_from_slice(store.as_slice());
        bs.extend_from_slice(goods.as_slice());

        // define order by time
        bs.extend_from_slice(WarehouseItem::time_to_bytes(time).as_slice());

        // order by operations
        let b: u8 = match op {
            OpWarehouse::In(..) => u8::MAX,
            OpWarehouse::Out(..) => u8::MIN,
        };

        bs.extend([b].into_iter());

        bs
    }

    pub(crate) fn get_memo(s: &Snapshot, store: ID, goods: ID, time: Time) -> Result<Balance, DBError> {
        // TODO move method to Ops manager
        let ops_manager = s.rf.ops_manager.clone();

        let position = WarehouseItem::local_topology_position_of_memo(store, goods, time);

        debug!("pining memo at {:?}", position);

        let balance = if let Some((r_position, mut balance)) = ops_manager.get_closest_memo::<Balance>(s, &position)? {
            debug!("closest memo {:?} at {:?}", balance, r_position);
            if r_position != position {
                debug!("calculate from closest memo {:?}", r_position);
                // TODO write test for this branch
                // calculate on interval between memo position and requested position
                for (_,op) in ops_manager.ops_between(s, &r_position, &position) {
                    balance = balance.apply(&op);
                }

                // store memo
                s.rf.db.put_cf(&s.cf_memos(), &position, balance.to_bytes()?)?;
            }
            balance
        } else {
            debug!("calculate from zero position");
            let zero_position = WarehouseItem::local_topology_position_of_zero(store, goods);
            let mut balance = Balance::default();

            for (k,op) in ops_manager.ops_following::<OpWarehouse>(s, &zero_position)? {
                let ordering = k.cmp(&position);
                if ordering <= Ordering::Equal {
                    balance = balance.apply(&op);
                } else {
                    break;
                }
            }

            // store memo
            s.rf.db.put_cf(&s.cf_memos(), position, balance.to_bytes()?)?;

            balance
        };
        Ok(balance)
    }
}

#[derive(Debug, Hash, Eq, PartialEq)]
struct WarehouseItemOpGenerator();

impl OperationGenerator for WarehouseItemOpGenerator {

    fn depends_on(&self) -> Vec<ID> {
        vec![
            *SPECIFIC_OF,
            *STORE, *DATE,
            *GOODS, *QTY, *COST
        ]
    }

    fn generate_op(&self, env: &mut Env, cs: HashSet<Context>) -> Result<(), DBError> {
        // GoodsReceive, GoodsIssue

        // TODO handle delete case

        // filter contexts by "object type"
        let mut contexts = Vec::with_capacity(cs.len());
        for c in cs {
            if let Some(instance_of) = env.resolve(&c, *SPECIFIC_OF)? {
                if instance_of.into.one_of(vec![*GOODS_RECEIVE, *GOODS_ISSUE]) {
                    contexts.push(c);
                }
            }
        }

        // TODO resolve up-dependent contexts

        for context in contexts {
            let instance_of = env.resolve_as_id(&context, *SPECIFIC_OF)?;
            let store = env.resolve_as_id(&context, *STORE)?;
            let goods = env.resolve_as_id(&context, *GOODS)?;
            let date = env.resolve_as_time(&context, *DATE)?;

            let qty = env.resolve_as_number(&context, *QTY)?;
            let cost = env.resolve_as_number(&context, *COST)?;

            let op = OpWarehouse::new(instance_of, Qty(qty), Money(cost));

            let local_topology_position = WarehouseItem::local_topology_position_of_operation(store, goods, date, &op);

            env.ops_manager().write(env, local_topology_position, op)?;
        }

        Ok(())
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
    use crate::animo::Animo;
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
            op_producers: Vec::default(),
            what_to_node_producers: HashMap::new(),
        };
        animo.register_op_producer(Arc::new(WarehouseItemOpGenerator()));
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
        let g1_balance = WarehouseItem::get_memo(&s, wh1, g1, time("2022-05-31")).expect("Ok");
        assert_eq!(Balance(Qty(7.into()),Money(35.into())), g1_balance);

        debug!("READING 2022-05-28");
        let s = db.snapshot();
        let g1_balance = WarehouseItem::get_memo(&s, wh1, g1, time("2022-05-28")).expect("Ok");
        assert_eq!(Balance(Qty(5.into()),Money(25.into())), g1_balance);

        debug!("READING 2022-05-31");
        let s = db.snapshot();
        let g1_balance = WarehouseItem::get_memo(&s, wh1, g1, time("2022-05-31")).expect("Ok");
        assert_eq!(Balance(Qty(7.into()),Money(35.into())), g1_balance);

        debug!("MODIFY D");
        db.modify(event("D", "2022-05-31", *GOODS_ISSUE, g1, 1, Some(5))).expect("Ok");

        debug!("READING 2022-05-31");
        let s = db.snapshot();
        let g1_balance = WarehouseItem::get_memo(&s, wh1, g1, time("2022-05-31")).expect("Ok");
        assert_eq!(Balance(Qty(6.into()),Money(30.into())), g1_balance);
    }

    #[test]
    fn test_warehouse_stock() {
        init();

        let tmp_dir = tempfile::tempdir().unwrap();
        let tmp_path = tmp_dir.path().to_str().unwrap();
        let mut db: RocksDB = Memory::init(tmp_path).unwrap();
        let mut animo = Animo {
            op_producers: Vec::default(),
            what_to_node_producers: HashMap::new(),
        };
        // animo.register_op_producer(Arc::new(WarehouseItemOpGenerator()));
        animo.register_op_producer(Arc::new(WarehouseStockOpGenerator()));
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
        let g1_balance = WarehouseStock::get_memo(&s, wh1, g1, time("2022-05-31")).expect("Ok");
        assert_eq!(Balance(Qty(7.into()),Money(35.into())), g1_balance);

        debug!("READING 2022-05-28");
        let s = db.snapshot();
        let g1_balance = WarehouseStock::get_memo(&s, wh1, g1, time("2022-05-28")).expect("Ok");
        assert_eq!(Balance(Qty(5.into()),Money(25.into())), g1_balance);

        debug!("READING 2022-05-31");
        let s = db.snapshot();
        let g1_balance = WarehouseStock::get_memo(&s, wh1, g1, time("2022-05-31")).expect("Ok");
        assert_eq!(Balance(Qty(7.into()),Money(35.into())), g1_balance);

        debug!("MODIFY D");
        db.modify(event("D", "2022-05-31", *GOODS_ISSUE, g1, 1, Some(5))).expect("Ok");

        debug!("READING 2022-05-31");
        let s = db.snapshot();
        let g1_balance = WarehouseStock::get_memo(&s, wh1, g1, time("2022-05-31")).expect("Ok");
        assert_eq!(Balance(Qty(6.into()),Money(30.into())), g1_balance);
    }
}