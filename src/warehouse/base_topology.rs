use std::collections::HashSet;
use std::fmt::Debug;
use chrono::{TimeZone, Utc};
use serde::{Deserialize, Serialize};
use crate::animo::error::DBError;
use crate::animo::memory::{Context, ID, ID_BYTES};
use crate::AnimoDB;
use crate::animo::db::{FromBytes, FromKVBytes, Snapshot, ToBytes, ToKVBytes};
use crate::animo::*;
use crate::animo::ops_manager::*;
use crate::animo::shared::*;
use crate::warehouse::balance::WHBalance;
use crate::warehouse::balance_operation::BalanceOperation;
use crate::warehouse::primitives::{Money, Qty};
use crate::warehouse::turnover::{Goods, Store};

#[derive(Debug)]
struct WHQueryBalance {
    prefix: usize,
    position: Vec<u8>,
}

impl WHQueryBalance {
    fn bytes(prefix: usize, position: Vec<u8>) -> Self {
        // TODO check that position starts with prefix
        WHQueryBalance { prefix, position }
    }

    fn new(store: Store, goods: Goods, date: Time) -> Self {
        WHQueryBalance {
            prefix: WHTopology::position_prefix(),
            position: WHTopology::position_at_end(store, goods, date),
        }
    }
}

impl PositionInTopology for WHQueryBalance {
    fn prefix(&self) -> usize {
        self.prefix
    }

    fn position(&self) -> &Vec<u8> {
        &self.position
    }
}

impl QueryValue<WHBalance> for WHQueryBalance {
    fn closest_before(&self, s: &Snapshot) -> Option<(Vec<u8>, WHBalance)> {
        LightIterator::preceding_values(s, self).next()
    }

    fn values_after<'a>(&'a self, s: &'a Snapshot<'a>) -> LightIterator<'a, WHBalance> {
        following_light(s, &s.cf_values(), self)
    }
}

pub(crate) struct WHQueryOperation {
    prefix: usize,
    position: Vec<u8>,
}

impl WHQueryOperation {
    fn zero(store: Store, goods: Goods) -> Self {
        WHQueryOperation {
            prefix: WHTopology::position_prefix(),
            position: WHTopology::position_of_zero(store, goods),
        }
    }

    fn start(store: Store, goods: Goods, date: Time) -> Self {
        WHQueryOperation {
            prefix: WHTopology::position_prefix(),
            position: WHTopology::position_at_start(store, goods, date),
        }
    }

    fn end(store: Store, goods: Goods, date: Time) -> Self {
        WHQueryOperation {
            prefix: WHTopology::position_prefix(),
            position: WHTopology::position_at_end(store, goods, date),
        }
    }
}

impl PositionInTopology for WHQueryOperation {
    fn prefix(&self) -> usize {
        self.prefix
    }

    fn position(&self) -> &Vec<u8> {
        &self.position
    }
}

#[derive(Debug, Hash, Eq, PartialEq)]
pub struct WHTopology();

impl WHTopology {
    pub(crate) fn get_ops_till(store: Store, goods: Goods, till: Time) -> (WHQueryOperation, WHQueryOperation) {
        let from = WHQueryOperation::zero(store, goods);
        let till = WHQueryOperation::end(store, goods, till);

        (from, till)
    }

    pub(crate) fn get_ops(store: Store, goods: Goods, from: Time, till: Time) -> (WHQueryOperation, WHQueryOperation) {
        let from = WHQueryOperation::start(store, goods, from);
        let till = WHQueryOperation::end(store, goods, till);

        (from, till)
    }

    pub(crate) fn balance(db: &AnimoDB, store: Store, goods: Goods, date: Time) -> Result<Memo<WarehouseBalance>,DBError> {
        let s = db.snapshot();
        let mut tx = Txn::new(&s);

        let memo = WHTopology::balance_tx(&mut tx, store, goods, date)?;

        // TODO: unregister memo if case of error
        tx.commit()?;

        Ok(memo)
    }

    pub(crate) fn balance_tx(tx: &mut Txn, store: Store, goods: Goods, date: Time) -> Result<Memo<WarehouseBalance>,DBError>{
        // TODO move method to Ops manager
        let query = WHQueryBalance::new(store, goods, date.clone());

        log::debug!("pining memo at {:?}", query);

        let balance = if let Some((position, mut balance)) = query.closest_before(tx.s) {
            log::debug!("closest memo {:?} at {:?}", balance, position);
            let loaded = WHQueryBalance::bytes(query.prefix().clone(), position);
            if loaded.position() != query.position() {
                log::debug!("calculate from closest value");
                // TODO write test for this branch
                // calculate on interval between memo position and requested position
                for (_,op) in tx.operations(&loaded, &query) {
                    balance = balance.apply(&op)?;
                }

                // store memo
                tx.update_value(query.position(), &balance)?;
            }
            WarehouseBalance { store, goods, date, balance }
        } else {
            log::debug!("calculate from zero position");
            let mut balance = WHBalance::default();

            let (from, till) = WHTopology::get_ops_till(store, goods, date.clone());
            for (_,op) in tx.operations(&from, &till) {
                balance = balance.apply(&op)?;
            }

            // store memo
            tx.update_value(&query.position(), &balance)?;

            WarehouseBalance { store, goods, date, balance }
        };

        Ok(Memo::new(balance))
    }

    fn decode_position_from_bytes(bs: &[u8]) -> Result<(ID,ID,Time), DBError> {
        let expected = (ID_BYTES * 3) + 9 + 1;
        if bs.len() != expected {
            Err(format!("Warehouse topology: incorrect number ({}) of bytes, expected {}", bs.len(), expected).into())
        } else {
            let prefix: ID = bs[0..ID_BYTES].try_into()?;
            if prefix != *WH_BASE_TOPOLOGY {
                Err(format!("incorrect prefix id ({:?}), expected {:?}", prefix, *WH_BASE_TOPOLOGY).into())
            } else {
                let store = bs[1*ID_BYTES..2*ID_BYTES].try_into()?;
                let goods = bs[2*ID_BYTES..3*ID_BYTES].try_into()?;
                let date = Time::from_bytes(bs, 3*ID_BYTES)?;

                Ok((store, goods, date))
            }
        }
    }

    fn position_prefix() -> usize {
        ID_BYTES * 3
    }

    fn position(store: ID, goods: ID, time: Time, op: u8) -> Vec<u8> {
        let mut bs = Vec::with_capacity((ID_BYTES * 3) + 9 + 1);

        // operation prefix
        bs.extend_from_slice((*WH_BASE_TOPOLOGY).as_slice());

        // prefix define calculation context
        bs.extend_from_slice(store.as_slice());
        bs.extend_from_slice(goods.as_slice());

        // define order by time
        bs.extend_from_slice(time.to_bytes().as_slice());

        // order by operations
        bs.extend([op].into_iter());

        bs
    }

    fn position_of_operation(store: Store, goods: Goods, time: Time, op: &BalanceOperation) -> Vec<u8> {
        let op: u8 = match op {
            BalanceOperation::In(..) => u8::MIN + 2,
            BalanceOperation::Out(..) => u8::MIN + 1,
        };

        WHTopology::position(store.into(), goods.into(), time.start(), op)
    }

    fn position_of_zero(store: Store, goods: Goods) -> Vec<u8> {
        WHTopology::position(store.into(), goods.into(), Time::zero(), u8::MIN)
    }

    fn position_at_start(store: Store, goods: Goods, time: Time) -> Vec<u8> {
        WHTopology::position(store.into(), goods.into(), time.start(), u8::MIN)
    }

    fn position_at_end(store: Store, goods: Goods, time: Time) -> Vec<u8> {
        WHTopology::position(store.into(), goods.into(), time.end(), u8::MAX)
    }
}

impl OperationsTopology for WHTopology {
    type Obj = WHBalance;
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

    fn on_mutation(&self, tx: &mut Txn, cs: HashSet<Context>) -> Result<Vec<DeltaOp<Self::Obj,Self::Op,Self::TObj,Self::TOp>>, DBError> {
        // GoodsReceive, GoodsIssue

        // TODO handle delete case

        // filter contexts by "object type"
        let mut contexts = HashSet::with_capacity(cs.len());
        for c in cs {
            if let Some(instance_of) = tx.resolve(&c, *SPECIFIC_OF)? {
                if instance_of.into_before.one_of(&[*GOODS_RECEIVE, *GOODS_ISSUE])
                    || instance_of.into_after.one_of(&[*GOODS_RECEIVE, *GOODS_ISSUE]){
                    contexts.insert(c);
                }
            }
        }

        // TODO resolve up-dependent contexts

        let mut ops = Vec::with_capacity(contexts.len());
        for context in contexts {
            let (before,after) = WarehouseMovement::resolve(tx, &context)?;

            if before.is_some() || after.is_some() {
                ops.push(DeltaOp::new(context, before, after));
            }
        }
        tx.ops_manager().write_ops(tx, &ops)?;

        Ok(ops)
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub(crate) struct WarehouseBalance {
    pub(crate) balance: WHBalance,

    pub(crate) date: Time,

    pub(crate) store: Store,
    pub(crate) goods: Goods,
}

impl ToKVBytes for WarehouseBalance {
    fn to_kv_bytes(&self) -> Result<(Vec<u8>, Vec<u8>), DBError> {
        let k = WHTopology::position_at_end(self.store, self.goods, self.date.clone());
        let v = self.balance.to_bytes()?;
        Ok((k,v))
    }
}

impl FromKVBytes<Self> for WarehouseBalance {
    fn from_kv_bytes(key: &[u8], value: &[u8]) -> Result<WarehouseBalance, DBError> {
        let (store, goods, date) = WHTopology::decode_position_from_bytes(key)?;
        let balance = WHBalance::from_bytes(value)?;
        Ok(WarehouseBalance { store: Store::from(store), goods: Goods::from(goods), date, balance })
    }
}

impl From<WarehouseBalance> for WHBalance {
    fn from(v: WarehouseBalance) -> Self {
        v.balance
    }
}

impl From<&WarehouseBalance> for WHBalance {
    fn from(v: &WarehouseBalance) -> Self {
        v.balance.clone()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct WarehouseMovement {
    pub(crate) op: BalanceOperation,

    // TODO avoid serialization & deserialize of prefix & position
    prefix: usize,
    position: Vec<u8>,

    pub(crate) date: Time,

    pub(crate) store: Store,
    pub(crate) goods: Goods,
}

impl WarehouseMovement {
    fn new(store: Store, goods: Goods, date: Time, op: BalanceOperation) -> Self {
        let prefix = WHTopology::position_prefix();
        let position = WHTopology::position_at_end(store, goods, date.clone());
        WarehouseMovement { store, goods, date, op, prefix, position }
    }

}

impl ToKVBytes for WarehouseMovement {
    fn to_kv_bytes(&self) -> Result<(Vec<u8>, Vec<u8>), DBError> {
        let k = WHTopology::position_of_operation(self.store, self.goods, self.date.clone(), &self.op);
        let v = self.op.to_bytes()?;
        Ok((k,v))
    }
}

impl FromKVBytes<WarehouseMovement> for WarehouseMovement {
    fn from_kv_bytes(key: &[u8], value: &[u8]) -> Result<WarehouseMovement, DBError> {
        let (store, goods, date) = WHTopology::decode_position_from_bytes(key)?;
        let op = BalanceOperation::from_bytes(value)?;
        Ok(WarehouseMovement::new(Store::from(store), Goods::from(goods), date, op))
    }
}

impl PositionInTopology for WarehouseMovement {
    fn prefix(&self) -> usize {
        self.prefix
    }

    fn position(&self) -> &Vec<u8> {
        &self.position
    }
}

impl OperationInTopology<WHBalance,BalanceOperation,WarehouseBalance> for WarehouseMovement {
    fn resolve(env: &Txn, context: &Context) -> Result<(Option<Self>,Option<Self>), DBError> {

        let change_instance_of = if let Some(change) = env.resolve(context, *SPECIFIC_OF)? {
            change
        } else {
            return Ok((None,None));
        };
        let change_store = if let Some(change) = env.resolve(context, *STORE)? {
            change
        } else {
            return Ok((None,None));
        };
        let change_goods = if let Some(change) = env.resolve(context, *GOODS)? {
            change
        } else {
            return Ok((None,None));
        };
        let change_date = if let Some(change) = env.resolve(context, *DATE)? {
            change
        } else {
            return Ok((None,None));
        };

        let change_qty = if let Some(change) = env.resolve(context, *QTY)? {
            change
        } else {
            return Ok((None,None));
        };
        let change_cost = if let Some(change) = env.resolve(context, *COST)? {
            change
        } else {
            return Ok((None,None));
        };

        let op_before = loop {
            let instance_of = if let Some(before) = change_instance_of.into_before.as_id() {
                before
            } else {
                break None;
            };

            let qty = if let Some(before) = change_qty.into_before.as_number() {
                before
            } else {
                break None;
            };

            let cost = if let Some(before) = change_cost.into_before.as_number() {
                before
            } else {
                break None;
            };

            break BalanceOperation::resolve(instance_of, Qty(qty), Money(cost));
        };

        let op_after = loop {
            let instance_of = if let Some(before) = change_instance_of.into_after.as_id() {
                before
            } else {
                break None;
            };

            let qty = if let Some(before) = change_qty.into_after.as_number() {
                before
            } else {
                break None;
            };

            let cost = if let Some(before) = change_cost.into_after.as_number() {
                before
            } else {
                break None;
            };

            break BalanceOperation::resolve(instance_of, Qty(qty), Money(cost));
        };

        let before = loop {
            let op = if let Some(op) = op_before {
                op
            } else {
                break None;
            };

            let store = if let Some(before) = change_store.into_before.as_id() {
                Store::from(before)
            } else {
                break None;
            };

            let goods = if let Some(before) = change_goods.into_before.as_id() {
                Goods::from(before)
            } else {
                break None;
            };

            let date = if let Some(before) = change_date.into_before.as_time() {
                before
            } else {
                break None;
            };

            break Some(WarehouseMovement::new(store, goods, date, op));
        };

        let after = loop {
            let op = if let Some(op) = op_after {
                op
            } else {
                break None;
            };

            let store = if let Some(before) = change_store.into_after.as_id() {
                Store::from(before)
            } else {
                break None;
            };

            let goods = if let Some(before) = change_goods.into_after.as_id() {
                Goods::from(before)
            } else {
                break None;
            };

            let date = if let Some(before) = change_date.into_after.as_time() {
                before
            } else {
                break None;
            };

            break Some(WarehouseMovement::new(store, goods, date, op));
        };

        Ok((before,after))
    }

    fn operation(&self) -> BalanceOperation {
        self.op.clone()
    }

    fn to_value(&self) -> WarehouseBalance {
        WarehouseBalance {
            store: self.store, goods: self.goods, date: self.date.clone(),
            balance: self.op.to_value()
        }
    }
}

impl ObjectInTopology<WHBalance,BalanceOperation,WarehouseMovement> for WarehouseBalance {
    fn position(&self) -> Vec<u8> {
        WHTopology::position_at_end(self.store, self.goods, self.date.clone())
    }

    fn value(&self) -> WHBalance {
        self.balance.clone()
    }

    fn apply(&self, op: &WarehouseMovement) -> Result<Self, DBError> {
        // TODO check self.store == op.store && self.goods == op.goods && self.date >= op.date
        let balance = self.balance.apply(&op.op)?;
        Ok(WarehouseBalance {
            store: self.store, goods: self.goods, date: self.date.clone(),
            balance
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Memory;
    use crate::warehouse::test_util::{init, incoming, outgoing, delete};

    #[test]
    fn test_store_operations() {
        // animo.register_topology(Topology::Warehouse(Arc::new(WarehouseTopology())));
        let (tmp_dir, settings, db) = init();

        let wh1: Store = ID::from("wh1").into();
        let g1: Goods = ID::from("g1").into();

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

        log::debug!("READING 2022-05-31 [1]");
        let g1_balance = WHTopology::balance(&db, wh1, g1, d22_05_31.clone()).expect("Ok");
        assert_eq!(WHBalance(Qty(7.into()), Money(35.into())), g1_balance.value().into());

        log::debug!("READING 2022-05-28");
        let g1_balance = WHTopology::balance(&db, wh1, g1, Time::new("2022-05-28").unwrap()).expect("Ok");
        assert_eq!(WHBalance(Qty(5.into()), Money(25.into())), g1_balance.value().into());

        log::debug!("READING 2022-05-31 [2]");
        let g1_balance = WHTopology::balance(&db, wh1, g1, d22_05_31.clone()).expect("Ok");
        assert_eq!(WHBalance(Qty(7.into()), Money(35.into())), g1_balance.value().into());

        log::debug!("MODIFY D");
        db.modify(outgoing("D", "2022-05-31", wh1, g1, 1, Some(5))).expect("Ok");

        log::debug!("READING 2022-05-31 [3]");
        let g1_balance = WHTopology::balance(&db, wh1, g1, d22_05_31.clone()).expect("Ok");
        assert_eq!(WHBalance(Qty(6.into()), Money(30.into())), g1_balance.value().into());

        log::debug!("DELETE B");
        db.modify(delete(
            incoming("B", "2022-05-30", wh1, g1, 2, Some(10))
        )).expect("Ok");

        log::debug!("READING 2022-05-31 [4]");
        let g1_balance = WHTopology::balance(&db, wh1, g1, d22_05_31.clone()).expect("Ok");
        assert_eq!(WHBalance(Qty(4.into()), Money(20.into())), g1_balance.value().into());

        // stop db and delete data folder
        db.close();
        tmp_dir.close().unwrap();
    }
}