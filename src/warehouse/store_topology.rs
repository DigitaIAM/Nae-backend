use std::collections::HashSet;
use std::fmt::Debug;
use chrono::{TimeZone, Utc};
use serde::{Deserialize, Serialize};
use crate::animo::error::DBError;
use crate::animo::memory::{Context, ID, ID_BYTES, ID_MAX, ID_MIN};
use crate::AnimoDB;
use crate::animo::db::{FromBytes, FromKVBytes, Snapshot, ToBytes, ToKVBytes};
use crate::animo::*;
use crate::animo::ops_manager::*;
use crate::animo::shared::*;
use crate::warehouse::balance::WHBalance;
use crate::warehouse::balance_operation::BalanceOperation;
use crate::warehouse::primitives::{Money, Qty};
use crate::warehouse::turnover::{Goods, NamedValue, Store};

#[derive(Debug)]
struct WHQueryStoreBalance {
    prefix: Vec<u8>,
    position: Vec<u8>,
}

impl WHQueryStoreBalance {
    fn bytes(prefix: Vec<u8>, position: Vec<u8>) -> Self {
        // TODO check that position starts with prefix
        WHQueryStoreBalance { prefix, position }
    }

    fn new(store: Store, date: &Time) -> Self {
        let prefix = WHStoreTopology::position_prefix(store.into());
        let position = WHStoreTopology::position_at_end(store, date);
        WHQueryStoreBalance { prefix, position }
    }
}

impl PositionInTopology for WHQueryStoreBalance {
    fn prefix(&self) -> &Vec<u8> {
        &self.prefix
    }

    fn position(&self) -> &Vec<u8> {
        &self.position
    }
}

impl QueryValue<WHBalance> for WHQueryStoreBalance {
    fn closest_before(&self, s: &Snapshot) -> Option<(Vec<u8>, WHBalance)> {
        LightIterator::preceding_values(s, self).next()
    }

    fn values_after<'a>(&'a self, s: &'a Snapshot<'a>) -> LightIterator<'a, WHBalance> {
        following_light(s, &s.cf_values(), self)
    }
}

pub(crate) struct WHQueryStoreOperation {
    prefix: Vec<u8>,
    position: Vec<u8>,
}

impl WHQueryStoreOperation {
    // pub(crate) fn from_position(store: Store, position: Vec<u8>) -> Self {
    //     let prefix = WHStoreTopology::position_prefix(store.into());
    //
    //     WHQueryStoreOperation { prefix, position }
    // }

    fn zero(store: Store) -> Self {
        let prefix = WHStoreTopology::position_prefix(store.into());
        let position = WHStoreTopology::position_of_zero(store);

        WHQueryStoreOperation { prefix, position }
    }

    pub(crate) fn start(store: Store, date: &Time) -> Self {
        let prefix = WHStoreTopology::position_prefix(store.into());
        let position = WHStoreTopology::position_at_start(store, date.start());

        WHQueryStoreOperation { prefix, position }
    }

    pub(crate) fn start_exclude(store: Store, time: &Time) -> Self {
        let time = time.add_quantum();

        let prefix = WHStoreTopology::position_prefix(store.into());
        let position = WHStoreTopology::position_at_start(store, time);

        WHQueryStoreOperation { prefix, position }
    }


    pub(crate) fn end(store: Store, date: &Time) -> Self {
        let prefix = WHStoreTopology::position_prefix(store.into());
        let position = WHStoreTopology::position_at_end(store, date);

        WHQueryStoreOperation { prefix, position }
    }

    pub(crate) fn end_exclude(store: Store, time: &Time) -> Self {
        let time = time.sub_quantum();

        let prefix = WHStoreTopology::position_prefix(store.into());
        let position = WHStoreTopology::position_at_end(store, &time);

        WHQueryStoreOperation { prefix, position }
    }
}

impl PositionInTopology for WHQueryStoreOperation {
    fn prefix(&self) -> &Vec<u8> {
        &self.prefix
    }

    fn position(&self) -> &Vec<u8> {
        &self.position
    }
}

#[derive(Debug, Hash, Eq, PartialEq)]
pub struct WHStoreTopology();

impl WHStoreTopology {
    pub(crate) fn get_ops_till(store: Store, till: Time) -> (WHQueryStoreOperation, WHQueryStoreOperation) {
        let from = WHQueryStoreOperation::zero(store);
        let till = WHQueryStoreOperation::end(store, &till);

        (from, till)
    }

    pub(crate) fn get_ops(store: Store, from: &Time, till: &Time) -> (WHQueryStoreOperation, WHQueryStoreOperation) {
        let from = WHQueryStoreOperation::start(store, from);
        let till = WHQueryStoreOperation::end(store, till);

        (from, till)
    }

    pub(crate) fn balance(db: &AnimoDB, store: Store, date: Time) -> Result<Memo<NamedValue<Store, Money>>,DBError> {
        let s = db.snapshot();
        let mut tx = Txn::new(&s);

        let memo = WHStoreTopology::balance_tx(&mut tx, store, date)?;

        // TODO: unregister memo if case of error
        tx.commit()?;

        Ok(memo)
    }

    pub(crate) fn balance_tx(tx: &mut Txn, store: Store, date: Time) -> Result<Memo<NamedValue<Store, Money>>,DBError> {
        // TODO move method to Ops manager
        let query = WHQueryStoreBalance::new(store, &date);

        log::debug!("pining memo at {:?}", query);

        let balance = if let Some((position, mut balance)) = query.closest_before(tx.s) {
            log::debug!("closest memo {:?} at {:?}", balance, position);
            let loaded = WHQueryStoreBalance::bytes(query.prefix().clone(), position);
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
            balance.1
        } else {
            log::debug!("calculate from zero position");
            let mut balance = WHBalance::default();

            let (from, till) = WHStoreTopology::get_ops_till(store, date);
            for (_,op) in tx.operations(&from, &till) {
                balance = balance.apply(&op)?;
            }

            // store memo
            tx.update_value(&query.position(), &balance)?;

            balance.1
        };

        Ok(Memo::new(NamedValue::new(store, balance)))
    }

    fn decode_position_from_bytes(bs: &[u8]) -> Result<(Store,Goods,Time), DBError> {
        let expected = (ID_BYTES * 3) + 10 + 1;
        if bs.len() != expected {
            Err(format!("Warehouse store topology: incorrect number ({}) of bytes, expected {}", bs.len(), expected).into())
        } else {
            let prefix: ID = bs[0..ID_BYTES].try_into()?;
            if prefix != *WH_STORE_TOPOLOGY {
                Err(format!("incorrect prefix id ({:?}), expected {:?}", prefix, *WH_STORE_TOPOLOGY).into())
            } else {
                let convert = |bs: &[u8]| -> [u8; 8] {
                    bs.try_into().expect("slice with incorrect length")
                };
                let store: ID = bs[1*ID_BYTES..2*ID_BYTES].try_into()?;
                let date = Time::from_bytes(bs, 2*ID_BYTES)?;
                let goods: ID = bs[(2*ID_BYTES+10)..(3*ID_BYTES+10)].try_into()?;

                Ok((store.into(), goods.into(), date))
            }
        }
    }

    fn position_prefix(store: ID) -> Vec<u8> {
        let mut bs = Vec::with_capacity(ID_BYTES * 2);

        // operation prefix
        bs.extend_from_slice((*WH_STORE_TOPOLOGY).as_slice());

        // prefix define calculation context
        bs.extend_from_slice(store.as_slice());

        bs
    }


    fn position(store: ID, goods: ID, time: Time, op: u8) -> Vec<u8> {
        let mut bs = Vec::with_capacity((ID_BYTES * 3) + 10 + 1);

        // operation prefix
        bs.extend_from_slice((*WH_STORE_TOPOLOGY).as_slice());

        // prefix define calculation context
        bs.extend_from_slice(store.as_slice());

        // define order by time
        bs.extend_from_slice(time.to_bytes().as_slice());

        // order by operations
        bs.extend_from_slice(goods.as_slice());

        // order by operations
        bs.extend([op].into_iter());

        bs
    }

    fn position_of_operation(store: Store, goods: Goods, time: Time, op: &BalanceOperation) -> Vec<u8> {
        let op: u8 = match op {
            BalanceOperation::In(..) => u8::MIN + 2,
            BalanceOperation::Out(..) => u8::MIN + 1,
        };

        WHStoreTopology::position(store.into(), goods.into(), time.start(), op)
    }

    fn position_of_zero(store: Store) -> Vec<u8> {
        WHStoreTopology::position(store.into(), ID_MIN, Time::zero(), u8::MIN)
    }

    fn position_at_start(store: Store, time: Time) -> Vec<u8> {
        WHStoreTopology::position(store.into(), ID_MIN, time, u8::MIN)
    }

    fn position_at_end(store: Store, time: &Time) -> Vec<u8> {
        WHStoreTopology::position(store.into(), ID_MAX, time.end(), u8::MAX)
    }
}

impl OperationsTopology for WHStoreTopology {
    type Obj = WHBalance;
    type Op = BalanceOperation;

    type TObj = StoreBalance;
    type TOp = StoreMovement;

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
            let (before,after) = StoreMovement::resolve(tx, &context)?;

            if before.is_some() || after.is_some() {
                ops.push(DeltaOp::new(context, before, after));
            }
        }
        tx.ops_manager().write_ops(tx, &ops)?;

        Ok(ops)
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub(crate) struct StoreBalance {
    pub(crate) balance: WHBalance,

    pub(crate) date: Time,

    pub(crate) store: Store,
    pub(crate) goods: Goods,
}

impl ToKVBytes for StoreBalance {
    fn to_kv_bytes(&self) -> Result<(Vec<u8>, Vec<u8>), DBError> {
        let k = WHStoreTopology::position_at_end(self.store, &self.date);
        let v = self.balance.to_bytes()?;
        Ok((k,v))
    }
}

impl FromKVBytes<Self> for StoreBalance {
    fn from_kv_bytes(key: &[u8], value: &[u8]) -> Result<StoreBalance, DBError> {
        let (store, goods, date) = WHStoreTopology::decode_position_from_bytes(key)?;
        let balance = WHBalance::from_bytes(value)?;
        Ok(StoreBalance { store, goods, date, balance })
    }
}

impl From<StoreBalance> for WHBalance {
    fn from(v: StoreBalance) -> Self {
        v.balance
    }
}

impl From<&StoreBalance> for WHBalance {
    fn from(v: &StoreBalance) -> Self {
        v.balance.clone()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct StoreMovement {
    pub(crate) op: BalanceOperation,

    // TODO avoid serialization & deserialize of prefix & position
    prefix: Vec<u8>,
    position: Vec<u8>,

    pub(crate) date: Time,

    pub(crate) store: Store,
    pub(crate) goods: Goods,
}

impl StoreMovement {
    fn new(store: Store, goods: Goods, date: Time, op: BalanceOperation) -> Self {
        let prefix = WHStoreTopology::position_prefix(store.into());
        let position = WHStoreTopology::position_at_end(store, &date);
        StoreMovement { store, goods, date, op, prefix, position }
    }

}

impl ToKVBytes for StoreMovement {
    fn to_kv_bytes(&self) -> Result<(Vec<u8>, Vec<u8>), DBError> {
        let k = WHStoreTopology::position_of_operation(self.store, self.goods, self.date.clone(), &self.op);
        let v = self.op.to_bytes()?;
        Ok((k,v))
    }
}

impl FromKVBytes<StoreMovement> for StoreMovement {
    fn from_kv_bytes(key: &[u8], value: &[u8]) -> Result<StoreMovement, DBError> {
        let (store, goods, date) = WHStoreTopology::decode_position_from_bytes(key)?;
        let op = BalanceOperation::from_bytes(value)?;
        Ok(StoreMovement::new(store, goods, date, op))
    }
}

impl PositionInTopology for StoreMovement {
    fn prefix(&self) -> &Vec<u8> {
        &self.prefix
    }

    fn position(&self) -> &Vec<u8> {
        &self.position
    }
}

impl OperationInTopology<WHBalance,BalanceOperation,StoreBalance> for StoreMovement {
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

            break Some(StoreMovement::new(store, goods, date, op));
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

            break Some(StoreMovement::new(store, goods, date, op));
        };

        Ok((before,after))
    }

    fn operation(&self) -> BalanceOperation {
        self.op.clone()
    }

    fn to_value(&self) -> StoreBalance {
        StoreBalance {
            store: self.store, goods: self.goods, date: self.date.clone(),
            balance: self.op.to_value()
        }
    }
}

impl ObjectInTopology<WHBalance,BalanceOperation,StoreMovement> for StoreBalance {
    fn position(&self) -> Vec<u8> {
        WHStoreTopology::position_at_end(self.store, &self.date)
    }

    fn value(&self) -> WHBalance {
        self.balance.clone()
    }

    fn apply(&self, op: &StoreMovement) -> Result<Self, DBError> {
        // TODO check self.store == op.store && self.goods == op.goods && self.date >= op.date
        let balance = self.balance.apply(&op.op)?;
        Ok(StoreBalance {
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
        let db = init();

        let wh1: Store = ID::from("wh1").into();
        let g1: Goods = ID::from("g1").into();

        let d22_05_28 = Time::new("2022-05-28").unwrap();
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
        let store_balance = WHStoreTopology::balance(&db, wh1, d22_05_31.clone()).expect("Ok");
        assert_eq!(
            NamedValue::new(wh1, Money(35.into())),
            store_balance.value().clone()
        );

        log::debug!("READING [2] 2022-05-28");
        let store_balance = WHStoreTopology::balance(&db, wh1, d22_05_28.clone()).expect("Ok");
        assert_eq!(
            NamedValue::new(wh1, Money(25.into())),
            store_balance.value().clone()
        );

        log::debug!("READING [3] 2022-05-31");
        let store_balance = WHStoreTopology::balance(&db, wh1, d22_05_31.clone()).expect("Ok");
        assert_eq!(
            NamedValue::new(wh1, Money(35.into())),
            store_balance.value().clone()
        );

        log::debug!("MODIFY D");
        db.modify(outgoing("D", "2022-05-31", wh1, g1, 1, Some(5))).expect("Ok");

        log::debug!("READING [4] 2022-05-31");
        let store_balance = WHStoreTopology::balance(&db, wh1, d22_05_31.clone()).expect("Ok");
        assert_eq!(
            NamedValue::new(wh1, Money(30.into())),
            store_balance.value().clone()
        );

        log::debug!("DELETE B");
        db.modify(delete(
            incoming("B", "2022-05-30", wh1, g1, 2, Some(10))
        )).expect("Ok");

        log::debug!("READING 2022-05-31 [4]");
        let store_balance = WHStoreTopology::balance(&db, wh1, d22_05_31.clone()).expect("Ok");
        assert_eq!(
            NamedValue::new(wh1, Money(20.into())),
            store_balance.value().clone()
        );
    }
}