use std::collections::HashSet;
use std::fmt::Debug;
use chrono::{TimeZone, Utc};
use serde::{Deserialize, Serialize};
use crate::animo::{BetweenLightIterator, following_light, LightIterator, Memo, Object, ObjectInTopology, Operation, OperationInTopology, OperationsTopology, Txn};
use crate::error::DBError;
use crate::memory::{Context, ID, ID_BYTES, Time};
use crate::RocksDB;
use crate::rocksdb::{FromBytes, FromKVBytes, ToBytes, ToKVBytes};
use crate::shared::*;
use crate::warehouse::balance::Balance;
use crate::warehouse::balance_operation::BalanceOperation;
use crate::warehouse::{time_to_u64, ts_to_bytes};
use crate::warehouse::primitives::{Money, Qty};

#[derive(Debug, Hash, Eq, PartialEq)]
pub struct WarehouseTopology();

impl WarehouseTopology {
    pub(crate) fn get_ops_till<'a,O: FromBytes<O> + Debug>(tx: &'a Txn, store: ID, goods: ID, till: Time) -> BetweenLightIterator<'a,O> {
        let from_point = WarehouseTopology::position_of_zero(store, goods);
        let till_point = WarehouseTopology::position_at_end(store, goods, till);

        tx.operations(from_point, till_point)
    }

    pub(crate) fn get_ops<'a,O>(tx: &'a Txn, store: ID, goods: ID, from: Time, till: Time) -> BetweenLightIterator<'a,O> {
        let from_point = WarehouseTopology::position_at_start(store, goods, from);
        let till_point = WarehouseTopology::position_at_end(store, goods, till);

        tx.operations(from_point, till_point)
    }

    pub(crate) fn balance(db: &RocksDB, store: ID, goods: ID, date: Time) -> Result<Memo<WarehouseBalance>,DBError> {
        let s = db.snapshot();
        let mut tx = Txn::new(&s);

        let memo = WarehouseTopology::balance_tx(&mut tx, store, goods, date)?;

        // TODO: unregister memo if case of error
        tx.commit()?;

        Ok(memo)
    }

    pub(crate) fn balance_tx(tx: &mut Txn, store: ID, goods: ID, date: Time) -> Result<Memo<WarehouseBalance>,DBError>{
        // TODO move method to Ops manager
        let ops_manager = tx.s.rf.ops_manager.clone();

        let position = WarehouseTopology::position_at_end(store, goods, date);

        debug!("pining memo at {:?}", position);

        let s = tx.s;
        let k = WarehouseTopology::position_of_zero(store, goods);
        let it: LightIterator<Balance> = following_light(s, &s.cf_values(), &k);
        for (k,v) in it {
            debug!("> {:?} {:?}",v,k)
        }

        debug!("============");

        let balance = if let Some((r_position, mut balance)) = ops_manager.get_closest_light_value::<Balance>(tx.s, position.clone()) {
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

    fn decode_position_from_bytes(bs: &[u8]) -> Result<(ID,ID,Time), DBError> {
        let expected = (ID_BYTES * 3) + 8 + 1;
        if bs.len() != expected {
            Err(format!("incorrect number ({}) of bytes, expected {}", bs.len(), expected).into())
        } else {
            let prefix: ID = bs[0..ID_BYTES].try_into()?;
            if prefix != *WH_BASE_TOPOLOGY {
                Err(format!("incorrect prefix id ({:?}), expected {:?}", prefix, *WH_BASE_TOPOLOGY).into())
            } else {
                let convert = |bs: &[u8]| -> [u8; 8] {
                    bs.try_into().expect("slice with incorrect length")
                };
                let store = bs[1*ID_BYTES..2*ID_BYTES].try_into()?;
                let goods = bs[2*ID_BYTES..3*ID_BYTES].try_into()?;
                let ts = u64::from_be_bytes(convert(&bs[3*ID_BYTES..(3*ID_BYTES+8)]));

                Ok((store, goods, Utc.timestamp_millis(ts as i64)))
            }
        }
    }

    fn position(store: ID, goods: ID, time: u64, op: u8) -> Vec<u8> {
        let mut bs = Vec::with_capacity((ID_BYTES * 3) + 8 + 1);

        // operation prefix
        bs.extend_from_slice((*WH_BASE_TOPOLOGY).as_slice());

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
            BalanceOperation::In(..) => u8::MIN + 2,
            BalanceOperation::Out(..) => u8::MIN + 1,
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

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub(crate) struct WarehouseBalance {
    pub(crate) balance: Balance,

    pub(crate) date: Time,

    pub(crate) store: ID,
    pub(crate) goods: ID,
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
        let (store, goods, date) = WarehouseTopology::decode_position_from_bytes(key)?;
        let balance = Balance::from_bytes(value)?;
        Ok(WarehouseBalance { store, goods, date, balance })
    }
}

impl From<WarehouseBalance> for Balance {
    fn from(v: WarehouseBalance) -> Self {
        v.balance
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct WarehouseMovement {
    pub(crate) op: BalanceOperation,

    pub(crate) date: Time,

    pub(crate) store: ID,
    pub(crate) goods: ID,
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
        let (store, goods, date) = WarehouseTopology::decode_position_from_bytes(key)?;
        let op = BalanceOperation::from_bytes(value)?;
        Ok(WarehouseMovement { store, goods, date, op })
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

    fn to_value(&self) -> WarehouseBalance {
        WarehouseBalance {
            store: self.store, goods: self.goods, date: self.date,
            balance: self.op.to_value()
        }
    }
}

impl ObjectInTopology<Balance,BalanceOperation,WarehouseMovement> for WarehouseBalance {
    fn position(&self) -> Vec<u8> {
        WarehouseTopology::position_at_end(self.store, self.goods, self.date)
    }

    fn value(&self) -> Balance {
        self.balance.clone()
    }

    fn apply(&self, op: &WarehouseMovement) -> Result<Self, DBError> {
        // TODO check self.store == op.store && self.goods == op.goods && self.date >= op.date
        let balance = self.balance.apply(&op.op)?;
        Ok(WarehouseBalance {
            store: self.store, goods: self.goods, date: self.date,
            balance
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use crate::{Memory, RocksDB};
    use crate::animo::{Animo, Topology};
    use crate::warehouse::test_util::{incoming, outgoing, time_end};

    fn init() {
        std::env::set_var("RUST_LOG", "actix_web=debug,nae_backend=debug");
        let _ = env_logger::builder().is_test(true).try_init();
    }

    #[test]
    fn test_store_operations() {
        init();

        let tmp_dir = tempfile::tempdir().unwrap();
        let tmp_path = tmp_dir.path().to_str().unwrap();
        let mut db: RocksDB = Memory::init(tmp_path).unwrap();
        let mut animo = Animo::default();
        animo.register_topology(Topology::Warehouse(Arc::new(WarehouseTopology())));
        db.register_dispatcher(Arc::new(animo)).unwrap();

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
        let g1_balance = WarehouseTopology::balance(&db, wh1, g1, time_end("2022-05-31")).expect("Ok");
        assert_eq!(Balance(Qty(7.into()), Money(35.into())), g1_balance.value().into());

        debug!("READING 2022-05-28");
        let g1_balance = WarehouseTopology::balance(&db, wh1, g1, time_end("2022-05-28")).expect("Ok");
        assert_eq!(Balance(Qty(5.into()), Money(25.into())), g1_balance.value().into());

        debug!("READING 2022-05-31");
        let g1_balance = WarehouseTopology::balance(&db, wh1, g1, time_end("2022-05-31")).expect("Ok");
        assert_eq!(Balance(Qty(7.into()), Money(35.into())), g1_balance.value().into());

        debug!("MODIFY D");
        db.modify(outgoing("D", "2022-05-31", wh1, g1, 1, Some(5))).expect("Ok");

        debug!("READING 2022-05-31");
        let g1_balance = WarehouseTopology::balance(&db, wh1, g1, time_end("2022-05-31")).expect("Ok");
        assert_eq!(Balance(Qty(6.into()), Money(30.into())), g1_balance.value().into());
    }
}