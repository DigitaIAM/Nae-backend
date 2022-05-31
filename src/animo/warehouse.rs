use std::cmp::Ordering;
use std::collections::HashSet;
use serde::{Deserialize, Serialize};
use crate::animo::{Env, Object, Operation, OperationGenerator};
use crate::animo::primitives::{Qty, Money};
use crate::error::DBError;
use crate::memory::{Context, ID, ID_BYTES, Time};
use crate::rocksdb::{FromBytes, Snapshot, ToBytes};
use crate::shared::*;

#[derive(Debug, Serialize, Deserialize)]
pub enum OpWarehouse {
    In(Qty, Money),
    Out(Qty, Money),
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

#[derive(Debug, Clone, Hash, Eq, PartialEq, Default, Serialize, Deserialize)]
pub struct Balance(pub Qty, pub Money);

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

impl Balance {
    fn time_to_bytes(ts: u64) -> [u8; 8] {
        ts.to_be_bytes()
    }

    fn local_topology_position_of_zero(store: &ID, goods: &ID) -> Vec<u8> {
        let mut bs = Vec::with_capacity((ID_BYTES * 2) + 8 + 1);

        // prefix define calculation context
        bs.extend_from_slice(store.as_slice());
        bs.extend_from_slice(goods.as_slice());

        // define order by time
        bs.extend_from_slice(Balance::time_to_bytes(u64::MIN).as_slice());

        // order by operations
        bs.extend([u8::MIN].into_iter());

        bs
    }

    fn local_topology_position_of_memo(store: &ID, goods: &ID, time: &Time) -> Vec<u8> {
        let mut bs = Vec::with_capacity((ID_BYTES * 2) + 8 + 1);

        // prefix define calculation context
        bs.extend_from_slice(store.as_slice());
        bs.extend_from_slice(goods.as_slice());

        // define order by time
        bs.extend_from_slice(Balance::time_to_bytes(time.timestamp().try_into().unwrap()).as_slice());

        // order by operations
        bs.extend([u8::MAX].into_iter());

        bs
    }

    fn local_topology_position_of_operation(store: &ID, goods: &ID, time: &Time, op: &OpWarehouse) -> Vec<u8> {
        let mut bs = Vec::with_capacity((ID_BYTES * 2) + 8 + 1);

        // prefix define calculation context
        bs.extend_from_slice(store.as_slice());
        bs.extend_from_slice(goods.as_slice());

        // define order by time
        bs.extend_from_slice(Balance::time_to_bytes(time.timestamp().try_into().unwrap()).as_slice());

        // order by operations
        let b: u8 = match op {
            OpWarehouse::In(..) => u8::MAX,
            OpWarehouse::Out(..) => u8::MIN,
        };

        bs.extend([b].into_iter());

        bs
    }

    pub(crate) fn get_memo(s: &Snapshot, store: &ID, goods: &ID, time: &Time) -> Result<Self, DBError> {
        // TODO move method to Ops manager
        let ops_manager = s.rf.ops_manager.clone();

        let position = Balance::local_topology_position_of_memo(store, goods, time);

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
            let zero_position = Balance::local_topology_position_of_zero(store, goods);
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

impl OperationGenerator for Balance {

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

            // TODO calculate Op
            let op = if instance_of == *GOODS_RECEIVE {
                OpWarehouse::In(Qty(qty), Money(cost))
            } else {
                OpWarehouse::Out(Qty(qty), Money(cost))
            };

            let local_topology_position = Balance::local_topology_position_of_operation(&store, &goods, &date, &op);

            env.ops_manager().write(env, local_topology_position, op)?;
        }

        Ok(())
    }
}
