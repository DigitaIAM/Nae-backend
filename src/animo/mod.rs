mod primitives;
mod ops_manager;
mod warehouse;

use std::collections::{HashMap, HashSet};
use std::hash::Hash;
use std::sync::Arc;
use rocksdb::{Error, WriteBatch};
use rust_decimal::Decimal;
use crate::error::DBError;
use crate::memory::{ChangeTransformation, Context, ID, Time, Transformation, Value};
use crate::rocksdb::{Dispatcher, FromBytes, Snapshot, ToBytes};

pub use ops_manager::OpsManager;
use crate::animo::ops_manager::ItemsIterator;
use crate::animo::warehouse::WarehouseStock;


// Report for dates
//           | open       | in         | out        | close      |
//           | qty | cost | qty | cost | qty | cost | qty | cost |
// store     |  -  |  +   |  -  |  +   |  -  |  +   |  -  |  +   |
//  goods    |  +  |  +   |  +  |  +   |  +  |  +   |  +  |  +   |
//   docs    |  +  |  +   |  +  |  +   |  +  |  +   |  +  |  +   |
//    rec?   |  +  |  +   |  +  |  +   |  +  |  +   |  +  |  +   |

// store     |  -  |  +   |  -  |  +   |  -  |  +   |  -  |  +   |
//  docs     |  +  |  +   |  +  |  +   |  +  |  +   |  +  |  +   |
//   goods   |  +  |  +   |  +  |  +   |  +  |  +   |  +  |  +   |
//    rec?   |  +  |  +   |  +  |  +   |  +  |  +   |  +  |  +   |


// расход1 storeB копыта 1
// расход1 storeB рога   2
// расход2 storeB копыта 3

// отчет о движение
// storeB    |     | =100 |     |      |     |  =80 |     |  =20 |
//  копыта   |  5  |  100 |     |      | =4  |  =80 |  =1 |  =20 |
//   расход1 |  5  |  100 |     |      |  1  |  =20 |  =4 |  =80 |
//   расход2 |  4  |  80  |     |      |  3  |  =60 |  =1 |  =20 |

//реестр документов
// storeB    |     | =100 |     |      |     |  =80 |     |  =20 |
//  расход1  |     |  100 |     |      |     |  =20 |     |  =80 |
//   копыта  |  5  |  100 |     |      |  1  |  =20 |  =4 |  =80 |
//  расход2  |     |  80  |     |      |     |  =60 |     |  =20 |
//   копыта  |  4  |  80  |     |      | =3  |  =60 |  =1 |  =20 |

pub(crate) trait Calculation {
    fn depends_on(&self) -> Vec<ID>;
    fn produce(&self) -> ID;
}

// Objects and operations  at topology
pub(crate) trait Object<V, O> where O: Operation<V> {
    fn apply_delta(&self, delta: &Self) -> Self;

    fn apply(&self, op: &O) -> Self;
}

pub(crate) trait Operation<V> {
    fn delta_after_operation(&self) -> V;
    fn delta_between_operations(&self, other: &Self) -> V;
}

// pub(crate) trait Topology {
//     fn is_operations_topology(&self) -> bool;
//     fn as_operations_topology<V,O>(&self) -> Arc<dyn OperationsTopology<V,O>>;
//
//     fn is_dependent_topology(&self) -> bool;
//     fn as_aggregation_topology<T,O,V,VV>(&self) -> Arc<dyn AggregationTopology<T,O,V,V>>;
// }

pub(crate) trait OperationsTopology {
    type Obj: Object<Self::Obj, Self::Op>;
    type Op: Operation<Self::Obj>;

    // TODO remove `&self`
    fn depends_on(&self) -> Vec<ID>;

    // TODO remove `&self`
    fn on_mutation(&self, tx: &mut Txn, contexts: HashSet<Context>) -> Result<Vec<Op>, DBError>;
}

pub(crate) trait AggregationTopology {
    type DependOn: OperationsTopology;
    type Op = <DependOn as OperationsTopology>::Op;

    // TODO remove `&self`
    fn depends_on(&self) -> DependOn;

    // TODO remove `&self`
    fn on_operation(&self, env: &mut Txn, ops: Vec<DependOn::Op>) -> Result<(), DBError>;
}

trait Memo<T,V> {
    fn position(&self) -> &[u8];

    fn value(&self) -> V;
}

trait AggregationDelta<V> {
    fn position(&self) -> Vec<u8>;
    fn position_of_aggregation(&self) -> Result<Vec<u8>, DBError>;
    fn delta(&self) -> V;
}

pub(crate) struct Txn<'a> {
    s: &'a Snapshot<'a>,
    batch: WriteBatch,
}

impl<'a> Txn<'a> {

    pub(crate) fn get_operation<V, O: Operation<V> + FromBytes<O>>(&self, op: &O) -> Result<Option<O>, DBError> {
        match self.s.pit.get_cf(&s.cf_operations(), op.position().as_slice()) {
            Ok(bs) => {
                match bs {
                    None => Ok(None),
                    Some(_) => Ok(O::from_bytes(bs.as_slice())?)
                }
            }
            Err(e) => Err(e.to_string().into())
        }
    }

    pub(crate) fn put_operation<V, O: Operation<V> + ToBytes>(&mut self, op: &O) -> Result<(), DBError> {
        self.batch.put_cf(&s.cf_operations(), op.position().as_slice(), op.to_bytes()?);
        Ok(())
    }

    pub(crate) fn memos_after<'a,O>(&self, position: &Vec<u8>) -> Result<ItemsIterator<'a,O>, DBError> {
        self.s.rf.ops_manager.memos_after(self.s, position)
    }

    pub(crate) fn put_memo<T,V: ToBytes>(&mut self, memo: &impl Memo<T,V>) -> Result<(), DBError> {
        self.batch.put_cf(&s.cf_memos(), &memo.position(), memo.value().to_bytes()?);
        Ok(())
    }

    pub(crate) fn commit(self) -> Result<Self,DBError> {
        self.s.rf.db.write(self.batch)
            .map_err(|e| e.to_string().into())
            .map(|_| self)
    }

    pub(crate) fn ops_manager(&mut self) -> Arc<OpsManager> {
        self.s.rf.ops_manager.clone()
    }

    pub(crate) fn resolve(&self, context: &Context, what: ID) -> Result<Option<Transformation>, DBError> {
        // TODO calculate

        // let what = ID::from(what);

        // read value for give `context` and `what`. In case it's not exist, repeat on "above" context
        let mut memory = self.s.load_by(context, &what)?;
        if memory != Value::Nothing {
            Ok(Some(Transformation { context: context.clone(), what, into: memory }))
        } else {
            let mut context = context.clone();
            loop {
                match context.0.split_last() {
                    Some((_, ids)) => {
                        context = Context(ids.to_vec());
                        memory = self.s.load_by(&context, &what)?;
                        if memory != Value::Nothing {
                            break Ok(Some(Transformation { context, what, into: memory }))
                        }
                    }
                    None => break Ok(None),
                }
            }
        }
    }

    pub(crate) fn resolve_as_id(&self, context: &Context, what: ID) -> Result<ID, DBError> {
        self.resolve(context, what)?
            .ok_or_else(|| format!("{:?} is not exist", what).into())
            .and_then(|t| t.into.as_id()
                .map_err(|_| format!("{:?} is not ID", what).into())
            )
    }

    pub(crate) fn resolve_as_time(&self, context: &Context, what: ID) -> Result<Time, DBError> {
        self.resolve(context, what)?
            .ok_or_else(|| format!("{:?} is not exist", what).into())
            .and_then(|t| t.into.as_time()
                .map_err(|_| format!("{:?} is not Time", what).into())
            )
    }

    pub(crate) fn resolve_as_number(&self, context: &Context, what: ID) -> Result<Decimal, DBError> {
        self.resolve(context, what)?
            .ok_or_else(|| format!("{:?} is not exist", what).into())
            .and_then(|t| t.into.as_number()
                .map_err(|_| format!("{:?} is not Number", what).into())
            )
    }
}

enum Topologies {
    Operation(Box<dyn OperationsTopology>),
    Aggregation(Box<dyn AggregationTopology>),
}

pub(crate) struct Animo {
    topologies: Vec<Topologies>,

    // list of node producers that depend on id
    what_to_topologies: HashMap<ID, HashSet<Topologies>>
}

impl Animo {
    pub fn register_topology(&mut self, topology: Topologies) {
        match &topology {
            Topologies::Operation(t) => {
                // update helper map for fast resolve of dependants on given mutation
                for id in topology.depends_on() {
                    match self.what_to_topologies.get_mut(&id) {
                        None => {
                            let mut set = HashSet::new();
                            set.insert(topology.clone());
                            self.what_to_topologies.insert(id, set);
                        }
                        Some(v) => {
                            v.insert(topology.clone());
                        }
                    }
                }
            }
            _ => {}
        }

        // add to list of op-producers
        self.topologies.push(topology);
    }
}

impl<T,V,O> Dispatcher for Animo<T,V,O> where
    V: Object<V, O>,
    T: OperationsTopology<V> + Eq + Hash
{
    // push propagation of mutations
    fn on_mutation(&self, s: &Snapshot, mutations: &[ChangeTransformation]) -> Result<(), DBError> {
        // calculate node_producers that affected by mutations
        let mut producers: HashMap<Arc<T>, HashSet<Context>> = HashMap::new();
        for mutation in mutations {
            if let Some(set) = self.what_to_topologies.get(&mutation.what) {
                for item in set {
                    match producers.get_mut(item) {
                        Some(contexts) => {
                            contexts.insert(mutation.context.clone());
                        },
                        None => {
                            let mut contexts = HashSet::new();
                            contexts.insert(mutation.context.clone());
                            producers.insert(item.clone(), contexts);
                        }
                    }
                }
            }
        }

        // TODO calculate up-dependant contexts here or at producer?

        let mut env = Txn { s: s };

        // generate new operations or overwrite existing
        for (producer, contexts) in producers.into_iter() {
            producer.on_mutation(&mut env, contexts)?;
        }

        Ok(())
    }
}