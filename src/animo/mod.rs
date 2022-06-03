mod primitives;
mod ops_manager;
mod warehouse;

use std::collections::{HashMap, HashSet};
use std::hash::Hash;
use std::sync::Arc;
use rocksdb::{AsColumnFamilyRef, Error, WriteBatch};
use rust_decimal::Decimal;
use crate::error::DBError;
use crate::memory::{ChangeTransformation, Context, ID, Time, Transformation, Value};
use crate::rocksdb::{Dispatcher, FromBytes, FromKVBytes, Snapshot, ToBytes, ToKVBytes};

pub use ops_manager::OpsManager;
use crate::animo::ops_manager::{BetweenIterator, ItemsIterator};
use crate::animo::warehouse::{WarehouseStockTopology, WarehouseTopology};


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
pub(crate) trait Object<O>: Sized + ToBytes + FromBytes<Self> {
    // same as apply operation
    // fn apply_delta(&self, delta: &Self) -> Self;

    fn apply(&self, op: &O) -> Result<Self,DBError>;
}

// TO - operation in topology
// BV - base object
pub(crate) trait ObjectInTopology<BV,BO,TO: OperationInTopology<BV,BO,Self>>: Sized + ToKVBytes + FromKVBytes<Self> {
    fn apply(&self, op: &TO) -> Result<Self,DBError>;
}

pub(crate) trait DeltaOperation<V>: Sized {
    fn position(&self) -> Vec<u8>;
    fn delta_between(&self, other: &Self) -> Self;
    fn to_value(&self) -> V;
}

pub(crate) trait Operation<V>: Sized + FromBytes<Self> + ToBytes {
    fn delta_between(&self, other: &Self) -> Self;
    fn to_value(&self) -> V;
}

// TV - object in topology
// BV - base object
pub(crate) trait OperationInTopology<BV,BO,TV: ObjectInTopology<BV,BO,Self>>: Sized + FromKVBytes<Self> + ToKVBytes {
    fn resolve(env: &Txn, context: &Context) -> Result<Self, DBError>;

    fn position(&self) -> Vec<u8>;
    fn operation(&self) -> BO;

    fn delta_between(&self, other: &Self) -> Self;
    fn to_value(&self) -> TV;
}

// pub(crate) trait Topology {
//     fn is_operations_topology(&self) -> bool;
//     fn as_operations_topology<V,O>(&self) -> Arc<dyn OperationsTopology<V,O>>;
//
//     fn is_dependent_topology(&self) -> bool;
//     fn as_aggregation_topology<T,O,V,VV>(&self) -> Arc<dyn AggregationTopology<T,O,V,V>>;
// }

pub(crate) trait OperationsTopology {
    type Obj: Object<Self::Op>;
    type Op: Operation<Self::Obj>;

    type TObj: ObjectInTopology<Self::Obj,Self::Op,Self::TOp>;
    type TOp: OperationInTopology<Self::Obj,Self::Op,Self::TObj>;

    // TODO remove `&self`
    fn depends_on(&self) -> Vec<ID>;

    // TODO remove `&self`
    fn on_mutation(&self, tx: &mut Txn, contexts: HashSet<Context>) -> Result<Vec<Self::TOp>, DBError>;
}

pub(crate) trait AggregationObject<O>: Sized + ToBytes + FromBytes<Self> {
    // same as apply operation
    // fn apply_delta(&self, delta: &Self) -> Self;

    fn apply_aggregation(&self, op: &O) -> Result<Self,DBError>;
}

pub(crate) trait AggregationOperation<TV>: Sized + ToBytes + FromBytes<Self> {
    fn to_value(&self) -> TV;
}

pub(crate) trait AggregationObjectInTopology<BV,BO,TO: AggregationOperationInTopology<BV,BO,Self>>: Sized + ToKVBytes + FromKVBytes<Self> {
    fn apply(&self, op: &TO) -> Result<Self,DBError>;
}

pub(crate) trait AggregationOperationInTopology<BV,BO,TV: AggregationObjectInTopology<BV,BO,Self>>: Sized + FromKVBytes<Self> + ToKVBytes {
    fn position(&self) -> Vec<u8>;
    fn position_of_aggregation(&self) -> Result<Vec<u8>, DBError>;

    fn operation(&self) -> BO;

    fn delta_between(&self, other: &Self) -> Self;
    fn to_value(&self) -> TV;
}

// TODO Self::DependantOn::Obj;
pub(crate) trait AggregationTopology {
    type DependantOn: OperationsTopology;

    type InObj: Object<Self::InOp>;
    type InOp: Operation<Self::InObj>;

    type InTObj: ObjectInTopology<Self::InObj,Self::InOp,Self::InTOp>;
    type InTOp: OperationInTopology<Self::InObj,Self::InOp,Self::InTObj>;

    // TODO remove `&self`
    fn depends_on(&self) -> Self::DependantOn;

    // TODO remove `&self`
    fn on_operation(&self, env: &mut Txn, ops: &Vec<Self::InTOp>) -> Result<(), DBError>;
}

pub(crate) struct Memo<V> {
    object: V,
}

impl<V> Memo<V> {
    fn new(object: V) -> Self {
        Memo { object }
    }

    fn value(self) -> V {
        self.object
    }
}

pub(crate) struct Txn<'a> {
    s: &'a Snapshot<'a>,
    batch: WriteBatch,
}

impl<'a> Txn<'a> {

    pub(crate) fn new(s: &'a Snapshot) -> Txn<'a> {
        Txn { s, batch: WriteBatch::default(), }
    }

    fn get_light<O: FromBytes<O>>(&self, cf: &impl AsColumnFamilyRef, position: &[u8]) -> Result<Option<O>, DBError> {
        match self.s.pit.get_cf(cf, position) {
            Ok(bs) => {
                match bs {
                    None => Ok(None),
                    Some(bs) => Ok(Some(O::from_bytes(bs.as_slice())?))
                }
            }
            Err(e) => Err(e.to_string().into())
        }
    }

    fn get<O: FromKVBytes<O>>(&self, cf: &impl AsColumnFamilyRef, position: &[u8]) -> Result<Option<O>, DBError> {
        match self.s.pit.get_cf(cf, position) {
            Ok(bs) => {
                match bs {
                    None => Ok(None),
                    Some(bs) => Ok(Some(O::from_kv_bytes(position, bs.as_slice())?))
                }
            }
            Err(e) => Err(e.to_string().into())
        }
    }

    pub(crate) fn operations<O>(&self, from: Vec<u8>, till: Vec<u8>) -> BetweenIterator<'a,O> {
        self.s.rf.ops_manager.ops_between::<O>(self.s, from, till)
    }

    pub(crate) fn get_operation<BV,BO,TV,TO>(&self, op: &TO) -> Result<Option<BO>, DBError>
    where
        BV: Object<BO>,
        BO: Operation<BV>,
        TV: ObjectInTopology<BV,BO,TO>,
        TO: OperationInTopology<BV,BO,TV>
    {
        self.get_light(&self.s.cf_operations(), op.position().as_slice())
    }

    pub(crate) fn put_operation<BV,BO,TV,TO>(&mut self, op: &TO) -> Result<(), DBError>
    where
        BV: Object<BO>,
        BO: Operation<BV>,
        TV: ObjectInTopology<BV,BO,TO>,
        TO: OperationInTopology<BV,BO,TV>
    {
        let (k,v) = op.to_kv_bytes()?;
        self.batch.put_cf(&self.s.cf_operations(), k.as_slice(), v);
        Ok(())
    }

    pub(crate) fn put_operation_at<V, O: Operation<V> + ToBytes>(&mut self, position: Vec<u8>, op: &O) -> Result<(), DBError> {
        let v = op.to_bytes()?;
        self.batch.put_cf(&self.s.cf_operations(), position.as_slice(), v);
        Ok(())
    }

    pub(crate) fn memos_after<'b,O>(&'b self, position: &Vec<u8>) -> ItemsIterator<'b,O> {
        self.s.rf.ops_manager.memos_after::<O>(self.s, position)
    }

    pub(crate) fn get_memo<O: FromBytes<O>>(&self, position: &Vec<u8>) -> Result<Option<O>, DBError> {
        self.get_light(&self.s.cf_memos(), position.as_slice())
    }

    pub(crate) fn put_value<V: ToKVBytes>(&mut self, v: &V) -> Result<(), DBError> {
        let (k,v) = v.to_kv_bytes()?;
        self.batch.put_cf(&self.s.cf_memos(), k.as_slice(), v.as_slice());
        Ok(())
    }

    pub(crate) fn update_value<V: ToBytes>(&mut self, position: &Vec<u8>, value: &V) -> Result<(), DBError> {
        self.batch.put_cf(&self.s.cf_memos(), position, value.to_bytes()?);
        Ok(())
    }

    pub(crate) fn commit(self) -> Result<(),DBError> {
        self.s.rf.db.write(self.batch)
            .map_err(|e| e.to_string().into())
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

#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub(crate) enum Topology {
    Warehouse(Arc<WarehouseTopology>),
    WarehouseStock(Arc<WarehouseStockTopology>),
}

impl Topology {
    fn create() -> Vec<Topology> {
        todo!()
    }
}

pub(crate) struct Animo {
    topologies: Vec<Topology>,

    // list of node producers that depend on id
    what_to_topologies: HashMap<ID, HashSet<Topology>>,

    op_to_topologies: HashMap<Topology, HashSet<Topology>>
}

impl Animo {
    pub fn register_topology(&mut self, topology: Topology) {
        match &topology {
            Topology::Warehouse(top) => {
                // update helper map for fast resolve of dependants on given mutation
                for id in top.depends_on() {
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

impl Dispatcher for Animo {
    // push propagation of mutations
    fn on_mutation(&self, s: &Snapshot, mutations: &[ChangeTransformation]) -> Result<(), DBError> {
        // calculate node_producers that affected by mutations
        let mut topologies: HashMap<Topology, HashSet<Context>> = HashMap::new();
        for mutation in mutations {
            if let Some(set) = self.what_to_topologies.get(&mutation.what) {
                for item in set {
                    match topologies.get_mut(item) {
                        Some(contexts) => {
                            contexts.insert(mutation.context.clone());
                        },
                        None => {
                            let mut contexts = HashSet::new();
                            contexts.insert(mutation.context.clone());
                            topologies.insert(item.clone(), contexts);
                        }
                    }
                }
            }
        }

        // TODO calculate up-dependant contexts here or at producer?

        let mut tx = Txn::new(s);

        // generate new operations or overwrite existing
        for (topology, contexts) in topologies.into_iter() {
            match topology {
                Topology::Warehouse(top) => {
                    let ops = top.on_mutation(&mut tx, contexts)?;

                    match self.op_to_topologies.get(&Topology::Warehouse(top)) {
                        None => {}
                        Some(set) => {
                            for dependant in set {
                                match dependant {
                                    Topology::Warehouse(_) => {}
                                    Topology::WarehouseStock(top) => {
                                        top.on_operation(&mut tx, &ops)?;
                                    }
                                }
                            }
                        }
                    }
                },
                Topology::WarehouseStock(_) => {}
            }
        }

        Ok(())
    }
}