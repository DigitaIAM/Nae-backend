pub mod ops_manager;
pub mod db;
pub mod memory;
pub mod shared;
pub mod error;

use std::collections::{HashMap, HashSet};
use std::fmt::Debug;
use std::hash::Hash;
use std::marker::PhantomData;
use std::slice::Iter;
use std::sync::Arc;
use rocksdb::{AsColumnFamilyRef, WriteBatch};
use error::DBError;

use crate::animo::db::*;
use crate::animo::memory::*;
use crate::animo::ops_manager::*;
use crate::warehouse::{WHStockTopology, WHTopology};

pub(crate) trait Calculation {
    fn depends_on(&self) -> Vec<ID>;
    fn produce(&self) -> ID;
}

// Objects and operations  at topology
pub(crate) trait Object<O>: Sized + Clone + Debug
    + ToBytes + FromBytes<Self>
    + std::ops::Add<Self, Output = Self>
    + std::ops::Sub<Self, Output = Self>
    + std::ops::Neg<Output = Self>
{
    // same as apply operation
    // fn apply_delta(&self, delta: &Self) -> Self;

    fn apply(&self, op: &O) -> Result<Self,DBError>;
}

// TO - operation in topology
// BV - base object
pub(crate) trait ObjectInTopology<BV,BO,TO: OperationInTopology<BV,BO,Self>>: Sized + ToKVBytes + FromKVBytes<Self> {
    fn position(&self) -> Vec<u8>;
    fn value(&self) -> BV;

    fn apply(&self, op: &TO) -> Result<Self,DBError>;
}

pub(crate) trait Operation<V>: Sized + FromBytes<Self> + ToBytes {
    fn delta_between(&self, other: &Self) -> Self;

    fn to_value(&self) -> V;
}

pub(crate) trait Delta<V> {
    fn to_value(&self) -> V;
}

// TV - object in topology
// BV - base object
pub(crate) trait OperationInTopology<BV,BO,TV: ObjectInTopology<BV,BO,Self>>: PositionInTopology + Sized + Debug + FromKVBytes<Self> + ToKVBytes {
    fn resolve(env: &Txn, context: &Context) -> Result<(Option<Self>,Option<Self>), DBError>;

    fn operation(&self) -> BO;

    fn to_value(&self) -> TV;
}

// pub(crate) trait Topology {
//     fn is_operations_topology(&self) -> bool;
//     fn as_operations_topology<V,O>(&self) -> Arc<dyn OperationsTopology<V,O>>;
//
//     fn is_dependent_topology(&self) -> bool;
//     fn as_aggregation_topology<T,O,V,VV>(&self) -> Arc<dyn AggregationTopology<T,O,V,V>>;
// }

pub(crate) struct DeltaOp<
    BV: Object<BO>,
    BO: Operation<BV>,

    TV: ObjectInTopology<BV,BO,TO>,
    TO: OperationInTopology<BV,BO,TV>,
> {
    context: Context,
    pub(crate) before: Option<TO>,
    pub(crate) after: Option<TO>,
    phantom: PhantomData<(BV,BO,TV)>,
}

impl<BV,BO,TV,TO> DeltaOp<BV,BO,TV,TO>
where
    BV: Object<BO> + std::ops::Sub<Output = BV> + std::ops::Neg<Output = BV>,
    BO: Operation<BV>,

    TV: ObjectInTopology<BV,BO,TO>,
    TO: OperationInTopology<BV,BO,TV>,
{
    pub(crate) fn new(context: Context, before: Option<TO>, after: Option<TO>) -> Self {
        DeltaOp { context, before, after, phantom: PhantomData }
    }

    pub(crate) fn delta(&self) -> BV {
        if let Some(before) = self.before.as_ref() {
            if let Some(after) = self.after.as_ref() {
                after.operation().to_value() - before.operation().to_value()
            } else {
                -before.operation().to_value()
            }
        } else if let Some(after) = self.after.as_ref() {
            after.operation().to_value()
        } else {
            unreachable!("internal error")
        }
   }
}

impl<BV,BO,TV,TO> PositionInTopology for DeltaOp<BV,BO,TV,TO>
where
    BV: Object<BO>,
    BO: Operation<BV>,

    TV: ObjectInTopology<BV,BO,TO>,
    TO: OperationInTopology<BV,BO,TV>,
{
    fn prefix(&self) -> &Vec<u8> {
        if let Some(after) = self.after.as_ref() {
            after.prefix()
        } else if let Some(before) = self.before.as_ref() {
            before.prefix()
        } else {
            unreachable!("internal error")
        }
    }

    fn position(&self) -> &Vec<u8> {
        if let Some(after) = self.after.as_ref() {
            after.position()
        } else if let Some(before) = self.before.as_ref() {
            before.position()
        } else {
            unreachable!("internal error")
        }
    }
}

pub(crate) trait OperationsTopology {
    type Obj: Object<Self::Op>;
    type Op: Operation<Self::Obj>;

    type TObj: ObjectInTopology<Self::Obj,Self::Op,Self::TOp>;
    type TOp: OperationInTopology<Self::Obj,Self::Op,Self::TObj>;

    // TODO remove `&self`
    fn depends_on(&self) -> Vec<ID>;

    // TODO remove `&self`
    fn on_mutation(&self, tx: &mut Txn, contexts: HashSet<Context>) -> Result<Vec<DeltaOp<Self::Obj,Self::Op,Self::TObj,Self::TOp>>, DBError>;
}

// Aggregation object
pub(crate) trait AObject<O>: Sized + ToBytes + FromBytes<Self> {
    fn is_zero(&self) -> bool;

    fn apply_aggregation(&self, op: &O) -> Result<Self,DBError>;
}

// Aggregation operation
pub(crate) trait AOperation<TV>: Sized + ToBytes + FromBytes<Self> {
    fn to_value(&self) -> TV;
}

// Aggregation object in topology
pub(crate) trait AObjectInTopology<BV,BO,TO: AOperationInTopology<BV,BO,Self>>: Sized + ToKVBytes + FromKVBytes<Self> {
    fn position(&self) -> Vec<u8>;
    fn value(&self) -> &BV;

    fn apply(&self, op: &TO) -> Result<Self,DBError>;
}

// Aggregation operation in topology
pub(crate) trait AOperationInTopology<BV,BO,TV: AObjectInTopology<BV,BO,Self>>: Sized + PositionInTopology {

    fn position_of_aggregation(&self) -> Result<Vec<u8>, DBError>;

    fn operation(&self) -> BO;

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
    fn depends_on(&self) -> Arc<Self::DependantOn>;

    // TODO remove `&self`
    fn on_operation(&self, env: &mut Txn, ops: &Vec<DeltaOp<Self::InObj,Self::InOp,Self::InTObj,Self::InTOp>>) -> Result<(), DBError>;
}

pub(crate) struct Memo<V> {
    object: V,
}

impl<V> Memo<V> {
    pub fn new(object: V) -> Self {
        Memo { object }
    }

    pub fn value(self) -> V {
        self.object
    }
}

pub(crate) struct MemoOfList<V> {
    list: Vec<Memo<V>>,
}

impl<V> MemoOfList<V> {
    pub fn new(list: Vec<Memo<V>>) -> Self {
        MemoOfList { list }
    }

    pub fn iter(&self) -> Iter<'_, Memo<V>> {
        self.list.iter()
    }

    pub fn len(&self) -> usize {
        self.list.len()
    }

    pub fn get(&self, index: usize) -> Option<&Memo<V>> {
        self.list.get(index)
    }
}

pub(crate) struct Txn<'a> {
    pub(crate) s: &'a Snapshot<'a>,
    batch: WriteBatch,
    changes: Option<HashMap<&'a Context, HashMap<&'a ID, &'a ChangeTransformation>>>
}

impl<'a> Txn<'a> {

    pub(crate) fn new(s: &'a Snapshot) -> Txn<'a> {
        Txn { s, batch: WriteBatch::default(), changes: None}
    }

    pub(crate) fn new_with(s: &'a Snapshot, mutations: &'a[ChangeTransformation]) -> Txn<'a> {
        let mut changes = HashMap::with_capacity(mutations.len());

        for change in mutations {
            if !changes.contains_key(&change.context) {
                changes.insert(&change.context, HashMap::new());
            }
            let map = changes.get_mut(&change.context).unwrap();

            map.insert(&change.what, change);
        }

        Txn { s, batch: WriteBatch::default(), changes: Some(changes) }
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

    fn get_heavy<O: FromKVBytes<O>>(&self, cf: &impl AsColumnFamilyRef, position: &[u8]) -> Result<Option<O>, DBError> {
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

    pub(crate) fn operations<'b,O,PIT>(&'b self, from: &'b PIT, till: &'b PIT) -> BetweenLightIterator<'b,O>
    where
        PIT: PositionInTopology
    {
        self.s.rf.ops_manager.ops_between_light(self.s, from, till)
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

        log::debug!("write op {:?} at {:?}", op, k);

        self.batch.put_cf(&self.s.cf_operations(), k.as_slice(), v);
        Ok(())
    }

    pub(crate) fn del_operation<BV,BO,TV,TO>(&mut self, op: &TO) -> Result<(), DBError>
        where
            BV: Object<BO>,
            BO: Operation<BV>,
            TV: ObjectInTopology<BV,BO,TO>,
            TO: OperationInTopology<BV,BO,TV>
    {
        let (k,_) = op.to_kv_bytes()?;

        log::debug!("delete op {:?} at {:?}", op, k);

        self.batch.delete_cf(&self.s.cf_operations(), k.as_slice());
        Ok(())
    }

    pub(crate) fn put_operation_at<V, O: Operation<V> + ToBytes>(&mut self, position: Vec<u8>, op: &O) -> Result<(), DBError> {
        let v = op.to_bytes()?;
        self.batch.put_cf(&self.s.cf_operations(), position.as_slice(), v);
        Ok(())
    }

    pub(crate) fn values<O: FromKVBytes<O>>(&self, from: Vec<u8>, till:  Vec<u8>) -> BetweenHeavyIterator<'a,O> {
        self.s.rf.ops_manager.values_between_heavy::<O>(self.s, from, till)
    }

    pub(crate) fn value<O: FromBytes<O>>(&self, position: &Vec<u8>) -> Result<Option<O>, DBError> {
        self.get_light(&self.s.cf_values(), position.as_slice())
    }

    pub(crate) fn put_value<V: ToKVBytes>(&mut self, v: &V) -> Result<(), DBError> {
        let (k,v) = v.to_kv_bytes()?;

        log::debug!("put value {:?} {:?}", k, v);

        self.batch.put_cf(&self.s.cf_values(), k.as_slice(), v.as_slice());
        Ok(())
    }

    pub(crate) fn update_value<V: ToBytes + Debug>(&mut self, position: &Vec<u8>, value: &V) -> Result<(), DBError> {

        log::debug!("update value {:?} {:?}", value, position);

        self.batch.put_cf(&self.s.cf_values(), position, value.to_bytes()?);
        Ok(())
    }

    pub(crate) fn delete_value(&mut self, position: &Vec<u8>) -> Result<(), DBError> {
        log::debug!("delete value {:?}", position);
        self.batch.delete_cf(&self.s.cf_values(), position);
        Ok(())
    }

    pub(crate) fn commit(self) -> Result<(),DBError> {
        log::debug!("commit");
        self.s.rf.db.write(self.batch)
            .map_err(|e| e.to_string().into())
    }

    pub(crate) fn ops_manager(&mut self) -> Arc<OpsManager> {
        self.s.rf.ops_manager.clone()
    }

    fn load_by(&self, context: &Context, what: &ID) -> Result<Option<ChangeTransformation>, DBError> {
        if let Some(changes) = self.changes.as_ref() {
            if let Some(map) = changes.get(context) {
                if let Some(tr) = map.get(what) {
                    return Ok(Some((**tr).clone()));
                }
            }
        }

        let memory = self.s.load_by(context, &what)?;
        if memory != Value::Nothing {
            return Ok(Some(ChangeTransformation {
                context: context.clone(),
                what: what.clone(),
                into_before: memory.clone(),
                into_after: memory
            }));
        }
        Ok(None)
    }

    pub(crate) fn resolve(&self, context: &Context, what: ID) -> Result<Option<ChangeTransformation>, DBError> {
        // TODO calculate

        // read value for give `context` and `what`. In case it's not exist, repeat on "above" context
        if let Some(tr) = self.load_by(context, &what)? {
            Ok(Some(tr))
        } else {
            let mut context = context.clone();
            loop {
                match context.0.split_last() {
                    Some((_, ids)) => {
                        context = Context(ids.to_vec());

                        if let Some(tr) = self.load_by(&context, &what)? {
                            break Ok(Some(tr))
                        }
                    }
                    None => break Ok(None),
                }
            }
        }
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub(crate) enum Topology {
    Warehouse(Arc<WHTopology>),
    WarehouseStock(Arc<WHStockTopology>),
}

#[derive(Default)]
pub(crate) struct Animo {
    topologies: Vec<Topology>,

    // HashMap<ID, ...>

    // Vec<impl OperationInTopology>
    // Vec<impl AggregationObjectInTopology>

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
            Topology::WarehouseStock(top) => {
                // TODO fix it
                let mut set = HashSet::new();
                set.insert(Topology::WarehouseStock(top.clone()));
                self.op_to_topologies.insert(self.topologies[0].clone(), set);
            }
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

        let mut tx = Txn::new_with(s, mutations);

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

        tx.commit()?;

        Ok(())
    }
}
