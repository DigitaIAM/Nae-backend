mod primitives;
mod ops_manager;
mod warehouse;

use std::collections::{HashMap, HashSet};
use std::hash::Hash;
use std::sync::Arc;
use rust_decimal::Decimal;
use crate::error::DBError;
use crate::memory::{ChangeTransformation, Context, ID, Time, Transformation, Value};
use crate::rocksdb::{Dispatcher, Snapshot};

pub use ops_manager::OpsManager;


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

pub(crate) trait Object<V, O> where O: Operation<V> {
    fn apply_delta(&self, delta: &Self) -> Self;

    fn apply(&self, op: &O) -> Self;
}

pub(crate) trait Operation<V> {
    fn delta_after_operation(&self) -> V;
    fn delta_between_operations(&self, other: &Self) -> V;
}

pub(crate) trait OperationGenerator {
    // TODO remove `&self`
    fn depends_on(&self) -> Vec<ID>;

    // TODO remove `&self`
    fn generate_op(&self, env: &mut Env, contexts: HashSet<Context>) -> Result<(), DBError>;
}

pub(crate) struct Env<'a> {
    pub(crate) pit: &'a Snapshot<'a>,
}

impl<'a> Env<'a> {

    pub(crate) fn ops_manager(&mut self) -> Arc<OpsManager> {
        self.pit.rf.ops_manager.clone()
    }

    pub(crate) fn resolve(&self, context: &Context, what: ID) -> Result<Option<Transformation>, DBError> {
        // TODO calculate

        // let what = ID::from(what);

        // read value for give `context` and `what`. In case it's not exist, repeat on "above" context
        let mut memory = self.pit.load_by(context, &what)?;
        if memory != Value::Nothing {
            Ok(Some(Transformation { context: context.clone(), what, into: memory }))
        } else {
            let mut context = context.clone();
            loop {
                match context.0.split_last() {
                    Some((_, ids)) => {
                        context = Context(ids.to_vec());
                        memory = self.pit.load_by(&context, &what)?;
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

pub(crate) struct Animo<T> where
    T: OperationGenerator + Eq + Hash
{
    op_producers: Vec<Arc<T>>,

    // list of node producers that depend on id
    what_to_node_producers: HashMap<ID, HashSet<Arc<T>>>
}

impl<T> Animo<T> where
    T: OperationGenerator + Eq + Hash
{
    pub fn register_op_producer(&mut self, node_producer: Arc<T>)
        where T: OperationGenerator + Eq + Hash
    {
        // update helper map for fast resolve of dependants on given mutation
        for id in node_producer.depends_on() {
            match self.what_to_node_producers.get_mut(&id) {
                None => {
                    let mut set = HashSet::new();
                    set.insert(node_producer.clone());
                    self.what_to_node_producers.insert(id, set);
                }
                Some(v) => {
                    v.insert(node_producer.clone());
                }
            }
        }

        // add to list of op-producers
        self.op_producers.push(node_producer);
    }
}

impl<T> Dispatcher for Animo<T> where
    T: OperationGenerator + Eq + Hash + Sync + Send
{
    // push propagation of mutations
    fn on_mutation(&self, s: &Snapshot, mutations: &[ChangeTransformation]) -> Result<(), DBError> {
        // calculate node_producers that affected by mutations
        let mut producers: HashMap<Arc<T>, HashSet<Context>> = HashMap::new();
        for mutation in mutations {
            if let Some(set) = self.what_to_node_producers.get(&mutation.what) {
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

        let mut env = Env { pit: s };

        // generate new operations or overwrite existing
        for (producer, contexts) in producers.into_iter() {
            producer.generate_op(&mut env, contexts)?;
        }

        Ok(())
    }
}