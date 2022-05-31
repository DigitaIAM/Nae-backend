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
    fn depends_on(&self) -> Vec<ID>;

    fn generate_op(&self, env: &mut Env, contexts: HashSet<Context>) -> Result<(), DBError>;
}

pub(crate) struct Env<'a> {
    pub(crate) pit: &'a Snapshot<'a>,
}

impl<'a> Env<'a> {

    pub(crate) fn ops_manager(&mut self) -> Arc<OpsManager> {
        self.pit.rf.ops_manager.clone()
    }

    pub(crate) fn resolve(&self, context: &Context, what: &str) -> Result<Option<Transformation>, DBError> {
        // TODO calculate

        let what = ID::from(what);

        // read value for give `context` and `what`. In case it's not exist, repeat on "above" context
        let mut memory = self.pit.load_by(context, &what)?;
        if memory != Value::Nothing {
            Ok(Some(Transformation { context: context.clone(), what: what.into(), into: memory }))
        } else {
            let mut context = context.clone();
            loop {
                match context.0.split_last() {
                    Some((_, ids)) => {
                        context = Context(ids.to_vec());
                        memory = self.pit.load_by(&context, &what)?;
                        if memory != Value::Nothing {
                            break Ok(Some(Transformation { context, what: what.into(), into: memory }))
                        }
                    }
                    None => break Ok(None),
                }
            }
        }
    }

    pub(crate) fn resolve_as_id(&self, context: &Context, what: &str) -> Result<ID, DBError> {
        let id = self.resolve(context, what)?
            .expect(format!("{} is not exist", what).as_str())
            .into.as_id()
            .expect(format!("{} is not ID", what).as_str());
        Ok(id)
    }

    pub(crate) fn resolve_as_time(&self, context: &Context, what: &str) -> Result<Time, DBError> {
        let time = self.resolve(context, what)?
            .expect(format!("{} is not exist", what).as_str())
            .into.as_time()
            .expect(format!("{} is not Time", what).as_str());
        Ok(time)
    }

    pub(crate) fn resolve_as_number(&self, context: &Context, what: &str) -> Result<Decimal, DBError> {
        let number = self.resolve(context, what)?
            .expect(format!("{} is not exist", what).as_str())
            .into.as_number()
            .expect(format!("{} is not Number", what).as_str());
        Ok(number)
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
    pub fn register_op_producer(&mut self, node_producer: Arc<T>) {
        // update helper map for fast resolve of dependants on given mutation
        for id in  node_producer.depends_on() {
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
    fn on_mutation(&self, s: &Snapshot, mutations: &Vec<ChangeTransformation>) -> Result<(), DBError> {
        // calculate node_producers that affected by mutations
        let mut producers: HashMap<Arc<T>, HashSet<Context>> = HashMap::new();
        for mutation in mutations {
            match self.what_to_node_producers.get(&mutation.what) {
                Some(set) => {
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
                        };
                    }
                }
                _ => {},
            };
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

#[cfg(test)]
mod tests {
    use std::cmp::Ordering;
    use chrono::DateTime;
    use crate::{Memory, RocksDB};
    use crate::animo::primitives::{Money, Qty};
    use crate::animo::warehouse::Balance;
    use super::*;


    #[test]
    fn test_bytes_order() {
        let mut bs1 = 0_u64.to_ne_bytes();
        for num in 1_u64..u64::MAX {
            if num % 1_000_000 == 0 {
                print!(".");
            }
            let bs2 = num.to_be_bytes();
            assert_eq!(Ordering::Less, bs1.as_slice().cmp(bs2.as_slice()));
            bs1 = bs2;
        }
    }

    #[test]
    fn test_store_operations() {
        std::env::set_var("RUST_LOG", "actix_web=debug,nae_backend=debug");
        env_logger::init();

        let tmp_dir = tempfile::tempdir().unwrap();
        let tmp_path = tmp_dir.path().to_str().unwrap();
        let mut db: RocksDB = Memory::init(tmp_path).unwrap();
        let mut animo = Animo {
            op_producers: vec![],
            what_to_node_producers: HashMap::new(),
        };
        animo.register_op_producer(Arc::new(Balance::default()));
        db.register_dispatcher(Arc::new(animo)).unwrap();

        let time = |dt: &str| -> Time {
            DateTime::parse_from_rfc3339(format!("{}T00:00:00Z", dt).as_str()).unwrap().into()
        };

        let event = |doc: &str, date: &str, class: &str, goods: &str, qty: i32, cost: Option<i32>| {
            let mut records = vec![
                Transformation {
                    context: vec![doc.into()].into(),
                    what: "specific-of".into(),
                    into: Value::ID(class.into()),
                },
                Transformation {
                    context: vec![doc.into()].into(),
                    what: "date".into(),
                    into: Value::DateTime(time(date)),
                },
                Transformation {
                    context: vec![doc.into()].into(),
                    what: "store".into(),
                    into: Value::ID("wh1".into()),
                },
                Transformation {
                    context: vec![doc.into()].into(),
                    what: "goods".into(),
                    into: Value::ID(goods.into()),
                },
                Transformation {
                    context: vec![doc.into()].into(),
                    what: "qty".into(),
                    into: Value::Number(qty.into()),
                }
            ];
            if let Some(cost) = cost {
                records.push(
                    Transformation {
                        context: vec![doc.into()].into(),
                        what: "cost".into(),
                        into: Value::Number(cost.into()),
                    }
                );
            }
            records.iter().map(|t| ChangeTransformation {
                context: t.context.clone(),
                what: t.what.clone(),
                into_before: Value::Nothing,
                into_after: t.into.clone()
            }).collect::<Vec<_>>()
        };

        db.modify(event("docA", "2022-05-27", "GoodsReceive", "g1", 10, Some(50))).expect("Ok");
        db.modify(event("docB", "2022-05-30", "GoodsReceive", "g1", 2, Some(10))).expect("Ok");
        db.modify(event("docC", "2022-05-28", "GoodsIssue", "g1", 5, Some(25))).expect("Ok");

        // 2022-05-27	qty	10	cost	50	=	10	50
        // 2022-05-28	qty	-5	cost	-25	=	5	25		< 2022-05-28
        // 2022-05-30	qty	2	cost	10	=	7 	35
        // 													< 2022-05-31

        debug!("READING 2022-05-31");
        let s = db.snapshot();
        let g1_balance = Balance::get_memo(&s, &"wh1".into(), &"g1".into(), &time("2022-05-31")).expect("Ok");
        assert_eq!(Balance(Qty(7.into()),Money(35.into())), g1_balance);

        debug!("READING 2022-05-28");
        let s = db.snapshot();
        let g1_balance = Balance::get_memo(&s, &"wh1".into(), &"g1".into(), &time("2022-05-28")).expect("Ok");
        assert_eq!(Balance(Qty(5.into()),Money(25.into())), g1_balance);

        debug!("READING 2022-05-31");
        let s = db.snapshot();
        let g1_balance = Balance::get_memo(&s, &"wh1".into(), &"g1".into(), &time("2022-05-31")).expect("Ok");
        assert_eq!(Balance(Qty(7.into()),Money(35.into())), g1_balance);
    }
}