pub(crate) mod primitives;
pub(crate) mod balance;
pub(crate) mod balance_operation;
pub(crate) mod balance_operations;
pub(crate) mod turnover;
pub(crate) mod base_topology;
pub(crate) mod goods_topology;
pub(crate) mod store_topology;
pub(crate) mod store_aggregation_topology;
pub(crate) mod report_topology;

pub use base_topology::WHTopology;
pub use goods_topology::WHGoodsTopology;
use crate::animo::Time;

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

#[cfg(test)]
pub mod test_util {
    use std::sync::Arc;
    use chrono::DateTime;
    use tempfile::TempDir;
    use crate::animo::memory::{ChangeTransformation, Context, ID, Transformation, Value};
    use crate::{Memory, AnimoDB, Settings};
    use crate::animo::{Animo, Time, Topology};
    use crate::animo::shared::*;
    use crate::warehouse::{WHGoodsTopology, WHTopology};
    use crate::warehouse::store_aggregation_topology::WHStoreAggregationTopology;
    use crate::warehouse::store_topology::WHStoreTopology;
    use crate::warehouse::turnover::{Goods, Store};

    pub(crate) fn init() -> (TempDir, Settings, AnimoDB) {
        std::env::set_var("RUST_LOG", "actix_web=debug,nae_backend=debug");
        let _ = env_logger::builder().is_test(true).try_init();

        let tmp_dir = tempfile::tempdir().unwrap();
        let tmp_path = tmp_dir.path().to_str().unwrap();

        let settings = Settings::test(tmp_path.into());

        let mut db: AnimoDB = Memory::init(tmp_path).unwrap();
        let mut animo = Animo::default();

        let wh_base = Arc::new(WHTopology());
        let wh_store = Arc::new(WHStoreTopology());

        // animo.register_topology(Topology::Warehouse(wh_base.clone()));
        animo.register_topology(Topology::WarehouseStore(wh_store.clone()));
        animo.register_topology(Topology::WarehouseStoreAggregation(Arc::new(WHStoreAggregationTopology(wh_store.clone()))));
        // animo.register_topology(Topology::WarehouseGoods(Arc::new(WHGoodsTopology(wh_base.clone()))));
        db.register_dispatcher(Arc::new(animo)).unwrap();
        (tmp_dir, settings, db)
    }

    fn event(doc: &str, date: &str, class: ID, store: Store, goods: Goods, qty: u32, cost: Option<u32>) -> Vec<ChangeTransformation> {
        let context: Context = vec![doc.into()].into();
        let mut records = vec![
            Transformation::new(&context, *SPECIFIC_OF, class.into()),
            Transformation::new(&context, *DATE, Time::new(date).unwrap().into()),
            Transformation::new(&context, *STORE, store.into()),
            Transformation::new(&context, *GOODS, goods.into()),
            Transformation::new(&context, *QTY, qty.into()),
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
    }

    pub(crate) fn incoming(doc: &str, date: &str, store: Store, goods: Goods, qty: u32, cost: Option<u32>) -> Vec<ChangeTransformation> {
        event(doc, date, *GOODS_RECEIVE, store, goods, qty, cost)
    }

    pub(crate) fn outgoing(doc: &str, date: &str, store: Store, goods: Goods, qty: u32, cost: Option<u32>) -> Vec<ChangeTransformation> {
        event(doc, date, *GOODS_ISSUE, store, goods, qty, cost)
    }

    pub(crate) fn delete(changes: Vec<ChangeTransformation>) -> Vec<ChangeTransformation> {
        changes.iter().map(|t| ChangeTransformation {
            context: t.context.clone(),
            what: t.what.clone(),
            into_before: t.into_after.clone(),
            into_after: Value::Nothing,
        }).collect::<Vec<_>>()
    }
}
