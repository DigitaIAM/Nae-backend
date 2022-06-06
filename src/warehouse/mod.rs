pub mod primitives;
pub(crate) mod balance;
pub(crate) mod balance_operation;
pub(crate) mod balance_operations;
pub(crate) mod base_topology;
pub(crate) mod stock_topology;

pub use base_topology::WarehouseTopology;
pub use stock_topology::WarehouseStockTopology;
use crate::memory::Time;

pub(crate) fn time_to_u64(time: Time) -> u64 {
    time.timestamp().try_into().unwrap()
}

pub(crate) fn ts_to_bytes(ts: u64) -> [u8; 8] {
    ts.to_be_bytes()
}

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
    use crate::memory::{ChangeTransformation, Context, ID, Time, Transformation, Value};
    use crate::{Memory, RocksDB};
    use crate::animo::{Animo, Topology};
    use crate::shared::*;
    use crate::warehouse::{WarehouseStockTopology, WarehouseTopology};

    pub fn init() -> RocksDB {
        std::env::set_var("RUST_LOG", "actix_web=debug,nae_backend=debug");
        let _ = env_logger::builder().is_test(true).try_init();

        let tmp_dir = tempfile::tempdir().unwrap();
        let tmp_path = tmp_dir.path().to_str().unwrap();

        let mut db: RocksDB = Memory::init(tmp_path).unwrap();
        let mut animo = Animo::default();

        let wh_topology = Arc::new(WarehouseTopology());

        animo.register_topology(Topology::Warehouse(wh_topology.clone()));
        animo.register_topology(Topology::WarehouseStock(Arc::new(WarehouseStockTopology(wh_topology.clone()))));
        db.register_dispatcher(Arc::new(animo)).unwrap();
        db
    }

    pub fn time(dt: &str) -> Time {
        DateTime::parse_from_rfc3339(format!("{}T00:00:00Z", dt).as_str()).unwrap().into()
    }

    pub fn time_end(dt: &str) -> Time {
        DateTime::parse_from_rfc3339(format!("{}T23:59:59Z", dt).as_str()).unwrap().into()
    }

    fn event(doc: &str, date: &str, class: ID, store: ID, goods: ID, qty: u32, cost: Option<u32>) -> Vec<ChangeTransformation> {
        let context: Context = vec![doc.into()].into();
        let mut records = vec![
            Transformation::new(&context, *SPECIFIC_OF, class.into()),
            Transformation::new(&context, *DATE, time(date).into()),
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

    pub fn incoming(doc: &str, date: &str, store: ID, goods: ID, qty: u32, cost: Option<u32>) -> Vec<ChangeTransformation> {
        event(doc, date, *GOODS_RECEIVE, store, goods, qty, cost)
    }

    pub fn outgoing(doc: &str, date: &str, store: ID, goods: ID, qty: u32, cost: Option<u32>) -> Vec<ChangeTransformation> {
        event(doc, date, *GOODS_ISSUE, store, goods, qty, cost)
    }

    pub fn delete(changes: Vec<ChangeTransformation>) -> Vec<ChangeTransformation> {
        changes.iter().map(|t| ChangeTransformation {
            context: t.context.clone(),
            what: t.what.clone(),
            into_before: t.into_after.clone(),
            into_after: Value::Nothing,
        }).collect::<Vec<_>>()
    }
}
