use rocksdb::{DBIteratorWithThreadMode, DBWithThreadMode, Direction, IteratorMode, ReadOptions, SingleThreaded, WriteBatch};
use rust_decimal::Decimal;
use crate::error::DBError;
use crate::{Memory, memory};
use crate::memory::{ChangeTransformation, ID, ID_BYTES, IDS, Value};
use crate::rocksdb::Snapshot;

use serde::{Deserialize, Serialize};

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

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Qty(Decimal); // TODO UOM,

impl<'a, 'b> std::ops::Add<&'b Qty> for &'a Qty {
    type Output = Qty;

    fn add(self, other: &'b Qty) -> Qty {
        Qty(self.0 + other.0)
    }
}

impl<'a, 'b> std::ops::Sub<&'b Qty> for &'a Qty {
    type Output = Qty;

    fn sub(self, other: &'b Qty) -> Qty {
        Qty(self.0 - other.0)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Cost(Decimal); // TODO Currency,

impl<'a, 'b> std::ops::Add<&'b Cost> for &'a Cost {
    type Output = Cost;

    fn add(self, other: &'b Cost) -> Cost {
        Cost(self.0 + other.0)
    }
}

impl<'a, 'b> std::ops::Sub<&'b Cost> for &'b Cost {
    type Output = Cost;

    fn sub(self, other: &'b Cost) -> Cost {
        Cost(self.0 - other.0)
    }
}

#[derive(Debug, Serialize, Deserialize)]
enum Op {
    In(Qty, Cost),
    Out(Qty, Cost),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Balance(Qty, Cost);

impl Balance {
    fn apply(&self, op: &Op) -> Self {
        let (qty, cost) = match op {
            Op::In(qty, cost) => (&self.0 + qty, &self.1 + cost),
            Op::Out(qty, cost) => (&self.0 - qty, &self.1 - cost),
        };

        Balance(qty, cost)
    }
}

#[derive(Debug, Serialize, Deserialize)]
struct Record {
    op: Op,
    balance: Balance,
    context: Vec<ID>,
}

impl Record {
    pub(crate) fn zero() -> Self {
        Record {
            context: vec![],
            op: Op::In(Qty(0.into()), Cost(0.into())),
            balance: Balance(Qty(0.into()), Cost(0.into())),
        }
    }

    pub(crate) fn to_bytes(&self) -> Result<Vec<u8>, DBError> {
        // bincode::serialize(self)
        //     .map_err(|_| "fail to encode record".into())
        serde_json::to_string(self)
            .and_then(|s| Ok(s.as_bytes().to_vec()))
            .map_err(|_| "fail to encode record".into())
    }

    pub(crate) fn from_bytes(bs: Box<[u8]>) -> Result<Self, DBError> {
        // bincode::deserialize(&bs)
        //     .map_err(|_| "fail to decode record".into())
        serde_json::from_slice(&bs)
            .map_err(|_| "fail to decode record".into())
    }
}

struct RecordsIterator<'a>(DBIteratorWithThreadMode<'a, DBWithThreadMode<SingleThreaded>>);

impl<'a> Iterator for RecordsIterator<'a> {
    type Item = (Vec<u8>, Record);

    fn next(&mut self) -> Option<(Vec<u8>, Record)> {
        match self.0.next() {
            None => None,
            Some((k, v)) => {
                let record = Record::from_bytes(v).unwrap();
                Some((k.to_vec(), record))
            }
        }
    }
}

// ledger
#[derive(Default)]
pub(crate) struct OrderedCalculator {
}

impl OrderedCalculator {

    fn key(store: &ID, goods: &ID, time: &memory::Time, context: &IDS) -> Vec<u8> {
        let mut bs = Vec::with_capacity(16 + ID_BYTES * (3));

        bs.extend_from_slice(store.as_slice());
        bs.extend_from_slice(goods.as_slice());

        bs.extend_from_slice(time.timestamp().to_ne_bytes().as_slice());

        // TODO use hash of IDS to guaranty uniqueness
        bs.extend_from_slice(context.0[0].as_slice());

        bs
    }

    fn store(&self, s: &Snapshot, w: &mut WriteBatch, key: &Vec<u8>, record: &Record) {
        let value = record.to_bytes().unwrap();

        debug!("store {:?} = {:?}", key, value);

        w.put_cf(s.cf_animo(), key, value)
    }

    fn preceding<'a>(&self, s: &'a Snapshot, key: &Vec<u8>) -> RecordsIterator<'a> {
        let it = s.pit.iterator_cf_opt(
            s.cf_animo(),
            ReadOptions::default(),
            IteratorMode::From(key.as_slice(), Direction::Reverse)
        );

        RecordsIterator(it)
    }

    fn following<'a>(&self, s: &'a Snapshot, key: &Vec<u8>) -> RecordsIterator<'a> {
        let it = s.pit.iterator_cf_opt(
            s.cf_animo(),
            ReadOptions::default(),
            IteratorMode::From(key.as_slice(), Direction::Forward)
        );

        RecordsIterator(it)
    }

    fn from_core(&self, s: &Snapshot, context: &IDS, what: &str) -> Result<Value, DBError> {
        let k = ID::bytes(context, &ID::from(what));
        let v = s.pit.get_cf(s.cf_core(), &k)?;

        debug!("get {:?} = {:?}", k, v);

        let value = Value::from_bytes(v)?;

        Ok(value)
    }

    pub fn on_mutation(&self, s: &Snapshot, mut mutations: Vec<ChangeTransformation>) -> Result<(), DBError> {
        // TODO find contexts of mutations
        let change = mutations.remove(0);

        // GoodsReceive, GoodsIssue

        // docA specific-of GoodsReceive
        let instance_of = self.from_core(s, &change.context, "specific-of")?;
        if instance_of.one_of(vec!["GoodsReceive".into()]) {
            let store = self.from_core(s, &change.context, "store").expect("store").as_id()?;
            let goods = self.from_core(s, &change.context, "goods").expect("goods").as_id()?;
            let date = self.from_core(s, &change.context, "date").expect("date").as_time()?;

            let qty = self.from_core(s, &change.context, "qty").expect("qty").as_number()?;
            let cost = self.from_core(s, &change.context, "cost").expect("cost").as_number()?;

            // TODO evaluate op base on context data
            let op = Op::In(Qty(qty), Cost(cost));

            let key = OrderedCalculator::key(&store, &goods, &date, &change.context);

            println!("key {:?}", key);

            let mut batch = WriteBatch::default();

            // calculation balance for event
            let (_, previous_record) = self.preceding(s, &key)
                .next()
                .unwrap_or((vec![], Record::zero()));

            println!("previous {:?}", previous_record);

            let mut balance = previous_record.balance.apply(&op);

            // store event
            let record = Record {
                context: change.context.clone().to_vec(),
                op,
                balance
            };
            self.store(s, &mut batch, &key, &record);

            balance = record.balance;

            println!("balance {:?}", balance);

            // update following records balance
            let mut it = self.following(s, &key);
            while let Some((k, mut record)) = it.next() {
                // skip current record
                if k == key {
                    continue;
                }

                let new_balance = balance.apply(&record.op);

                record.balance = new_balance.clone();
                balance = new_balance;

                println!("balance {:?}", balance);

                self.store(s, &mut batch, &k, &record);
            }

            let wr: Result<(), DBError> = s.rf.db.write(batch)
                .map_err(|e| e.to_string().into());
            wr?;
        }
        Ok(())
    }
}

// trait AggregationView {
// }
//
// trait Calc {
//     fn eval(&self, db: RocksDB, input_context: IDS, output_context: IDS) -> Result<Transformation, DBError>;
// }
//
// struct Multiply {
//     input: Vec<ID>,
//     output: ID,
// }
//
// impl Calc for Multiply {
//     fn eval(&self, db: RocksDB, input_context: IDS, output_context: IDS) -> Result<Transformation, DBError> {
//         let input_keys = self.input.iter()
//             .map(|what| TransformationKey { context: input_context.clone(), what: what.clone() })
//             .collect::<Vec<TransformationKey>>();
//
//         let mut records = db.query(input_keys)?;
//
//         let multiplier = records.remove(0).into.as_number()?;
//         let multiplicand = records.remove(0).into.as_number()?;
//
//         let product = multiplier * multiplicand;
//
//         Ok(Transformation {
//             context: output_context,
//             what: self.output.clone(),
//             into: Value::Number(product)
//         })
//     }
// }
//
// struct Animo {
//     functions: Vec<(IDS, Box<dyn Calc>)>,
// }

#[cfg(test)]
mod tests {
    use std::thread;
    use std::time::Duration;
    use super::*;
    use crate::memory::{Transformation, Value};
    use crate::RocksDB;


    // #[test]
    // fn test_keys() {
    //     let context = IDS(vec![]);
    //     let store = "store".into();
    //     let goods = "goods".into();
    //
    //     let t1 = chrono::Utc::now();
    //     thread::sleep(Duration::from_millis(1000));
    //     let t2 = chrono::Utc::now();
    //
    //     let k1 = OrderedCalculator::key(&store, &goods, &t1, &context);
    //     let k2 = OrderedCalculator::key(&store, &goods, &t2, &context);
    // }

    // let cost = Multiply {
    //     input: vec!["qty".into(), "price".into()],
    //     output: "cost".into()
    // };

    #[test]
    fn test_animo() {
        std::env::set_var("RUST_LOG", "actix_web=debug,nae_backend=debug");
        env_logger::init();

        let tmp_dir = tempfile::tempdir().unwrap();
        let tmp_path = tmp_dir.path().to_str().unwrap();
        let db: RocksDB = Memory::init(tmp_path).unwrap();

        let event = |doc: &str, goods: &str, qty: i32, cost: i32| {
            vec![
                Transformation {
                    context: vec![doc.into()].into(),
                    what: "specific-of".into(),
                    into: Value::ID("GoodsReceive".into()),
                },
                Transformation {
                    context: vec![doc.into()].into(),
                    what: "date".into(),
                    into: Value::DateTime(chrono::Utc::now()),
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
                },
                Transformation {
                    context: vec![doc.into()].into(),
                    what: "cost".into(),
                    into: Value::Number(cost.into()),
                }
            ].iter().map(|t| ChangeTransformation {
                context: t.context.clone(),
                what: t.what.clone(),
                into_before: Value::Nothing,
                into_after: t.into.clone()
            }).collect::<Vec<_>>()
        };

        db.modify(event("docA", "g1", 10, 50)).expect("Ok");
        thread::sleep(Duration::from_millis(1000));
        db.modify(event("docB", "g1", 3, 15)).expect("Ok");
    }
}