use std::sync::Arc;
use rocksdb::{ColumnFamily, DB,  DBWithThreadMode, Options, SingleThreaded, SnapshotWithThreadMode, WriteBatch};
use crate::error::DBError;
use crate::Memory;
use crate::animo::OrderedCalculator;
use crate::memory::{ChangeTransformation, ID, Transformation, TransformationKey, Value};

const CF_CORE: &str = "cf_core";
pub(crate) const CF_ANIMO: &str = "cf_animo";

#[derive(Clone)]
pub struct RocksDB {
    pub(crate) db: Arc<DB>,
    dispatcher: Arc<OrderedCalculator> // TODO Arc<Vec<Box<dyn Dispatcher>>>
}

pub struct Snapshot<'a> {
    pub rf: &'a RocksDB,
    pub pit: SnapshotWithThreadMode<'a, DBWithThreadMode<SingleThreaded>>,
    // pub mutations: Vec<ChangeTransformation>,
}

impl<'a> Snapshot<'a> {
    pub fn cf_core(&self) -> &ColumnFamily {
        self.rf.db.cf_handle(CF_CORE).unwrap()
    }

    pub fn cf_animo(&self) -> &ColumnFamily {
        self.rf.db.cf_handle(CF_ANIMO).unwrap()
    }
}

trait Dispatcher {
    fn on_mutations(&self, pit: RocksDB, mutations: Vec<ChangeTransformation>);
}

impl Memory for RocksDB {
    fn init(path: &str) -> Result<Self, DBError> {
        let mut options = Options::default();
        options.set_error_if_exists(false);
        options.create_if_missing(true);
        options.create_missing_column_families(true);

        // list existing ColumnFamilies
        let cfs = DB::list_cf(&options, path).unwrap_or(vec![]);

        // open DB
        let mut db = DB::open_cf(&options, path, cfs.clone()).unwrap();

        let create_cf = |db: &mut DB, cfs: &Vec<String>, cf_name: &str| {
            if cfs.iter().find(|cf| cf == &cf_name).is_none() {
                let options = Options::default();
                db.create_cf(cf_name, &options).unwrap();
            }
        };

        // create ColumnFamilies if not exist
        create_cf(&mut db, &cfs, CF_CORE);
        create_cf(&mut db, &cfs, CF_ANIMO);

        Ok(RocksDB {
            db: Arc::new(db),
            dispatcher: Arc::new(OrderedCalculator::default()),
        })
    }

    fn modify(&self, mutations: Vec<ChangeTransformation>) -> Result<(), DBError> {
        let cf = self.db.cf_handle(CF_CORE).unwrap();

        let mut batch = WriteBatch::default();
        for change in &mutations {
            let k = ID::bytes(&change.context, &change.what);
            // TODO let b = change.into_before.to_bytes()?;
            let v = change.into_after.to_bytes()?;

            debug!("put {:?} = {:?}", k, v);

            batch.put_cf(cf, k, v);
        }

        let wr: Result<(), DBError> = self.db.write(batch)
            .map_err(|e| e.to_string().into());
        wr?;

        // TODO require snapshot with modification
        let pit = self.db.snapshot();
        let s = Snapshot {
            rf: self,
            pit
        };

        // TODO how to handle error?
        self.dispatcher.on_mutation(&s, mutations)?;

        Ok(())
    }

    fn query(&self, keys: Vec<TransformationKey>) -> Result<Vec<Transformation>, DBError> {
        let cf = self.db.cf_handle(CF_CORE).unwrap();

        let mut result = Vec::with_capacity(keys.len());

        let pit = self.db.snapshot();
        for key in keys {
            let k = key.to_bytes();
            let v = pit.get_cf(cf, &k)?;

            debug!("get {:?} = {:?}", k, v);

            let value = Value::from_bytes(v)?;

            result.push(Transformation { context: key.context, what: key.what, into: value });
        }

        Ok(result)
    }
}