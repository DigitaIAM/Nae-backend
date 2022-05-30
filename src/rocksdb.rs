use std::sync::Arc;
use rocksdb::{BoundColumnFamily, DB, DBWithThreadMode, MultiThreaded, Options, SnapshotWithThreadMode, WriteBatch};
use crate::error::DBError;
use crate::Memory;
use crate::animo::OpsManager;
use crate::memory::{ChangeTransformation, Context, ID, Transformation, TransformationKey, Value};

const CF_CORE: &str = "cf_core";
const CF_OPERATIONS: &str = "cf_operations";
const CF_MEMOS: &str = "cf_memos";

pub(crate) trait ToBytes {
    fn to_bytes(&self) -> Result<Vec<u8>, DBError>;
}

pub(crate) trait FromBytes<V> {
    fn from_bytes(bs: &[u8]) -> Result<V, DBError>;
}

#[derive(Clone)]
pub struct RocksDB {
    pub(crate) db: Arc<DB>,
    pub(crate) dispatchers: Vec<Arc<dyn Dispatcher>>,
    pub(crate) ops_manager: Arc<OpsManager>,
}

impl RocksDB {
    pub(crate) fn snapshot(&self) -> Snapshot {
        let pit = self.db.snapshot();
        Snapshot { rf: self, pit }
    }
}

pub struct Snapshot<'a> {
    pub(crate) rf: &'a RocksDB,
    pub(crate) pit: SnapshotWithThreadMode<'a, DBWithThreadMode<MultiThreaded>>,
    // pub mutations: Vec<ChangeTransformation>,
}

impl<'a> Snapshot<'a> {
    pub fn cf_core(&self) -> Arc<BoundColumnFamily> {
        self.rf.db.cf_handle(CF_CORE).expect("core cf")
    }

    pub fn cf_operations(&self) -> Arc<BoundColumnFamily> {
        self.rf.db.cf_handle(CF_OPERATIONS).expect("operations cf")
    }

    pub fn cf_memos(&self) -> Arc<BoundColumnFamily> {
        self.rf.db.cf_handle(CF_MEMOS).expect("memos cf")
    }

    pub fn load_by(&self, context: &Context, what: &str) -> Result<Value, DBError> {
        let k = ID::bytes(context, &ID::from(what));
        let v = self.pit.get_cf(&self.cf_core(), &k)?;

        let value = Value::from_bytes(v)?;

        debug!("load {:?} by {:?}", value, k);

        Ok(value)
    }
}

pub trait Dispatcher {
    fn on_mutation(&self, s: &Snapshot, mutations: &Vec<ChangeTransformation>) -> Result<(), DBError>;
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
        create_cf(&mut db, &cfs, CF_OPERATIONS);
        create_cf(&mut db, &cfs, CF_MEMOS);

        let rf = Arc::new(db);

        Ok(RocksDB {
            db: rf.clone(),
            dispatchers: vec![],
            ops_manager: Arc::new(OpsManager { db: rf.clone() }),
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

            batch.put_cf(&cf, k, v);
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
        for dispatcher in &self.dispatchers {
            dispatcher.on_mutation(&s, &mutations)?;
        }

        Ok(())
    }

    fn query(&self, keys: Vec<TransformationKey>) -> Result<Vec<Transformation>, DBError> {
        let cf = self.db.cf_handle(CF_CORE).unwrap();

        let mut result = Vec::with_capacity(keys.len());

        let pit = self.db.snapshot();
        for key in keys {
            let k = key.to_bytes();
            let v = pit.get_cf(&cf, &k)?;

            debug!("get {:?} = {:?}", k, v);

            let value = Value::from_bytes(v)?;

            result.push(Transformation { context: key.context, what: key.what, into: value });
        }

        Ok(result)
    }
}