use std::sync::{Arc, Mutex};
use rocksdb::{BoundColumnFamily, DB, DBWithThreadMode, MultiThreaded, Options, SnapshotWithThreadMode, WriteBatch};
use crate::error::DBError;
use crate::Memory;
use crate::animo::OpsManager;
use crate::memory::{ChangeTransformation, Context, ID, Transformation, TransformationKey, Value};

const CF_CORE: &str = "cf_core";
const CF_OPERATIONS: &str = "cf_operations";
const CF_VALUES: &str = "cf_memos";

pub(crate) trait ToBytes {
    fn to_bytes(&self) -> Result<Vec<u8>, DBError>;
}

pub(crate) trait FromBytes<V> {
    fn from_bytes(bs: &[u8]) -> Result<V, DBError>;
}

pub(crate) trait ToKVBytes {
    fn to_kv_bytes(&self) -> Result<(Vec<u8>,Vec<u8>), DBError>;
}

pub(crate) trait FromKVBytes<V> {
    fn from_kv_bytes(key: &[u8], value: &[u8]) -> Result<V, DBError>;
}

#[derive(Clone)]
pub struct RocksDB {
    pub(crate) db: Arc<DB>,
    pub(crate) dispatchers: Arc<Mutex<Vec<Arc<dyn Dispatcher>>>>,
    pub(crate) ops_manager: Arc<OpsManager>,
}

impl RocksDB {
    pub(crate) fn register_dispatcher(&mut self, dispatcher: Arc<dyn Dispatcher>) -> Result<(), DBError> {
        match self.dispatchers.lock() {
            Ok(mut v) => {
                v.push(dispatcher);
                Ok(())
            }
            Err(e) => Err(DBError::from(e.to_string()))
        }
    }
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

    pub fn cf_values(&self) -> Arc<BoundColumnFamily> {
        self.rf.db.cf_handle(CF_VALUES).expect("values cf")
    }

    pub(crate) fn load_by(&self, context: &Context, what: &ID) -> Result<Value, DBError> {
        let k = ID::bytes(context, what);
        let v = self.pit.get_cf(&self.cf_core(), &k)?;

        let value = match v {
            None => Value::Nothing,
            Some(bs) => Value::from_bytes(bs.as_slice())?
        };

        debug!("load {:?} by {:?}", value, k);

        Ok(value)
    }
}

pub trait Dispatcher: Sync + Send {
    fn on_mutation(&self, s: &Snapshot, mutations: &[ChangeTransformation]) -> Result<(), DBError>;
}

impl Memory for RocksDB {
    fn init(path: &str) -> Result<Self, DBError> {
        let mut options = Options::default();
        options.set_error_if_exists(false);
        options.create_if_missing(true);
        options.create_missing_column_families(true);

        // list existing ColumnFamilies
        let cfs = DB::list_cf(&options, path).unwrap_or_default();

        // open DB
        let mut db = DB::open_cf(&options, path, cfs.clone()).unwrap();

        let create_cf = |db: &mut DB, cfs: &Vec<String>, cf_name: &str| {
            if !cfs.iter().any(|cf| cf == cf_name) {
                let options = Options::default();
                db.create_cf(cf_name, &options).unwrap();
            }
        };

        // create ColumnFamilies if not exist
        create_cf(&mut db, &cfs, CF_CORE);
        create_cf(&mut db, &cfs, CF_OPERATIONS);
        create_cf(&mut db, &cfs, CF_VALUES);

        let rf = Arc::new(db);
        Ok(RocksDB {
            db: rf.clone(),
            dispatchers: Arc::new(Mutex::new(vec![])),
            ops_manager: Arc::new(OpsManager()),
        })
    }

    fn modify(&self, mutations: Vec<ChangeTransformation>) -> Result<(), DBError> {
        let cf = self.db.cf_handle(CF_CORE).unwrap();

        // write to core storage
        {
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
        }

        // TODO require snapshot with modification
        let s = self.snapshot();

        // TODO how to handle error?
        {
            let dispatchers = self.dispatchers.lock()
                .map_err(|e| DBError::from(e.to_string()))?;
            for dispatcher in dispatchers.iter() {
                dispatcher.on_mutation(&s, &mutations)?;
            }
        }

        Ok(())
    }

    fn query(&self, keys: Vec<TransformationKey>) -> Result<Vec<Transformation>, DBError> {
        let s = self.snapshot();

        let mut result = Vec::with_capacity(keys.len());
        for key in keys {
            let value = s.load_by(&key.context, &key.what)?;

            result.push(Transformation { context: key.context, what: key.what, into: value });
        }

        Ok(result)
    }
}