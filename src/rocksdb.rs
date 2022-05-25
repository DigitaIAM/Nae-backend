use std::sync::Arc;
use rocksdb::{DB, Options, WriteBatch};
use crate::error::DBError;
use crate::Memory;
use crate::memory::{Change, ID, IDS, Record, Value};

const CF_CORE: &str = "cf_core";

#[derive(Clone)]
pub struct RocksDB {
    db: Arc<DB>,
}

impl Memory for RocksDB {
    fn init(path: &str) -> Result<Self, DBError> {
        let mut options = Options::default();
        options.set_error_if_exists(false);
        options.create_if_missing(true);
        options.create_missing_column_families(true);

        // list existing ColumnFamilies
        let cfs = DB::list_cf(&options, path).unwrap_or(vec![]);
        let cf_exist = cfs.iter().find(|cf| cf == &CF_CORE).is_none();

        // open DB
        let mut db = DB::open_cf(&options, path, cfs).unwrap();

        // create ColumnFamilies if not exist
        if cf_exist {
            let options = Options::default();
            db.create_cf(CF_CORE, &options).unwrap();
        }

        Ok(RocksDB { db: Arc::new(db) })
    }

    fn modify(&self, mutations: Vec<Change>) -> Result<(), DBError> {
        let cf = self.db.cf_handle(CF_CORE).unwrap();

        let mut batch = WriteBatch::default();
        for change in mutations {
            let k = ID::bytes(&change.primary, &change.relation);
            // TODO let b = change.before.to_bytes()?;
            let v = change.after.to_bytes()?;

            debug!("put {:?} = {:?}", k, v);

            batch.put_cf(cf, k, v);
        }

        self.db.write(batch)
            .map_err(|e| e.to_string().into())
    }

    fn query(&self, keys: Vec<IDS>) -> Result<Vec<Record>, DBError> {
        let cf = self.db.cf_handle(CF_CORE).unwrap();

        let mut result = Vec::with_capacity(keys.len());

        let pit = self.db.snapshot();
        for key in keys {
            let k = key.to_bytes();
            let v = pit.get_cf(cf, &k)?;

            debug!("get {:?} = {:?}", k, v);

            let value = Value::from_bytes(v)?;

            result.push(Record { key, value });
        }

        Ok(result)
    }
}