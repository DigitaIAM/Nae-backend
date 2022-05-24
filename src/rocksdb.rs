use std::sync::Arc;
use rocksdb::{DB, Options};
use crate::error::Error;
use crate::Memory;
use crate::memory::{Change, ID, IDS, Record, Value};

const CF_CORE: &str = "cf_core";

#[derive(Clone)]
pub struct RocksDB {
    db: Arc<DB>,
}

impl Memory for RocksDB {
    fn init(path: &str) -> Result<Self, Error> {
        let mut options = Options::default();
        options.set_error_if_exists(false);
        options.create_if_missing(true);
        options.create_missing_column_families(true);

        // list existing ColumnFamilies
        let cfs = rocksdb::DB::list_cf(&options, path).unwrap_or(vec![]);
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

    fn modify(&self, mutations: Vec<Change>) -> Result<(), Error> {
        let cf = self.db.cf_handle(CF_CORE).unwrap();

        for change in mutations {
            let k = ID::bytes(&change.primary, &change.relation);
            let v = change.after.to_bytes()?;

            debug!("put {:?} = {:?}", k, v);

            self.db.put_cf(cf, k, v)?;
        }
        Ok(())
    }

    fn query(&self, keys: Vec<IDS>) -> Result<Vec<Record>, Error> {
        let cf = self.db.cf_handle(CF_CORE).unwrap();

        let mut result = Vec::with_capacity(keys.len());

        for key in keys {
            let k = key.to_bytes();
            let v = self.db.get_cf(cf, &k)?;

            debug!("get {:?} = {:?}", k, v);

            let value = Value::from_bytes(v)?;

            result.push(Record { key, value });
        }

        Ok(result)
    }
}