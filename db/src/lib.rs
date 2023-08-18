use rocksdb::{AsColumnFamilyRef, BoundColumnFamily, Direction, Error, IteratorMode, DB};
use std::cmp::Ordering;
use std::sync::Arc;

pub trait PrefixIterator {
  fn prefix(
    &self,
    cf_handle: &impl AsColumnFamilyRef,
    prefix: Vec<u8>,
  ) -> Result<Vec<(Box<[u8]>, Box<[u8]>)>, rocksdb::Error>;
}

impl PrefixIterator for DB {
  fn prefix(
    &self,
    cf_handle: &impl AsColumnFamilyRef,
    prefix: Vec<u8>,
  ) -> Result<Vec<(Box<[u8]>, Box<[u8]>)>, rocksdb::Error> {
    let mut result = vec![];

    let iter = self.iterator_cf(cf_handle, IteratorMode::From(&prefix, Direction::Forward));
    for res in iter {
      let (k, v) = res?;

      if prefix[..] == k[0..prefix.len()] {
        result.push((k, v))
      } else {
        break;
      }
    }

    Ok(result)
  }
}

pub trait RangeIterator {
  fn lookup(
    &self,
    db: &Arc<DB>,
    cf: &Arc<BoundColumnFamily>,
  ) -> Result<Vec<(Vec<u8>, Vec<u8>)>, Error>;
}

impl RangeIterator for std::ops::Range<Vec<u8>> {
  fn lookup(
    &self,
    db: &Arc<DB>,
    cf: &Arc<BoundColumnFamily>,
  ) -> Result<Vec<(Vec<u8>, Vec<u8>)>, Error> {
    let mut result = vec![];

    let from = &self.start;
    let till = &self.end;

    for r in db.iterator_cf(cf, IteratorMode::From(from, Direction::Forward)) {
      let (k, v) = r?;
      if k.iter().as_slice().cmp(till) >= Ordering::Equal {
        break;
      }
      result.push((k.to_vec(), v.to_vec()))
    }

    Ok(result)
  }
}

impl RangeIterator for std::ops::RangeInclusive<Vec<u8>> {
  fn lookup(
    &self,
    db: &Arc<DB>,
    cf: &Arc<BoundColumnFamily>,
  ) -> Result<Vec<(Vec<u8>, Vec<u8>)>, Error> {
    let mut result = vec![];

    let from = self.start();
    let till = self.end();

    for r in db.iterator_cf(cf, IteratorMode::From(from, Direction::Forward)) {
      let (k, v) = r?;
      if k.iter().as_slice().cmp(till) == Ordering::Greater {
        break;
      }
      result.push((k.to_vec(), v.to_vec()))
    }

    Ok(result)
  }
}

#[actix_web::test]
async fn test_prefix() {
  use rocksdb::{
    ColumnFamilyDescriptor, Error, IteratorMode, Options, ReadOptions, SliceTransform, DB,
  };
  use std::cmp::Ordering;
  use std::path::Path;

  pub fn open_db<P: AsRef<Path>>(location: P) -> Result<DB, Error> {
    let prefix_extractor = SliceTransform::create_fixed_prefix(1);
    let mut options = Options::default();
    options.create_if_missing(true);
    options.set_prefix_extractor(prefix_extractor);

    let cf_list = match DB::list_cf(&options, &location) {
      Ok(list) => list,
      Err(_) => Vec::new(),
    };
    let cf_descriptors = cf_list.into_iter().map(|name| {
      let prefix_extractor = SliceTransform::create_fixed_prefix(1);
      let mut cf_opts = Options::default();
      cf_opts.set_prefix_extractor(prefix_extractor);
      ColumnFamilyDescriptor::new(name, cf_opts)
    });

    DB::open_cf_descriptors(&options, &location, cf_descriptors)
  }

  fn init(db: &mut DB) {
    let prefix_extractor = SliceTransform::create_fixed_prefix(1);
    let mut options = Options::default();
    options.create_if_missing(true);
    options.set_prefix_extractor(prefix_extractor);

    let _ = db.create_cf("cf1", &options);
    let _ = db.create_cf("cf2", &options);
    let _ = db.create_cf("cf3", &options);

    let _ = db.put_cf(&db.cf_handle("cf1").unwrap(), b"11", b"a1");
    let _ = db.put_cf(&db.cf_handle("cf1").unwrap(), b"21", b"b1");
    let _ = db.put_cf(&db.cf_handle("cf1").unwrap(), b"31", b"c1");

    let _ = db.put_cf(&db.cf_handle("cf2").unwrap(), b"11", b"a2");
    let _ = db.put_cf(&db.cf_handle("cf2").unwrap(), b"21", b"b2");
    let _ = db.put_cf(&db.cf_handle("cf2").unwrap(), b"31", b"c2");

    let _ = db.put_cf(&db.cf_handle("cf3").unwrap(), b"11", b"a3");
    let _ = db.put_cf(&db.cf_handle("cf3").unwrap(), b"21", b"b3");
    let _ = db.put_cf(&db.cf_handle("cf3").unwrap(), b"31", b"c3");
  }

  const DB_PATH: &str = "/tmp/db_path";

  let new_db = !Path::new(DB_PATH).exists();
  let mut db = open_db(DB_PATH).unwrap();

  if new_db {
    init(&mut db);
  }

  for r in db.prefix_iterator_cf(&db.cf_handle("cf1").unwrap(), b"11") {
    let (k, v) = r.unwrap();
    println!("{:?} - {:?}", k, v);
  }

  println!("---------");

  for r in db.prefix_iterator_cf(&db.cf_handle("cf2").unwrap(), b"21") {
    let (k, v) = r.unwrap();
    println!("{:?} - {:?}", k, v);
  }

  println!("---------");

  let from = b"11".to_vec();
  let till = b"21".to_vec();

  for r in
    db.iterator_cf(&db.cf_handle("cf3").unwrap(), IteratorMode::From(&from, Direction::Forward))
  {
    let (k, v) = r.unwrap();
    if k.iter().as_slice().cmp(&till) == Ordering::Greater {
      break;
    }
    println!("{:?} - {:?}", k, v);
  }
}
