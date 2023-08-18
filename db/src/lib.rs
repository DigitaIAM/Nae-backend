use rocksdb::AsColumnFamilyRef;
use rocksdb::Direction;
use rocksdb::IteratorMode;
use rocksdb::DB;

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
