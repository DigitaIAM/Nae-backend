use std::sync::Arc;

use chrono::{DateTime, Utc};
use rocksdb::DB;

use super::{
  balance::BalanceForGoods,
  elements::{first_day_next_month, Balance, CheckpointTopology, OpMutation,
   OrderedTopology, Store, Report},
  error::WHError,
};

#[derive(Clone)]
pub struct Db {
  pub db: Arc<DB>,
  pub checkpoint_topologies: Arc<Vec<Box<dyn CheckpointTopology + Sync + Send>>>,
  pub ordered_topologies: Arc<Vec<Box<dyn OrderedTopology + Sync + Send>>>,
}

impl Db {
  pub fn put(&self, key: &Vec<u8>, value: &String) -> Result<(), WHError> {
    match self.db.put(key, value) {
      Ok(_) => Ok(()),
      Err(_) => Err(WHError::new("Can't put into a database")),
    }
  }

  fn get(&self, key: &Vec<u8>) -> Result<String, WHError> {
    match self.db.get(key) {
      Ok(Some(res)) => Ok(String::from_utf8(res)?),
      Ok(None) => Err(WHError::new("Can't get from database - no such value")),
      Err(_) => Err(WHError::new("Something wrong with getting from database")),
    }
  }

  fn find_checkpoint(&self, op: &OpMutation, name: &str) -> Result<Option<Balance>, WHError> {
    let bal = Balance {
      date: first_day_next_month(op.date),
      store: op.store,
      goods: op.goods,
      batch: op.batch.clone(),
      number: BalanceForGoods::default(),
    };

    if let Some(cf) = self.db.cf_handle(name) {
      if let Ok(Some(v1)) = self.db.get_cf(&cf, bal.key(name)?) {
        let b = serde_json::from_slice(&v1)?;
        Ok(b)
      } else {
        Ok(None)
      }
    } else {
      Err(WHError::new("Can't get cf from db in fn find_checkpoint"))
    }
  }

  pub fn record_ops(&self, ops: &Vec<OpMutation>) -> Result<(), WHError> {
    for op in ops {
      for checkpoint_topology in self.checkpoint_topologies.iter() {
        checkpoint_topology.checkpoint_update(op)?;
      }

      for ordered_topology in self.ordered_topologies.iter() {
        ordered_topology.data_update(op);
      }
    }

    Ok(())
  }

  pub fn get_checkpoints_before_date(
    &self,
    store: Store,
    date: DateTime<Utc>,
  ) -> Result<Vec<Balance>, WHError> {
    for checkpoint_topology in self.checkpoint_topologies.iter() {
      match checkpoint_topology.get_checkpoints_before_date(store, date) {
        Ok(result) => return Ok(result),
        Err(e) => {
          if e.message() == "Not supported".to_string() {
            continue;
          } else {
            return Err(e);
          }
        },
      }
    }
    Err(WHError::new("can't get checkpoint before date"))
  }

  pub fn get_report(
    &self,
    storage: Store,
    from_date: DateTime<Utc>,
    till_date: DateTime<Utc>,
  ) -> Result<Report, WHError> {
    for ordered_topology in self.ordered_topologies.iter() {
      match ordered_topology.get_report(&self, storage, from_date, till_date) {
        Ok(report) => return Ok(report),
        Err(_) => {}, // ignore
      }
    }

    Err(WHError::new("not implemented"))
  }
}
