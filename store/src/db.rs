use std::sync::Arc;

use chrono::{DateTime, Utc};
use rocksdb::DB;

use super::{
  balance::BalanceForGoods,
  elements::{
    first_day_next_month, Balance, CheckpointTopology, OpMutation, OrderedTopology, Report, Store,
  },
  error::WHError,
};
use crate::elements::{Batch, Goods};
use std::collections::HashMap;
use uuid::Uuid;

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
      let balances: Vec<Balance> = if op.is_issue() && op.batch.is_empty() {
        self.get_checkpoints_for_goods(op.store, op.goods, op.date)?
      } else {
        Vec::new()
      };

      let mut new_ops = vec![];

      for ordered_topology in self.ordered_topologies.iter() {
        new_ops = ordered_topology.data_update(op, balances.clone())?;
      }

      println!("NEW_OPS IN FN_RECORD_OPS: {:?}", new_ops);
      if new_ops.is_empty() {
        // println!("OPERATION IN FN_RECORD_OPS: {:?}", op);
        new_ops.push(op.clone());
      }

      for checkpoint_topology in self.checkpoint_topologies.iter() {
        // TODO pass balances.clone() as an argument
        checkpoint_topology.checkpoint_update(new_ops.clone())?;
      }
    }

    Ok(())
  }

  pub fn get_checkpoints_for_goods(
    &self,
    store: Store,
    goods: Goods,
    date: DateTime<Utc>,
  ) -> Result<Vec<Balance>, WHError> {
    for checkpoint_topology in self.checkpoint_topologies.iter() {
      match checkpoint_topology.get_checkpoints_for_one_goods(store, goods, date) {
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

    Err(WHError::new("fn get_report not implemented"))
  }

  pub fn get_balance(
    &self,
    date: DateTime<Utc>,
    goods: &Vec<Goods>
  ) -> Result<HashMap<Uuid, BalanceForGoods>, WHError> {
    let (mut from_date, mut checkpoints)= (Utc::now(), HashMap::new());

    for checkpoint_topology in self.checkpoint_topologies.iter() {
      match checkpoint_topology.get_checkpoints_for_many_goods(date, goods) {
        Ok(res) => { from_date = res.0; checkpoints = res.1 },
        Err(_) => {}, // ignore
      }
    };

    for ordered_topology in self.ordered_topologies.iter() {
      match ordered_topology.get_balances(from_date,date, goods, checkpoints.clone()) {
        Ok(res) => return Ok(res),
        Err(_) => {}, // ignore
      }
    }

    Err(WHError::new("fn get_balance not implemented"))
  }
}
