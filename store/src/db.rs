use std::sync::Arc;

use chrono::{DateTime, NaiveDateTime, Utc};
use rocksdb::DB;

use super::{
  balance::BalanceForGoods,
  elements::{Report, Store},
  error::WHError,
};
use crate::balance::Balance;
use crate::batch::Batch;
use crate::checkpoints::CheckpointTopology;
use crate::elements::Goods;
use crate::operations::{Op, OpMutation};
use crate::ordered_topology::OrderedTopology;
use json::JsonValue;
use log::debug;
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

  pub fn record_ops(&self, ops: &Vec<OpMutation>) -> Result<(), WHError> {
    for op in ops {
      let mut changes = self.ordered_topologies[0].data_update(self, op)?;

      for ordered_topology in self.ordered_topologies.iter().skip(1) {
        for (op, balance) in changes.iter() {
          if let Some(after) = op.to_op_after() {
            ordered_topology.put(&after, balance)?;
          } else if let Some(before) = op.to_op_before() {
            ordered_topology.del(&before)?;
          }
        }
      }

      println!("NEW_OPS IN FN_RECORD_OPS: {:#?}", changes);
      // if new_ops.is_empty() {
      //   // println!("OPERATION IN FN_RECORD_OPS: {:?}", op);
      //   new_ops.push(op.clone());
      // }

      for checkpoint_topology in self.checkpoint_topologies.iter() {
        // TODO pass balances.clone() as an argument
        for (op, balance) in changes.iter() {
          checkpoint_topology.checkpoint_update(op)?;
        }
      }
    }

    Ok(())
  }

  pub fn balances_for_store_goods_before_operation(
    &self,
    operation: &Op,
  ) -> Result<HashMap<Batch, BalanceForGoods>, WHError> {
    // balances at closest checkpoint
    let (from, mut balances) = self.closest_checkpoint_balances_for_store_goods(operation)?;

    log::debug!("closest_checkpoint_balances_for_store_goods: {balances:?}");

    // apply operation between from and till
    for op in self.operations_for_store_goods(from, operation)? {
      let bal = balances.entry(op.batch.clone()).or_default();
      *bal += &op.op;
    }

    // remove zero balances
    let balances = balances.into_iter().filter(|(k, v)| !v.is_zero()).collect();

    // log::debug!("balances_for_store_goods_before_operation: {balances:?}");

    Ok(balances)
  }

  fn closest_checkpoint_balances_for_store_goods(
    &self,
    op: &Op,
  ) -> Result<(DateTime<Utc>, HashMap<Batch, BalanceForGoods>), WHError> {
    for checkpoint_topology in self.checkpoint_topologies.iter() {
      match checkpoint_topology.balances_for_store_goods(op.date, op.store, op.goods) {
        Ok(result) => return Ok(result),
        Err(_) => {},
      }
    }
    Err(WHError::new("unimplemented"))
  }

  fn operations_for_store_goods(&self, from: DateTime<Utc>, till: &Op) -> Result<Vec<Op>, WHError> {
    for ordered_topology in self.ordered_topologies.iter() {
      match ordered_topology.operations_for_store_goods(from, till) {
        Ok(ops) => return Ok(ops),
        Err(_) => {}, // ignore
      }
    }
    Err(WHError::new("fn operations_for_store_goods not implemented"))
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

  pub fn ops_for_store_goods_and_batch(
    &self,
    store: Store,
    goods: Goods,
    batch: &Batch,
    from_date: DateTime<Utc>,
    till_date: DateTime<Utc>,
  ) -> Result<Vec<Op>, WHError> {
    for ordered_topology in self.ordered_topologies.iter() {
      match ordered_topology.ops_for_store_goods_and_batch(store, goods, batch, from_date, till_date)
      {
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

  pub fn get_checkpoint_for_goods_and_batch(
    &self,
    store: Store,
    goods: Goods,
    batch: &Batch,
    date: DateTime<Utc>,
  ) -> Result<Option<Balance>, WHError> {
    for checkpoint_topology in self.checkpoint_topologies.iter() {
      match checkpoint_topology.get_checkpoint_for_goods_and_batch(store, goods, batch, date) {
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

  pub fn get_checkpoints_for_one_storage_before_date(
    &self,
    store: Store,
    date: DateTime<Utc>,
  ) -> Result<Vec<Balance>, WHError> {
    for checkpoint_topology in self.checkpoint_topologies.iter() {
      match checkpoint_topology.get_checkpoints_for_one_storage_before_date(store, date) {
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

  pub fn get_report_for_goods(
    &self,
    storage: Store,
    goods: Goods,
    batch: &Batch,
    from_date: DateTime<Utc>,
    till_date: DateTime<Utc>,
  ) -> Result<JsonValue, WHError> {
    for ordered_topology in self.ordered_topologies.iter() {
      match ordered_topology.get_report_for_goods(&self, storage, goods, batch, from_date, till_date)
      {
        Ok(report) => return Ok(report),
        Err(_) => {}, // ignore
      }
    }

    Err(WHError::new("fn get_report not implemented"))
  }

  pub fn get_report_for_storage(
    &self,
    storage: Store,
    from_date: DateTime<Utc>,
    till_date: DateTime<Utc>,
  ) -> Result<Report, WHError> {
    for ordered_topology in self.ordered_topologies.iter() {
      match ordered_topology.get_report_for_storage(&self, storage, from_date, till_date) {
        Ok(report) => return Ok(report),
        Err(_) => {}, // ignore
      }
    }

    Err(WHError::new("fn get_report not implemented"))
  }

  pub fn get_balance(
    &self,
    date: DateTime<Utc>,
    goods: &Vec<Goods>,
  ) -> Result<HashMap<Uuid, BalanceForGoods>, WHError> {
    let mut it = self.checkpoint_topologies.iter();
    let (from_date, checkpoints) = loop {
      if let Some(checkpoint_topology) = it.next() {
        match checkpoint_topology.get_checkpoints_for_many_goods(date, goods) {
          Ok(result) => {
            break result;
          },
          Err(e) => {
            // ignore only "not implemented"
            println!("{e:?}");
          },
        }
      } else {
        break (
          DateTime::<Utc>::from_utc(NaiveDateTime::from_timestamp_millis(0).unwrap(), Utc),
          HashMap::new(),
        );
      }
    };

    for ordered_topology in self.ordered_topologies.iter() {
      match ordered_topology.get_balances(from_date, date, goods, checkpoints.clone()) {
        Ok(res) => return Ok(res),
        Err(_) => {}, // ignore
      }
    }

    Err(WHError::new("fn get_balance not implemented"))
  }

  pub fn get_balance_for_one_goods_and_store(
    &self,
    date: DateTime<Utc>,
    storage: &Store,
    goods: &Goods,
  ) -> Result<HashMap<Uuid, BalanceForGoods>, WHError> {
    let mut it = self.checkpoint_topologies.iter();
    let (from_date, checkpoints) = loop {
      if let Some(checkpoint_topology) = it.next() {
        match checkpoint_topology.get_checkpoints_for_one_goods_with_date(
          storage.clone(),
          goods.clone(),
          date,
        ) {
          Ok(result) => {
            break result;
          },
          Err(e) => {
            // ignore only "not implemented"
            println!("{e:?}");
          },
        }
      } else {
        break (
          DateTime::<Utc>::from_utc(NaiveDateTime::from_timestamp_millis(0).unwrap(), Utc),
          HashMap::new(),
        );
      }
    };

    for ordered_topology in self.ordered_topologies.iter() {
      match ordered_topology.get_balances_for_one_goods_and_store(
        from_date,
        date,
        storage,
        goods,
        checkpoints.clone(),
      ) {
        Ok(res) => return Ok(res),
        Err(_) => {}, // ignore
      }
    }

    Err(WHError::new("fn get_balance not implemented"))
  }

  pub fn get_balance_for_all(
    &self,
    date: DateTime<Utc>,
  ) -> Result<HashMap<Store, HashMap<Goods, HashMap<Batch, BalanceForGoods>>>, WHError> {
    let mut it = self.checkpoint_topologies.iter();
    let (from_date, checkpoints) = loop {
      if let Some(checkpoint_topology) = it.next() {
        match checkpoint_topology.get_checkpoints_for_all(date) {
          Ok(result) => {
            break result;
          },
          Err(e) => {
            // ignore only "not implemented"
            println!("{e:?}");
          },
        }
      } else {
        break (
          DateTime::<Utc>::from_utc(NaiveDateTime::from_timestamp_millis(0).unwrap(), Utc),
          HashMap::new(),
        );
      }
    };

    println!("CHECKPOINTS: {checkpoints:?}");

    for ordered_topology in self.ordered_topologies.iter() {
      match ordered_topology.get_balances_for_all(from_date, date, checkpoints.clone()) {
        Ok(res) => return Ok(res),
        Err(_) => {}, // ignore
      }
    }

    Err(WHError::new("fn get_balance not implemented"))
  }
}
