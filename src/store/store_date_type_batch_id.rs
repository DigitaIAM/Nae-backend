use std::{str::FromStr, sync::Arc};

use super::{
  balance::BalanceForGoods, Db, InternalOperation, KeyValueStore, Op, OpMutation, OrderedTopology,
  Store, WHError,
};
use crate::store::{first_day_current_month, new_get_aggregations, Balance, Report};
use chrono::{DateTime, Utc};
use json::array;
use rocksdb::{BoundColumnFamily, ColumnFamilyDescriptor, IteratorMode, Options, ReadOptions, DB};
use rust_decimal::Decimal;

const CF_NAME: &str = "cf_store_date_type_batch_id";

pub struct StoreDateTypeBatchId {
  pub db: Arc<DB>,
}

impl StoreDateTypeBatchId {
  pub fn cf_name() -> &'static str {
    CF_NAME
  }

  fn cf(&self) -> Result<Arc<BoundColumnFamily>, WHError> {
    if let Some(cf) = self.db.cf_handle(StoreDateTypeBatchId::cf_name()) {
      Ok(cf)
    } else {
      Err(WHError::new("can't get CF"))
    }
  }
}

impl OrderedTopology for StoreDateTypeBatchId {
  fn put(&self, op: &Op, balance: &BalanceForGoods) -> Result<(), WHError> {
    Ok(self.db.put_cf(&self.cf()?, self.key(op), self.to_bytes(op, balance))?)
  }

  fn get(&self, op: &Op) -> Result<Option<(Op, BalanceForGoods)>, WHError> {
    if let Some(bytes) = self.db.get_cf(&self.cf()?, self.key(&op))? {
      Ok(Some(self.from_bytes(&bytes)?))
    } else {
      Ok(None)
    }
  }

  fn del(&self, op: &Op) -> Result<(), WHError> {
    Ok(self.db.delete_cf(&self.cf()?, self.key(op))?)
  }

  fn mutate_op(&self, op_mut: &OpMutation) -> Result<(), WHError> {
    let mut ops: Vec<Op> = vec![];
    ops.push(op_mut.to_op());

    while ops.len() > 0 {
      let op = ops.remove(0);

      // calculate balance
      let before_balance: BalanceForGoods = self.balance_before(&op)?;
      let (calculated_op, new_balance) = self.evaluate(&before_balance, &op);

      let current_balance =
        if let Some((o, b)) = self.get(&op)? { b } else { BalanceForGoods::default() };

      println!("before_balance {before_balance:?}");
      println!("calculated_op {calculated_op:?}");
      println!("current_balance {current_balance:?}");
      println!("new_balance {new_balance:?}");

      // store update op with balance or delete
      if calculated_op.is_zero() {
        self.db.delete_cf(&self.cf()?, self.key(&calculated_op))?;
      } else {
        self.db.put_cf(
          &self.cf()?,
          self.key(&calculated_op),
          self.to_bytes(&calculated_op, &new_balance),
        )?;
      }

      // if next op have dependant add it to ops
      if let Some(dep) = calculated_op.dependent() {
        ops.push(dep);
      }

      // propagate delta
      if !current_balance.delta(&new_balance).is_zero() {
        let mut before_balance = new_balance;
        for (next_operation, next_current_balance) in self.operations_after(&calculated_op)? {
          let (calculated_op, new_balance) = self.evaluate(&before_balance, &next_operation);

          if calculated_op.is_zero() {
            self.db.delete_cf(&self.cf()?, self.key(&calculated_op))?;
          } else {
            self.db.put_cf(
              &self.cf()?,
              self.key(&calculated_op),
              self.to_bytes(&calculated_op, &new_balance),
            )?;
          }

          if !next_current_balance.delta(&new_balance).is_zero() {
            break;
          }

          before_balance = new_balance;

          // if next op have dependant add it to ops
          if let Some(dep) = calculated_op.dependent() {
            ops.push(dep);
          }
        }
      }
    }
    Ok(())
  }

  fn create_cf(&self, opts: Options) -> ColumnFamilyDescriptor {
    ColumnFamilyDescriptor::new(StoreDateTypeBatchId::cf_name(), opts)
  }

  fn get_ops(
    &self,
    storage: Store,
    from_date: DateTime<Utc>,
    till_date: DateTime<Utc>,
  ) -> Result<Vec<Op>, WHError> {
    let from_date = from_date.timestamp() as u64;
    let from: Vec<u8> = storage
      .as_bytes()
      .iter()
      .chain(from_date.to_be_bytes().iter())
      .chain(0_u8.to_be_bytes().iter())
      .map(|b| *b)
      .collect();

    let till_date = till_date.timestamp() as u64;
    let till = storage
      .as_bytes()
      .iter()
      .chain(till_date.to_be_bytes().iter())
      .chain(u8::MAX.to_be_bytes().iter())
      .map(|b| *b)
      .collect();

    let mut options = ReadOptions::default();
    options.set_iterate_range(from..till);

    let iter = self.db.iterator_cf_opt(&self.cf()?, options, IteratorMode::Start);

    let mut res = Vec::new();
    for item in iter {
      let (_, v) = item?;
      let (op, _) = self.from_bytes(&v)?;
      res.push(op);
    }

    Ok(res)
  }

  fn get_report(
    &self,
    db: &Db,
    storage: Store,
    from_date: DateTime<Utc>,
    till_date: DateTime<Utc>,
  ) -> Result<Report, WHError> {
    println!("STORE_DATE_TYPE_BATCH.get_report");

    let balances = db.get_checkpoints_before_date(storage, from_date)?;

    let ops = self.get_ops(storage, first_day_current_month(from_date), till_date)?;

    let items = new_get_aggregations(balances, ops, from_date);

    Ok(Report { from_date, till_date, items })
    // Err(WHError::new("test"))
  }

  fn data_update(&self, op: &OpMutation) -> Result<(), WHError> {
    if op.before.is_none() {
      if let Ok(None) = self.db.get_cf(&self.cf()?, self.key(&op.to_op())) {
        self.mutate_op(op)
      } else {
        return Err(WHError::new("Wrong 'before' state, expected something"));
      }
    } else {
      if let Ok(Some(bytes)) = self.db.get_cf(&self.cf()?, self.key(&op.to_op())) {
        let (o, balance) = self.from_bytes(&bytes)?;
        if Some(o.op) == op.after {
          self.mutate_op(op)
        } else {
          return Err(WHError::new("Wrong 'before' state in operation"));
        }
      } else {
        return Err(WHError::new("There is no such operation in db"));
      }
    }
  }

  fn key(&self, op: &Op) -> Vec<u8> {
    let ts = op.date.timestamp() as u64;

    let op_type = match op.op {
      InternalOperation::Receive(..) => 1_u8,
      InternalOperation::Issue(..) => 2_u8,
    };

    op.store
      .as_bytes()
      .iter()
      .chain(ts.to_be_bytes().iter())
      .chain(op_type.to_be_bytes().iter())
      .chain(op.batch().iter())
      .chain(op.id.as_bytes().iter())
      .map(|b| *b)
      .collect()
  }

  fn balance_before(&self, op: &Op) -> Result<BalanceForGoods, WHError> {
    let mut iter = self
      .db
      .iterator_cf(&self.cf()?, IteratorMode::From(&self.key(op), rocksdb::Direction::Reverse));

    if let Some(bytes) = iter.next() {
      let value: String = match bytes {
        Ok((_, v)) => serde_json::from_slice(&v)?,
        Err(_) => return Ok(BalanceForGoods::default()),
      };
      let js = array![value];
      if let (Some(q), Some(c)) = (js[0]["qty"].as_str(), js[0]["cost"].as_str()) {
        Ok(BalanceForGoods { qty: Decimal::from_str(q)?, cost: Decimal::from_str(c)? })
      } else {
        Ok(BalanceForGoods::default())
      }
    } else {
      Ok(BalanceForGoods::default())
    }
  }

  fn operations_after(&self, calculated_op: &Op) -> Result<Vec<(Op, BalanceForGoods)>, WHError> {
    let mut res = Vec::new();

    // TODO change iterator with range from..till?
    let mut iter = self.db.iterator_cf(
      &self.cf()?,
      IteratorMode::From(&self.key(calculated_op), rocksdb::Direction::Forward),
    );

    while let Some(bytes) = iter.next() {
      if let Ok((k, v)) = bytes {
        let (op, balance) = self.from_bytes(&v)?;

        if op.batch == calculated_op.batch {
          res.push((op, balance));
        }
      }
    }

    Ok(res)
  }
}
