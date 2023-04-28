use crate::balance::{Balance, BalanceForGoods};
use crate::batch::Batch;
use crate::elements::{first_day_next_month, Goods, Store, WHError};
use crate::operations::{Op, OpMutation};
use chrono::{DateTime, Utc};
use std::collections::HashMap;
use uuid::Uuid;

pub trait CheckpointTopology {
  fn key(&self, op: &Op, date_of_checkpoint: DateTime<Utc>) -> Vec<u8>;
  fn key_checkpoint(&self, balance: &Balance, date_of_checkpoint: DateTime<Utc>) -> Vec<u8>;

  fn get_balance(&self, key: &Vec<u8>) -> Result<BalanceForGoods, WHError>;
  fn set_balance(&self, key: &Vec<u8>, balance: BalanceForGoods) -> Result<(), WHError>;
  fn del_balance(&self, key: &Vec<u8>) -> Result<(), WHError>;
  fn key_latest_checkpoint_date(&self) -> Vec<u8>;
  fn get_latest_checkpoint_date(&self) -> Result<DateTime<Utc>, WHError>;
  fn set_latest_checkpoint_date(&self, date: DateTime<Utc>) -> Result<(), WHError>;
  fn get_checkpoints_for_one_goods(
    &self,
    store: Store,
    goods: Goods,
    date: DateTime<Utc>,
  ) -> Result<Vec<Balance>, WHError>;

  fn get_checkpoint_for_goods_and_batch(
    &self,
    store: Store,
    goods: Goods,
    batch: &Batch,
    date: DateTime<Utc>,
  ) -> Result<Option<Balance>, WHError>;

  fn get_checkpoints_for_all(
    &self,
    date: DateTime<Utc>,
  ) -> Result<
    (DateTime<Utc>, HashMap<Store, HashMap<Goods, HashMap<Batch, BalanceForGoods>>>),
    WHError,
  >;

  fn get_checkpoints_for_many_goods(
    &self,
    date: DateTime<Utc>,
    goods: &Vec<Goods>,
  ) -> Result<(DateTime<Utc>, HashMap<Uuid, BalanceForGoods>), WHError>;

  fn checkpoint_update(&self, ops: Vec<OpMutation>) -> Result<(), WHError> {
    log::debug!("ops len: {}", ops.len());
    for op in ops {
      log::debug!("================================");
      log::debug!("checkpoint_update {:?} {:?} {:?} {:?}", op.store, op.goods, op.batch, op.after);
      let mut tmp_date = op.date;
      let mut check_point_date = op.date;
      let mut last_checkpoint_date = self.get_latest_checkpoint_date()?;

      if last_checkpoint_date <= op.date {
        let old_checkpoints =
          self.get_checkpoints_for_all_storages_before_date(last_checkpoint_date)?;

        last_checkpoint_date = first_day_next_month(op.date);

        for old_checkpoint in old_checkpoints.iter() {
          let mut new_checkpoint_date = first_day_next_month(old_checkpoint.date);
          while new_checkpoint_date <= last_checkpoint_date {
            let key = self.key_checkpoint(old_checkpoint, new_checkpoint_date);
            self.set_balance(&key, old_checkpoint.clone().number)?;
            new_checkpoint_date = first_day_next_month(new_checkpoint_date);
          }
        }
      }

      while check_point_date <= last_checkpoint_date {
        check_point_date = first_day_next_month(tmp_date);

        let key = self.key(&op.to_op(), check_point_date);

        let mut balance = self.get_balance(&key)?;
        log::debug!("balance on {check_point_date} before operation {balance:?}");
        balance += op.to_delta();
        log::debug!("dates: op {} last checkpoint {last_checkpoint_date}", op.date);
        log::debug!("balance after {:?} : {balance:?}", op.after);

        if balance.is_zero() {
          log::debug!("del_balance: {key:?}");
          self.del_balance(&key)?;
        } else {
          log::debug!("set_balance: {balance:?} {key:?}");
          self.set_balance(&key, balance)?;
        }
        tmp_date = check_point_date;

        if check_point_date == last_checkpoint_date {
          break;
        }
      }

      self.set_latest_checkpoint_date(check_point_date)?;
    }

    Ok(())
  }

  fn get_checkpoints_for_one_storage_before_date(
    &self,
    store: Store,
    date: DateTime<Utc>,
  ) -> Result<Vec<Balance>, WHError>;

  fn get_checkpoints_for_all_storages_before_date(
    &self,
    date: DateTime<Utc>,
  ) -> Result<Vec<Balance>, WHError>;
}
