use crate::balance::{Balance, BalanceDelta, BalanceForGoods, Cost};
use crate::batch::Batch;
use crate::db::Db;
use crate::elements::{Goods, Mode, Report, Store, ToJson, WHError};
use crate::operations::{Dependant, InternalOperation, Op, OpMutation};
use chrono::{DateTime, Utc};
use json::{array, JsonValue};
use rocksdb::{ColumnFamilyDescriptor, Options};
use rust_decimal::Decimal;
use std::collections::HashMap;
use uuid::Uuid;

pub trait OrderedTopology {
  fn put(
    &self,
    op: &Op,
    balance: &BalanceForGoods,
  ) -> Result<Option<(Op, BalanceForGoods)>, WHError>;
  fn get(&self, op: &Op) -> Result<Option<(Op, BalanceForGoods)>, WHError>;
  fn del(&self, op: &Op) -> Result<(), WHError>;

  fn balance_before(&self, op: &Op) -> Result<BalanceForGoods, WHError>;
  fn goods_balance_before(
    &self,
    op: &Op,
    balances: Vec<Balance>,
  ) -> Result<Vec<(Batch, BalanceForGoods)>, WHError>;

  fn operations_after(
    &self,
    op: &Op,
    no_batches: bool,
  ) -> Result<Vec<(Op, BalanceForGoods)>, WHError>;

  fn create_cf(&self, opts: Options) -> ColumnFamilyDescriptor;

  fn get_ops_for_storage(
    &self,
    storage: Store,
    from_date: DateTime<Utc>,
    till_date: DateTime<Utc>,
  ) -> Result<Vec<Op>, WHError>;

  fn get_ops_for_all(
    &self,
    from_date: DateTime<Utc>,
    till_date: DateTime<Utc>,
  ) -> Result<Vec<Op>, WHError>;

  fn get_ops_for_one_goods(
    &self,
    store: Store,
    goods: Goods,
    from_date: DateTime<Utc>,
    till_date: DateTime<Utc>,
  ) -> Result<Vec<Op>, WHError>;

  fn get_ops_before_op(&self, op: &Op) -> Result<Vec<Op>, WHError>;

  fn get_ops_for_one_goods_and_batch(
    &self,
    store: Store,
    goods: Goods,
    batch: &Batch,
    from_date: DateTime<Utc>,
    till_date: DateTime<Utc>,
  ) -> Result<Vec<Op>, WHError>;

  fn get_ops_for_many_goods(
    &self,
    goods: &Vec<Goods>,
    from_date: DateTime<Utc>,
    till_date: DateTime<Utc>,
  ) -> Result<Vec<Op>, WHError>;

  fn operations_for_store_goods(&self, from: DateTime<Utc>, till: &Op) -> Result<Vec<Op>, WHError>;

  fn get_report_for_goods(
    &self,
    db: &Db,
    storage: Store,
    goods: Goods,
    batch: &Batch,
    from_date: DateTime<Utc>,
    till_date: DateTime<Utc>,
  ) -> Result<JsonValue, WHError>;

  fn get_report_for_storage(
    &self,
    db: &Db,
    storage: Store,
    from_date: DateTime<Utc>,
    till_date: DateTime<Utc>,
  ) -> Result<Report, WHError>;

  fn key(&self, op: &Op) -> Vec<u8>;

  fn data_update(&self, db: &Db, op: &OpMutation) -> Result<Vec<OpMutation>, WHError> {
    self.mutate_op(db, op)

    // TODO review logic after enable transaction
    // if op.before.is_none() {
    //   if let Ok(None) = self.get(&op.to_op()) {
    //     self.mutate_op(op, balances)
    //   } else {
    //     let err = WHError::new("Wrong 'before' state, expected something");
    //     log::debug!("ERROR: {err:?}");
    //     return Err(err);
    //   }
    // } else {
    //   if let Ok(Some((o, balance))) = self.get(&op.to_op()) {
    //     // let (o, balance) = self.from_bytes(&bytes)?;
    //     if Some(o.op) == op.before {
    //       self.mutate_op(op, balances)
    //     } else {
    //       let err = WHError::new("Wrong 'before' state in operation: {o.op:?}");
    //       log::debug!("ERROR: {err:?}");
    //       return Err(err);
    //     }
    //   } else {
    //     let err = WHError::new("There is no such operation in db");
    //     log::debug!("ERROR: {err:?}");
    //     return Err(err);
    //   }
    // }
  }

  fn mutate_op(&self, db: &Db, op_mut: &OpMutation) -> Result<Vec<OpMutation>, WHError> {
    let mut ops: Vec<Op> = vec![];
    let mut result: Vec<OpMutation> = vec![];

    ops.push(op_mut.to_op());

    while ops.len() > 0 {
      let mut op = ops.remove(0);

      log::debug!("processing {:?}", op);

      let mut batches = vec![];

      if op.is_inventory() && op.batch.is_empty() && !op.is_dependent {
        // batch is always empty in inventory for now
        self.mutate_inventory_with_empty_batch(db, &mut result, &mut ops, &mut op, &mut batches)?;
      } else if op.is_issue() && op.batch.is_empty() && !op.is_dependent {
        self.mutate_issue_with_empty_batch(db, &mut result, &mut ops, &mut op, &mut batches)?;
      } else {
        self.calculate_op(&mut result, &mut ops, &mut op, true)?;
      }
    }

    Ok(result)
  }

  fn cleanup_dependent(&self, op: &Op, new: Vec<Dependant>, ops: &mut Vec<Op>) -> Vec<Dependant> {
    // TODO calculate diffs between dependant
    'old: for o in op.dependant.iter() {
      for n in new.iter() {
        if n == o {
          continue 'old;
        }
      }
      match o {
        Dependant::Receive(store, batch) => {
          let mut zero = op.clone();
          zero.store = store.clone();
          zero.batch = batch.clone();
          zero.op = InternalOperation::Receive(Decimal::ZERO, Cost::ZERO);
          ops.push(zero);
        },
        Dependant::Issue(store, batch) => {
          let mut zero = op.clone();
          zero.store = store.clone();
          zero.batch = batch.clone();
          zero.op = InternalOperation::Issue(Decimal::ZERO, Cost::ZERO, Mode::Auto);
          ops.push(zero);
        },
      }
    }

    new
  }

  fn mutate_inventory_with_empty_batch(
    &self,
    db: &Db,
    result: &mut Vec<OpMutation>,
    ops: &mut Vec<Op>,
    op: &mut Op,
    batches: &mut Vec<Batch>,
  ) -> Result<(), WHError> {
    let balance_before_operation = db.balances_for_store_goods_before_operation(&op)?;

    log::debug!("INVENTORY_BEFORE_BALANCE: {:?}", balance_before_operation);

    let mut stock_balance = BalanceForGoods::default();
    for (batch, balance) in balance_before_operation.iter() {
      if balance.qty > Decimal::ZERO {
        stock_balance.qty += balance.qty;
        stock_balance.cost += balance.cost;
      }
    }

    let diff_balance = op.op.apply(&stock_balance);
    log::debug!("diff_balance: {diff_balance:?}");

    // TODO cover cost difference

    let mut new_dependant: Vec<Dependant> = vec![];

    if diff_balance.qty == Decimal::ZERO && diff_balance.cost == Cost::ZERO {
    } else if diff_balance.qty > Decimal::ZERO {
      let batch = Batch { id: op.id, date: op.date };
      let mut new = op.clone();
      new.is_dependent = true;
      new.batch = batch.clone();
      new.op = InternalOperation::Receive(diff_balance.qty, diff_balance.cost);
      log::debug!("NEW_OP inventory receive: op {new:?}");

      new_dependant.push(Dependant::from(&new));
      ops.push(new);

      op.dependant = self.cleanup_dependent(&op, new_dependant, ops);
    } else {
      let (mut qty, cost, mode) = (diff_balance.qty, Decimal::ZERO, Mode::Auto);

      for (batch, balance) in balance_before_operation {
        if balance.qty <= Decimal::ZERO {
          continue;
        } else if qty.abs() >= balance.qty {
          batches.push(batch.clone());

          let mut new = op.clone();
          new.is_dependent = true;
          new.batch = batch;
          new.op = InternalOperation::Issue(balance.qty, balance.cost, Mode::Auto);
          log::debug!("NEW_OP inventory partly: qty {qty} balance {balance:?} op {new:?}");

          new_dependant.push(Dependant::from(&new));
          ops.push(new);

          qty += balance.qty; // qty is always negative here
        } else if qty.abs() < balance.qty {
          batches.push(batch.clone());

          let mut new = op.clone();
          new.is_dependent = true;
          new.batch = batch;
          new.op = InternalOperation::Issue(qty.abs(), balance.price().cost(qty.abs()), Mode::Auto);
          log::debug!("NEW_OP inventory full: qty {qty} balance {balance:?} op {new:?}");

          new_dependant.push(Dependant::from(&new));
          ops.push(new);

          // zero the qty
          qty -= qty;
        }

        if qty == Decimal::ZERO {
          break;
        }
      }

      log::debug!("inventory qty left {qty}");

      op.dependant = self.cleanup_dependent(&op, new_dependant, ops);
      self.calculate_op(result, ops, op, false)?;
    }

    Ok(())
  }

  fn mutate_issue_with_empty_batch(
    &self,
    db: &Db,
    result: &mut Vec<OpMutation>,
    ops: &mut Vec<Op>,
    op: &mut Op,
    batches: &mut Vec<Batch>,
  ) -> Result<(), WHError> {
    // calculate balance
    let balance_before_operation = db.balances_for_store_goods_before_operation(&op)?;

    log::debug!("ISSUE_BEFORE_BALANCE: {:?}", balance_before_operation);

    let mut qty = match op.op {
      InternalOperation::Receive(_, _) | InternalOperation::Inventory(_, _, _) => unreachable!(),
      InternalOperation::Issue(qty, _, _) => qty,
    };

    let mut new_dependant: Vec<Dependant> = vec![];

    for (batch, balance) in balance_before_operation {
      if balance.qty <= Decimal::ZERO {
        continue;
      } else if qty >= balance.qty {
        batches.push(batch.clone());

        let mut new = op.clone();
        new.is_dependent = true;
        new.batch = batch;
        new.op = InternalOperation::Issue(balance.qty, balance.cost, Mode::Auto);
        log::debug!("NEW_OP partly: qty {qty} balance {balance:?} op {new:?}");

        new_dependant.push(Dependant::from(&new));
        ops.push(new);

        qty -= balance.qty;

        log::debug!("NEW_OP: qty {:?}", qty);
      } else {
        batches.push(batch.clone());

        let mut new = op.clone();
        new.is_dependent = true;
        new.batch = batch;
        new.op = InternalOperation::Issue(qty, balance.price().cost(qty), Mode::Auto);
        log::debug!("NEW_OP full: qty {qty} balance {balance:?} op {new:?}");

        new_dependant.push(Dependant::from(&new));
        ops.push(new);

        qty -= qty;
      }

      if qty <= Decimal::ZERO {
        break;
      }
    }

    log::debug!("issue qty left {qty}");

    // TODO operation loosing information?

    if qty > Decimal::ZERO {
      let mut new = op.clone();
      new.is_dependent = true;
      new.batch = Batch::no(); // TODO here the problem
      new.op = InternalOperation::Issue(qty, Cost::ZERO, Mode::Auto);
      log::debug!("NEW_OP full: qty {qty} qty {qty:?} op {new:?}");

      new_dependant.push(Dependant::from(&new));
      ops.push(new);
    }

    op.dependant = self.cleanup_dependent(&op, new_dependant, ops);
    self.calculate_op(result, ops, op, false)?;

    Ok(())
  }

  fn calculate_op(
    &self,
    result: &mut Vec<OpMutation>,
    ops: &mut Vec<Op>,
    op: &mut Op,
    need_propagation: bool,
  ) -> Result<(), WHError> {
    // calculate balance
    let before_balance: BalanceForGoods = self.balance_before(&op)?; // Vec<(Batch, BalanceForGoods)>
    let (calculated_op, new_balance) = self.evaluate(&before_balance, &op);

    let current_balance =
      if let Some((o, b)) = self.get(&op)? { b } else { BalanceForGoods::default() };

    log::debug!("_before_balance: {before_balance:?}");
    log::debug!("_calculated_op: {calculated_op:?}");
    log::debug!("_current_balance: {current_balance:?}");
    log::debug!("_new_balance: {new_balance:?}");

    // store update op with balance or delete
    if calculated_op.can_delete() && op.dependant.is_empty() {
      self.del(&calculated_op)?;
    } else {
      //   self.put(&calculated_op, &new_balance, batches)?;
      self.put(&calculated_op, &new_balance)?;
      result.push(OpMutation {
        id: calculated_op.id,
        date: calculated_op.date,
        store: calculated_op.store,
        transfer: calculated_op.store_into,
        goods: calculated_op.goods,
        batch: calculated_op.batch.clone(),
        before: None,
        after: Some(calculated_op.op.clone()),
        is_dependent: calculated_op.is_dependent,
        dependant: calculated_op.dependant.clone(),
      });
    }

    // if next op have dependant add it to ops
    if let Some(dep) = calculated_op.dependent() {
      log::debug!("_new dependent: {dep:?}");
      ops.push(dep);
    }

    if need_propagation {
      // propagate delta
      if !current_balance.delta(&new_balance).is_zero() {
        let mut before_balance = new_balance;
        for (next_operation, next_current_balance) in self.operations_after(&calculated_op, true)? {
          let (calc_op, new_balance) = self.evaluate(&before_balance, &next_operation);
          if calc_op.can_delete() {
            self.del(&calc_op)?;
          } else {
            //   self.put(&calc_op, &new_balance, batches)?;
            self.put(&calc_op, &new_balance)?;
          }

          // if next op have dependant add it to ops
          if let Some(dep) = calc_op.dependent() {
            log::debug!("update dependent: {dep:?}");
            ops.push(dep);
          }

          if !next_current_balance.delta(&new_balance).is_zero() {
            break;
          }

          before_balance = new_balance;
        }
      }
    }

    Ok(())
  }

  fn evaluate(&self, balance: &BalanceForGoods, op: &Op) -> (Op, BalanceForGoods) {
    match &op.op {
      InternalOperation::Inventory(b, d, m) => {
        let mut cost = d.cost;
        let op = if m == &Mode::Auto {
          cost = balance.price().cost(d.qty);
          Op {
            id: op.id,
            date: op.date,
            store: op.store,
            goods: op.goods,
            batch: op.batch.clone(),
            store_into: op.store_into,
            op: InternalOperation::Inventory(
              b.clone(),
              BalanceDelta { qty: d.qty, cost },
              m.clone(),
            ),
            is_dependent: op.is_dependent,
            dependant: op.dependant.clone(),
          }
        } else {
          op.clone()
        };

        (op, BalanceForGoods { qty: balance.qty + d.qty, cost: balance.cost - cost })
      },
      InternalOperation::Receive(q, c) => {
        (op.clone(), BalanceForGoods { qty: balance.qty + q, cost: balance.cost + *c })
      },
      InternalOperation::Issue(q, c, m) => {
        let mut cost = c.clone();
        let op = if m == &Mode::Auto {
          cost = balance.price().cost(*q);
          Op {
            id: op.id,
            date: op.date,
            store: op.store,
            goods: op.goods,
            batch: op.batch.clone(),
            store_into: op.store_into,
            op: InternalOperation::Issue(q.clone(), cost.clone(), m.clone()),
            is_dependent: op.is_dependent,
            dependant: op.dependant.clone(),
          }
        } else {
          op.clone()
        };

        (op, BalanceForGoods { qty: balance.qty - q, cost: balance.cost - cost })
      },
    }
  }

  fn to_bytes(&self, op: &Op, balance: &BalanceForGoods) -> String {
    // let b = vec![];
    // for batch in batches {
    //     b.push(batch.to_json());
    // }
    array![op.to_json(), balance.to_json()].dump()
  }

  fn from_bytes(&self, bytes: &[u8]) -> Result<(Op, BalanceForGoods), WHError> {
    let data = String::from_utf8_lossy(bytes).to_string();
    let array = json::parse(&data)?;

    if array.is_array() {
      let op = Op::from_json(array[0].clone())?;
      let balance = BalanceForGoods::from_json(array[1].clone())?;

      //   let mut batches = vec![];
      //   if array[2].is_array() {
      //       for b in array[2].members() {
      //         batches.push(Batch::from_json(b)?);
      //       }
      //   }

      Ok((op, balance))
    } else {
      Err(WHError::new("unexpected structure"))
    }
  }

  fn get_balances(
    &self,
    from_date: DateTime<Utc>,
    till_date: DateTime<Utc>,
    goods: &Vec<Goods>,
    checkpoints: HashMap<Uuid, BalanceForGoods>,
  ) -> Result<HashMap<Uuid, BalanceForGoods>, WHError> {
    let mut result = checkpoints.clone();

    // get operations between checkpoint date and requested date
    let ops = self.get_ops_for_many_goods(goods, from_date, till_date)?;

    for op in ops {
      result.entry(op.goods).and_modify(|bal| *bal += &op.op).or_insert(match &op.op {
        InternalOperation::Inventory(_, d, _) => {
          BalanceForGoods { qty: d.qty.clone(), cost: d.cost.clone() }
        },
        InternalOperation::Receive(q, c) => BalanceForGoods { qty: q.clone(), cost: c.clone() },
        InternalOperation::Issue(q, c, _) => BalanceForGoods { qty: -q.clone(), cost: -c.clone() },
      });
    }

    Ok(result)
  }

  fn get_balances_for_one_goods_and_store(
    &self,
    from_date: DateTime<Utc>,
    till_date: DateTime<Utc>,
    store: &Store,
    goods: &Goods,
    checkpoints: HashMap<Uuid, BalanceForGoods>,
  ) -> Result<HashMap<Uuid, BalanceForGoods>, WHError> {
    let mut result = checkpoints.clone();

    // get operations between checkpoint date and requested date
    let ops = self.get_ops_for_one_goods(store.clone(), goods.clone(), from_date, till_date)?;

    for op in ops {
      result.entry(op.goods).and_modify(|bal| *bal += &op.op).or_insert(match &op.op {
        InternalOperation::Inventory(_, d, _) => {
          BalanceForGoods { qty: d.qty.clone(), cost: d.cost.clone() }
        },
        InternalOperation::Receive(q, c) => BalanceForGoods { qty: q.clone(), cost: c.clone() },
        InternalOperation::Issue(q, c, _) => BalanceForGoods { qty: -q.clone(), cost: -c.clone() },
      });
    }

    Ok(result)
  }

  fn get_balances_for_all(
    &self,
    from_date: DateTime<Utc>,
    till_date: DateTime<Utc>,
    checkpoints: HashMap<Store, HashMap<Goods, HashMap<Batch, BalanceForGoods>>>,
  ) -> Result<HashMap<Store, HashMap<Goods, HashMap<Batch, BalanceForGoods>>>, WHError> {
    let mut result = checkpoints;

    // get operations between checkpoint date and requested date
    let ops = self.get_ops_for_all(from_date, till_date)?;

    for op in ops {
      result
        .entry(op.store)
        .or_insert_with(|| HashMap::new())
        .entry(op.goods)
        .or_insert_with(|| HashMap::new())
        .entry(op.batch)
        .and_modify(|bal| *bal += &op.op)
        .or_insert_with(|| BalanceForGoods::default() + op.op);
    }

    // TODO remove zero balances

    Ok(result)
  }
}
