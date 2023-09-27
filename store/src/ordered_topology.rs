use crate::aggregations::get_aggregations_for_one_goods;
use crate::balance::{BalanceDelta, BalanceForGoods, Cost};
use crate::batch::Batch;
use crate::db::Db;
use crate::elements::{dt, Goods, Mode, Report, Store, WHError};
use crate::operations::{Dependant, InternalOperation, Op, OpMutation};

use chrono::{DateTime, Utc};
use json::JsonValue;
use rocksdb::{BoundColumnFamily, ColumnFamilyDescriptor, IteratorMode, Options, DB};
use rust_decimal::Decimal;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Arc;
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
  fn balance_on_op_or_before(&self, op: &Op) -> Result<BalanceForGoods, WHError>;

  fn operation_after(&self, op: &Op) -> Result<Option<(Op, BalanceForGoods)>, WHError>;
  fn operations_after(&self, op: &Op) -> Result<Vec<(Op, BalanceForGoods)>, WHError>;

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

  fn ops_for_store_goods_and_batch(
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

  fn operations_for_store_goods(
    &self,
    from: DateTime<Utc>,
    till_exclude: &Op,
  ) -> Result<Vec<Op>, WHError>;

  fn get_report_for_goods(
    &self,
    db: &Db,
    store: Store,
    goods: Goods,
    batch: &Batch,
    from_date: DateTime<Utc>,
    till_date: DateTime<Utc>,
  ) -> Result<JsonValue, WHError> {
    log::debug!("DATE_TYPE_STORE_BATCH.get_report_for_goods");
    let mut balances = Vec::new();

    let op_from_date = if let Some(balance) =
      db.get_checkpoint_for_goods_and_batch(store, goods, batch, from_date)?
    {
      let d = balance.date;
      balances.push(balance);
      d
    } else {
      dt("1970-01-01")?
    };

    let ops = db.ops_for_store_goods_and_batch(store, goods, batch, op_from_date, till_date)?;

    let items = get_aggregations_for_one_goods(balances, ops, from_date, till_date)?;

    Ok(items)
  }

  fn get_report_for_storage(
    &self,
    db: &Db,
    storage: Store,
    from_date: DateTime<Utc>,
    till_date: DateTime<Utc>,
  ) -> Result<Report, WHError>;

  fn key(&self, op: &Op) -> Vec<u8> {
    self.key_build(
      op.store,
      op.goods,
      op.batch.clone(),
      op.date.timestamp(),
      op.op.order(),
      op.id,
      op.is_dependent,
    )
  }

  fn key_build(
    &self,
    store: Store,
    goods: Goods,
    batch: Batch,
    date: i64,
    op_order: u8,
    op_id: Uuid,
    is_dependent: bool,
  ) -> Vec<u8>;

  fn data_update(&self, db: &Db, op: &OpMutation) -> Result<(), WHError> {
    self.mutate_op(db, op)?;
    Ok(())

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

  fn mutate_op(&self, db: &Db, op_mut: &OpMutation) -> Result<(), WHError> {
    let mut pf = PropagationFront::new(db, db.ordered_topologies.get(0).unwrap());

    let (is_dependent, dependant) = if let Some(before) = op_mut.to_op_before() {
      if let Some((o, _b)) = self.get(&before)? {
        log::debug!("actual_op after {o:?}");
        (o.is_dependent, o.dependant)
      } else {
        log::debug!("actual_op after not found");
        (false, vec![])
      }
    } else {
      (false, vec![])
    };

    // dependent created internally only
    assert_eq!(is_dependent, false);

    if let Some(mut op) = op_mut.to_op_after() {
      op.is_dependent = is_dependent;
      op.dependant = dependant;

      // let balance_before: BalanceForGoods = self.balance_before(&op)?;
      pf.push(op)?;

      // log::debug!("mutate ops.push {ops:#?}");
    } else if let Some(mut before) = op_mut.to_op_before() {
      before.is_dependent = is_dependent;
      before.dependant = dependant;

      self.delete_op(db, &mut pf, &before)?;
    }

    let mut uniq = HashSet::new();

    while let Some(op) = pf.next() {
      log::debug!("processing {:#?}", op);
      pf.debug();

      // assert!(uniq.insert(op.clone()));
      // workaround to avoid recursive processing
      if !uniq.insert(op.clone()) {
        log::debug!("ignore because already processed");
        continue;
      }

      let (op, balance_changed) = if op.is_inventory() && op.batch.is_empty() && !op.is_dependent {
        // batch is always empty in inventory for now
        (pf.distribution_inventory(op)?, true)
      } else if op.is_issue() && op.batch.is_empty() && !op.is_dependent {
        (pf.distribution_issue(op)?, true)
      } else {
        let (old, op, new) = self.calculate_op(db, &mut pf, op)?;
        log::debug!("old_balance: {old:?} vs new_balance: {new:?}");
        (op, !old.delta(&new).is_zero())
      };

      // propagate change ... note: virtual nodes do not change balance
      if op.batch.is_empty() || balance_changed {
        log::debug!(
          "op.batch.is_empty() = {}, balance_changed = {balance_changed}",
          op.batch.is_empty()
        );
        self.propagate(&op, &mut pf)?;

        // check empty batched topology for changes
        if !op.batch.is_empty() {
          let mut empty_batch_op = op.clone();
          empty_batch_op.batch = Batch::no();
          // empty_batch_op.is_dependent = false; // help to avoid recursion
          empty_batch_op.dependant = vec![];

          self.propagate(&empty_batch_op, &mut pf)?;
        }
      }
    }

    Ok(())
  }

  fn save_op(
    &self,
    db: &Db,
    op: &Op,
    balance_after: BalanceForGoods,
    before_op: Option<Option<Op>>,
  ) -> Result<(), WHError> {
    // get balance
    // let balance_after: BalanceForGoods =
    //   if let Some(b) = balance { b } else { self.balance_before(&op)? };

    let before_op = if op.dependant.is_empty() {
      if let Some(before) = before_op {
        before
      } else if let Some((o, _)) = self.get(op)? {
        Some(o)
      } else {
        None
      }
    } else {
      None
    };

    // store update op with balance or delete
    if op.can_delete() {
      log::debug!("DEL: {op:#?}");
      self.del(op)?;

      if op.dependant.is_empty() {
        if let Some(before_op) = before_op {
          db.update(
            OpMutation {
              id: op.id,
              date: op.date,
              store: op.store,
              transfer: op.store_into,
              goods: op.goods,
              batch: op.batch.clone(),
              before: Some((before_op.op.clone(), before_op.is_dependent)),
              after: None,
            },
            balance_after,
          )?;
        }
      }
    } else {
      log::debug!("PUT: {op:#?} {balance_after:#?}");
      self.put(op, &balance_after)?;

      if op.dependant.is_empty() {
        let before = if let Some(before_op) = before_op {
          Some((before_op.op.clone(), before_op.is_dependent))
        } else {
          None
        };
        db.update(
          OpMutation {
            id: op.id,
            date: op.date,
            store: op.store,
            transfer: op.store_into,
            goods: op.goods,
            batch: op.batch.clone(),
            before,
            after: Some((op.op.clone(), op.is_dependent)),
          },
          balance_after,
        )?;
      }
    }

    Ok(())
  }

  fn remove_op(&self, db: &Db, pf: &mut PropagationFront, op: &Op) -> Result<bool, WHError> {
    log::debug!("fn_remove_op");
    let balance_before: BalanceForGoods = self.balance_before(op)?;

    let (before, balance_changed) = if op.dependant.is_empty() {
      if let Some((o, balance_after)) = self.get(op)? {
        // log::debug!("before__op is Some");
        (Some((o.op, o.is_dependent)), !balance_before.delta(&balance_after).is_zero())
      } else {
        // log::debug!("before__op is not find in db");
        (None, false)
      }
    } else {
      (None, false)
    };

    self.del(&op)?;

    // delete dependant
    pf.cleanup_dependent(&op, vec![])?;

    pf.remove(&op);

    if op.dependant.is_empty() {
      db.update(
        OpMutation {
          id: op.id,
          date: op.date,
          store: op.store,
          transfer: op.store_into,
          goods: op.goods,
          batch: op.batch.clone(),
          before,
          after: None,
        },
        balance_before.clone(),
      )?;
    }

    Ok(balance_changed)
  }

  fn calculate_op(
    &self,
    db: &Db,
    pf: &mut PropagationFront,
    op: Op,
  ) -> Result<(BalanceForGoods, Op, BalanceForGoods), WHError> {
    // calculate balance
    let before_balance: BalanceForGoods = self.balance_before(&op)?; // Vec<(Batch, BalanceForGoods)>
    log::debug!("before evaluating: old balance {before_balance:?}");
    let (calculated_op, new_balance) = self.evaluate(&before_balance, &op);
    log::debug!("after evaluating: calculated_op {calculated_op:?}\nnew_balance{new_balance:?}");

    let (before_op, current_balance) = if let Some((o, b)) = self.get(&op)? {
      // if no changes exit
      if o == calculated_op && b == new_balance {
        log::debug!(
          "EXIT * EXIT * EXIT * EXIT * EXIT * EXIT * EXIT * EXIT * EXIT * EXIT * EXIT * EXIT"
        );
        return Ok((b, o, new_balance));
      }

      (Some(o), b)
    } else {
      (None, before_balance.clone())
    };

    log::debug!("_calculated_op: {calculated_op:#?}\n = {before_balance:?}\n > {new_balance:?} vs old {current_balance:?}");

    // store update op with balance or delete
    self.save_op(db, &calculated_op, new_balance.clone(), Some(before_op))?;

    // if transfer op create dependant op
    if let Some(dep) = calculated_op.dependent_on_transfer() {
      log::debug!("_new transfer dependent: {dep:#?}");
      pf.insert(dep)?;
    }

    // TODO: process dependant?
    assert!(calculated_op.dependant.is_empty());

    Ok((current_balance, calculated_op, new_balance))
  }

  fn delete_op(&self, db: &Db, pf: &mut PropagationFront, op: &Op) -> Result<(), WHError> {
    // store update op with balance or delete
    let balance_changed = self.remove_op(db, pf, op)?;

    // propagate change
    if balance_changed {
      // log::debug!("start propagation");
      self.propagate(op, pf)?;

      // check empty batched topology for changes
      if op.batch != Batch::no() {
        let mut empty_batch_op = op.clone().into_zero();
        empty_batch_op.batch = Batch::no();
        // empty_batch_op.is_dependent = false; // help to avoid recursion
        empty_batch_op.dependant = vec![];

        self.propagate(&empty_batch_op, pf)?;
      }
    }

    // if transfer op create dependant op
    if let Some(dep) = op.dependent_on_transfer() {
      // log::debug!("_new transfer dependent: {dep:?}");
      self.delete_op(db, pf, &dep)?;
    }

    Ok(())
  }

  fn propagate(&self, op: &Op, pf: &mut PropagationFront) -> Result<(), WHError> {
    log::debug!("propagating"); // from {op:#?}");

    if let Some((next_op, _)) = self.operation_after(op)? {
      pf.push(next_op)?;
    }

    Ok(())
  }

  fn evaluate(&self, balance: &BalanceForGoods, op: &Op) -> (Op, BalanceForGoods) {
    match &op.op {
      InternalOperation::Inventory(b, d, m) => {
        let mut cost = d.cost;
        let op = if m == &Mode::Auto {
          cost = d.qty.cost(balance);

          Op {
            id: op.id,
            date: op.date,
            store: op.store,
            goods: op.goods,
            batch: op.batch.clone(),
            store_into: op.store_into,
            op: InternalOperation::Inventory(
              b.clone(),
              BalanceDelta { qty: d.qty.clone(), cost },
              m.clone(),
            ),
            is_dependent: op.is_dependent,
            dependant: op.dependant.clone(),
          }
        } else {
          op.clone()
        };

        (op, BalanceForGoods { qty: &balance.qty + &d.qty, cost: balance.cost - cost })
      },
      InternalOperation::Receive(q, c) => {
        (op.clone(), BalanceForGoods { qty: &balance.qty + &q, cost: balance.cost + *c })
      },
      InternalOperation::Issue(q, c, m) => {
        let mut cost = *c;
        let op = if m == &Mode::Auto {
          cost = if balance.qty == *q {
            balance.cost
          } else {
            // balance.clone().price(common.clone()).cost(q.clone(), common)
            q.cost(balance)
          };
          Op {
            id: op.id,
            date: op.date,
            store: op.store,
            goods: op.goods,
            batch: op.batch.clone(),
            store_into: op.store_into,
            op: InternalOperation::Issue(q.clone(), cost, m.clone()),
            is_dependent: op.is_dependent,
            dependant: op.dependant.clone(),
          }
        } else {
          op.clone()
        };

        (op, BalanceForGoods { qty: &balance.qty - &q, cost: balance.cost - cost })
      },
    }
  }

  fn to_bytes(&self, op: &Op, balance: &BalanceForGoods) -> Result<Vec<u8>, WHError> {
    // let b = vec![];
    // for batch in batches {
    //     b.push(batch.to_json());
    // }
    // array![op.to_json(), balance.to_json()].dump()

    // bincode::serialize(&(op, balance)).map_err(|e| e.into())

    let mut bs = Vec::new();
    ciborium::ser::into_writer(&(op, balance), &mut bs)?;
    Ok(bs)
  }

  fn from_bytes(&self, bytes: &[u8]) -> Result<(Op, BalanceForGoods), WHError> {
    Ok(ciborium::de::from_reader(bytes)?)

    // Ok(bincode::deserialize(bytes)?)

    // let data = String::from_utf8_lossy(bytes).to_string();
    // let array = json::parse(&data)?;
    //
    // if array.is_array() {
    //   let op = Op::from_json(array[0].clone())?;
    //   let balance = BalanceForGoods::from_json(array[1].clone())?;
    //
    //   //   let mut batches = vec![];
    //   //   if array[2].is_array() {
    //   //       for b in array[2].members() {
    //   //         batches.push(Batch::from_json(b)?);
    //   //       }
    //   //   }
    //
    //   Ok((op, balance))
    // } else {
    //   Err(WHError::new("unexpected structure"))
    // }
  }

  fn get_balances(
    &self,
    from_date: DateTime<Utc>,
    till_date: DateTime<Utc>,
    goods: &Vec<Goods>,
    checkpoints: HashMap<Uuid, BalanceForGoods>,
  ) -> Result<HashMap<Uuid, BalanceForGoods>, WHError> {
    let mut result = checkpoints;

    // get operations between checkpoint date and requested date
    let ops = self.get_ops_for_many_goods(goods, from_date, till_date)?;

    for op in ops {
      result
        .entry(op.goods)
        .and_modify(|bal| bal.apply(&op.op))
        .or_insert(match &op.op {
          InternalOperation::Inventory(_, d, _) => {
            BalanceForGoods { qty: d.qty.clone(), cost: d.cost }
          },
          InternalOperation::Receive(q, c) => BalanceForGoods { qty: q.clone(), cost: *c },
          InternalOperation::Issue(q, c, _) => BalanceForGoods { qty: -q.clone(), cost: -*c },
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
    let mut result = checkpoints;

    // get operations between checkpoint date and requested date
    let ops = self.get_ops_for_one_goods(*store, *goods, from_date, till_date)?;

    for op in ops {
      result.entry(op.goods).and_modify(|bal| bal.apply(&op.op)).or_insert_with(|| {
        let mut b = BalanceForGoods::default();
        b.apply(&op.op);
        b
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
        .or_insert_with(HashMap::new)
        .entry(op.goods)
        .or_insert_with(HashMap::new)
        .entry(op.batch)
        .and_modify(|bal| bal.apply(&op.op))
        .or_insert_with(|| {
          let mut b = BalanceForGoods::default();
          b.apply(&op.op);
          b
        });
    }

    // TODO remove zero balances

    Ok(result)
  }

  // fn cleanup(&self, ops: &mut Vec<Op>, op: &Op) {
  //   ops.retain(|o| o.is_independent(&op));
  //   log::debug!("ops.retain {ops:#?}");
  // }

  // fn cleanup_and_push(&self, ops: &mut Vec<Op>, new: Op) {
  //   ops.retain(|o| o.is_independent(&new));
  //   ops.push(new);
  //   log::debug!("cleanup_and_push ops.push {ops:#?}");
  // }

  fn cf(&self) -> Result<Arc<BoundColumnFamily>, WHError>;

  fn db(&self) -> Arc<DB>;

  fn debug(&self) -> Result<(), WHError> {
    log::debug!("DEBUG: ordered_topology");
    for record in self.db().full_iterator_cf(&self.cf()?, IteratorMode::Start) {
      let (_k, value) = record?;
      let (op, b) = self.from_bytes(&value)?;
      log::debug!("{op:#?} > {b:?}");
    }

    Ok(())
  }
}

pub struct PropagationFront<'a> {
  db: &'a Db,
  mt: &'a Box<dyn OrderedTopology + Send + Sync>,
  points: BTreeMap<Vec<u8>, Op>,
}

impl<'a> PropagationFront<'a> {
  fn new(db: &'a Db, mt: &'a Box<dyn OrderedTopology + Send + Sync>) -> Self {
    PropagationFront { db, mt, points: BTreeMap::new() }
  }

  // | ts | type | store | goods | batch | id | dependant |
  fn key_build(&self, op: &Op) -> Vec<u8> {
    let op_order = op.op.order();
    let op_dependant = if op.is_dependent { 0_u8 } else { 1_u8 };

    (op.date.timestamp() as u64)
      .to_be_bytes()
      .iter()
      .chain(op_order.to_be_bytes().iter())
      .chain(op_dependant.to_be_bytes().iter())
      .chain(op.store.as_bytes().iter())
      .chain(op.batch.to_bytes(&op.goods).iter())
      .chain(op.id.as_bytes().iter())
      .copied()
      .collect()
  }

  fn push(&mut self, op: Op) -> Result<(), WHError> {
    log::debug!("push {op:#?}");

    let key = self.key_build(&op);

    log::debug!("existing {:#?}", self.points.get(&key));
    self.points.entry(key).or_insert(op);

    Ok(())
  }

  fn insert(&mut self, op: Op) -> Result<(), WHError> {
    log::debug!("insert {op:#?}");

    let key = self.key_build(&op);

    log::debug!("existing {:#?}", self.points.get(&key));
    self.points.insert(key, op);

    Ok(())
  }

  fn remove(&mut self, op: &Op) {
    log::debug!("remove {op:#?}");

    let key = self.key_build(op);

    log::debug!("existing {:#?}", self.points.get(&key));
    self.points.remove(&key);
  }

  fn next(&mut self) -> Option<Op> {
    self.debug();
    self.points.first_entry().map(|e| e.remove())
  }

  fn debug(&self) {
    for (_k, o) in &self.points {
      log::debug!(" > {o:#?}");
    }
  }

  fn distribution_inventory(&mut self, mut op: Op) -> Result<Op, WHError> {
    // self.cleanup(ops, op);

    let balance_before_operation = self.db.balances_for_store_goods_before_operation(&op)?;
    let balance_before = balance_before_operation.get(&op.batch).cloned().unwrap_or_default();

    // sort for FIFO
    let mut balance_before_operation: Vec<(Batch, BalanceForGoods)> =
      balance_before_operation.into_iter().map(|(k, v)| (k, v)).collect();
    balance_before_operation.sort_by(|(a, _), (b, _)| a.date.cmp(&b.date));

    log::debug!("INVENTORY BEFORE BALANCE: {:#?}", balance_before_operation);

    let mut stock_balance = BalanceForGoods::default();
    for (_batch, balance) in balance_before_operation.iter() {
      // we need all balances (including negative) for stock
      // if balance.qty.is_positive() {
      stock_balance.qty += &balance.qty;
      stock_balance.cost += balance.cost;
      // }
    }

    let diff_balance = op.op.apply(&stock_balance);
    // log::debug!("diff_balance: {diff_balance:?}");

    // TODO cover cost difference

    let mut new_dependant: Vec<Dependant> = vec![];

    if diff_balance.qty.is_zero() && diff_balance.cost == Cost::ZERO {
    } else if diff_balance.qty.is_positive() {
      let batch = Batch { id: op.id, date: op.date };
      let mut new = op.clone();
      new.is_dependent = true;
      new.dependant = vec![];
      new.batch = batch;
      new.op = InternalOperation::Receive(diff_balance.qty, diff_balance.cost);
      // log::debug!("NEW_OP inventory receive: op {new:?}");

      new_dependant.push(Dependant::from(&new));
      self.insert(new)?;

      op.dependant = self.cleanup_dependent(&op, new_dependant)?;
    } else {
      let (mut qty, _cost, _mode) = (diff_balance.qty, Decimal::ZERO, Mode::Auto);

      for (batch, balance) in balance_before_operation {
        if !balance.qty.is_positive() || batch == Batch::no() {
          continue;
        } else if qty.abs().is_greater_or_equal(&balance.qty)? {
          let mut new = op.clone();
          new.is_dependent = true;
          new.dependant = vec![];
          new.batch = batch;
          new.op = InternalOperation::Issue(balance.qty.clone(), balance.cost, Mode::Auto);
          // log::debug!("NEW_OP inventory partly: qty {qty} balance {balance:?} op {new:?}");

          new_dependant.push(Dependant::from(&new));
          self.insert(new)?;

          qty += &balance.qty; // qty is always negative here
        } else {
          let mut new = op.clone();
          new.is_dependent = true;
          new.dependant = vec![];
          new.batch = batch;
          let cost = if let Some(common) = balance.qty.common(&qty) {
            // balance.price(common.clone()).cost(qty.abs(), common)
            qty.abs().cost(&balance)
          } else {
            Cost::ZERO
          };
          new.op = InternalOperation::Issue(qty.abs(), cost, Mode::Auto);
          // log::debug!("NEW_OP inventory full: qty {qty} balance {balance:?} op {new:?}");

          new_dependant.push(Dependant::from(&new));
          self.insert(new)?;

          // zero the qty
          let q = qty.clone();
          qty -= &q;
        }

        if qty.is_zero() {
          break;
        }
      }

      // log::debug!("inventory qty left {qty}");

      op.dependant = self.cleanup_dependent(&op, new_dependant)?;

      // let (op, balance_after) = self.mt.evaluate(&balance_before, &op);
      self.mt.save_op(self.db, &op, balance_before, None)?;
    }

    Ok(op)
  }

  fn distribution_issue(&mut self, mut op: Op) -> Result<Op, WHError> {
    // self.cleanup(ops, op);

    // calculate balance
    let balance_before_operation = self.db.balances_for_store_goods_before_operation(&op)?;
    let balance_before = balance_before_operation.get(&op.batch).cloned().unwrap_or_default();

    // sort for FIFO
    let mut balance_before_operation: Vec<(Batch, BalanceForGoods)> =
      balance_before_operation.into_iter().map(|(k, v)| (k, v)).collect();
    balance_before_operation.sort_by(|(a, _), (b, _)| a.date.cmp(&b.date));

    log::debug!("BEFORE BALANCE: {:#?}\nISSUE: {:#?}", balance_before_operation, op);

    let mut qty = match &op.op {
      InternalOperation::Receive(_, _) | InternalOperation::Inventory(_, _, _) => unreachable!(),
      InternalOperation::Issue(qty, _, _) => qty.clone(),
    };

    // assert!(!qty.is_zero(), "{:#?}", op);

    let mut new_dependant: Vec<Dependant> = vec![];

    for (batch, balance) in balance_before_operation {
      let price = if let Some(price) = qty.price(&balance) {
        price
      } else {
        continue;
      };

      if !balance.qty.is_positive() || batch == Batch::no() {
        continue;
      } else if qty.is_greater_or_equal(&balance.qty)? {
        let mut new = op.clone();
        new.is_dependent = true;
        new.dependant = vec![];
        new.batch = batch;
        new.op = InternalOperation::Issue(balance.qty.clone(), balance.cost, Mode::Auto);
        log::debug!("NEW_OP partly: qty {qty:?} balance {balance:?} op {new:#?}");

        // let balance_before = self.mt.balance_before(&new)?;
        // assert_eq!(balance, balance_before);

        new_dependant.push(Dependant::from(&new));
        self.insert(new)?;

        qty -= &balance.qty;

        // log::debug!("NEW_OP: qty {:?}", qty);
      } else {
        let mut new = op.clone();
        new.is_dependent = true;
        new.dependant = vec![];
        new.batch = batch;
        let cost = &qty * price;
        new.op = InternalOperation::Issue(qty.clone(), cost, Mode::Auto);
        log::debug!("NEW_OP full: qty {qty:?} balance {balance:?} op {new:#?}");

        // let balance_before = self.balance_before(&new)?;
        // assert_eq!(balance, balance_before);

        new_dependant.push(Dependant::from(&new));
        self.insert(new)?;

        qty -= &qty.clone();
        // log::debug!("NEW_OP: qty {:?}", qty);
      }

      if !qty.is_positive() {
        break;
      }
    }

    log::debug!("issue qty left {qty:?}");

    if !qty.is_zero() {
      let mut new = op.clone();
      new.is_dependent = true;
      new.dependant = vec![];
      new.batch = Batch::no(); // TODO here the problem
      new.op = InternalOperation::Issue(qty, Cost::ZERO, Mode::Auto);
      // log::debug!("NEW_OP left: qty {qty} op {new:#?}");

      // let balance_before = self.balance_before(&new)?;
      // assert_eq!(BalanceForGoods::default(), balance_before);

      new_dependant.push(Dependant::from(&new));
      self.insert(new)?;
    }

    op.dependant = self.cleanup_dependent(&op, new_dependant)?;

    // let (op, balance_after) = self.mt.evaluate(&balance_before, &op);
    self.mt.save_op(self.db, &op, balance_before, None)?;

    Ok(op)
  }

  fn cleanup_dependent(&mut self, op: &Op, new: Vec<Dependant>) -> Result<Vec<Dependant>, WHError> {
    'old: for o in op.dependant.iter() {
      for n in new.iter() {
        if n == o {
          continue 'old;
        }
      }
      let zero = o.to_op(op);
      // TODO: add to propagation front
      self.mt.delete_op(self.db, self, &zero)?;
    }

    Ok(new)
  }
}
