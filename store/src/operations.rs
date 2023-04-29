use crate::balance::{BalanceDelta, BalanceForGoods};
use crate::batch::Batch;
use crate::elements::{Cost, Goods, Mode, Qty, Store, ToJson, WHError};
use chrono::{DateTime, Utc};
use json::{object, JsonValue};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use service::utils::json::JsonParams;
use uuid::Uuid;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Op {
  // key
  pub id: Uuid,
  pub date: DateTime<Utc>,
  pub store: Store, // TODO: from_store: Option<Store>
  pub goods: Goods,
  pub batch: Batch,

  pub store_into: Option<Store>, // TODO: to_store: Option<Store>

  // TODO operation_store = Receive > to_store, Issue > from_store
  // TODO contra_store = Receive > from_store, Issue > from_store

  // TODO not_allowed = from_store = None, to_store = None
  // TODO is_receive  = from_store = None, to_store = Some
  // TODO is_issue    = from_store = Some, to_store = None
  // TODO is_transfer = from_store = Some, to_store = Some

  // value
  pub op: InternalOperation, // TODO qty, cost, mode

  pub is_dependent: bool,
  pub batches: Vec<Batch>,
}

impl Op {
  pub(crate) fn qty(&self) -> Decimal {
    match &self.op {
      InternalOperation::Inventory(b, _, _) => b.qty,
      InternalOperation::Receive(q, _) => q.clone(),
      InternalOperation::Issue(q, _, _) => q.clone(),
    }
  }

  pub(crate) fn cost(&self) -> Decimal {
    match &self.op {
      InternalOperation::Inventory(b, _, _) => b.cost,
      InternalOperation::Receive(_, c) => c.clone(),
      InternalOperation::Issue(_, c, _) => c.clone(),
    }
  }

  pub(crate) fn from_json(data: JsonValue) -> Result<Self, WHError> {
    let op = &data["op"];
    let mode = if op["mode"].as_str() == Some("Auto") { Mode::Auto } else { Mode::Manual };

    let operation = match op["type"].as_str() {
      Some("Inventory") => InternalOperation::Inventory(
        BalanceForGoods { qty: op["balance"]["qty"].number(), cost: op["balance"]["cost"].number() },
        BalanceDelta { qty: op["delta"]["qty"].number(), cost: op["delta"]["cost"].number() },
        mode,
      ),
      Some("Receive") => InternalOperation::Receive(op["qty"].number(), op["cost"].number()),
      Some("Issue") => InternalOperation::Issue(op["qty"].number(), op["cost"].number(), mode),
      _ => return Err(WHError::new(&format!("unknown operation type {}", op["type"]))),
    };

    let mut batches = vec![];

    match &data["batches"] {
      JsonValue::Array(array) => {
        for batch in array {
          batches.push(Batch { id: batch["id"].uuid()?, date: batch["date"].date_with_check()? });
        }
      },
      _ => (),
    }

    let op = Op {
      id: data["id"].uuid()?,
      date: data["date"].date_with_check()?,
      store: data["store"].uuid()?,
      goods: data["goods"].uuid()?,
      batch: Batch {
        id: data["batch"]["id"].uuid()?,
        date: data["batch"]["date"].date_with_check()?,
      },
      store_into: data["transfer"].uuid_or_none(),
      op: operation,
      is_dependent: data["is_dependent"].boolean(),
      batches,
    };
    Ok(op)
  }

  pub(crate) fn to_delta(&self) -> BalanceDelta {
    self.op.clone().into()
  }

  pub(crate) fn batch(&self) -> Vec<u8> {
    let dt = self.batch.date.timestamp() as u64;

    self
      .goods
      .as_bytes()
      .iter()
      .chain(dt.to_be_bytes().iter())
      .chain(self.batch.id.as_bytes().iter())
      .map(|b| *b)
      .collect()
  }

  pub(crate) fn dependent(&self) -> Option<Op> {
    // if self.is_dependent {
    //   None
    // } else
    if let Some(store_into) = self.store_into {
      match &self.op {
        InternalOperation::Issue(q, c, m) => Some(Op {
          id: self.id,
          date: self.date,
          store: store_into,
          goods: self.goods,
          batch: self.batch.clone(),
          store_into: Some(self.store),
          op: InternalOperation::Receive(q.clone(), c.clone()),
          is_dependent: true,
          batches: vec![],
        }),
        _ => None,
      }
    } else {
      None
    }
  }

  pub(crate) fn is_zero(&self) -> bool {
    match &self.op {
      InternalOperation::Inventory(q, c, _) => q.is_zero() && c.is_zero(),
      InternalOperation::Receive(q, c) => q.is_zero() && c.is_zero(),
      InternalOperation::Issue(q, c, _) => q.is_zero() && c.is_zero(),
    }
  }

  pub fn is_inventory(&self) -> bool {
    match self.op {
      InternalOperation::Inventory(_, _, _) => true,
      InternalOperation::Receive(_, _) => false,
      InternalOperation::Issue(_, _, _) => false,
    }
  }

  pub fn is_issue(&self) -> bool {
    match self.op {
      InternalOperation::Inventory(_, _, _) => false,
      InternalOperation::Receive(_, _) => false,
      InternalOperation::Issue(_, _, _) => true,
    }
  }
}

impl ToJson for Op {
  fn to_json(&self) -> JsonValue {
    object! {
      id: self.id.to_json(),
      date: self.date.to_json(),
      store: self.store.to_json(),
      goods: self.goods.to_json(),
      batch: self.batch.to_json(),
      transfer: match self.store_into {
        Some(t) => t.to_json(),
        None => JsonValue::Null,
      },
      op: self.op.to_json(),
    }
  }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct OpMutation {
  // key
  pub(crate) id: Uuid,
  pub(crate) date: DateTime<Utc>,
  pub(crate) store: Store,
  pub(crate) transfer: Option<Store>,
  pub(crate) goods: Goods,
  pub(crate) batch: Batch,
  // value
  pub(crate) before: Option<InternalOperation>,
  pub(crate) after: Option<InternalOperation>,

  pub is_dependent: bool,
  pub batches: Vec<Batch>,
}

impl Default for OpMutation {
  fn default() -> Self {
    Self {
      id: Default::default(),
      date: Default::default(),
      store: Default::default(),
      transfer: Default::default(),
      goods: Default::default(),
      batch: Batch::new(),
      before: None,
      after: None,
      is_dependent: false,
      batches: vec![],
    }
  }
}

impl OpMutation {
  pub fn new(
    id: Uuid,
    date: DateTime<Utc>,
    store: Store,
    transfer: Option<Store>,
    goods: Goods,
    batch: Batch,
    before: Option<InternalOperation>,
    after: Option<InternalOperation>,
  ) -> OpMutation {
    OpMutation {
      id,
      date,
      store,
      transfer,
      goods,
      batch,
      before,
      after,
      is_dependent: false,
      batches: vec![],
    }
  }

  pub fn receive_new(
    id: Uuid,
    date: DateTime<Utc>,
    store: Store,
    goods: Goods,
    batch: Batch,
    qty: Qty,
    cost: Cost,
  ) -> OpMutation {
    OpMutation {
      id,
      date,
      store,
      transfer: None,
      goods,
      batch,
      before: None,
      after: Some(InternalOperation::Receive(qty, cost)),
      is_dependent: false,
      batches: vec![],
    }
  }

  fn value(&self) -> Result<String, WHError> {
    Ok(serde_json::to_string(&self)?)
  }

  pub fn to_op(&self) -> Op {
    if let Some(op) = self.after.as_ref() {
      Op {
        id: self.id.clone(),
        date: self.date.clone(),
        store: self.store.clone(),
        goods: self.goods.clone(),
        batch: self.batch.clone(),
        store_into: self.transfer.clone(),
        op: op.clone(),
        is_dependent: self.is_dependent,
        batches: self.batches.clone(),
      }
    } else {
      Op {
        id: self.id.clone(),
        date: self.date.clone(),
        store: self.store.clone(),
        goods: self.goods.clone(),
        batch: self.batch.clone(),
        store_into: self.transfer.clone(),
        op: if let Some(b) = self.before.clone() {
          b
        } else {
          InternalOperation::Receive(0.into(), 0.into())
        },
        is_dependent: self.is_dependent,
        batches: self.batches.clone(),
      }
    }
  }

  pub(crate) fn to_delta(&self) -> BalanceDelta {
    let n: BalanceDelta = self.after.as_ref().map(|i| i.clone().into()).unwrap_or_default();
    let o: BalanceDelta = self.before.as_ref().map(|i| i.clone().into()).unwrap_or_default();

    n - o
  }

  pub(crate) fn new_from_ops(before: Option<Op>, after: Option<Op>) -> OpMutation {
    if let (Some(b), Some(a)) = (&before, &after) {
      OpMutation {
        id: a.id,
        date: a.date,
        store: a.store,
        transfer: a.store_into,
        goods: a.goods,
        batch: a.batch.clone(),
        before: Some(b.op.clone()),
        after: Some(a.op.clone()),
        is_dependent: a.is_dependent,
        batches: a.batches.clone(),
      }
    } else if let Some(b) = &before {
      OpMutation {
        id: b.id,
        date: b.date,
        store: b.store,
        transfer: b.store_into,
        goods: b.goods,
        batch: b.batch.clone(),
        before: Some(b.op.clone()),
        after: None,
        is_dependent: b.is_dependent,
        batches: b.batches.clone(),
      }
    } else if let Some(a) = &after {
      OpMutation {
        id: a.id,
        date: a.date,
        store: a.store,
        transfer: a.store_into,
        goods: a.goods,
        batch: a.batch.clone(),
        before: None,
        after: Some(a.op.clone()),
        is_dependent: a.is_dependent,
        batches: a.batches.clone(),
      }
    } else {
      OpMutation::default()
    }
  }

  fn dependent(&self) -> Option<OpMutation> {
    if let Some(transfer) = self.transfer {
      let before = match self.before.clone() {
        Some(b) => Some(b),
        None => None,
      };
      // TODO check if cost in operation already calculated - No!
      match self.after.as_ref() {
        Some(InternalOperation::Issue(q, c, _)) => Some(OpMutation {
          id: self.id,
          date: self.date,
          store: transfer,
          transfer: None,
          goods: self.goods,
          batch: self.batch.clone(),
          before,
          after: Some(InternalOperation::Receive(q.clone(), c.clone())),
          is_dependent: self.is_dependent,
          batches: self.batches.clone(),
        }),
        _ => None,
      }
    } else {
      None
    }
  }

  pub fn is_issue(&self) -> bool {
    match &self.after {
      Some(o) => match o {
        InternalOperation::Inventory(_, _, _) => false,
        InternalOperation::Receive(_, _) => false,
        InternalOperation::Issue(_, _, _) => true,
      },
      None => false,
    }
  }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum InternalOperation {
  // TODO Inventory(Qty, Cost), // actual qty, calculated qty; calculated qty +/- delta = actual qty (delta qty, delta cost)
  Inventory(BalanceForGoods, BalanceDelta, Mode),
  Receive(Qty, Cost),     // FROM // TODO Option<Store>
  Issue(Qty, Cost, Mode), // INTO // TODO Option<Store>
}

impl InternalOperation {
  pub(crate) fn is_zero(&self) -> bool {
    match self {
      InternalOperation::Inventory(q, c, _) => q.is_zero() && c.is_zero(),
      InternalOperation::Receive(q, c) => q.is_zero() && c.is_zero(),
      InternalOperation::Issue(q, c, _) => q.is_zero() && c.is_zero(),
    }
  }
}

impl ToJson for InternalOperation {
  fn to_json(&self) -> JsonValue {
    // JsonValue::String(serde_json::to_string(&self).unwrap_or_default())

    match self {
      InternalOperation::Inventory(b, d, m) => {
        object! {
          type: JsonValue::String("Inventory".to_string()),
          balance: b.to_json(),
          delta: d.to_json(),
          mode: match m {
            Mode::Auto => JsonValue::String("Auto".to_string()),
            Mode::Manual => JsonValue::String("Manual".to_string()),
          }
        }
      },
      InternalOperation::Receive(q, c) => {
        object! {
          type: JsonValue::String("Receive".to_string()),
          qty: q.to_json(),
          cost: c.to_json(),
        }
      },
      InternalOperation::Issue(q, c, m) => {
        object! {
          type: JsonValue::String("Issue".to_string()),
          qty: q.to_json(),
          cost: c.to_json(),
          mode: match m {
            Mode::Auto => JsonValue::String("Auto".to_string()),
            Mode::Manual => JsonValue::String("Manual".to_string()),
          }
        }
      },
    }
  }
}

trait Operation {}

impl Into<BalanceDelta> for InternalOperation {
  fn into(self) -> BalanceDelta {
    match self {
      InternalOperation::Inventory(b, d, _) => BalanceDelta { qty: d.qty, cost: d.cost },
      InternalOperation::Receive(qty, cost) => BalanceDelta { qty, cost },
      InternalOperation::Issue(qty, cost, _) => BalanceDelta { qty: -qty, cost: -cost },
    }
  }
}
