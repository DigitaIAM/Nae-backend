use crate::balance::{BalanceDelta, BalanceForGoods, Cost};
use crate::batch::Batch;
use crate::elements::{Goods, Mode, Store, ToJson, WHError};
use crate::ordered_topology::OrderedTopology;
use crate::qty::Qty;
use chrono::{DateTime, Utc};
use json::{object, JsonValue};
use serde::{Deserialize, Serialize};
use service::utils::json::JsonParams;
use uuid::Uuid;

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum Dependant {
  Receive(Store, Batch),
  Issue(Store, Batch),
}

impl Dependant {
  pub fn tuple(self) -> (Store, Batch, u8) {
    match self {
      Dependant::Receive(s, b) => (s, b, ORDER_RECEIVE),
      Dependant::Issue(s, b) => (s, b, ORDER_ISSUE),
    }
  }

  pub fn to_op(&self, op: &Op) -> Op {
    match self {
      Dependant::Receive(store, batch) => {
        let mut zero = op.clone();
        zero.store = *store;
        zero.batch = batch.clone();
        zero.dependant = vec![];
        zero.is_dependent = true;
        zero.op = InternalOperation::Receive(Qty::new(Vec::new()), Cost::ZERO);
        zero
      },
      Dependant::Issue(store, batch) => {
        let mut zero = op.clone();
        zero.store = *store;
        zero.batch = batch.clone();
        zero.dependant = vec![];
        zero.is_dependent = true;
        zero.op = InternalOperation::Issue(Qty::new(Vec::new()), Cost::ZERO, Mode::Manual);
        zero
      },
    }
  }
}

impl ToJson for Dependant {
  fn to_json(&self) -> JsonValue {
    match self {
      Dependant::Receive(store, batch) => object! {
        type: "receive",
          store: store.to_json(),
          batch: batch.to_json(),
      },
      Dependant::Issue(store, batch) => object! {
        type: "issue",
          store: store.to_json(),
          batch: batch.to_json(),
      },
    }
  }
}

impl From<&Op> for Dependant {
  fn from(op: &Op) -> Self {
    match op.op {
      InternalOperation::Inventory(..) => unreachable!(),
      InternalOperation::Receive(..) => Dependant::Receive(op.store, op.batch.clone()),
      InternalOperation::Issue(..) => Dependant::Issue(op.store, op.batch.clone()),
    }
  }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
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
  pub dependant: Vec<Dependant>,
}

impl Op {
  // pub(crate) fn qty(&self) -> Decimal {
  //   match &self.op {
  //     InternalOperation::Inventory(b, _, _) => b.qty,
  //     InternalOperation::Receive(q, _) => q.clone(),
  //     InternalOperation::Issue(q, _, _) => q.clone(),
  //   }
  // }
  //
  // pub(crate) fn cost(&self) -> Decimal {
  //   match &self.op {
  //     InternalOperation::Inventory(b, _, _) => b.cost,
  //     InternalOperation::Receive(_, c) => c.clone(),
  //     InternalOperation::Issue(_, c, _) => c.clone(),
  //   }
  // }

  // pub fn compare(&self, other: &Op) -> Ordering {
  //   let cmp_store = self.store.as_bytes().iter().cmp(other.store.as_bytes().iter());
  //   if cmp_store != Ordering::Equal {
  //     return cmp_store;
  //   }
  //
  //   let cmp_goods = self.goods.as_bytes().iter().cmp(other.goods.as_bytes().iter());
  //   if cmp_goods != Ordering::Equal {
  //     return cmp_goods;
  //   }
  //
  //   let cmp_date = (self.date.timestamp() as u64)
  //     .to_be_bytes()
  //     .iter()
  //     .cmp((other.batch.timestamp() as u64).to_be_bytes().iter());
  //
  //   if cmp_date != Ordering::Equal {
  //     return cmp_date;
  //   }
  // }

  pub fn into_zero(mut self) -> Self {
    match self.op {
      InternalOperation::Inventory(..) => {
        self.op = InternalOperation::Inventory(
          BalanceForGoods::default(),
          BalanceDelta::default(),
          Mode::Auto,
        )
      },
      InternalOperation::Receive(..) => {
        self.op = InternalOperation::Receive(Qty::new(Vec::new()), Cost::ZERO)
      },
      InternalOperation::Issue(..) => {
        self.op = InternalOperation::Issue(Qty::new(Vec::new()), Cost::ZERO, Mode::Auto)
      },
    }

    self
  }

  pub(crate) fn from_json(data: JsonValue) -> Result<Self, WHError> {
    let op = &data["op"];
    let mode = if op["mode"].as_str() == Some("Auto") { Mode::Auto } else { Mode::Manual };

    let operation = match op["type"].as_str() {
      Some("inventory") => InternalOperation::Inventory(
        BalanceForGoods {
          qty: op["balance"].clone().try_into()?,
          cost: op["balance"]["cost"].number().into(),
        },
        BalanceDelta {
          qty: op["delta"]["qty"].clone().try_into()?,
          cost: op["delta"]["cost"].number().into(),
        },
        mode,
      ),
      Some("receive") => {
        InternalOperation::Receive(op["qty"].clone().try_into()?, op["cost"].number().into())
      },
      Some("issue") => {
        InternalOperation::Issue(op["qty"].clone().try_into()?, op["cost"].number().into(), mode)
      },
      _ => return Err(WHError::new(&format!("unknown operation type {}", op["type"]))),
    };

    let mut dependant = vec![];

    match &data["dependant"] {
      JsonValue::Array(array) => {
        for item in array {
          let store = item["store"].uuid()?;
          let batch = &item["batch"];
          let batch = Batch { id: batch["id"].uuid()?, date: batch["date"].date_with_check()? };
          match item["type"].as_str() {
            Some("receive") => dependant.push(Dependant::Receive(store, batch)),
            Some("issue") => dependant.push(Dependant::Issue(store, batch)),
            _ => return Err(WHError::new(&format!("unknown dependant type {}", item["type"]))),
          }
        }
      },
      _ => (),
    }

    let op = Op {
      id: data["id"].uuid()?,
      date: data["date"].date_with_check()?,
      // store: data["store"].uuid()?,
      store: data["from"].uuid()?,
      goods: data["goods"].uuid()?,
      batch: Batch {
        id: data["batch"]["id"].uuid()?,
        date: data["batch"]["date"].date_with_check()?,
      },
      store_into: data["into"].uuid_or_none(),
      op: operation,
      is_dependent: data["is_dependent"].boolean(),
      dependant,
    };
    Ok(op)
  }

  pub(crate) fn to_delta(&self) -> BalanceDelta {
    match &self.op {
      InternalOperation::Inventory(_, d, _) => d.clone(),
      InternalOperation::Receive(_, _) | InternalOperation::Issue(_, _, _) => self.op.clone().into(),
    }
  }

  pub(crate) fn batch(&self) -> Vec<u8> {
    self.batch.to_bytes(&self.goods)
  }

  pub(crate) fn is_independent(&self, other: &Op) -> bool {
    if self.is_dependent {
      if self.id == other.id && self.goods == other.goods {
        match other.op {
          InternalOperation::Inventory(..) => false,
          InternalOperation::Receive(..) => true,
          InternalOperation::Issue(..) => match self.op {
            InternalOperation::Inventory(..) => true,
            InternalOperation::Receive(..) => self.batch != other.batch,
            InternalOperation::Issue(..) => !(other.batch.is_empty() || other.batch == self.batch),
          },
        }
      } else {
        true
      }
    } else {
      true
    }
  }

  pub(crate) fn dependent_on_transfer(&self) -> Option<Op> {
    // if self.is_dependent {
    //   None
    // } else
    if let Some(store_into) = self.store_into {
      match &self.op {
        InternalOperation::Issue(q, c, _m) => Some(Op {
          id: self.id,
          date: self.date,
          store: store_into,
          goods: self.goods,
          batch: self.batch.clone(),
          store_into: Some(self.store),
          op: InternalOperation::Receive(q.clone(), *c),
          is_dependent: true,
          dependant: vec![],
        }),
        _ => None,
      }
    } else {
      None
    }
  }

  pub(crate) fn can_delete(&self) -> bool {
    match &self.op {
      InternalOperation::Inventory(..) => false,
      InternalOperation::Receive(q, c) => q.is_zero() && c.is_zero(),
      InternalOperation::Issue(q, c, _) => q.is_zero() && c.is_zero(),
    }
  }

  pub fn is_inventory(&self) -> bool {
    match self.op {
      InternalOperation::Inventory(..) => true,
      InternalOperation::Receive(..) => false,
      InternalOperation::Issue(..) => false,
    }
  }

  pub fn is_receive(&self) -> bool {
    match self.op {
      InternalOperation::Inventory(..) => false,
      InternalOperation::Receive(..) => true,
      InternalOperation::Issue(..) => false,
    }
  }

  pub fn is_issue(&self) -> bool {
    match self.op {
      InternalOperation::Inventory(..) => false,
      InternalOperation::Receive(..) => false,
      InternalOperation::Issue(..) => true,
    }
  }
}

impl ToJson for Op {
  fn to_json(&self) -> JsonValue {
    let mut obj = object! {
      id: self.id.to_json(),
      date: self.date.to_json(),
      from: self.store.to_json(),
      goods: self.goods.to_json(),

      op: self.op.to_json(),

      is_dependent: self.is_dependent
    };

    match self.store_into.as_ref() {
      None => {},
      Some(into) => obj["into"] = into.to_json(),
    }

    obj["batch"] = self.batch.to_json();

    let dependant: Vec<JsonValue> = self.dependant.iter().map(|d| d.to_json()).collect();
    obj["dependant"] = dependant.into();

    obj
  }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct OpMutation {
  // key
  pub id: Uuid,
  pub date: DateTime<Utc>,
  pub store: Store,
  pub transfer: Option<Store>,
  pub goods: Goods,
  pub batch: Batch,
  // value
  pub before: Option<(InternalOperation, bool)>,
  pub after: Option<(InternalOperation, bool)>,
  // internal
  // pub is_dependent: bool,
  // pub dependant: Vec<Dependant>,
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
      before: before.map(|o| (o, false)),
      after: after.map(|o| (o, false)),
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
      after: Some((InternalOperation::Receive(qty, cost), false)),
    }
  }

  fn value(&self) -> Result<String, WHError> {
    Ok(serde_json::to_string(&self)?)
  }

  pub fn to_op_before(&self) -> Option<Op> {
    self.before.as_ref().map(|(op, is_dependent)| Op {
      id: self.id,
      date: self.date,
      store: self.store,
      goods: self.goods,
      batch: self.batch.clone(),
      store_into: self.transfer,
      op: op.clone(),
      is_dependent: *is_dependent,
      dependant: vec![],
    })
  }

  pub fn to_op_after(&self) -> Option<Op> {
    self.after.as_ref().map(|(op, is_dependent)| Op {
      id: self.id,
      date: self.date,
      store: self.store,
      goods: self.goods,
      batch: self.batch.clone(),
      store_into: self.transfer,
      op: op.clone(),
      is_dependent: *is_dependent,
      dependant: vec![],
    })
  }

  pub(crate) fn to_delta(&self) -> BalanceDelta {
    if let Some((before, _)) = self.before.as_ref() {
      if let Some((after, _)) = self.after.as_ref() {
        let before: BalanceDelta = before.clone().into();
        let after: BalanceDelta = after.clone().into();
        after - before
      } else {
        let before: BalanceDelta = before.clone().into();

        BalanceDelta { qty: -before.qty, cost: -before.cost }
      }
    } else if let Some((after, _)) = self.after.as_ref() {
      after.clone().into()
    } else {
      BalanceDelta::default()
    }
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
        before: Some((b.op.clone(), false)),
        after: Some((a.op.clone(), false)),
      }
    } else if let Some(b) = &before {
      OpMutation {
        id: b.id,
        date: b.date,
        store: b.store,
        transfer: b.store_into,
        goods: b.goods,
        batch: b.batch.clone(),
        before: Some((b.op.clone(), false)),
        after: None,
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
        after: Some((a.op.clone(), false)),
      }
    } else {
      panic!("must no happen")
    }
  }

  pub fn is_issue(&self) -> bool {
    match &self.after {
      Some((o, _)) => match o {
        InternalOperation::Inventory(..) => false,
        InternalOperation::Receive(..) => false,
        InternalOperation::Issue(..) => true,
      },
      None => false,
    }
  }

  pub fn is_inventory(&self) -> bool {
    match &self.after {
      Some((o, _)) => match o {
        InternalOperation::Inventory(..) => true,
        InternalOperation::Receive(..) => false,
        InternalOperation::Issue(..) => false,
      },
      None => false,
    }
  }
}

const ORDER_INVENTORY: u8 = 1_u8;
const ORDER_RECEIVE: u8 = 2_u8;
// const ORDER_RECEIVE_DEPENDANT: u8 = 3_u8;
const ORDER_ISSUE: u8 = 4_u8;
// const ORDER_ISSUE_DEPENDANT: u8 = 5_u8;

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum InternalOperation {
  Inventory(BalanceForGoods, BalanceDelta, Mode),
  Receive(Qty, Cost),     // FROM // TODO Option<Store>
  Issue(Qty, Cost, Mode), // INTO // TODO Option<Store>
}

impl InternalOperation {
  pub fn apply(&self, balance: &BalanceForGoods) -> BalanceDelta {
    match self {
      InternalOperation::Inventory(b, _, m) => {
        let qty = &b.qty - &balance.qty;

        let cost = if m == &Mode::Auto {
          // balance.clone().price(common.clone()).cost(qty.clone(), common)
          qty.cost(balance)
        } else {
          b.cost - balance.cost
        };

        BalanceDelta { qty: qty.clone(), cost }
      },
      InternalOperation::Receive(..) => unimplemented!(),
      InternalOperation::Issue(..) => unimplemented!(),
    }
  }
}

impl InternalOperation {
  pub(crate) fn order(&self) -> u8 {
    match self {
      InternalOperation::Inventory(..) => ORDER_INVENTORY,
      InternalOperation::Receive(..) => ORDER_RECEIVE,
      InternalOperation::Issue(..) => ORDER_ISSUE,
    }
  }

  pub(crate) fn is_zero(&self) -> bool {
    match self {
      InternalOperation::Inventory(_q, _c, _) => false,
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
          type: "inventory",
          balance: b.to_json(),
          delta: d.to_json(),
          mode: m.to_json()
        }
      },
      InternalOperation::Receive(q, c) => {
        let q: JsonValue = q.into();
        object! {
          type: "receive",
          qty: q,
          cost: c.to_json(),
        }
      },
      InternalOperation::Issue(q, c, m) => {
        let q: JsonValue = q.into();
        object! {
          type: "issue",
          qty: q,
          cost: c.to_json(),
          mode: m.to_json(),
        }
      },
    }
  }
}

trait Operation {}

impl From<InternalOperation> for BalanceDelta {
  fn from(val: InternalOperation) -> Self {
    match val {
      InternalOperation::Inventory(_, d, _) => d, // TODO: ("undefined?"), don't know how to replace it
      InternalOperation::Receive(qty, cost) => BalanceDelta { qty, cost },
      InternalOperation::Issue(qty, cost, _) => BalanceDelta { qty: -qty, cost: -cost },
    }
  }
}
