use crate::services::Error;
use crate::utils::json::JsonParams;
use crate::utils::time::now_in_seconds;
use crate::{Application, ID};
use async_trait::async_trait;
use json::number::Number;
use json::JsonValue;

pub enum Stage {
  Created(u64),
  Requested(u64),
  Completed(u64),
}

impl Stage {
  pub(crate) fn from_json(json: &JsonValue) -> Result<Self, ()> {
    let ts = match json["ts"].as_number() {
      Some(ts) => match ts.try_into() {
        Ok(ts) => ts,
        Err(_) => return Err(()),
      },
      None => return Err(()),
    };

    match json["status"].string().as_str() {
      "created" => Ok(Stage::Created(ts)),
      "requested" => Ok(Stage::Requested(ts)),
      "completed" => Ok(Stage::Completed(ts)),
      _ => Err(()),
    }
  }

  pub(crate) fn created() -> Self {
    Stage::Created(now_in_seconds())
  }

  pub(crate) fn requested() -> Self {
    Stage::Requested(now_in_seconds())
  }

  pub(crate) fn completed() -> Self {
    Stage::Completed(now_in_seconds())
  }

  pub(crate) fn to_json(&self) -> JsonValue {
    let (status, ts) = match self {
      Stage::Created(ts) => ("created", ts),
      Stage::Requested(ts) => ("requested", ts),
      Stage::Completed(ts) => ("completed", ts),
    };
    json::object! {
      "status": status.to_string(),
      "ts": *ts,
    }
  }
}

pub struct CommandMeta {
  pub(crate) id: ID,
  pub(crate) command: String,
  pub(crate) params: JsonValue,

  pub(crate) master: ID,
  pub(crate) sub: Vec<ID>,

  pub(crate) state: Stage,
  pub(crate) result: Option<Result<JsonValue, Error>>,
}

impl CommandMeta {
  pub(crate) fn new(id: ID, command: String, params: JsonValue) -> Self {
    CommandMeta {
      master: id.clone(),
      id,
      command,
      params,
      sub: vec![],
      state: Stage::created(),
      result: None,
    }
  }

  pub(crate) fn master(id: ID, sub: Vec<ID>, command: String, params: JsonValue) -> Self {
    CommandMeta {
      master: id.clone(),
      id,
      command,
      params,
      sub,
      state: Stage::created(),
      result: None,
    }
  }

  pub(crate) fn sub(master: ID, id: ID, command: String, params: JsonValue) -> Self {
    CommandMeta { master, id, command, params, sub: vec![], state: Stage::created(), result: None }
  }

  pub fn to_json(&self) -> JsonValue {
    let mut o = json::object! {
      "_id": self.id.to_base64(),
      "command": self.command.clone(),
      "params": self.params.clone(),
      state: self.state.to_json(),
    };

    if let Some(result) = self.result.as_ref() {
      match result {
        Ok(data) => o.insert("data", data.clone()),
        Err(error) => o.insert("error", error.to_string()),
      };
    };

    return o;
  }
}
