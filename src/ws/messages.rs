use crate::ws::{engine_io, socket_io};
use actix::prelude::*;
use json::JsonValue;
use uuid::Uuid;

#[derive(Message)]
#[rtype(result = "()")]
pub(crate) struct WsMessage {
  pub(crate) data: String,
  pub(crate) engine_code: String,
  pub(crate) socket_code: Option<String>,
}

impl WsMessage {
  pub(crate) fn open(sid: &Uuid) -> Self {
    let data = format!(
      "{{\"sid\":\"{}\",\"upgrades\":[\"websocket\"],\"pingInterval\":{},\"pingTimeout\":{}}}",
      sid.to_string(),
      crate::websocket::PING_INTERVAL,
      crate::websocket::PING_TIMEOUT
    );
    WsMessage { data, engine_code: engine_io::OPEN.into(), socket_code: None }
  }

  pub(crate) fn connect(sid: &Uuid) -> Self {
    let data = format!("{{\"sid\":\"{}\"}}", sid.to_string());
    println!("connect {data}");
    WsMessage {
      data,
      engine_code: engine_io::MESSAGE.into(),
      socket_code: Some(socket_io::CONNECT.into()),
    }
  }

  pub(crate) fn event<S: Convertable>(response: S) -> Self {
    let data = response.data();
    WsMessage {
      data,
      engine_code: engine_io::MESSAGE.into(),
      socket_code: Some(socket_io::EVENT.into()),
    }
  }

  pub(crate) fn ack<S: Convertable>(event_id: String, response: S) -> Self {
    let response = response.data();
    let data = if response.starts_with("[") {
      format!("{}{}", event_id, response)
    } else {
      format!("{}[{}]", event_id, response)
    };
    WsMessage {
      data,
      engine_code: engine_io::MESSAGE.into(),
      socket_code: Some(socket_io::ACK.into()),
    }
  }

  pub(crate) fn data(self) -> String {
    format!("{}{}{}", self.engine_code, self.socket_code.unwrap_or("".into()), self.data,)
  }
}

#[derive(Message, Debug)]
#[rtype(result = "()")]
pub(crate) struct Event {
  pub(crate) ctx: service::Context,
  pub(crate) sid: Uuid,
  pub(crate) event_id: String,
  pub(crate) path: String,
  pub(crate) command: String,

  pub(crate) data: JsonValue,
}

#[derive(Message)]
#[rtype(result = "()")]
pub(crate) struct Connect {
  pub(crate) sid: Uuid,
  pub(crate) socket: Recipient<WsMessage>,
}

#[derive(Message)]
#[rtype(result = "()")]
pub(crate) struct Disconnect {
  pub(crate) sid: Uuid,
}

pub(crate) trait Convertable {
  fn data(self) -> String;
}

impl Convertable for &str {
  fn data(self) -> String {
    self.to_string()
  }
}

impl Convertable for String {
  fn data(self) -> String {
    self
  }
}

impl Convertable for JsonValue {
  fn data(self) -> String {
    self.to_string()
  }
}
