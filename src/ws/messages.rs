use actix::prelude::*;
use json::JsonValue;
use uuid::Uuid;
use crate::ws::{engine_io, socket_io};

#[derive(Message)]
#[rtype(result = "()")]
pub(crate) struct WsMessage(pub String);

impl WsMessage {

  pub(crate) fn open(sid: &Uuid) -> Self {
    let data = format!(
      "{}{{\"sid\":\"{}\",\"upgrades\":[\"websocket\"],\"pingInterval\":{},\"pingTimeout\":{}}}",
      engine_io::OPEN,
      sid.to_string(),
      crate::websocket::PING_INTERVAL,
      crate::websocket::PING_TIMEOUT
    );
    WsMessage(data)
  }

  pub(crate) fn ack(event_id: String, response: String) -> Self {
    let data = format!(
      "{}{}[{}]",
      socket_io::ACK,
      event_id,
      response,
    );
    WsMessage(data)
  }

  pub(crate) fn data(&self) -> String {
    format!(
      "{}{}",
      engine_io::MESSAGE,
      self.0,
    )
  }
}

#[derive(Message, Debug)]
#[rtype(result = "()")]
pub(crate) struct Event {
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
