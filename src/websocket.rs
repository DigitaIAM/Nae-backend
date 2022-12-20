use crate::animo::error::DBError;
use crate::ws::{engine_io, error_general, socket_io, Connect, Disconnect, Event, WsMessage};
use crate::{Commutator, ID};
use actix::{
  fut, Actor, ActorContext, ActorFutureExt, Addr, AsyncContext, ContextFutureSpawner, Handler,
  Running, StreamHandler, WrapFuture,
};
use actix_web::web::Data;
use actix_web::{get, web, Error, HttpRequest, HttpResponse};
use actix_web_actors::ws;
use bytestring::ByteString;
use serde_json::Value;
use std::process::Command;
use std::time::{Duration, Instant};
use uuid::Uuid;

const CHECKS_INTERVAL: Duration = Duration::from_secs(11);
const HEARTBEAT_INTERVAL: Duration = Duration::from_secs(29);
const CLIENT_TIMEOUT: Duration = Duration::from_secs(41);

pub(crate) const PING_INTERVAL: u16 = 25000;
pub(crate) const PING_TIMEOUT: u16 = 20000;

pub(crate) struct WsConn {
  id: Uuid,
  hb: Instant,
  com: Addr<Commutator>,
}

impl WsConn {
  pub(crate) fn new(com: Addr<Commutator>) -> Self {
    WsConn { id: Uuid::new_v4(), hb: Instant::now(), com }
  }

  fn hb(&self, ctx: &mut ws::WebsocketContext<Self>) {
    ctx.run_interval(CHECKS_INTERVAL, |act, ctx| {
      let duration = Instant::now().duration_since(act.hb);
      if duration > CLIENT_TIMEOUT {
        // TODO act.app.do_send(Disconnect::new(act.id));
        ctx.stop();
        todo!()
      } else if duration > HEARTBEAT_INTERVAL {
        ctx.ping(engine_io::PING.as_bytes());
      }
    });
  }
}

impl Actor for WsConn {
  type Context = ws::WebsocketContext<Self>;

  fn started(&mut self, ctx: &mut Self::Context) {
    let addr = ctx.address();
    self
      .com
      .send(Connect { socket: addr.recipient(), sid: self.id })
      .into_actor(self)
      .then(|res, _, ctx| {
        match res {
          Ok(_res) => (),
          _ => ctx.stop(),
        }
        fut::ready(())
      })
      .wait(ctx);
  }

  fn stopping(&mut self, _: &mut Self::Context) -> Running {
    self.com.do_send(Disconnect { sid: self.id });
    Running::Stop
  }
}

impl StreamHandler<Result<ws::Message, ws::ProtocolError>> for WsConn {
  fn handle(&mut self, msg: Result<ws::Message, ws::ProtocolError>, ctx: &mut Self::Context) {
    println!("msg {:?}", msg);
    match msg {
      Ok(ws::Message::Ping(msg)) => ctx.pong(&msg),
      Ok(ws::Message::Text(text)) => {
        let payload = text.to_string();
        let code = &payload[..1];
        let data = &payload[1..];
        match code {
          engine_io::OPEN => todo!(),
          engine_io::CLOSE => todo!(),
          engine_io::PING => {
            self.hb = Instant::now();
            ctx.text(engine_io::PONG);
          },
          engine_io::PONG => {
            self.hb = Instant::now();
          },
          engine_io::MESSAGE => {
            let code = &data[..1];
            let data = &data[1..];
            match code {
              socket_io::CONNECT => {
                ctx.text(WsMessage::connect(&self.id).data());
              },
              socket_io::DISCONNECT => todo!(),
              socket_io::EVENT => {
                println!("data: {:?}", data);
                // "16[\"create\",\"authentication\",{\"strategy\":\"local\",\"email\":\"admin\",\"password\":\"111\"},{}]"
                if let Some(i) = data.find("[") {
                  if data.ends_with("]") {
                    let event_id = data[..i].to_string();
                    let data = &data[i..];

                    match json::parse(data) {
                      Ok(mut data) => {
                        if !data.is_array() {
                          ctx.text(
                            WsMessage::ack(event_id, error_general("unsupported event")).data(),
                          );
                          return;
                        }

                        let command = if let Some(str) = data.array_remove(0).as_str() {
                          str.to_string()
                        } else {
                          ctx.text(
                            WsMessage::ack(event_id, error_general("command is missing")).data(),
                          );
                          return;
                        };

                        let path = if let Some(str) = data.array_remove(0).as_str() {
                          str.to_string()
                        } else {
                          ctx.text(
                            WsMessage::ack(event_id, error_general("service path is missing"))
                              .data(),
                          );
                          return;
                        };

                        self.com.do_send(Event { sid: self.id, event_id, path, command, data });
                      },
                      Err(msg) => {
                        ctx.text(WsMessage::ack(event_id, error_general(msg.to_string())).data());
                      },
                    }
                  }
                }
              },
              engine_io::UPGRADE => todo!(),
              engine_io::NOON => todo!(),
              _ => todo!("handle error"),
            }
          },
          _ => todo!("handle unknown message"),
        }
      },
      Ok(ws::Message::Binary(bin)) => {
        todo!("binary {:?}", bin);
      },
      _ => (),
    }
  }
}

impl Handler<WsMessage> for WsConn {
  type Result = ();

  fn handle(&mut self, msg: WsMessage, ctx: &mut Self::Context) {
    let data = msg.data();
    println!("sending: {}", data);
    ctx.text(data);
  }
}
