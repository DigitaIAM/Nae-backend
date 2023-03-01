use actix::Addr;
use actix_web::{web, get, HttpRequest, HttpResponse, Error};
use actix_web_actors::ws;
use crate::Commutator;
use crate::websocket::WsConn;

#[get("/")]
pub(crate) async fn start_connection(
  req: HttpRequest,
  stream: web::Payload,
  srv: web::Data<Addr<Commutator>>
) -> Result<HttpResponse, Error> {
  let connection = WsConn::new(srv.get_ref().clone());
  let resp = ws::start(connection, &req, stream);
  println!("resp {:?}", resp);
  resp
}
