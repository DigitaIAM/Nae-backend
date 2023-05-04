use crate::commutator::Commutator;
use crate::websocket::WsConn;
use actix::Addr;
use actix_web::{get, web, Error, HttpRequest, HttpResponse};
use actix_web_actors::ws;

#[get("/")]
pub(crate) async fn start_connection(
  req: HttpRequest,
  stream: web::Payload,
  srv: web::Data<Addr<Commutator>>,
) -> Result<HttpResponse, Error> {
  // println!("account {account:?}");
  let connection = WsConn::new(req.head().clone(), srv.get_ref().clone());
  let resp = ws::start(connection, &req, stream);
  println!("resp {:?}", resp);
  resp
}
