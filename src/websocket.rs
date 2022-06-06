use actix::{Actor, Addr, StreamHandler};
use actix_web::{get, web, Error, HttpRequest, HttpResponse};
use actix_web::web::Data;
use actix_web_actors::ws;
use uuid::Uuid;
use crate::commutator::Commutator;

struct WsConn {
    id: Uuid,
    animotron: Addr<Commutator>,
}

impl WsConn {
    fn new(animotron: Addr<Commutator>) -> Self {
        WsConn {
            id: Uuid::new_v4(),
            animotron
        }
    }
}

impl Actor for WsConn {
    type Context = ws::WebsocketContext<Self>;

    fn started(&mut self, ctx: &mut Self::Context) {

    }
}

impl StreamHandler<Result<ws::Message, ws::ProtocolError>> for WsConn {
    fn handle(&mut self, msg: Result<ws::Message, ws::ProtocolError>, ctx: &mut Self::Context) {
        match msg {
            Ok(ws::Message::Ping(msg)) => ctx.pong(&msg),
            Ok(ws::Message::Text(text)) => ctx.text(text),
            Ok(ws::Message::Binary(bin)) => ctx.binary(bin),
            _ => (),
        }
    }
}

#[get("/ws")]
pub(crate) async fn start_connection_route(req: HttpRequest, stream: web::Payload, srv: Data<Addr<Commutator>>) -> Result<HttpResponse, Error> {
    let connection = WsConn::new(srv.get_ref().clone());
    let resp = ws::start(connection, &req, stream);
    println!("{:?}", resp);
    resp
}