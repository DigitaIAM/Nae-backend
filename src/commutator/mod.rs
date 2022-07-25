use std::collections::{HashMap, HashSet};
use std::sync::{Arc, RwLock};
use actix::prelude::*;
use json::JsonValue;
use uuid::Uuid;
use crate::animo::memory::ChangeTransformation;
use crate::{AnimoDB, ID, Settings};
use crate::services::{Services, Service};
use crate::ws::{Connect, Disconnect, engine_io, error_general, Event, socket_io, WsMessage};

type Socket = Recipient<WsMessage>;

#[derive(Clone)]
pub(crate) struct Application {
    pub(crate) settings: Arc<Settings>,
    pub(crate) db: Arc<AnimoDB>,
    services: Arc<RwLock<HashMap<String, Arc<dyn Service>>>>,
    channels: Arc<HashMap<String, HashSet<Uuid>>>,
}

impl Application {
    pub(crate) fn new(settings: Arc<Settings>, db: Arc<AnimoDB>) -> Self {
        Application {
            settings, db,
            services: Arc::new(RwLock::new(HashMap::new())),
            channels: Arc::new(HashMap::new()),
        }
    }
}

impl Services for Application {
    fn register(&mut self, service: Arc<dyn Service>) {
        let path = service.path().to_string();

        let mut services = self.services.write().unwrap();
        if let Some(current) = services.insert(path, service) {
            panic!("service for path {:?} already registered", current.path());
        }
    }

    fn service(&self, name: &str) -> Arc<dyn Service> {
        let services = self.services.read().unwrap();
        if let Some(service) = services.get(name) {
            service.clone()
        } else {
            Arc::new(crate::services::NoService(name.to_string()))
        }
    }
}

#[derive(Clone)]
pub(crate) struct Commutator {
    app: Application,
    sessions: HashMap<Uuid, Socket>,
}

impl Commutator {
    pub(crate) fn new(app: Application) -> Commutator {
        Commutator {
            app,
            sessions: HashMap::new(),
            // rooms: HashMap::new(),
        }
    }

    fn open(&self, id_to: &Uuid) {
        if let Some(socket) = self.sessions.get(id_to) {
            socket.do_send(WsMessage::open(id_to));

            // version 3
            let data = format!("{}{}", engine_io::MESSAGE, socket_io::CONNECT);
            // version 4
            // "40{\"sid\":\"...\"}"
            socket.do_send(WsMessage(data));
        } else {
            println!("attempting to send message but couldn't find user id.");
        }
    }

    fn ack(&self, event_id: String, response: String, id_to: &Uuid) {
        if let Some(socket) = self.sessions.get(id_to) {
            socket.do_send(WsMessage::ack(event_id, response));
        } else {
            println!("attempting to send message but couldn't find user id.");
        }
    }
}

impl Actor for Commutator {
    type Context = Context<Self>;
}

fn data_params(mut data: JsonValue) -> Result<(JsonValue,JsonValue),String> {
    Ok((data.array_remove(0), data.array_remove(0)))
}

fn id_params(mut data: JsonValue) -> Result<(ID,JsonValue),String> {
    if let Some(id) = data.array_remove(0).as_str() {
        match ID::new(id.as_bytes()) {
            Ok(id) => Ok((id, data.array_remove(0))),
            Err(_) => Err(format!("incorrect id {}", id)),
        }
    } else {
        Err("not found id".to_string())
    }
}

fn id_data_params(mut data: JsonValue) -> Result<(ID,JsonValue,JsonValue),String> {
    if let Some(id) = data.array_remove(0).as_str() {
        match ID::new(id.as_bytes()) {
            Ok(id) => Ok((id, data.array_remove(0), data.array_remove(0))),
            Err(_) => Err(format!("incorrect id {}", id)),
        }
    } else {
        Err("not found id".to_string())
    }
}

impl Handler<Event> for Commutator {
    type Result = ();

    fn handle(&mut self, msg: Event, ctx: &mut Self::Context) -> Self::Result {
        let service = self.app.service(msg.path.as_str());
        let response = match msg.command.as_str() {
            "find" => service.find(msg.data),
            "get" => {
                let (id, params) = match id_params(msg.data) {
                    Ok(v) => v,
                    Err(err) => {
                        self.ack(msg.event_id,error_general(err.as_str()).to_string(), &msg.sid);
                        return;
                    }
                };
                service.get(id, params)
            },
            "create" => {
                let (data, params) = match data_params(msg.data) {
                    Ok(v) => v,
                    Err(err) => {
                        self.ack(msg.event_id,error_general(err.as_str()).to_string(), &msg.sid);
                        return;
                    }
                };
                service.create(data, params)
            },
            "update" => {
                let (id, data, params) = match id_data_params(msg.data) {
                    Ok(v) => v,
                    Err(err) => {
                        self.ack(msg.event_id,error_general(err.as_str()).to_string(), &msg.sid);
                        return;
                    }
                };
                service.update(id, data, params)
            },
            "patch" => {
                let (id, data, params) = match id_data_params(msg.data) {
                    Ok(v) => v,
                    Err(err) => {
                        self.ack(msg.event_id,error_general(err.as_str()).to_string(), &msg.sid);
                        return;
                    }
                };
                service.patch(id, data, params)
            },
            "remove" => {
                let (id, params) = match id_params(msg.data) {
                    Ok(v) => v,
                    Err(err) => {
                        self.ack(msg.event_id,error_general(err.as_str()).to_string(), &msg.sid);
                        return;
                    }
                };
                service.remove(id, params)
            },
            _ => {
                // format!("can't find command {:?}", msg.path)
                todo!()
            }
        };
        self.ack(msg.event_id,response.to_string(), &msg.sid);
    }
}

impl Handler<Connect> for Commutator {
    type Result = ();

    fn handle(&mut self, msg: Connect, ctx: &mut Self::Context) -> Self::Result {
        self.sessions.insert(
            msg.sid,
            msg.socket,
        );

        self.open(&msg.sid);
    }
}

impl Handler<Disconnect> for Commutator {
    type Result = ();

    fn handle(&mut self, msg: Disconnect, ctx: &mut Self::Context) -> Self::Result {
        if self.sessions.remove(&msg.sid).is_some() {
            // TODO remove from channels
        }
    }
}