use crate::{Application, Services, ID};
use actix::ContextFutureSpawner;
use async_trait::async_trait;
use chrono::{DateTime, Datelike, SecondsFormat, Utc};
use crossbeam::channel::{bounded, Receiver, Sender, TryRecvError};
use futures::StreamExt;
use json::JsonValue;
use reqwest::{header, Client, Response, Url};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::io::Write;
use std::rc::Rc;
use std::slice::Iter;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::time::{Duration, SystemTime};
use tantivy::fastfield::FastValue;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tracing::{debug, error, event, info, info_span, trace, warn, Instrument};
use uuid::Uuid;

use crate::header::CONTENT_TYPE;
use crate::hik::auth::WithDigestAuth;
use crate::hik::data::alert_item::AlertItem;
use crate::hik::data::triggers_parser::TriggerItem;
use crate::hik::error::{Error, Result};
use crate::services::Mutation;
use crate::storage::SCamera;
use crate::utils::time::now_in_seconds;

#[derive(Debug, Serialize, Deserialize, Clone)]
// #[derive(Debug, PartialEq, Serialize, Deserialize, Clone)]
pub enum StatusCamera {
  Connecting(u64),
  Connected(u64),
  LastEvent(u64),
  Disconnect(u64),
  Error(u64, String),
}

impl Default for StatusCamera {
  fn default() -> Self {
    StatusCamera::disconnect()
  }
}

impl StatusCamera {
  pub(crate) fn ts(&self) -> u64 {
    match self {
      StatusCamera::Connecting(ts) => *ts,
      StatusCamera::Connected(ts) => *ts,
      StatusCamera::LastEvent(ts) => *ts,
      StatusCamera::Disconnect(ts) => *ts,
      StatusCamera::Error(ts, _) => *ts,
    }
  }

  pub fn connecting() -> Self {
    StatusCamera::Connecting(now_in_seconds())
  }

  pub fn connected() -> Self {
    StatusCamera::Connected(now_in_seconds())
  }

  pub fn event() -> Self {
    StatusCamera::LastEvent(now_in_seconds())
  }

  pub fn disconnect() -> Self {
    StatusCamera::Disconnect(now_in_seconds())
  }

  pub fn error(msg: String) -> Self {
    StatusCamera::Error(now_in_seconds(), msg)
  }

  pub fn from_json(data: &JsonValue) -> Option<Self> {
    println!("from_json {}", data.dump());
    let ts = match data["ts"].as_u64() {
      None => {
        println!("ERROR {} {}", data["ts"], data.dump());
        return None;
      },
      Some(n) => n,
    };
    println!("ts {ts}");
    match data["name"].as_str() {
      Some(name) => {
        let status = match name {
          "connecting" => StatusCamera::Connecting(ts),
          "connected" => StatusCamera::Connected(ts),
          "last-event" => StatusCamera::LastEvent(ts),
          "disconnect" => StatusCamera::Disconnect(ts),
          "error" => {
            let msg = data["msg"].as_str().unwrap_or("");
            StatusCamera::Error(ts, msg.to_string())
          },
          _ => return None,
        };
        Some(status)
      },
      None => None,
    }
  }

  pub fn to_json(&self) -> JsonValue {
    let (name, ts, msg) = {
      match self {
        StatusCamera::Connecting(ts) => ("connecting", ts, None),
        StatusCamera::Connected(ts) => ("connected", ts, None),
        StatusCamera::LastEvent(ts) => ("last-event", ts, None),
        StatusCamera::Disconnect(ts) => ("disconnect", ts, None),
        StatusCamera::Error(ts, msg) => ("error", ts, Some(msg.clone())),
      }
    };

    if let Some(msg) = msg {
      json::object! {
        name: name,
        ts: *ts,
        msg: msg
      }
    } else {
      json::object! {
        name: name,
        ts: *ts as f64,
      }
    }
  }
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub enum States {
  // 0 - switched OFF
  Disabled = 0,
  // 1 - switching ON
  Enabling = 1,
  // 2 - switched ON
  Enabled = 2,
  // 3 - switching OFF
  Disabling = 3,
}

#[derive(Debug, Serialize, Deserialize, Clone, Default)]
pub struct State {
  #[serde(default)]
  phase: Arc<AtomicUsize>,
}

impl State {
  pub(crate) fn get(&self) -> States {
    match self.phase.load(Ordering::SeqCst) {
      0 => States::Disabled,
      1 => States::Enabling,
      2 => States::Enabled,
      3 => States::Disabling,
      _ => panic!("must not happen"),
    }
  }

  pub(crate) fn is_on(&self) -> bool {
    let s = self.get();
    s == States::Enabling || s == States::Enabled
  }

  pub(crate) fn is_off(&self) -> bool {
    let s = self.get();
    s == States::Disabling || s == States::Disabled
  }

  fn set(&mut self, f: usize, t: usize) -> bool {
    f == match self.phase.compare_exchange(f, t, Ordering::SeqCst, Ordering::SeqCst) {
      Ok(v) => v,
      Err(v) => v,
    }
  }

  pub(crate) fn force(&mut self, phase: States) {
    self.phase.store(phase as usize, Ordering::SeqCst);
  }

  // from "switched OFF" to "switching ON"
  pub(crate) fn enabling(&mut self) -> bool {
    self.set(0, 1)
  }

  // from "switching ON" to "switched ON"
  pub(crate) fn enabled(&mut self) -> bool {
    self.set(1, 2)
  }

  // from "switched ON" to "switching OFF"
  pub(crate) fn disabling(&mut self) -> bool {
    self.set(2, 3)
  }

  // from "switching OFF" to "switched OFF"
  pub(crate) fn disabled(&mut self) -> bool {
    self.set(3, 0)
  }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
// #[derive(Debug, PartialEq, Serialize, Deserialize, Clone)]
pub struct ConfigCamera {
  // #[serde(skip_deserializing)]
  pub id: crate::ID,
  pub oid: crate::ID,

  pub name: String,

  pub dev_index: String,
  pub event_type: String,

  pub protocol: String,
  pub ip: String,
  pub port: Option<u16>,
  pub username: String,
  pub password: String,

  #[serde(default)]
  pub state: State,
  pub status: StatusCamera,

  #[serde(skip_serializing)]
  #[serde(skip_deserializing)]
  pub jh: Option<Arc<JoinHandle<()>>>,
}

impl ConfigCamera {
  pub(crate) fn connect(config: Arc<Mutex<ConfigCamera>>, app: Application, storage: SCamera) {
    let state = {
      let config = config.lock().unwrap();
      config.state.get()
    };

    if state == States::Enabling {
      println!("connect {state:?}");
      crate::hik::connection(&config, app, storage);
      // {
      //   let mut config = config.lock().unwrap();
      //   // TODO config.jh = Some(Arc::new(jh));
      // }
    }
  }

  pub(crate) fn to_json(&self) -> JsonValue {
    let enabled = match self.state.get() {
      States::Disabled => false,
      States::Enabling => true,
      States::Enabled => true,
      States::Disabling => false,
    };

    json::object! {
      _id: self.id.to_base64(),
      oid: self.oid.to_base64(),
      eventType: self.event_type.clone(),
      devIndex: self.dev_index.clone(),
      name: self.name.clone(),
      protocol: self.protocol.clone(),
      ip: self.ip.clone(),
      port: self.port.map(|n|n.to_string()).unwrap_or("".to_string()),
      username: self.username.clone(),
      status: self.status.to_json(),
      enabled: enabled,
    }
  }

  pub(crate) fn data(&self) -> Result<String> {
    serde_json::to_string(self).map_err(|e| Error::IOError(format!("fail to generate data json")))
  }
}

fn send_update(app: &Application, oid: ID, cid: ID, status: StatusCamera) {
  println!("send_update {:?}", status);
  let change = json::object! {
    status: status.to_json()
  };
  // app.service("cameras")
  //   .patch(id, change, JsonValue::Null);
  let mutation = Mutation::Patch(
    "cameras".into(),
    cid.to_base64(),
    change,
    json::object! {
      oid: oid.to_base64()
    },
  );
  match app.handle(mutation) {
    Ok(_) => {},
    Err(e) => {
      println!("error {e}");
    },
  }
}

pub fn connection(
  config: &Arc<Mutex<crate::hik::ConfigCamera>>,
  app: Application,
  storage: SCamera,
) {
  println!("Start camera connection...");
  let config = config.clone();
  // let id = {
  //   let config = config.lock().unwrap();
  //   config.id
  // };
  // let logging_span = info_span!("Camera coms", id=%id);
  // tokio::spawn(move || { processing(app, config, path); })
  // tokio::task::spawn(processing(app, config, path))

  // tokio::task::spawn_blocking(move ||
  tokio::spawn(async move {
    {
      let mut config = config.lock().unwrap();
      config.state.enabled();
    }

    processing(app, config.clone(), storage).await;

    {
      let mut config = config.lock().unwrap();
      config.state.disabled();
    }
  });

  // std::thread::spawn(move || processing(app, config, path));
  // .instrument(logging_span),
}

fn should_stop(config: &Arc<Mutex<crate::hik::ConfigCamera>>) -> bool {
  let state = {
    let config = config.lock().unwrap();
    config.state.is_off()
  };
  if state {
    // info!("Configuration disabled, exiting");
    println!("Configuration disabled, exiting");
    true
  } else {
    // println!("Configuration enabled, continue");
    false
  }
}

async fn wait(secs: i64, config: &Arc<Mutex<crate::hik::ConfigCamera>>) -> bool {
  println!("wait {secs}");
  if secs > 0 {
    let wait_till = Utc::now() + chrono::Duration::seconds(secs);
    while Utc::now() <= wait_till {
      tokio::time::sleep(Duration::from_secs(1)).await;
      if should_stop(config) {
        return true;
      }
    }
  }
  should_stop(config)
}

async fn processing(
  app: Application,
  config: Arc<Mutex<crate::hik::ConfigCamera>>,
  storage: SCamera,
) {
  // info!("Initiating camera connection...");
  println!("Initiating camera connection...");
  let (cid, oid) = {
    let config = config.lock().unwrap();
    (config.id, config.oid)
  };

  let mut wait_on_error = 0;

  'outer: loop {
    // wait on error
    if wait(wait_on_error, &config).await {
      break;
    }

    let mut cam = {
      let config = {
        let config = config.lock().unwrap();
        config.clone()
      };
      send_update(&app, oid, cid, StatusCamera::connecting());
      match crate::hik::Camera::connect(config, storage.clone()).await {
        Ok(cam) => {
          send_update(&app, oid, cid, StatusCamera::connected());
          wait_on_error = 0;

          cam
        },
        Err(e) => {
          // warn!("Camera errored: {}. Attempting reconnection...", e);
          println!("Camera errored: {}. Attempting reconnection...", e);

          send_update(&app, oid, cid, StatusCamera::error(e.to_string()));

          let mins15 = 15 * 60;
          if wait_on_error < mins15 {
            wait_on_error = mins15;
          }
          // if wait_on_error < 120 {
          //   wait_on_error += 15;
          // }
          continue;
        },
      }
    };

    loop {
      if wait(wait_on_error, &config).await {
        break;
      }

      let next = cam.next_event().await;
      match next {
        Ok(event) => {
          println!("event: {event}");
          if !event.is_object() {
            if wait(61, &config).await {
              break 'outer;
            }
          } else {
            // TODO debounce it
            send_update(&app, oid, cid, StatusCamera::event());

            let data = json::object! {
              oid: oid.to_base64(),
              cid: cid.to_base64(),
              event: event
            };

            app.service("events").create(data, JsonValue::Null);

            // tokio::time::sleep(Duration::from_secs(5)).await;
          }
        },
        Err(e) => {
          // warn!("Camera errored: {}. Attempting reconnection...", e);
          println!("Camera errored: {}. Attempting reconnection...", e);

          send_update(&app, oid, cid, StatusCamera::error(e.to_string()));

          if wait_on_error < 15 * 60 {
            wait_on_error += 5 * 60;
          }
          break;
        },
      }
    }
  }

  send_update(&app, oid, cid, StatusCamera::disconnect());
}

pub struct Camera {
  storage: SCamera,
  pub config: ConfigCamera,
  pub info: crate::hik::data::device_info::DeviceInfo,
  pub triggers: Option<Vec<TriggerItem>>,
  events: Box<dyn Events + Send>,
}

impl Camera {
  pub async fn connect(config: ConfigCamera, storage: SCamera) -> Result<Camera> {
    let client = reqwest::Client::builder()
      .tcp_keepalive(Duration::from_secs(60))
      .danger_accept_invalid_certs(true)
      .build()
      .map_err(Error::ConnectionError)?;

    let info = {
      let info_text = Self::get("/ISAPI/System/deviceInfo", &client, &config)
        .await?
        .text()
        .await
        .map_err(Error::CameraInvalidResponseBody)?;

      let info_text = info_text.trim().to_string();

      println!("deviceInfo: '{info_text}'");

      crate::hik::data::device_info::DeviceInfo::parse(&info_text)
        .map_err(Error::DeviceInfoInvalid)?
    };

    println!("deviceInfo: {:?}", info);

    // let triggers = {
    //   let triggers_text = Self::get("/ISAPI/Event/triggers", &client, &config)
    //     .await?
    //     .text()
    //     .await
    //     .map_err(Error::CameraInvalidResponseBody)?;
    //   println!("triggers_text {}", triggers_text);
    //   TriggerItem::parse(&triggers_text)
    //     .map_err(Error::TriggersInvalid)?
    // };

    let events: Box<dyn Events + Send> = {
      if config.dev_index.is_empty() {
        let res = Self::get("/ISAPI/Event/notification/alertStream", &client, &config).await?;
        let content_type: mime::Mime = res
          .headers()
          .get(header::CONTENT_TYPE)
          .ok_or_else(|| Error::StreamInvalid("Content type header missing on stream".into()))?
          .to_str()
          .map_err(|e| Error::StreamInvalid(format!("Content type header invalid string: {}", e)))?
          .parse()
          .map_err(|e| Error::StreamInvalid(format!("Content type invalid format: {}", e)))?;
        if content_type.type_() != "multipart" {
          return Err(Error::StreamInvalid(format!(
            "Content type on stream should have been multipart. Instead it was {}",
            content_type.type_()
          )));
        }
        let boundary = content_type
          .get_param(mime::BOUNDARY)
          .ok_or_else(|| Error::StreamInvalid("Multipart stream has no boundary set".to_string()))?;

        let stream: std::pin::Pin<
          Box<
            dyn futures::Stream<
                Item = std::result::Result<multipart_stream::Part, multipart_stream::parser::Error>,
              > + Send,
          >,
        > = Box::pin(multipart_stream::parse(res.bytes_stream(), boundary.as_str()));
        Box::new(EventsOnStream { stream, storage: storage.clone(), config: config.clone() })
      } else {
        Box::new(EventsOnRequest::new(config.clone(), client))
      }
    };

    Ok(Camera { storage, info, config, triggers: None, events })
  }

  pub async fn next_event(&mut self) -> Result<JsonValue> {
    self.events.next().await
    // trace!(cam=?self.config.identifier(), contents=?part_str, "Camera Alert");
    // AlertItem::parse(&part_str)
    //   .map_err(Error::AlertInvalid)
  }

  fn url(path: &str, client: &reqwest::Client, config: &ConfigCamera) -> Result<Url> {
    let url = if config.dev_index.is_empty() {
      format!(
        "{}://{}{}{}",
        config.protocol,
        config.ip,
        config.port.map(|p| format!(":{}", p)).unwrap_or_default(),
        path
      )
    } else if path.contains("?") {
      format!(
        "{}://{}{}{}&devIndex={}",
        config.protocol,
        config.ip,
        config.port.map(|p| format!(":{}", p)).unwrap_or_default(),
        path,
        config.dev_index
      )
    } else {
      format!(
        "{}://{}{}{}?devIndex={}",
        config.protocol,
        config.ip,
        config.port.map(|p| format!(":{}", p)).unwrap_or_default(),
        path,
        config.dev_index
      )
    };

    println!("url {}", url);

    reqwest::Url::parse(url.as_str()).map_err(|e| Error::UrlError(e.to_string()))
  }

  async fn get(path: &str, client: &reqwest::Client, config: &ConfigCamera) -> Result<Response> {
    client
      .get(Self::url(path, client, config)?)
      .send_with_digest_auth(&config.username, &config.password)
      .await
  }

  async fn post(
    path: &str,
    body: String,
    client: &reqwest::Client,
    config: &ConfigCamera,
  ) -> Result<Response> {
    client
      .post(Self::url(path, client, config)?)
      .body(body)
      .send_with_digest_auth(&config.username, &config.password)
      .await
  }
}

#[async_trait]
trait Events {
  async fn next(&mut self) -> Result<JsonValue>;
}

struct EventsOnRequest {
  config: ConfigCamera,
  client: Client,

  response: Option<JsonValue>,

  items: Vec<JsonValue>,

  position: usize,
  total: usize,

  wait_till: DateTime<Utc>,
}

impl EventsOnRequest {
  fn new(config: ConfigCamera, client: Client) -> Self {
    EventsOnRequest {
      config,
      client,
      response: None,
      items: vec![],
      position: 0,
      total: 0,
      wait_till: Utc::now(),
    }
  }
}

#[async_trait]
impl Events for EventsOnRequest {
  async fn next(&mut self) -> Result<JsonValue> {
    if self.response.is_none() {
      let secs = (self.wait_till - Utc::now()).num_seconds();
      println!("Duration {secs}");
      if secs > 0 {
        tokio::time::sleep(std::time::Duration::from_secs(secs as u64)).await;
      }
      let body = json::object! {
        "AcsEventSearchDescription": json::object! {
          "searchID": "1",
          "searchResultPosition": self.position,
          "maxResults": 100,
          "AcsEventFilter": json::object! {
            "major": 5,
            "minor": 75
          }
        }
      }
      .dump();

      let data =
        Camera::post("/ISAPI/AccessControl/AcsEvent?format=json", body, &self.client, &self.config)
          .await?
          .text()
          .await
          .map_err(Error::CameraInvalidResponseBody)?;

      let data = json::parse(data.as_str()).map_err(|e| Error::IOError(e.to_string()))?;

      self.response = Some(data);

      if let Some(data) = self.response.as_ref() {
        let result = &data["AcsEventSearchResult"];
        let total = &result["totalMatches"].as_usize().unwrap_or(0);
        let list = &result["MatchList"];

        self.items = list.members().into_iter().map(|item| item.clone()).collect();
        self.total = *total;
      }
    }

    if self.items.len() != 0 {
      self.position += 1;

      return Ok(self.items.remove(0));
    }

    if self.position >= self.total {
      self.wait_till = Utc::now() + chrono::Duration::minutes(15);
      self.position = 0;
      self.response = None;
    } else {
      self.response = None;
    }

    Ok(JsonValue::Null)
  }
}

struct EventsOnStream {
  storage: SCamera,
  config: ConfigCamera,
  stream: std::pin::Pin<
    Box<
      dyn futures::Stream<
          Item = std::result::Result<multipart_stream::Part, multipart_stream::parser::Error>,
        > + Send,
    >,
  >,
}

#[async_trait]
impl Events for EventsOnStream {
  async fn next(&mut self) -> Result<JsonValue> {
    let next = self
      .stream
      .next()
      .await
      .ok_or(Error::ConnectionClosed)?
      .map_err(|e| Error::StreamInvalid(format!("Couldn't get next part of stream: {}", e)))?;
    match next.headers.get(CONTENT_TYPE) {
      Some(ct) => match ct.to_str() {
        Ok(v) => match v {
          "application/json; charset=\"UTF-8\"" => {
            let part_str = String::from_utf8(next.body.to_vec())
              .map_err(|e| Error::StreamInvalid(format!("Stream returned non-UTF-8 text: {}", e)))?;
            json::parse(part_str.as_str())
              .map_err(|e| Error::IOError(format!("fail to parse: {:?} {:?}", e, part_str)))
          },
          "image/jpeg" => {
            let ts = chrono::offset::Utc::now();

            let path = self
              .storage
              .save_binary(ts, "img_", ".jpeg", &next.body)
              .map_err(|e| Error::IOError(e.to_string()))?;

            Ok(json::object! {
              timestamp: ts.timestamp_millis(),
              contentType: "image/jpeg",
              path: path.to_string_lossy().to_string(),
            })
          },
          _ => Err(Error::StreamInvalid(format!("unknown content type {:?} {:?}", v, next))),
        },
        Err(e) => Err(Error::StreamInvalid(format!("fail to get content-type: {:?}", e))),
      },
      None => Err(Error::StreamInvalid(format!("no content-type case: {:?}", next))),
    }
  }
}
