use crate::hik::actions::task::CommandMeta;
use crate::hik::auth::{digest_auth, RequestGenerator, WithDigestAuth};
use crate::hik::error::Error;
use crate::hik::ConfigCamera;
use crate::services::Mutation;
use crate::websocket::WsConn;
use crate::{Application, ID};
use actix::prelude::*;
use async_trait::async_trait;
use json::JsonValue;
use reqwest::{multipart, Body, Client, RequestBuilder, Response, Url};
use std::io::{BufReader, Read};
use std::path::PathBuf;
use std::pin::Pin;
use std::time::Duration;
use tokio::fs::File;
use tokio::runtime::Handle;
use tokio_util::codec::{BytesCodec, FramedRead};

use crate::utils::json::JsonParams;
use actix_interop::{critical_section, with_ctx, FutureInterop};
use chrono::{Datelike, Utc};
use rand::Rng;
use reqwest::multipart::Form;
use tokio::io::AsyncReadExt;

#[async_trait]
trait ToHttpRequest {
  async fn request(&self, client: Client) -> Result<Response, Error>;
}

#[derive(Debug, Message)]
#[rtype(result = "()")]
pub struct GetRequest {
  pub id: ID,

  pub url: Url,
  pub username: String,
  pub password: String,
}

#[async_trait]
impl ToHttpRequest for GetRequest {
  async fn request(&self, client: Client) -> Result<Response, Error> {
    println!("request");
    client
      .get(self.url.clone())
      .send_with_digest_auth(&self.username, &self.password)
      .await
  }
}

#[derive(Debug, Message)]
#[rtype(result = "()")]
pub struct PostRequest {
  pub id: ID,

  pub url: Url,
  pub username: String,
  pub password: String,

  pub body: String,
}

#[async_trait]
impl ToHttpRequest for PostRequest {
  async fn request(&self, client: Client) -> Result<Response, Error> {
    client
      .post(self.url.clone())
      .body(self.body.clone())
      .send_with_digest_auth(&self.username, &self.password)
      .await
  }
}

#[derive(Debug, Message)]
#[rtype(result = "()")]
pub struct PutRequest {
  pub id: ID,

  pub url: Url,
  pub username: String,
  pub password: String,

  pub json: String,
}

struct DataJson {
  client: Client,
  url: Url,
  json: String,
}

#[async_trait]
impl RequestGenerator for DataJson {
  async fn request(&self) -> crate::hik::error::Result<RequestBuilder> {
    let bytes = self.json.as_bytes().to_vec();
    let record = multipart::Part::bytes(bytes)
      .mime_str("application/json")
      .map_err(|e| Error::IOError(e.to_string()))?;

    let parts = multipart::Form::new() // custom(gen_boundary())
      .percent_encode_noop()
      .part("FaceDataRecord", record);

    Ok(self.client.put(self.url.clone()).multipart(parts))
  }
}

#[derive(Debug, Message)]
#[rtype(result = "()")]
pub struct PutMultiRequest {
  pub id: ID,

  pub url: Url,
  pub username: String,
  pub password: String,

  pub json: String,
  pub file: PathBuf,
}

struct Data {
  client: Client,
  url: Url,
  json: String,
  file: PathBuf,
}

fn gen_boundary() -> String {
  // use crate::util::fast_random as random;
  //
  // let a = random();
  // let b = random();
  // let c = random();
  // let d = random();
  //
  // format!("{:016x}{:016x}", a, b)
  // "--------------------------489343052633429561762437".to_string()
  let mut rng = rand::thread_rng();
  let a: u32 = rng.gen_range(0..999999);
  let b: u32 = rng.gen_range(0..999999);
  let c: u32 = rng.gen_range(0..999999);
  let d: u32 = rng.gen_range(0..999999);
  format!("--------------------------{:06}{:06}{:06}{:06}", a, b, c, d)
}

#[async_trait]
impl RequestGenerator for Data {
  async fn request(&self) -> crate::hik::error::Result<RequestBuilder> {
    let bytes = self.json.as_bytes().to_vec();
    let record = multipart::Part::bytes(bytes)
      .mime_str("application/json")
      .map_err(|e| Error::IOError(e.to_string()))?;

    let mut file = File::open(&self.file).await.unwrap();

    // let stream = FramedRead::new(file, BytesCodec::new());
    // let body = Body::wrap_stream(stream);
    // let file = multipart::Part::stream(body)
    let bytes = std::fs::read(&self.file)?;
    let file = multipart::Part::bytes(bytes)
      .file_name("response.jpg")
      .mime_str("image/jpeg")
      .map_err(|e| Error::IOError(e.to_string()))?;

    let parts = multipart::Form::new() // custom(gen_boundary())
      .percent_encode_noop()
      .part("FaceDataRecord", record)
      .part("img", file);

    Ok(self.client.put(self.url.clone()).multipart(parts))
  }
}

#[async_trait]
impl ToHttpRequest for PutRequest {
  async fn request(&self, client: Client) -> Result<Response, Error> {
    let url = self.url.clone();
    let json = self.json.clone();

    let builder = DataJson { client, url, json };

    digest_auth(builder, &self.username, &self.password).await
  }
}

#[async_trait]
impl ToHttpRequest for PutMultiRequest {
  async fn request(&self, client: Client) -> Result<Response, Error> {
    let url = self.url.clone();
    let json = self.json.clone();
    let file = self.file.clone();

    let builder = Data { client, url, json, file };

    digest_auth(builder, &self.username, &self.password).await
  }
}

pub struct DeviceMgmt {
  pub protocol: String,
  pub ip: String,
  pub port: Option<String>,

  pub username: String,
  pub password: String,
}

impl DeviceMgmt {
  pub(crate) fn new(camera: &ConfigCamera) -> Self {
    let protocol = camera.protocol.clone();
    let ip = camera.ip.clone();
    let port = camera.port.clone().map(|n| n.to_string());
    let username = camera.username.clone();
    let password = camera.password.clone();

    DeviceMgmt { protocol, ip, port, username, password }
  }

  pub(crate) fn list_devices(&self, id: ID) -> Result<PostRequest, Error> {
    let protocol = &self.protocol;
    let ip = &self.ip;
    let port = self.port.as_ref().map(|p| format!(":{}", p)).unwrap_or_default();
    let path = "/ISAPI/ContentMgmt/DeviceMgmt/deviceList?format=json";

    let url = format!("{protocol}://{ip}{port}{path}");
    let url = Url::parse(url.as_str()).map_err(|e| Error::UrlError(e.to_string()))?;

    let body = json::object! {
        "SearchDescription": {
            "position": 0,
            "maxResult": 100,
            "filter": {
                "devType": "AccessControl",
            }
        }
    };

    Ok(PostRequest {
      id,
      url,
      body: body.dump(),
      username: self.username.clone(),
      password: self.password.clone(),
    })
  }

  pub(crate) fn create_user(
    &self,
    id: ID,
    dev_index: &String,
    employee: ID,
    name: String,
    gender: String,
  ) -> Result<PostRequest, Error> {
    let protocol = &self.protocol;
    let ip = &self.ip;
    let port = self.port.as_ref().map(|p| format!(":{}", p)).unwrap_or_default();
    let path = format!("/ISAPI/AccessControl/UserInfo/Record?format=json&devIndex={dev_index}");

    let url = format!("{protocol}://{ip}{port}{path}");
    let url = Url::parse(url.as_str()).map_err(|e| Error::UrlError(e.to_string()))?;

    let date = Utc::now();
    let begin_time = format!("{:0>4}-{:0>2}-{:0>2}T00:00:00", date.year(), date.month(), date.day());

    let body = json::object! {
        "UserInfo": [
            {
                "employeeNo": employee.to_clear(),
                "name": name.clone(),
                "userType": "normal",
                "gender": gender.clone(),
                "Valid": {
                    "enable": true,
                    "beginTime": begin_time.clone(), // clone() is workaround
                    "endTime": "2032-10-24T23:59:59",
                    "timeType": "local"
                }
            }
        ]
    };

    // println!("body: {}", body.dump());

    Ok(PostRequest {
      id,
      url,
      body: body.dump(),
      username: self.username.clone(),
      password: self.password.clone(),
    })
  }

  pub(crate) fn register_picture(
    &self,
    id: ID,
    dev_index: String,
    employee: ID,
    file: PathBuf,
  ) -> Result<PutMultiRequest, Error> {
    let protocol = &self.protocol;
    let ip = &self.ip;
    let port = self.port.as_ref().map(|p| format!(":{}", p)).unwrap_or_default();
    let path = format!("/ISAPI/Intelligent/FDLib/FDSetUp?format=json&devIndex={dev_index}");

    let url = format!("{protocol}://{ip}{port}{path}");
    // println!("url {url}");
    let url = Url::parse(url.as_str()).map_err(|e| Error::UrlError(e.to_string()))?;

    let record = json::object! {
      "faceLibType": "blackFD",
      "FDID": "1",
      "FPID": employee.to_clear()
    };
    let json = record.dump();

    // let record = json::object! {"faceLibType":"blackFD","FDID":"1","FPID":"1"};
    // let json = record.dump();
    // println!("json: {json}");
    // let json = "{\"faceLibType\":\"blackFD\",\"FDID\":\"1\",\"FPID\":\"1\"}".to_string();

    // let picture_path = std::fs::canonicalize(picture_path)?;

    Ok(PutMultiRequest {
      id,
      url,
      json,
      file,
      username: self.username.clone(),
      password: self.password.clone(),
    })
  }

  pub(crate) fn delete_picture(
    &self,
    id: ID,
    dev_index: &String,
    employee: ID,
  ) -> Result<PutRequest, Error> {
    let protocol = &self.protocol;
    let ip = &self.ip;
    let port = self.port.as_ref().map(|p| format!(":{}", p)).unwrap_or_default();
    let path = format!("/ISAPI/Intelligent/FDLib/FDSetUp?format=json&devIndex={dev_index}");

    let url = format!("{protocol}://{ip}{port}{path}");
    // println!("url {url}");
    let url = Url::parse(url.as_str()).map_err(|e| Error::UrlError(e.to_string()))?;

    let record = json::object! {
      "faceLibType": "blackFD",
      "FDID": "1",
      "FPID": employee.to_clear(),
      "deleteFP": true,
    };
    let json = record.dump();

    Ok(PutRequest {
      id,
      url,
      json,
      username: self.username.clone(),
      password: self.password.clone(),
    })
  }
}

pub struct HttpClient {
  app: Application,
  client: Client,
}

impl HttpClient {
  pub fn new(app: Application) -> Self {
    // let proxy = reqwest::Proxy::http("http://localhost:8080").unwrap();

    let client = reqwest::Client::builder() // reqwest::blocking::Client::builder()
      .http1_title_case_headers()
      // .gzip(true)
      // .deflate(true)
      // .brotli(true)
      // .tcp_keepalive(Duration::from_secs(60))
      .danger_accept_invalid_certs(true)
      // .proxy(proxy)
      .build()
      .unwrap();

    HttpClient { app, client }
  }

  async fn process(app: Application, client: Client, id: ID, msg: impl ToHttpRequest) {
    let data = json::object! { "state": crate::hik::actions::task::Stage::requested().to_json() };
    let mutation = Mutation::Patch("actions".into(), id.to_base64(), data, JsonValue::Null);
    app.handle(mutation);

    let response = msg.request(client).await;

    let mutation = match response {
      Ok(response) => {
        let body = response.text().await.unwrap();
        // println!("response body: {body}");
        let mut data = match json::parse(body.as_str()) {
          Ok(data) => json::object! { "data": data },
          Err(_) => json::object! { "body": body },
        };
        data["state"] = crate::hik::actions::task::Stage::completed().to_json();
        Mutation::Patch("actions".into(), id.to_base64(), data, JsonValue::Null)
      },
      Err(e) => {
        // println!("response error: {e}");
        Mutation::Patch(
          "actions".into(),
          id.to_base64(),
          json::object! { "error": e.to_string(), "state": crate::hik::actions::task::Stage::completed().to_json() },
          JsonValue::Null,
        )
      },
    };

    app.handle(mutation);
  }
}

impl Actor for HttpClient {
  type Context = Context<Self>;
}

impl Handler<GetRequest> for HttpClient {
  type Result = ();

  fn handle(&mut self, msg: GetRequest, ctx: &mut Self::Context) -> Self::Result {
    let app = self.app.clone();
    let client = self.client.clone();
    actix_web::rt::spawn(async move { HttpClient::process(app, client, msg.id, msg).await });
  }
}

impl Handler<PostRequest> for HttpClient {
  type Result = ();

  fn handle(&mut self, msg: PostRequest, ctx: &mut Self::Context) -> Self::Result {
    let app = self.app.clone();
    let client = self.client.clone();
    actix_web::rt::spawn(async move { HttpClient::process(app, client, msg.id, msg).await });
  }
}

impl Handler<PutRequest> for HttpClient {
  type Result = ();

  fn handle(&mut self, msg: PutRequest, ctx: &mut Self::Context) -> Self::Result {
    let app = self.app.clone();
    let client = self.client.clone();
    actix_web::rt::spawn(async move { HttpClient::process(app, client, msg.id, msg).await });
  }
}

impl Handler<PutMultiRequest> for HttpClient {
  type Result = ();

  fn handle(&mut self, msg: PutMultiRequest, ctx: &mut Self::Context) -> Self::Result {
    let app = self.app.clone();
    let client = self.client.clone();
    actix_web::rt::spawn(async move { HttpClient::process(app, client, msg.id, msg).await });
  }
}
