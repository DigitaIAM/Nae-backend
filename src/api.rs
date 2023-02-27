use std::collections::HashMap;

use actix_web::http::header;
use actix_web::{get, post, web, Error, HttpRequest, HttpResponse, Responder};
use json::{object, JsonValue};

use crate::animo::db::AnimoDB;
use crate::animo::memory::{ChangeTransformation, TransformationKey};
use crate::commutator::Application;
use crate::services::Services;
use crate::animo::memory::Memory;
use qstring::QString;

pub async fn not_implemented() -> impl Responder {
  HttpResponse::NotImplemented().json("")
}

#[post("/memory/query")]
pub(crate) async fn memory_query(
  db: web::Data<AnimoDB>,
  keys: web::Json<Vec<TransformationKey>>,
) -> Result<HttpResponse, Error> {
  // use web::block to offload db request
  let transformations = web::block(move || db.query(keys.0))
    .await?
    .map_err(actix_web::error::ErrorInternalServerError)?;

  Ok(HttpResponse::Ok().json(transformations))
}

#[post("/memory/modify")]
pub(crate) async fn memory_modify(
  db: web::Data<AnimoDB>,
  mutations: web::Json<Vec<ChangeTransformation>>,
) -> Result<HttpResponse, Error> {
  // use web::block to offload db request
  web::block(move || db.modify(mutations.0))
    .await?
    .map_err(actix_web::error::ErrorInternalServerError)?;

  Ok(HttpResponse::Ok().body(""))
}

#[post("/api/docs")]
pub async fn docs_create(
  req: HttpRequest,
  app: web::Data<Application>,
  data: web::Json<serde_json::Value>,
  params: web::Query<HashMap<String, String>>,
) -> Result<HttpResponse, Error> {
  let data = json::parse(&data.0.to_string()).unwrap();

  let ctx: Vec<String> = params["ctx"].split(",").map(|s| s.to_string()).collect();
  let oid = params["oid"].clone();

  let params: JsonValue = object! {"ctx": ctx, "oid": oid};

  let result = web::block(move || app.service("docs").create(data, params))
    .await?
    .map_err(actix_web::error::ErrorInternalServerError)?;

  let result: serde_json::Value = serde_json::from_str(&result.dump()).unwrap();

  Ok(HttpResponse::Ok().json(result))
}

#[post("/api/docs/update")]
pub async fn docs_update(
  req: HttpRequest,
  // path: web::Path<(String)>,
  app: web::Data<Application>,
  data: web::Json<serde_json::Value>,
  params: web::Query<HashMap<String, String>>,
) -> Result<HttpResponse, Error> {
  println!("docs_update {req:?}");

  // let (id) = path.into_inner();

  let data = json::parse(&data.0.to_string()).unwrap();

  let id = params["id"].clone();

  let ctx: Vec<String> = params["ctx"].split(",").map(|s| s.to_string()).collect();
  let oid = params["oid"].clone();

  let params: JsonValue = object! {"ctx": ctx, "oid": oid};

  let result = web::block(move || app.service("docs").update(id, data, params))
    .await?
    .map_err(actix_web::error::ErrorInternalServerError)?;

  let result: serde_json::Value = serde_json::from_str(&result.dump()).unwrap();

  Ok(HttpResponse::Ok().json(result))
}

#[get("/api/docs/find")]
pub async fn docs_find(
  req: HttpRequest,
  app: web::Data<Application>,
  params: web::Query<HashMap<String, String>>,
) -> Result<HttpResponse, Error> {

  let ctx: Vec<String> = params["ctx"].split(",").map(|s| s.to_string()).collect();

  let oid = params["oid"].clone();

  let filter = params["filter"].clone();

  let params: JsonValue = object! {"oid": oid, "ctx": ctx, "limit": 2, "skip": 0, "filter": filter};

  let result = web::block(move || app.service("docs").find(params))
      .await?
      .map_err(actix_web::error::ErrorInternalServerError)?;

  let result: serde_json::Value = serde_json::from_str(&result.dump()).unwrap();

  Ok(HttpResponse::Ok().json(result))
}

#[get("/api/inventory")]
pub async fn inventory_find(
  req: HttpRequest,
  app: web::Data<Application>,
  params: web::Query<HashMap<String, String>>,
) -> Result<HttpResponse, Error> {
  let ctx: Vec<String> = params["ctx"].split(",").map(|s| s.to_string()).collect();
  let oid = params["oid"].clone();

  let from_date = params["from_date"].clone();
  let till_date = params["till_date"].clone();
  let storage = params["storage"].clone();

  let params: JsonValue = object! {"ctx": ctx, "oid": oid, "storage": storage, dates: {"from": from_date, "till": till_date}};

  let result = web::block(move || app.service("inventory").find(params))
    .await?
    .map_err(actix_web::error::ErrorInternalServerError)?;

  // let result: serde_json::Value = serde_json::from_str(&result.dump()).unwrap();

  let res = HttpResponse::Ok()
    .append_header(header::ContentType(mime::APPLICATION_JSON))
    .body(result.dump());

  Ok(res)
}
