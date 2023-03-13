#![allow(dead_code, unused)]
extern crate core;

#[macro_use]
extern crate quick_error;
extern crate json;
extern crate store;
extern crate actix;
extern crate jsonwebtoken;
extern crate rust_decimal;
extern crate tracing;
extern crate rkyv;
extern crate dbase;
extern crate reqwest;
extern crate csv;
extern crate uuid;
extern crate actix_web;
extern crate service;
extern crate chrono;

use crate::commutator::{Application, Commutator};
use actix::{Actor, Addr};
use actix_cors::Cors;
use actix_ratelimit::{MemoryStore, MemoryStoreActor, RateLimiter};
use actix_web::dev::ServiceRequest;
use actix_web::http::header;
use actix_web::{http, middleware, web, App, HttpServer};
use actix_web_httpauth::extractors::bearer::{BearerAuth, Config};
use actix_web_httpauth::extractors::AuthenticationError;
use actix_web_httpauth::middleware::HttpAuthentication;

use dbase::{FieldValue, Record};
use lazy_static::lazy_static;
use std::collections::HashMap;
use std::fs::File;
use std::io::{BufWriter, Write};
use std::path::PathBuf;
use std::sync::{Arc, RwLock};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use std::{io, thread};
use structopt::StructOpt;

mod auth;
mod commutator;
mod file;
mod inventory;
mod services;
mod settings;
mod storage;
mod websocket;
mod ws;

mod accounts;
mod animo;
mod api;
mod hr;
mod memories;
mod text_search;
mod use_cases;
pub mod warehouse;

mod hik;

use crate::animo::memory::*;
use crate::animo::shared::*;
use crate::animo::{Animo, Time, Topology};
use crate::hik::error::Error;
use crate::hik::services::actions::Actions;
use crate::hik::services::{Cameras, Events};
use crate::hr::services::attendance_report::AttendanceReport;
use crate::hr::services::companies::Companies;
use crate::hr::services::departments::Departments;
use crate::hr::services::shifts::Shifts;
use crate::memories::MemoriesInFiles;
use crate::services::People;
use service::Services;
use crate::settings::Settings;
use crate::storage::SOrganizations;
use crate::warehouse::store_aggregation_topology::WHStoreAggregationTopology;
use crate::warehouse::store_topology::WHStoreTopology;
use animo::db::AnimoDB;
use animo::memory::Memory;
use inventory::service::Inventory;

#[derive(StructOpt, Debug)]
#[structopt(name = "basic")]
struct Opt {
  /// Run mode
  #[structopt(short, long, default_value = "server")]
  mode: String,

  #[structopt(short = "c", long = "case", default_value = "001")]
  case: String,

  /// Data folder
  #[structopt(short, long, parse(from_os_str))]
  data: PathBuf,
}

async fn server(
  settings: Arc<Settings>,
  app: Application,
  com: Addr<Commutator>,
) -> std::io::Result<()> {
  let address = "localhost"; // "127.0.0.1"
  let port = 3030;

  log::info!("starting up {address:?}:{port}");

  HttpServer::new(move || {
    // let auth = HttpAuthentication::bearer(auth::validator);

    let cors = Cors::default()
      .allowed_origin("https://www.rust-lang.org/")
      .allow_any_origin()
      .allow_any_method()
      // .allowed_origin_fn(|origin, _req_head| {
      //   println!("origin {origin}");
      //   origin.as_bytes().ends_with(b".rust-lang.org")
      // })
      // .allowed_methods(vec!["GET", "POST"])
      // .allowed_headers(vec![http::header::AUTHORIZATION, http::header::ACCEPT])
      // .allowed_header(http::header::CONTENT_TYPE)
      .max_age(3600);

    App::new()
      // .wrap(
      //   RateLimiter::new(MemoryStoreActor::from(store.clone()).start())
      //     .with_interval(Duration::from_secs(60))
      //     .with_max_requests(100),
      // )
      .wrap(cors)
      .app_data(web::Data::new(app.clone()))
      .app_data(web::Data::new(com.clone()))
      .wrap(middleware::Logger::default())
      // .wrap(auth)
      .service(web::scope("/socket.io").service(ws::start_connection))
      .service(web::scope("/"))
      .service(
        web::scope("/v1")
          // .service(websocket::start_connection_route)
          .service(crate::file::get_file)
          .service(crate::file::post_file)
          .service(api::memory_query)
          .service(api::memory_modify),
      )
      // .route("/ws/", web::get().to(websocket))
      .default_service(web::route().to(api::not_implemented))
  })
  .bind((address, port))?
  .run()
  .await
}

async fn startup() -> std::io::Result<()> {
  std::env::set_var("RUST_LOG", "actix_web=debug,actix_server=debug");
  env_logger::init();

  let opt = Opt::from_args();

  let settings = std::sync::Arc::new(Settings::new().unwrap());
  println!("db starting up");
  let db: AnimoDB = Memory::init(settings.database.memory.clone()).unwrap();
  println!("db started up");

  println!("app starting up");
  let (mut app, events_receiver) = Application::new(settings.clone(), Arc::new(db))
    .await
    .map_err(|e| io::Error::new(io::ErrorKind::Unsupported, e))?;

  let storage = SOrganizations::new("./data/companies/");
  app.storage = Some(storage.clone());

  app.register(Actions::new(app.clone(), "actions", storage.clone()));

  app.register(crate::services::Authentication::new(app.clone(), "authentication"));
  app.register(crate::services::Users::new(app.clone(), "users"));

  app.register(Companies::new(app.clone(), storage.clone()));
  app.register(Departments::new(app.clone(), storage.clone()));
  app.register(People::new(app.clone(), storage.clone()));
  app.register(Shifts::new(app.clone(), storage.clone()));
  app.register(AttendanceReport::new(app.clone(), storage.clone()));

  app.register(Cameras::new(app.clone(), "cameras", storage.clone()));
  app.register(Events::new(app.clone(), "events", storage.clone()));

  app.register(MemoriesInFiles::new(app.clone(), "memories", storage.clone()));
  app.register(Inventory::new(app.clone()));

  println!("app started up");

  println!("com starting up");
  let mut com = Commutator::new(app.clone(), events_receiver).start();
  println!("com started up");

  match opt.mode.as_str() {
    "server" => server(settings, app, com).await,
    "import" => {
      match opt.case.as_str() {
        "001" => use_cases::uc_001::import(&app.db),
        "002" => use_cases::uc_002::import(&app.db),
        "003" => use_cases::uc_003::import(&app),
        "005" => use_cases::uc_005::import(&app),
        "006" => use_cases::uc_006::import(&app),
        "007" => use_cases::uc_007::import(&app),
        _ => unreachable!(),
      }
      Ok(())
    },
    "report" => {
      match opt.case.as_str() {
        "001" => use_cases::uc_001::report(&app.db),
        "002" => use_cases::uc_002::report(&app.db),
        "003" => use_cases::uc_003::report(&app),
        "005" => use_cases::uc_005::report(&app),
        "006" => use_cases::uc_006::report(&app),
        "007" => use_cases::uc_007::report(&app),
        _ => unreachable!(),
      }
      Ok(())
    },
    _ => unreachable!(),
  }
}

// fn main() {
//     let mut rt = tokio::runtime::Runtime::new().unwrap();
//     let local = tokio::task::LocalSet::new();
//     local.block_on(&mut rt, async move {
//         tokio::task::spawn_local(async move {
//             let local = tokio::task::LocalSet::new();
//             let sys = actix_rt::System::run_in_tokio("server", &local);
//             // define your actix-web app
//             // define your actix-web server
//             sys.await;
//         });
//         // This still allows use of tokio::spawn
//     });
// }

#[actix_web::main]
async fn main() -> std::io::Result<()> {
  startup().await
}

#[cfg(test)]
mod tests {
  use super::*;
  use crate::animo::memory::{ChangeTransformation, Transformation, TransformationKey, Value};
  use crate::warehouse::test_util::init;
  use actix_web::web::Bytes;
  use actix_web::{test, web, App};

  #[actix_web::test]
  async fn test_put_get() {
    let (tmp_dir, settings, db) = init();

    let app = test::init_service(
      App::new()
        .app_data(web::Data::new(db.clone()))
        .wrap(middleware::Logger::default())
        .service(api::memory_modify)
        .service(api::memory_query)
        .default_service(web::route().to(api::not_implemented)),
    )
    .await;

    let changes = vec![ChangeTransformation {
      zone: *DESC,
      context: vec!["language".into(), "label".into()].into(),
      what: "english".into(),
      into_before: Value::Nothing,
      into_after: Value::String("language".into()),
    }];

    let req = test::TestRequest::post().uri("/memory/modify").set_json(changes).to_request();

    let response = test::call_and_read_body(&app, req).await;
    assert_eq!(response, Bytes::from_static(b""));

    let keys: Vec<TransformationKey> = vec![TransformationKey {
      context: vec!["language".into(), "label".into()].into(),
      what: "english".into(),
    }];

    let req = test::TestRequest::post().uri("/memory/query").set_json(keys).to_request();

    let response: Vec<Transformation> = test::call_and_read_body_json(&app, req).await;
    assert_eq!(
      response,
      vec![Transformation {
        context: vec!["language".into(), "label".into()].into(),
        what: "english".into(),
        into: Value::String("language".into())
      }]
    );

    let req = test::TestRequest::post().uri("/memory").set_json("").to_request();

    let response = test::call_service(&app, req).await;
    assert_eq!(response.status().to_string(), "501 Not Implemented");

    // stop db and delete data folder
    db.close();
    tmp_dir.close().unwrap();
  }
}
