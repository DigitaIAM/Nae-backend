#![allow(dead_code, unused)]
extern crate core;

use std::collections::HashMap;
use std::fs::File;
use std::io::{BufWriter, Write};
use lazy_static::lazy_static;
use std::sync::{Arc, RwLock};
use std::thread;
use std::time::{SystemTime, UNIX_EPOCH};
use actix::Actor;
use actix_web::{App, HttpServer, middleware, web};
use actix_web::dev::ServiceRequest;
use actix_web::http::header;
use actix_web_httpauth::extractors::AuthenticationError;
use actix_web_httpauth::extractors::bearer::{BearerAuth, Config};
use actix_web_httpauth::middleware::HttpAuthentication;
use dbase::{FieldValue, Record};
use crate::commutator::{Application, Commutator};
use std::path::PathBuf;
use structopt::StructOpt;

mod settings;
mod websocket;
mod commutator;
mod auth;
mod services;
mod ws;

mod api;
mod animo;
pub mod warehouse;
mod accounts;
mod text_search;
mod use_cases;

use animo::memory::Memory;
use animo::db::AnimoDB;
use crate::animo::memory::*;
use crate::animo::shared::*;
use crate::animo::{Animo, Time, Topology};
use crate::services::Services;
use crate::settings::Settings;
use crate::warehouse::store_aggregation_topology::WHStoreAggregationTopology;
use crate::warehouse::store_topology::WHStoreTopology;

pub type Decimal = f64; // rust_decimal::Decimal;

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

async fn server(settings: Arc<Settings>, db: AnimoDB) -> std::io::Result<()> {
    let address = "localhost"; // "127.0.0.1"
    let port = 3030;

    log::info!("starting up {:?}:{}", address, port);


    let mut app = Application::new(settings.clone(), Arc::new(db));
    app.register(crate::services::Authentication::new(app.clone(), "authentication"));

    let mut com = Commutator::new(app.clone()).start();

    HttpServer::new(move || {
        // let auth = HttpAuthentication::bearer(auth::validator);

        App::new()
            .app_data(web::Data::new(app.clone()))
          .app_data(web::Data::new(com.clone()))
            .wrap(middleware::Logger::default())
            // .wrap(auth)
            .service(
                web::scope("/socket.io")
                    .service(ws::start_connection)
            )
            .service(
                web::scope("/")
            )
            .service(
                web::scope("/v1")
                    // .service(websocket::start_connection_route)
                    .service(api::memory_query)
                    .service(api::memory_modify)
            )
            // .route("/ws/", web::get().to(websocket))
            .default_service(web::route().to(api::not_implemented))
    })
        .bind((address, port))?
        .run()
        .await
}

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    std::env::set_var("RUST_LOG", "actix_web=debug,actix_server=debug");
    env_logger::init();

    let opt = Opt::from_args();

    let settings = std::sync::Arc::new(Settings::new().unwrap());
    let db: AnimoDB = Memory::init(&settings.database.folder).unwrap();

    match opt.mode.as_str() {
        "server" => {
            server(settings, db).await
        }
        "import" => {
            match opt.case.as_str() {
                "001" => use_cases::uc_001::import(&db),
                "002" => use_cases::uc_002::import(&db),
                _ => unreachable!()
            }
            Ok(())
        }
        "report" => {
            match opt.case.as_str() {
                "001" => use_cases::uc_001::report(&db),
                "002" => use_cases::uc_002::report(&db),
                _ => unreachable!()
            }
            Ok(())
        }
        _ => unreachable!()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use actix_web::{App, test, web};
    use actix_web::web::Bytes;
    use crate::animo::memory::{ChangeTransformation, Transformation, TransformationKey, Value};
    use crate::warehouse::test_util::init;

    #[actix_web::test]
    async fn test_put_get() {
        let (tmp_dir, settings, db) = init();

        let app = test::init_service(
            App::new()
                .app_data(web::Data::new(db.clone()))
                .wrap(middleware::Logger::default())
                .service(api::memory_modify)
                .service(api::memory_query)
                .default_service(web::route().to(api::not_implemented))
        ).await;

        let changes = vec![
            ChangeTransformation {
                zone: *DESC,
                context: vec!["language".into(), "label".into()].into(),
                what: "english".into(),
                into_before: Value::Nothing,
                into_after: Value::String("language".into())
            }
        ];

        let req = test::TestRequest::post()
            .uri("/memory/modify")
            .set_json(changes)
            .to_request();

        let response = test::call_and_read_body(&app, req).await;
        assert_eq!(response, Bytes::from_static(b""));

        let keys: Vec<TransformationKey> = vec![
            TransformationKey {
                context: vec!["language".into(), "label".into()].into(),
                what: "english".into()
            }
        ];

        let req = test::TestRequest::post()
            .uri("/memory/query")
            .set_json(keys)
            .to_request();

        let response: Vec<Transformation> = test::call_and_read_body_json(&app, req).await;
        assert_eq!(response, vec![
            Transformation {
                context: vec!["language".into(), "label".into()].into(),
                what: "english".into(),
                into: Value::String("language".into())
            }
        ]);

        let req = test::TestRequest::post()
            .uri("/memory")
            .set_json("")
            .to_request();

        let response = test::call_service(&app, req).await;
        assert_eq!(response.status().to_string(), "501 Not Implemented");

        // stop db and delete data folder
        db.close();
        tmp_dir.close().unwrap();
    }
}