#![allow(dead_code, unused)]
extern crate core;

use lazy_static::lazy_static;
use std::sync::RwLock;
use actix::Actor;
use actix_web::{App, HttpServer, middleware, web};
use actix_web::dev::ServiceRequest;
use actix_web::http::header;
use actix_web_httpauth::extractors::AuthenticationError;
use actix_web_httpauth::extractors::bearer::{BearerAuth, Config};
use actix_web_httpauth::middleware::HttpAuthentication;
use crate::commutator::Commutator;

mod settings;
mod websocket;
mod commutator;
mod auth;

mod api;
mod animo;
mod warehouse;
mod accounts;

use animo::memory::Memory;
use animo::db::AnimoDB;
use crate::settings::Settings;

// lazy_static! {
//     static ref SETTINGS: RwLock<Config> = RwLock::new(Config::default());
// }

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    std::env::set_var("RUST_LOG", "actix_web=debug,actix_server=debug");
    env_logger::init();

    let address = "localhost"; // "127.0.0.1"
    let port = 8080;

    log::info!("starting up {:?}:{:?}", address, port);

    let settings = std::sync::Arc::new(Settings::new().unwrap());

    let db: AnimoDB = Memory::init(&settings.database.folder).unwrap();
    let communicator = Commutator::new(db.clone()).start();

    HttpServer::new(move || {
        // let auth = HttpAuthentication::bearer(auth::validator);

        App::new()
            .app_data(web::Data::new(settings.clone()))
            .app_data(web::Data::new(db.clone()))
            .app_data(web::Data::new(communicator.clone()))
            .wrap(middleware::Logger::default())
            // .wrap(auth)
            .service(
                web::scope("/")
            )
            .service(
                web::scope("/v1")
                    .service(websocket::start_connection_route)
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