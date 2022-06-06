#[macro_use]
extern crate log;

#[macro_use]
extern crate lazy_static;

extern crate core;

use actix::Actor;
use actix_web::{web, middleware, App, HttpServer};
use crate::commutator::Commutator;

mod websocket;
mod commutator;

mod error;
mod shared;
mod memory;
mod rocksdb;
mod api;
mod animo;
mod warehouse;

use crate::memory::Memory;
use crate::rocksdb::RocksDB;

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    std::env::set_var("RUST_LOG", "actix_web=debug,actix_server=debug");
    env_logger::init();

    info!("starting up 127.0.0.1:8080");

    let db: RocksDB = Memory::init("./data/memory").unwrap();
    let communicator = Commutator::new(db.clone()).start();

    HttpServer::new(move || {
        App::new()
            .app_data(web::Data::new(db.clone()))
            .app_data(communicator.clone())
            .wrap(middleware::Logger::default())
            .service(
                web::scope("/v1")
                    .service(websocket::start_connection_route)
                    .service(api::memory_query)
                    .service(api::memory_modify)
            )
            // .route("/ws/", web::get().to(websocket))
            .default_service(web::route().to(api::not_implemented))
    })
        .bind(("127.0.0.1", 8080))?
        .run()
        .await
}

#[cfg(test)]
mod tests {
    use super::*;
    use actix_web::{test, web, App};
    use actix_web::web::Bytes;
    use crate::memory::{ChangeTransformation, Transformation, TransformationKey, Value};

    fn init() {
        std::env::set_var("RUST_LOG", "actix_web=debug,nae_backend=debug");
        let _ = env_logger::builder().is_test(true).try_init();
    }

    #[actix_web::test]
    async fn test_put_get() {
        init();

        let db: RocksDB = Memory::init("./data/tests").unwrap();

        let app = test::init_service(
            App::new()
                .app_data(web::Data::new(db))
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

        // TODO db.clear();
    }
}