use actix_web::{post, web, Responder, HttpResponse};

use crate::Memory;
use crate::rocksdb::RocksDB;

const JSON: &str = "application/json";

pub(crate) async fn not_found() -> impl Responder {
    HttpResponse::NotImplemented().content_type(JSON).finish()
}

#[post("/memory/query")]
pub(crate) async fn memory_query(db: web::Data<RocksDB>, body: web::Bytes) -> impl Responder {
    match String::from_utf8(body.to_vec()) {
        Ok(body) => {
            match serde_json::from_str(body.as_str()) {
                Ok(keys) => {
                    match &db.query(keys) {
                        Ok(records) => {
                            match serde_json::to_string(records) {
                                Ok(json) => HttpResponse::Ok().content_type(JSON).body(json),
                                Err(_) => HttpResponse::InternalServerError().content_type(JSON).finish()
                            }
                        },
                        Err(_) => HttpResponse::InternalServerError().content_type(JSON).finish()
                    }
                }
                Err(_) => HttpResponse::InternalServerError().content_type(JSON).finish()
            }
        }
        Err(_) => HttpResponse::InternalServerError().content_type(JSON).finish()
    }
}

#[post("/memory/modify")]
pub(crate) async fn memory_modify(db: web::Data<RocksDB>, body: web::Bytes) -> impl Responder {
    match String::from_utf8(body.to_vec()) {
        Ok(body) => {
            match serde_json::from_str(body.as_str()) {
                Ok(mutations) => {
                    match &db.modify(mutations) {
                        Ok(_) => HttpResponse::Ok().content_type(JSON).body(""),
                        Err(_) => HttpResponse::InternalServerError().content_type(JSON).finish()
                    }
                }
                Err(_) => HttpResponse::InternalServerError().content_type(JSON).finish()
            }
        }
        Err(_) => HttpResponse::InternalServerError().content_type(JSON).finish()
    }
}