use actix_web::{post, web, Responder, HttpResponse, Error};

use crate::Memory;
use crate::animo::memory::{ChangeTransformation, TransformationKey};
use crate::animo::db::AnimoDB;

pub(crate) async fn not_implemented() -> impl Responder {
    HttpResponse::NotImplemented().json("")
}

#[post("/memory/query")]
pub(crate) async fn memory_query(db: web::Data<AnimoDB>, keys: web::Json<Vec<TransformationKey>>) -> Result<HttpResponse, Error> {
    // use web::block to offload db request
    let transformations = // TODO web::block(move || {
        db.query(keys.0)
    // })
    //     .await?
        .map_err(actix_web::error::ErrorInternalServerError)?;

    Ok(HttpResponse::Ok().json(transformations))
}

#[post("/memory/modify")]
pub(crate) async fn memory_modify(
    db: web::Data<AnimoDB>, mutations: web::Json<Vec<ChangeTransformation>>
) -> Result<HttpResponse, Error> {
    // use web::block to offload db request
    // TODO web::block(move || {
        db.modify(mutations.0)
    // })
    //     .await?
        .map_err(actix_web::error::ErrorInternalServerError)?;

    Ok(HttpResponse::Ok().body(""))
}