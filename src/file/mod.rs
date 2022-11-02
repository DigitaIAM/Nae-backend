use std::io::{Read, Write};
use std::path::Path;

use crate::animo::error::DBError;
use crate::{AnimoDB, Application, ID};
use actix_files::Files;
use actix_multipart::Multipart;
use actix_web::http::header::{ContentDisposition, ContentType};
use actix_web::{
  get, middleware, post, put, web, App, Error, HttpRequest, HttpResponse, HttpServer,
};
use futures::TryStreamExt;
use json::JsonValue;
use mime::Mime;
use qstring::QString;
use uuid::Uuid;

#[get("/picture")]
pub(crate) async fn get_file(
  req: HttpRequest,
  app: web::Data<Application>,
) -> Result<HttpResponse, Error> {
  let qs = QString::from(req.query_string());

  let oid = match ID::from_base64(qs.get("oid").unwrap_or_default().as_bytes()) {
    Ok(id) => id,
    Err(e) => return Ok(HttpResponse::from_error(e)),
  };

  let pid = match ID::from_base64(qs.get("pid").unwrap_or_default().as_bytes()) {
    Ok(id) => id,
    Err(e) => return Ok(HttpResponse::from_error(e)),
  };

  println!("get_file {oid} {pid}");

  let path = app.storage.as_ref().unwrap().get(&oid).person(&pid).picture().path();

  println!("path {path:?}");

  let file = actix_files::NamedFile::open_async(path).await?;

  // let reader = chunked::new_chunked_read(file.metadata().len(), 0, file.file());
  // Ok(
  //   HttpResponse::Ok()
  //     .content_type(ContentType::jpeg())
  //     .insert_header(ContentDisposition::attachment(filename))
  //     .streaming(reader),
  // )

  Ok(file.into_response(&req))
}

#[post("/picture")]
pub(crate) async fn post_file(
  req: HttpRequest,
  app: web::Data<Application>,
  mut payload: Multipart,
) -> Result<HttpResponse, Error> {
  let qs = QString::from(req.query_string());

  let oid = match ID::from_base64(qs.get("oid").unwrap_or_default().as_bytes()) {
    Ok(id) => id,
    Err(e) => return Ok(HttpResponse::from_error(e)),
  };

  let pid = match ID::from_base64(qs.get("pid").unwrap_or_default().as_bytes()) {
    Ok(id) => id,
    Err(e) => return Ok(HttpResponse::from_error(e)),
  };

  println!("put_file {oid} {pid}");
  let mut action = JsonValue::Null;

  // iterate over multipart stream
  while let Some(mut field) = payload.try_next().await? {
    // A multipart/form-data stream has to contain `content_disposition`
    let content_disposition = field.content_disposition();

    let filename = content_disposition
      .get_filename()
      .map_or_else(|| Uuid::new_v4().to_string(), sanitize_filename::sanitize);

    println!("filename {filename}");

    let path = app.storage.as_ref().unwrap().get(&oid).person(&pid).picture().path();

    let folder = match path.parent() {
      None => return Ok(HttpResponse::InternalServerError().finish()),
      Some(folder) => folder,
    };

    println!("create folder");
    match std::fs::create_dir_all(folder) {
      Err(e) => return Ok(HttpResponse::InternalServerError().finish()),
      Ok(_) => {},
    }

    println!("create file");
    // File::create is blocking operation, use threadpool
    let mut f = web::block(|| std::fs::File::create(path)).await??;

    println!("save file");
    // Field in turn is stream of *Bytes* object
    while let Some(chunk) = field.try_next().await? {
      // filesystem operations are blocking, we have to use threadpool
      f = web::block(move || f.write_all(&chunk).map(|_| f)).await??;
    }
  }

  Ok(HttpResponse::Ok().into())
}
