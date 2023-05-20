use std::io::Write;

use actix_multipart::Multipart;
use actix_web::{get, post, web, Error, HttpRequest, HttpResponse};
use futures::TryStreamExt;
use qstring::QString;

use crate::commutator::Application;
use values::ID;

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

  let path = app.wss.get(&oid).person(&pid).picture().path();

  let file = actix_files::NamedFile::open_async(path).await?;

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

  // let action = JsonValue::Null;

  // iterate over multipart stream
  while let Some(mut field) = payload.try_next().await? {
    // A multipart/form-data stream has to contain `content_disposition`
    // let content_disposition = field.content_disposition();

    // let filename = content_disposition
    //   .get_filename()
    //   .map_or_else(|| Uuid::new_v4().to_string(), sanitize_filename::sanitize);

    let path = app.wss.get(&oid).person(&pid).picture().path();

    let folder = match path.parent() {
      None => return Ok(HttpResponse::InternalServerError().finish()),
      Some(folder) => folder,
    };

    match std::fs::create_dir_all(folder) {
      Err(_) => return Ok(HttpResponse::InternalServerError().finish()),
      Ok(_) => {},
    }

    // File::create is blocking operation, use threadpool
    let mut f = web::block(|| std::fs::File::create(path)).await??;

    // Field in turn is stream of *Bytes* object
    while let Some(chunk) = field.try_next().await? {
      // filesystem operations are blocking, we have to use threadpool
      f = web::block(move || f.write_all(&chunk).map(|_| f)).await??;
    }
  }

  Ok(HttpResponse::Ok().into())
}
