use crate::commutator::Application;
use crate::storage;
use actix_web::web::patch;
use json::object;
use service::{Context, Services};
use std::fs::File;
use std::io::{BufRead, BufReader, Error};

pub fn delete(app: &Application) -> Result<(), Error> {
  let mut count = 0;
  let ws = app.wss.list()?[0].clone();
  for doc in ws.clone().into_iter() {
    // println!("{:?} {:?}", doc.id, doc.json()?);

    let ctx = doc.mem.ctx;

    let ctx_str: Vec<&str> = ctx.iter().map(|s| s.as_str()).collect();

    match ctx_str[..] {
      ["production", "produce"] => {
        let doc_id = doc.id;
        // println!("doc_id {}", doc_id);

        let filepath = "production_delete.txt";

        let file = File::open(filepath)?;
        let reader = BufReader::new(file);

        for line in reader.lines() {
          let line = line?;
          // println!("doc_id {}\nline {}", doc_id, line);
          if doc_id == line {
            let params = object! {oid: ws.id.to_string(), ctx: [], enrich: false };
            let mut document = match app.service("memories").get(
              Context::local(),
              format!("production/produce/{}", doc_id),
              params,
            ) {
              Ok(d) => d,
              Err(e) => return Err(Error::from(e)), // TODO handle IO error differently!!!!
            };
            document["status"] = "deleted".into();

            let params = object! {oid: ws.id.to_string(), ctx: vec!["production", "produce"] };
            let _doc = app.service("memories").patch(
              Context::local(),
              format!("production/produce/{}", doc_id),
              document,
              params,
            )?;
            log::debug!("__doc {:#?}", _doc.dump());

            count += 1;
          }
        }
      },
      _ => continue,
    }
  }

  println!("count {count}");

  Ok(())
}
