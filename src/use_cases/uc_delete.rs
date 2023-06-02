use crate::commutator::Application;
use crate::storage;
use std::fs::File;
use std::io::{BufRead, BufReader, Error};

pub fn delete(app: &Application) -> Result<(), Error> {
  let mut count = 0;
  for ws in app.wss.list()? {
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
              storage::remove_dir(&doc.path)?;
              count += 1;
            }
          }
        },
        _ => continue,
      }
    }
  }

  println!("count {count}");

  Ok(())
}
