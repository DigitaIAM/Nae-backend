use crate::commutator::Application;
use std::io::{Error, ErrorKind};

pub fn create_production(app: &Application) -> Result<(), Error> {
  store::process_records::receive_csv_to_json_for_production(app, "./import/production.csv")
    .map_err(|e| Error::new(ErrorKind::Other, e.to_string()))
}
