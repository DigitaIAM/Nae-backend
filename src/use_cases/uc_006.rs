use crate::commutator::Application;

const WAREHOUSE_RECEIVE: [&str; 2] = ["warehouse", "receive"];

pub fn import(app: &Application) {
  store::process_records::receive_csv_to_json_for_warehouse(
    app,
    "./import/receive.csv",
    WAREHOUSE_RECEIVE.to_vec(),
    None,
  )
  .unwrap();
}

pub fn report(app: &Application) {
  store::process_records::report(app, "Midas-Plastics", "склад", "2023-01-01", "2023-03-31");
}
