use crate::commutator::Application;

const WAREHOUSE_DISPATCH: [&str; 2] = ["warehouse", "dispatch"];

pub fn import(app: &Application) {
  store::process_records::receive_csv_to_json(
    app,
    "./import/dispatch.csv",
    WAREHOUSE_DISPATCH.to_vec(),
    None,
  )
  .unwrap();
}

pub fn report(app: &Application) {
  store::process_records::report(app, "Midas-Plastics", "склад", "2023-01-01", "2023-03-31");
}
