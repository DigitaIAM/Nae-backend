use crate::commutator::Application;

const WAREHOUSE_INVENTORY: [&str; 2] = ["warehouse", "inventory"];

pub fn import(app: &Application) {
  store::process_records::receive_csv_to_json(
    app,
    "./import/inventory.csv",
    WAREHOUSE_INVENTORY.to_vec(),
    None,
  )
  .unwrap();
}

pub fn report(app: &Application) {
  store::process_records::report(app, "Midas-Plastics", "склад", "2023-01-01", "2023-03-31");
}
