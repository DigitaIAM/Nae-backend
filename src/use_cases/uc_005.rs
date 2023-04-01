use crate::commutator::Application;

const WAREHOUSE_INVENTORY: [&str; 2] = ["warehouse", "inventory"];

pub(crate) fn import(app: &Application) {
  crate::use_cases::csv::receive_csv_to_json(
    app,
    "./import/inventory.csv",
    WAREHOUSE_INVENTORY.to_vec(),
    None,
  )
  .unwrap();
}

pub(crate) fn report(app: &Application) {
  crate::use_cases::csv::report(app, "Midas-Plastics", "склад", "2023-01-01", "2023-03-31");
}
