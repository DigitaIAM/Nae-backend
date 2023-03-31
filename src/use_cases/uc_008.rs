use crate::commutator::Application;

const WAREHOUSE_TRANSFER: [&str; 2] = ["warehouse", "transfer"];

pub(crate) fn import(app: &Application) {
  crate::use_cases::csv::receive_csv_to_json(
    app,
    "./tests/data/Dista_transfer_31.03.2023.csv",
    WAREHOUSE_TRANSFER.to_vec(),
    None,
  )
  .unwrap();
}

pub(crate) fn report(app: &Application) {
  crate::use_cases::csv::report(
    app,
    "Midas-Plastics",
    "склад",
    "2022-12-01",
    "2023-03-30",
  );
}
