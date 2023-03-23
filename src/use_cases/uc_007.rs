use crate::commutator::Application;

const WAREHOUSE_DISPATCH: [&str; 2] = ["warehouse", "dispatch"];

pub(crate) fn import(app: &Application) {
  crate::use_cases::csv::receive_csv_to_json(
    app,
    "./tests/data/Dista_dispatch_13.03.2023.csv",
    WAREHOUSE_DISPATCH.to_vec(),
    None,
  )
  .unwrap();
}

pub(crate) fn report(app: &Application) {
  crate::use_cases::csv::report(
    app,
    "Midas-Plastics",
    "Склад Midas Plastics",
    "2022-12-01",
    "2023-03-30",
  );
}
