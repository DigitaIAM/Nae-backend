use crate::commutator::Application;

const WAREHOUSE_DISPATCH: [&str; 2] = ["warehouse", "dispatch"];

pub(crate) fn import(app: &Application) {
  // crate::use_cases::csv::receive_csv_to_json(app, "./tests/data/test_dista_issue.csv", WAREHOUSE_DISPATCH.to_vec(), Some("1")).unwrap();
  crate::use_cases::csv::receive_csv_to_json(
    app,
    "./tests/data/test_dista_issue.csv",
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
    "2023-05-11",
    "2023-05-12",
  );
}
