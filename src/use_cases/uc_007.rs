use crate::commutator::Application;

const WAREHOUSE_ISSUE: [&str; 2] = ["warehouse","issue"];

pub(crate) fn import(app: &Application) {
    crate::use_cases::csv::receive_csv_to_json(app, "./tests/data/test_dista_issue.csv", WAREHOUSE_ISSUE.to_vec(), Some("1")).unwrap();
}

pub(crate) fn report(app: &Application) {
    crate::use_cases::csv::report(app, "Midas-Plastics", "Склад Midas Plastics", "2023-05-11", "2023-05-12");
}