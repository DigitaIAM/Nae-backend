use crate::commutator::Application;

const WAREHOUSE_RECEIVE: [&str; 2] = ["warehouse","receive"];

pub(crate) fn import(app: &Application) {
    crate::use_cases::csv::receive_csv_to_json(app, "./tests/data/test_dista_receive.csv", WAREHOUSE_RECEIVE.to_vec(), None).unwrap();
}

pub(crate) fn report(app: &Application) {
    crate::use_cases::csv::report(app, "Midas-Plastics", "Склад Midas Plastics", "2022-12-18", "2022-12-22");
}