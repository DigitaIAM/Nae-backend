use crate::commutator::Application;

const WAREHOUSE_INVENTORY: [&str; 2] = ["warehouse","inventory"];

pub(crate) fn import(app: &Application) {
    crate::use_cases::csv::receive_csv_to_json(app, "./tests/data/test_dista_inventory.csv", WAREHOUSE_INVENTORY.to_vec(), None).unwrap();
}

pub(crate) fn report(app: &Application) {
    crate::use_cases::csv::report(app, "Midas-Plastics", "Склад Midas Plastics", "2022-12-18", "2022-12-22");
}