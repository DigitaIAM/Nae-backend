use json::{JsonValue, object};
use csv::{ReaderBuilder, Trim};
use uuid::Uuid;
use rust_decimal::Decimal;
use actix_web::{web,
                test::{TestRequest, call_and_read_body},
                http::header::ContentType};
use std::future::Future;
use std::io;

use store::elements::ToJson;

use crate::commutator::Application;
use crate::animo::memory::ID;
use crate::services::Services;
use errors::Error;

const COUNTERPARTY: Vec<&str> = vec!["counterparty".into()];
const MATERIAL: Vec<&str> = vec!["material".into()];
const MATERIAL: Vec<&str> = vec!["material".into()];
const MATERIAL: Vec<&str> = vec!["material".into()];

struct Quantity {
    number: Decimal,
    uom: JsonValue,
}

struct Cost {
    number: Decimal,
    currency: String,
}

pub(crate) fn import(app: &Application) {
    receive_csv_to_json(app, "./tests/data/test_dista.csv").unwrap();
}

pub(crate) fn report(app: &Application) {}

pub fn receive_csv_to_json(app: &Application, path: &str) -> Result<(), Error> {
    let mut reader = ReaderBuilder::new().delimiter(b',').trim(Trim::All).from_path(path).unwrap();

    for record in reader.records() {
        let record = record.unwrap();

        let date = &record[5];
        let date = format!("{}-{}-{}", &date[6..=9], &date[3..=4], &date[0..=1]);

        let number = &record[0];
        if number.is_empty() {
            continue;
        }

        let counterparty_name = &record[6];
        let counterparty = if let Ok(counterparties) = service_find(app, object! { name: counterparty_name }, COUNTERPARTY) {
            match counterparties.len() {
                0 => {
                    let counterparty = object! { name: counterparty_name };
                    service_create(app, counterparty, COUNTERPARTY)?
                },
                1 => {
                    counterparties[0].clone()
                },
                _ => {
                    unreachable!("two or more same counterparties in db")
                },
            }
        } else {
            let counterparty = object! { name: counterparty_name };
            service_create(app, counterparty, COUNTERPARTY)?
        };

        let storage_name = &record[7];
        let storage = if let Ok(storages) = service_find(app, object! { name: storage_name }, "warehouse/receive/storage") {
            match storages.len() {
                0 => {
                    let storage = object! { name: storage_name };
                    service_create(app, storage, "warehouse/receive/storage")?
                },
                1 => storages[0].clone(),
                _ => {
                    unreachable!("two or more same storages in db")
                },
            }
        } else {
            let storage = object! { name: storage_name };
            service_create(app, storage, "warehouse/receive/storage")?
        };

        let document = if let Ok(documents) = service_find(app,object! {number: number}, "warehouse/receive/document") {
            match documents.len() {
                0 => {
                    let data = object! {
                    date: date,
                    counterparty: counterparty,
                    storage: storage,
                    number: number,
                };
                    service_create(app, data, "warehouse/receive/document")?
                },
                1 => {
                    documents[0].clone()
                },
                _ => {
                    unreachable!("two or more same documents in db")
                },
            }
        } else {
            let data = object! {
                date: date,
                counterparty: &record[6],
                storage: &record[7],
                number: number,
                };
            service_create(app, data, "warehouse/receive/document")?
        };

        let unit = &record[3];
        let uom = if let Ok(units) = service_find(app,object! {uom: unit}, "warehouse/receive/uom") {
            match units.len() {
                0 => {
                    let uom = object! { uom: unit };
                    service_create(app,uom, "warehouse/receive/uom")?
                },
                1 => {
                    units[0].clone()
                },
                _ => {
                    unreachable!("two or more same uom in db")
                },
            }
        } else {
            let uom = object! { uom: unit };
            service_create(app,uom, "warehouse/receive/uom")?
        };

        let vendor_code = &record[2];
        let item = if let Ok(items) = service_find(app, object! { vendor_code: vendor_code }, "warehouse/receive/material") {
            match items.len() {
                0 => {
                    let item = object!{
                    name: &record[1],
                    vendor_code: vendor_code,
                    uom: uom.clone(),
                    counterparty: &record[6],
                };
                    service_create(app,item, "warehouse/receive/material")?
                },
                1 => {
                    items[0].clone()
                },
                _ => {
                    unreachable!("two or more same items in db")
                },
            }
        } else {
            let item = object!{
                name: &record[1],
                vendor_code: vendor_code,
                uom: uom.clone(),
                counterparty: &record[6],
            };
            service_create(app,item, vec!["material".into()])?
        };

        // cell at the warehouse
        let cell: std::option::Option<String> = None;

        // let qty = Quantity { number: record[4].parse::<Decimal>().unwrap(), uom };

        let price: Decimal = 0.into();
        // let cost = Cost { number: 0.into(), currency: "rub".to_string() };

        let number = record[4].parse::<Decimal>().unwrap();

        let data = object! {
            document: document,
            item: item,
            storage: cell,
            qty: object! { number: number.to_json(), uom: uom },
            price: price.to_json(),
            cost: object! { number: Decimal::default().to_json(), currency: "rub".to_string() },
        };

        let res = service_create(app, data, vec!["warehouse".into(),"receive".into()])?;

        println!("data: {res:?}");
    }

    Ok(())
}

fn service_find(app: &Application, filter: JsonValue, ctx: Vec<&str>) -> Result<Vec<JsonValue>, Error> {
    let oid = ID::from("Midas-Plastics");
    let result = app.service("memories").find(object!{oid: oid.to_base64(), ctx: ctx, filter: filter})?;

    Ok(result["data"].members().map(|o|o.clone()).collect())
}

fn service_create(app: &Application, data: JsonValue, ctx: Vec<&str>) -> Result<JsonValue, Error> {
    let oid = ID::from("Midas-Plastics");

    let result = app.service("memories").create(data, object!{oid: oid.to_base64(), ctx: ctx })?;

    // println!("create_result: {result:?}");

    Ok(result)
}