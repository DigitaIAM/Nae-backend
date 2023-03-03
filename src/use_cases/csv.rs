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

use crate::storage::memories::{memories_create, memories_find};
use crate::commutator::Application;
use crate::animo::memory::ID;
use service::Services;
use service::error::Error;

const COUNTERPARTY: [&str; 1] = ["counterparty"];
const STORAGE: [&str; 1] = ["storage"];
const RECEIVE_DOCUMENT: [&str; 1] = ["warehouse/receive/document"];
const ISSUE_DOCUMENT: [&str; 1] = ["warehouse/issue/document"];
const UOM: [&str; 1] = ["uom"];
const MATERIAL: [&str; 1] = ["material"];
const CURRENCY: [&str; 1] = ["currency"];
const WAREHOUSE_RECEIVE: [&str; 2] = ["warehouse","receive"];

struct Quantity {
    number: Decimal,
    uom: JsonValue,
}

struct Cost {
    number: Decimal,
    currency: String,
}

pub(crate) fn report(app: &Application, company: &str, storage: &str, from_date: &str, till_date: &str, ) {
    println!("CSV_REPORT");
    let oid = ID::from(company);
    let ctx = vec!["report"];

    let storage = json(app,object! { name: storage }, STORAGE.to_vec(), &|| object! { name: storage }).unwrap();

    let result = app.service("inventory").find(object!{ctx: ctx, oid: oid.to_base64(), storage: storage["_uuid"].clone(), dates: {"from": from_date, "till": till_date}}).unwrap();

    println!("report: {:#?}", result);
}

fn json(app: &Application, filter: JsonValue, ctx: Vec<&str>, item: &dyn Fn() -> JsonValue) -> Result<JsonValue, Error> {
    if let Ok(items) = memories_find(app, filter, ctx.clone()) {
        match items.len() {
            0 => memories_create(app, item(), ctx),
            1 => Ok(items[0].clone()),
            _ => unreachable!("two or more same items in db"),
        }
    } else {
        memories_create(app, item(), ctx)
    }
}

pub(crate) fn receive_csv_to_json(app: &Application, path: &str, ctx: Vec<&str>, doc_from: Option<&str>) -> Result<(), Error> {
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
        let counterparty = json(app, object! { name: counterparty_name }, COUNTERPARTY.to_vec(), &|| object! { name: counterparty_name })?;

        let storage_name = &record[7];
        let storage = json(app, object! { name: storage_name }, STORAGE.to_vec(), &|| object! { name: storage_name })?;

        let doc_ctx = if ctx.get(1) == Some(&"receive") {
            RECEIVE_DOCUMENT.to_vec()
        } else {
            ISSUE_DOCUMENT.to_vec()
        };

        let document = json(app,
                            object! {number: number},
                            doc_ctx,
                            &|| object! {
                                date: date.clone(),
                                counterparty: counterparty["_uuid"].clone(),
                                storage: storage["_uuid"].clone(),
                                number: number,
                            })?;

        let unit = &record[3];
        let uom = json(app, object! {uom: unit}, UOM.to_vec(), &|| object! { uom: unit })?;

        let vendor_code = &record[2];
        let item = json(app,
                        object! { vendor_code: vendor_code },
                        MATERIAL.to_vec(),
                        &|| object!{
                            name: &record[1],
                            vendor_code: vendor_code,
                            uom: uom["_uuid"].clone(),
                            counterparty: counterparty["_uuid"].clone(),
                        })?;

        // cell at the warehouse
        let cell: std::option::Option<String> = None;

        let price: Decimal = 0.into();

        let float_number = &record[4].replace(",", ".");

        let number = float_number.parse::<Decimal>().unwrap();

        let currency = memories_create(app, object! {name: "rub"}, CURRENCY.to_vec())?;

        let document_from = match doc_from {
            Some(id) => memories_find(app, object! {number: id}, RECEIVE_DOCUMENT.to_vec())?,
            None => Vec::<JsonValue>::new(),
        };

        let data = object! {
            document: document["_uuid"].clone(),
            item: item["_uuid"].clone(),
            document_from: match document_from.get(0) {
                Some(o) => o.clone(),
                None => JsonValue::Null,
            },
            storage: cell,
            qty: object! { number: number.to_json(), uom: uom["_uuid"].clone() },
            price: price.to_json(),
            cost: object! { number: Decimal::default().to_json(), currency: currency["_uuid"].clone() },
        };

        let res = memories_create(app, data, ctx.clone())?;

        println!("data: {res:?}");
    }

    Ok(())
}