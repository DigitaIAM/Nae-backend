use std::fs::File;
use std::io::{BufRead, BufReader};
use csv::{ReaderBuilder, Trim};
use crate::*;
use crate::use_cases::write;

pub(crate) fn import(db: &AnimoDB) {
    let mut changes = Vec::with_capacity(1_000_000);

    let mut reader = ReaderBuilder::new()
        .delimiter(b',')
        .trim(Trim::All)
        .from_path("data/cases/002/tariff2022.csv").unwrap();

    let schneider_electric = ID::from("schneider-electric|company");

    let mut count = 0;

    for record in reader.records() {
        let record = record.unwrap();

        let rf = &record[0];
        if rf.is_empty() {
            continue;
        }
        let price = record[2].replace(",","");
        let min_order = record[4].replace(",","");

        let label = &record[1];
        let price = price.parse::<Decimal>().unwrap();
        let min_order = min_order.parse::<Decimal>().unwrap();

        let uom = match &record[3] {
            "За штуку" => *UOM_PIECE,
            "За метр" => *UOM_METER,
            _ => unreachable!("internal error")
        };

        let activity = &record[8];

        let collection = &record[10];
        let line = &record[12];
        let subline = &record[14];

        let cosl1 = &record[9];
        let cosl2 = &record[11];
        let cosl3 = &record[13];

        // println!("{} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {}", rf, label, price, min_order, collection, line, subline, activity, cosl1, cosl2, cosl3 );

        // zone: description
        // goods-id
        //  reference > "something"
        //  label > "text"
        //  "text" > label ?
        // company-A
        //  label > "A LLC"

        // zone: can-buy
        // company-A
        //  goods-id > { price: { number: 7, currency: eur }, minimum-order-qty: { number: 1, uom: piece }}

        let goods_id = ID::from(format!("schneider-electric|goods|{}",rf));
        changes.extend(create(*DESC, goods_id, vec![
            (*REFERENCE, rf.into()),
            (*LABEL, label.into())
        ]));
        changes.extend(create(*CAN_BUY_FROM, schneider_electric, vec![
            (goods_id, vec![
                (*PRICE, vec![
                    (*NUMBER, price.into()),
                    (*UOM, uom.into())
                ].into()),
                (*MINIMUM_ORDER_QTY, min_order.into()),
                (*DATE, Time::new("2022-03-05").unwrap().into())
            ].into())
        ]));

        count += 1;

        if changes.len() > 5_000 {
            println!("write {:?}", count);
            changes = write(db, changes);
            count = 0;
        }
    }

    println!("write {:?}", count);
    write(db, changes);
}


pub(crate) fn report(db: &AnimoDB) {

}