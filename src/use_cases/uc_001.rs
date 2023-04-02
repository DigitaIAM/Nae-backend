use std::collections::{HashMap, HashSet};
use std::fs::File;
use std::io::{BufWriter, Write};


use dbase::{FieldValue, Record};

use crate::animo::TimeInterval;
use crate::warehouse::store_aggregation_topology::WHStoreAggregationTopology;
use crate::animo::{db::AnimoDB, Time, memory::{Context, Value, ID, create}, shared::{DESC, SPECIFIC_OF, GOODS_RECEIVE, DATE, STORE, GOODS, QTY, COST, GOODS_ISSUE, CUSTOMER}};

pub(crate) fn import(db: &AnimoDB) {
    println!("running import");

    let string = |record: &Record, name| -> String {
        let field = record.get(name).unwrap();
        match field {
            FieldValue::Character(data) => {
                match data {
                    Some(str) => str.clone(),
                    None => unreachable!("internal errors")
                }
            }
            _ => unreachable!("internal errors")
        }
    };

    let date = |record: &Record, name| -> Time {
        let field = record.get(name).unwrap();
        match field {
            FieldValue::Date(data) => {
                match data {
                    Some(date) => Time::new(
                        format!(
                            "{:0>4}-{:0>2}-{:0>2}",
                            date.year(), date.month(), date.day()
                        ).as_str()
                    ).unwrap(),
                    None => unreachable!("internal errors")
                }
            }
            _ => unreachable!("internal errors")
        }
    };

    let number = |record: &Record, name| -> f64 {
        let field = record.get(name).unwrap();
        match field {
            FieldValue::Numeric(data) => {
                match data {
                    Some(number) => number.clone(),
                    None => unreachable!("internal errors")
                }
            }
            _ => unreachable!("internal errors")
        }
    };

    let mut changes = Vec::with_capacity(1_000_000);
    let mut doc_date = HashMap::with_capacity(1_000_000);

    let mut count = 0;
    {
        let f = File::create("data/journal.txt").expect("Unable to create file");
        let mut f = BufWriter::new(f);

        // 1SJOURN Журналы
        let mut reader = dbase::Reader::from_path("data/cases/001/1SJOURN.dbf").unwrap();
        for record in reader.iter_records() {
            let record = &record.unwrap();

            count += 1;
            if count % 50_000 == 0 {
                println!("1SJOURN {:?}", count);
            }

            // F=IDJOURNAL |ID of Journal       |C   |4     |0
            let journal_id = string(record, "IDJOURNAL");
            // F=IDDOC     |ID Document         |C   |9     |0
            let doc_id = string(record, "IDDOC");
            // F=DATE      |date                |D   |8     |0
            let date = date(record, "DATE");
            // F=TIME      |Time                |C   |6     |0
            let time = string(record, "TIME");

            let data = format!("{:?} - {:?} - {:?} - {:?}\n", journal_id, doc_id, date, time);
            f.write_all(data.as_bytes()).expect("Unable to write data");

            doc_date.insert(doc_id, date);
        }
        count = 0;
    }

    let mut head = HashMap::with_capacity(1_000_000);

    // DH95 Документ ТоварПриход
    let mut reader = dbase::Reader::from_path("data/cases/001/DH95.dbf").unwrap();
    for record in reader.iter_records() {
        let record = &record.unwrap();
        count += 1;

        if count % 50_000 == 0 {
            println!("DH95 {:?}", count);
        }

        // F=IDDOC     |ID Document's       |C   |9     |0
        let doc_id = string(record, "IDDOC");
        // F=SP88      |(P)СкладПриема      |C   |9     |0
        let store_id = string(record, "SP88");
        // F=SP87      |(P)Поставщик        |C   |9     |0
        let supplier_id = string(record, "SP87");
        // F=SP1446    |(P)ДатаПрибытия     |D   |8     |0
        // let date = date(record, "SP1446");

        // F=SP89      |(P)Официально       |N   |2     |0
        // F=SP280     |(P)ВозвратТовара    |N   |2     |0

        // let data = format!("{:?} {:?} {:?}\n", doc_id, store_id, supplier_id);
        // f.write_all(data.as_bytes()).expect("Unable to write data");

        head.insert(doc_id, (store_id, supplier_id));
    }
    println!("Head of ТоварПриход {:?}", count);
    count = 0;

    let f = File::create("data/receive_table.txt").expect("Unable to create file");
    let mut f = BufWriter::new(f);

    let mut head_added = HashSet::with_capacity(1_000_000);

    // DT95 Документ (Мн.ч.) ТоварПриход
    let mut reader = dbase::Reader::from_path("data/cases/001/DT95.dbf").unwrap();
    for record in reader.iter_records() {
        let record = &record.unwrap();
        count += 1;

        if count % 5_000 == 0 {
            println!("DT95 {:?}", count);

            changes = super::write(db, changes);
        }

        // F=IDDOC     |ID Document's       |C   |9     |0
        let doc_id = string(record, "IDDOC");

        // F=LINENO    |LineNo              |N   |4     |0
        let line_id = number(record, "LINENO").to_string();

        // F=SP92      |(P)Товар            |C   |9     |0
        let goods_id = string(record, "SP92");

        // F=SP90      |(P)Количество       |N   |16    |0
        let qty = number(record, "SP90");

        // F=SP91      |(P)Сумма            |N   |20    |2
        let cost = number(record, "SP91");

        let date = doc_date.get(doc_id.as_str()).unwrap().clone();

        if head_added.insert(doc_id.clone()) {
            if let Some((store_id, supplier_id)) = head.get(&doc_id) {
                let _context = Context(vec![doc_id.clone().into()]);
                changes.extend(create(*DESC, doc_id.clone().into(), vec![
                    (*SPECIFIC_OF, Value::ID(*GOODS_RECEIVE)),
                    (*DATE, Value::DateTime(date.clone())),
                    ("supplier".into(), ID::from(supplier_id.as_str()).into()),
                    (*STORE, ID::from(store_id.as_str()).into())
                ]));

                let data = format!("{:?} {:?} {:?} {:?}\n", doc_id, store_id, supplier_id, date);
                f.write_all(data.as_bytes()).expect("Unable to write data");
            } else {
                println!("doc_id {}", doc_id);
                // unreachable!("doc_id {}", doc_id);
            }
        }

        if head_added.contains(&doc_id) {
            let _context = Context(vec![doc_id.clone().into(), ]);
            changes.extend(create(*DESC, doc_id.clone().into(), vec![
              (line_id.clone().into(), vec![
                (*GOODS, ID::from(goods_id.as_str()).into()),
                (*QTY, qty.into()),
                (*COST, cost.into())
              ].into())
            ]));

            let data = format!("{:?} {:?} {:?} {:?}\n", doc_id, goods_id, qty, cost);
            f.write_all(data.as_bytes()).expect("Unable to write data");
        }
    }

    changes = super::write(db, changes);

    println!("Lines of ТоварПриход {:?}", count);
    count = 0;

    let f = File::create("data/issue_head.txt").expect("Unable to create file");
    let mut f = BufWriter::new(f);

    let mut head = HashMap::with_capacity(1_000_000);

    // DH112 Документ ТоварРасход
    let mut reader = dbase::Reader::from_path("data/cases/001/DH112.dbf").unwrap();
    for record in reader.iter_records() {
        let record = &record.unwrap();
        count += 1;

        if count % 50_000 == 0 {
            println!("DH112 {:?}", count);
        }

        // F=IDDOC     |ID Document's       |C   |9     |0
        let doc_id = string(record, "IDDOC");

        // F=SP384     |(P)СкладСписания    |C   |9     |0
        let store_id = string(record, "SP384");

        // F=SP98      |(P)Покупатель       |C   |9     |0
        let customer_id = string(record, "SP98");

        // F=SP421     |(P)Агент            |C   |9     |0

        let data = format!("{:?} {:?} {:?}\n", doc_id, store_id, customer_id);
        f.write_all(data.as_bytes()).expect("Unable to write data");

        head.insert(doc_id, (store_id, customer_id));
    }
    println!("Head of ТоварРасход {:?}", count);
    count = 0;

    // DT112 Документ (Мн.ч.) ТоварРасход
    let mut reader = dbase::Reader::from_path("data/cases/001/DT112.dbf").unwrap();
    for record in reader.iter_records() {
        let record = &record.unwrap();
        count += 1;

        if count % 5_000 == 0 {
            println!("DT112 {:?}", count);

            changes = super::write(db, changes);
        }

        // F=IDDOC     |ID Document's       |C   |9     |0
        let doc_id = string(record, "IDDOC");

        // F=LINENO    |LineNo              |N   |4     |0
        let line_id = number(record, "LINENO").to_string();

        // F=SP101     |(P)Товар            |C   |9     |0
        let goods_id = string(record, "IDDOC");

        // F=SP104     |(P)Количество       |N   |16    |3
        let qty = number(record, "SP104");

        // F=SP105     |(P)Сумма            |N   |20    |2
        let cost = number(record, "SP105");

        if let Some((store_id, customer_id)) = head.get(&doc_id) {
          changes.extend(create(*DESC, doc_id.clone().into(), vec![
            (*SPECIFIC_OF, Value::from(*GOODS_ISSUE)),
            (*DATE, Value::DateTime(doc_date.get(doc_id.as_str()).unwrap().clone())),
            (*CUSTOMER, ID::from(customer_id.as_str()).into()),
            (*STORE, ID::from(store_id.as_str()).into()),
          ]));

          changes.extend(create(*DESC, doc_id.into(), vec![
            (line_id.clone().into(), vec![
              (*GOODS, ID::from(goods_id.as_str()).into()),
              (*QTY, qty.into()),
              (*COST, cost.into())
            ].into())
          ]));
        } else {
            println!("doc_id: {}", doc_id);
        }
    }

    changes = super::write(db, changes);

    println!("Lines of ТоварРасход {:?}", count);
    count = 0;


    super::join();
}

pub(crate) fn report(db: &AnimoDB) {
    let interval = TimeInterval::new("2021-06-01", "2021-06-30").unwrap();

    let ts = std::time::Instant::now();

    let stores = WHStoreAggregationTopology::stores_turnover(
        &db, interval.clone(),
    ).expect("Ok");

    println!("done in {:?}", ts.elapsed());

    println!("count {:?}", stores.len());

    for store in stores.iter() {
        let v = store.value();
        println!("store {:?} {:?}", v.name, v.value);
    }
}