use std::collections::{HashMap, HashSet};
use std::fs::File;
use std::io::{BufWriter, Write};
use std::path::Path;

use crate::commutator::Application;
use dbase::{FieldValue, Record};

use crate::animo::TimeInterval;
use crate::animo::{
  db::AnimoDB,
  memory::{create, Context, Value, ID},
  shared::{
    COST, CUSTOMER, DATE, DESC, GOODS, GOODS_CODE, GOODS_ISSUE, GOODS_RECEIVE, QTY, SPECIFIC_OF,
    STORE, UOM,
  },
  Time,
};
use crate::warehouse::store_aggregation_topology::WHStoreAggregationTopology;

pub(crate) fn import(app: &Application) {
  if !Path::new("./import/receive_1s.csv").is_file() {
    println!("running import");

    let string = |record: &Record, name| -> String {
      let field = record.get(name).unwrap();
      // println!("FIELD: {name} {field:?}");
      match field {
        FieldValue::Character(data) => match data {
          Some(str) => {
            println!("FIELD: {name} {str}");
            str.clone().replace(',', ".")
          },
          None => unreachable!("internal errors"),
        },
        _ => unreachable!("internal errors"),
      }
    };

    let date = |record: &Record, name| -> String {
      let field = record.get(name).unwrap();
      match field {
        FieldValue::Date(data) => match data {
          Some(date) => {
            format!("{:0>2}.{:0>2}.{:0>4}", date.day(), date.month(), date.year())
          },
          None => unreachable!("internal errors"),
        },
        _ => unreachable!("internal errors"),
      }
    };

    let number = |record: &Record, name| -> f64 {
      let field = record.get(name).unwrap();
      match field {
        FieldValue::Numeric(data) => match data {
          Some(number) => number.clone(),
          None => unreachable!("internal errors"),
        },
        _ => unreachable!("internal errors"),
      }
    };

    let mut doc_date = HashMap::with_capacity(1_000_000);

    let mut count = 0;
    {
      let f = File::create("data/journal.txt").expect("Unable to create file");
      let mut f = BufWriter::new(f);

      // 1SJOURN Журналы
      let mut reader = dbase::Reader::from_path("./import/1S/1SJOURN.dbf").unwrap();
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

    // SC792 Справочник Контрагенты
    let mut counterparties = HashMap::with_capacity(1_000_000);

    let mut reader = dbase::Reader::from_path_with_encoding(
      "./import/1S/SC792.dbf",
      dbase::yore::code_pages::CP1251,
    )
    .unwrap();
    for record in reader.iter_records() {
      let record = &record.unwrap();
      count += 1;

      if count % 50_000 == 0 {
        println!("SC792 {:?}", count);
      }

      // F=ID       |ID object           |C   |9     |0
      let counterparty_id = string(record, "ID");
      // F=DESCR    |object description  |C   |50     |0
      let counterparty_descr = string(record, "DESCR");

      counterparties.insert(counterparty_id, counterparty_descr);
    }
    println!("Справочник Контрагенты {:?}", count);
    count = 0;

    // SC1305 Справочник МестаХранения
    let mut storages = HashMap::with_capacity(1_000_000);

    let mut reader = dbase::Reader::from_path_with_encoding(
      "./import/1S/SC1305.dbf",
      dbase::yore::code_pages::CP1251,
    )
    .unwrap();
    for record in reader.iter_records() {
      let record = &record.unwrap();
      count += 1;

      if count % 50_000 == 0 {
        println!("SC1305 {:?}", count);
      }

      // F=ID       |ID object           |C   |9     |0
      let storage_id = string(record, "ID");
      // F=DESCR    |object description  |C   |50     |0
      let storage_descr = string(record, "DESCR");

      storages.insert(storage_id, storage_descr);
    }
    println!("Справочник МестаХранения {:?}", count);
    count = 0;

    // SC725 Справочник ЕдиницыИзмерений
    let mut uoms = HashMap::with_capacity(1_000_000);

    let mut reader = dbase::Reader::from_path_with_encoding(
      "./import/1S/SC725.dbf",
      dbase::yore::code_pages::CP1251,
    )
    .unwrap();
    for record in reader.iter_records() {
      let record = &record.unwrap();
      count += 1;

      if count % 50_000 == 0 {
        println!("SC725 {:?}", count);
      }

      // F=ID       |ID object           |C   |9     |0
      let uom_id = string(record, "ID");
      // F=DESCR    |object description  |C   |50     |0
      let uom_descr = string(record, "DESCR");

      uoms.insert(uom_id, uom_descr);
    }
    println!("Справочник ЕдиницыИзмерений {:?}", count);
    count = 0;

    // SC2717 Справочник Материалы
    let mut materials = HashMap::with_capacity(1_000_000);

    let mut reader = dbase::Reader::from_path_with_encoding(
      "./import/1S/SC2717.dbf",
      dbase::yore::code_pages::CP1251,
    )
    .unwrap();
    for record in reader.iter_records() {
      let record = &record.unwrap();
      count += 1;

      if count % 50_000 == 0 {
        println!("SC2717 {:?}", count);
      }

      // F=ID       |ID object           |C   |9     |0
      let goods_id = string(record, "ID");
      // F=DESCR    |object description  |C   |50     |0
      let goods_descr = string(record, "DESCR");
      // F=SP7816   |(P)Артикул          |C   |10    |0
      // let goods_code = string(record, "SP7816");
      // F=CODE     |object code         |C   |7     |0
      let goods_code = string(record, "CODE");
      // F=SP2721   |(P)ЕдИзм            |C   |9     |0
      let goods_uom = string(record, "SP2721");

      materials.insert(goods_id, (goods_descr, goods_code, goods_uom));
    }
    println!("Справочник Материалы {:?}", count);
    count = 0;

    let mut documents = HashMap::with_capacity(1_000_000);

    // DH2726 Документ МатериалыПоступлениеОтПоставщиков
    let mut reader = dbase::Reader::from_path("./import/1S/DH2726.dbf").unwrap();
    for record in reader.iter_records() {
      let record = &record.unwrap();
      count += 1;

      if count % 50_000 == 0 {
        println!("DH2726 {:?}", count);
      }

      // F=IDDOC     |ID Document's       |C   |9     |0
      let doc_id = string(record, "IDDOC");
      // F=SP2754    |(P)СкладПриема      |C   |9     |0
      let store_id = string(record, "SP2754");
      // F=SP2730    |(P)Корреспондент    |C   |9     |0
      let supplier_id = string(record, "SP2730");

      documents.insert(doc_id, (store_id, supplier_id));
    }
    println!("Head of МатериалыПоступлениеОтПоставщиков {:?}", count);
    count = 0;

    let f = File::create("./import/receive_1s.csv").expect("Unable to create file");
    let mut f = BufWriter::new(f);

    // DT2726 Документ (Мн.ч.) МатериалыПоступлениеОтПоставщиков
    let mut reader = dbase::Reader::from_path("./import/1S/DT2726.dbf").unwrap();
    for record in reader.iter_records() {
      let record = &record.unwrap();
      count += 1;

      if count % 5_000 == 0 {
        println!("DT2726 {:?}", count);
      }

      // F=IDDOC     |ID Document's       |C   |9     |0
      let doc_id = string(record, "IDDOC");
      // F=LINENO    |LineNo              |N   |4     |0
      // let line_id = number(record, "LINENO").to_string();
      // F=SP2733    |(P)Материал         |C   |9     |0
      let goods_id = string(record, "SP2733");
      // F=SP2736    |(P)Количество       |N   |16    |3
      let qty = number(record, "SP2736");
      // F=SP2737    |(P)Сумма            |N   |20    |2
      // let cost = number(record, "SP2737");

      let date = doc_date.get(doc_id.as_str()).unwrap().clone();

      if let Some((store_id, supplier_id)) = documents.get(&doc_id) {
        // println!("store_id: {store_id}");
        // println!("storages: {storages:?}");
        let default_storage = String::from("склад");
        let store_name = storages.get(store_id).unwrap_or(&default_storage);
        let supplier_name = counterparties.get(supplier_id).unwrap();

        if let Some((goods_descr, goods_code, goods_uom)) = materials.get(&goods_id) {
          let uom_name = uoms.get(goods_uom).unwrap();

          let data = format!(
            "{:?},{:?},{:?},{:?},{:?},{:?},{:?},{:?},\n",
            doc_id, goods_descr, goods_code, uom_name, qty, date, supplier_name, store_name
          );
          f.write_all(data.as_bytes()).expect("Unable to write data");
        }
      }
    }

    println!("Lines of МатериалыПоступлениеОтПоставщиков {:?}", count);
  } else {
    println!("path ./import/receive_1s.csv already exists. Uploading from existing file to db...")
  }

  crate::use_cases::csv::receive_csv_to_json(
    app,
    "./import/receive_1s.csv",
    ["warehouse", "receive"].to_vec(),
    None,
  )
  .unwrap();

  super::join();
}

pub(crate) fn report(db: &AnimoDB) {
  let interval = TimeInterval::new("2021-06-01", "2021-06-30").unwrap();

  let ts = std::time::Instant::now();

  let stores = WHStoreAggregationTopology::stores_turnover(&db, interval.clone()).expect("Ok");

  println!("done in {:?}", ts.elapsed());

  println!("count {:?}", stores.len());

  for store in stores.iter() {
    let v = store.value();
    println!("store {:?} {:?}", v.name, v.value);
  }
}
