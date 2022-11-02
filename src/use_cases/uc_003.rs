use crate::use_cases::write;
use crate::*;
use csv::{ReaderBuilder, Trim};
use json::JsonValue;
use std::fs::File;
use std::io::{BufRead, BufReader};

pub(crate) fn import(app: &Application) {
  let mut reader = ReaderBuilder::new()
    .delimiter(b';')
    .trim(Trim::All)
    .from_path("data/cases/003/people.csv")
    .unwrap();

  let mut count = 0;

  for record in reader.records() {
    let record = record.unwrap();

    println!("{record:?}");

    // 0 - №;
    // 1 - Ф.И.О. должностного лица;
    // 2 - Должность;
    // 3 - Отдел;
    // 4 - ПодОтдел;
    // 5 - Первонач. дата приёма;
    // 6 - Дата рождения;
    // 7 - Паспортные данные;
    // 8 - Адрес по прописке;
    // 9 - ИНН;
    // 10 - ИНПС;
    // 11 - Телефоны

    let data = json::object! {
      "oid": "qRqeDWJFuKFXDQqRp_8cTSjqAZgUUOSWvwwdzPyT88Y",
      "no": record[0].to_string(),
      "name": record[1].to_string(),
      "position": record[2].to_string(),
      "division": record[3].to_string(),
      "sub_division": record[4].to_string(),
      "date_of_birth": record[6].to_string(),
    };

    app.service("people").create(data, JsonValue::Null);

    count += 1;
  }

  println!("write {:?}", count);
}

pub(crate) fn report(app: &Application) {}
