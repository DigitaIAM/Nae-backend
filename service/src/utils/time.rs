use crate::error::Error;
use chrono::{DateTime, Datelike, NaiveDateTime, SecondsFormat, Utc};
// use std::mem;
use std::time::SystemTime;

pub struct DateRange(pub DateTime<Utc>, pub DateTime<Utc>);

// impl DateRange {
//   fn iter(&self) -> DateRangeIter {
//     DateRangeIter { till: self.1, next: self.0 }
//   }
// }
//
// pub struct DateRangeIter {
//   till: DateTime<Utc>,
//   next: DateTime<Utc>,
// }
//
// impl Iterator for DateRangeIter {
//   type Item = DateTime<Utc>;
//   fn next(&mut self) -> Option<Self::Item> {
//     if self.next <= self.till {
//       let next = self.next + Duration::days(1);
//       Some(mem::replace(&mut self.next, next))
//     } else {
//       None
//     }
//   }
// }

pub fn timestamp_to_time(ts: u64) -> Result<DateTime<Utc>, Error> {
  if let Some(t) = NaiveDateTime::from_timestamp_opt(ts as i64, 0) {
    Ok(DateTime::<Utc>::from_utc(t, Utc))
  } else {
    Err(Error::GeneralError("Incorrect timestamp".to_string()))
  }
}

pub fn string_to_time<S: AsRef<str>>(data: S) -> Result<DateTime<Utc>, Error> {
  DateTime::parse_from_rfc3339(data.as_ref())
    .map(|ts| ts.into())
    .map_err(|_| Error::GeneralError(format!("incorrect data-time {}", data.as_ref())))
}

pub fn time_to_string(time: DateTime<Utc>) -> String {
  time.to_rfc3339_opts(SecondsFormat::Millis, true)
}

pub fn date_to_string(date: DateTime<Utc>) -> String {
  format!("{:0>4}-{:0>2}-{:0>2}", date.year(), date.month(), date.day())
  // time.to_rfc3339_opts(SecondsFormat::Millis, true)[0..10].to_string()
}

pub fn now_in_seconds() -> u64 {
  SystemTime::now()
    .duration_since(std::time::UNIX_EPOCH)
    .expect("system time is likely incorrect")
    .as_secs()
}

pub fn now_in_millis() -> u128 {
  SystemTime::now()
    .duration_since(std::time::UNIX_EPOCH)
    .expect("system time is likely incorrect")
    .as_millis()
}
