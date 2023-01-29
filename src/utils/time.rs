use crate::services::Error;
use chrono::{DateTime, Duration, SecondsFormat, Utc};
use std::mem;
use std::time::SystemTime;

pub struct DateRange(pub DateTime<Utc>, pub DateTime<Utc>);

impl DateRange {
  fn iter(&self) -> DateRangeIter {
    DateRangeIter { till: self.1, next: self.0 }
  }
}

pub struct DateRangeIter {
  till: DateTime<Utc>,
  next: DateTime<Utc>,
}

impl Iterator for DateRangeIter {
  type Item = DateTime<Utc>;
  fn next(&mut self) -> Option<Self::Item> {
    if self.next <= self.till {
      let next = self.next + Duration::days(1);
      Some(mem::replace(&mut self.next, next))
    } else {
      None
    }
  }
}

pub fn string_to_time<S: AsRef<str>>(data: S) -> Result<DateTime<Utc>, Error> {
  DateTime::parse_from_rfc3339(data.as_ref())
    .map(|ts| ts.into())
    .map_err(|e| Error::GeneralError(e.to_string()))
}

pub fn time_to_string(time: DateTime<Utc>) -> String {
  time.to_rfc3339_opts(SecondsFormat::Millis, true)
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
