use chrono::{Date, Duration, Utc};
use std::mem;
use std::time::SystemTime;

pub struct DateRange(pub Date<Utc>, pub Date<Utc>);

impl DateRange {
  fn iter(&self) -> DateRangeIter {
    DateRangeIter { till: self.1, next: self.0 }
  }
}

pub struct DateRangeIter {
  till: Date<Utc>,
  next: Date<Utc>,
}

impl Iterator for DateRangeIter {
  type Item = Date<Utc>;
  fn next(&mut self) -> Option<Self::Item> {
    if self.next <= self.till {
      let next = self.next + Duration::days(1);
      Some(mem::replace(&mut self.next, next))
    } else {
      None
    }
  }
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
