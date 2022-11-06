use bytecheck::CheckBytes;
use rkyv::{Archive, Deserialize, Serialize};

use crate::animo::error::DBError;
use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
use chrono::{
  DateTime, Datelike, Duration, Month, NaiveDate, NaiveDateTime, NaiveTime, ParseError, TimeZone,
  Timelike, Utc,
};
use rust_decimal::Decimal;
use std::cmp::Ordering;
use std::ops::{Add, Sub};

// #[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
#[derive(
  Clone, serde::Serialize, serde::Deserialize, Archive, Deserialize, Serialize, Debug, PartialEq,
)]
// This will generate a PartialEq impl between our unarchived and archived types
// #[archive(compare(PartialEq))]
// To use the safe API, you have to derive CheckBytes for the archived type
#[archive_attr(derive(CheckBytes, Debug))]
pub enum IntervalPosition {
  Start,
  End,
}

impl IntervalPosition {
  fn to_byte(&self) -> u8 {
    match self {
      IntervalPosition::Start => 1,
      IntervalPosition::End => 2,
    }
  }

  pub(crate) fn from_bytes(bs: &[u8], offset: usize) -> Result<Self, DBError> {
    match bs[offset..offset + 1] {
      [1] => Ok(IntervalPosition::Start),
      [2] => Ok(IntervalPosition::End),
      [b] => Err(format!("wrong byte {}", b).into()),
      [] => Err(format!("no byte").into()),
      [_, _, ..] => unreachable!("internal error"),
    }
  }
}

// #[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
#[derive(
  Clone, serde::Serialize, serde::Deserialize, Archive, Deserialize, Serialize, Debug, PartialEq,
)]
// This will generate a PartialEq impl between our unarchived and archived types
// #[archive(compare(PartialEq))]
// To use the safe API, you have to derive CheckBytes for the archived type
#[archive_attr(derive(CheckBytes, Debug))]
pub enum TimeAccuracy {
  Year,
  Month,
  Day,
  Hour,
  Minute,
  Second,
}

impl TimeAccuracy {
  fn to_byte(&self) -> u8 {
    match self {
      TimeAccuracy::Year => 1,
      TimeAccuracy::Month => 2,
      TimeAccuracy::Day => 3,
      TimeAccuracy::Hour => 4,
      TimeAccuracy::Minute => 5,
      TimeAccuracy::Second => 6,
    }
  }

  pub(crate) fn from_bytes(bs: &[u8], offset: usize) -> Result<Self, DBError> {
    match bs[offset..offset + 1] {
      [1] => Ok(TimeAccuracy::Year),
      [2] => Ok(TimeAccuracy::Month),
      [3] => Ok(TimeAccuracy::Day),
      [4] => Ok(TimeAccuracy::Hour),
      [5] => Ok(TimeAccuracy::Minute),
      [6] => Ok(TimeAccuracy::Second),
      [b] => Err(format!("wrong byte {}", b).into()),
      [] => Err(format!("no byte").into()),
      [_, _, ..] => unreachable!("internal error"),
    }
  }
}

// #[derive(Debug, Clone, Eq, Serialize, Deserialize)]
#[derive(Clone, serde::Serialize, serde::Deserialize, Archive, Deserialize, Serialize, Debug)] // , PartialEq
                                                                                               // This will generate a PartialEq impl between our unarchived and archived types
                                                                                               // #[archive(compare(PartialEq))]
                                                                                               // To use the safe API, you have to derive CheckBytes for the archived type
                                                                                               // #[archive_attr(derive(CheckBytes, Debug))]
pub struct Time(
  #[serde(with = "chrono::serde::ts_milliseconds")] DateTime<Utc>,
  TimeAccuracy,
  IntervalPosition,
);

impl Time {
  pub(crate) fn zero() -> Self {
    Time(std::time::UNIX_EPOCH.into(), TimeAccuracy::Day, IntervalPosition::Start)
  }

  pub(crate) fn new(dt: &str) -> Result<Self, DBError> {
    let ts: DateTime<Utc> = DateTime::parse_from_rfc3339(format!("{}T00:00:00Z", dt).as_str())
      .map_err(|e| DBError::from(e.to_string()))?
      .into();

    Ok(Time(ts.beginning_of_day(), TimeAccuracy::Day, IntervalPosition::Start))
  }

  pub(crate) fn ts(&self) -> i64 {
    self.0.timestamp_millis()
  }

  pub(crate) fn to_bytes(&self) -> Vec<u8> {
    let ts: u64 = self.0.timestamp_millis().try_into().unwrap();

    // let t = ts.to_be_bytes();   // timestamp
    let mut t = Vec::with_capacity(8);
    t.write_u64::<BigEndian>(ts).unwrap();
    let f = [self.1.to_byte()]; // accuracy
    let b = [self.2.to_byte()]; // interval position
    [t.as_slice(), f.as_slice(), b.as_slice()].concat()
  }

  pub(crate) fn from_bytes(bs: &[u8], offset: usize) -> Result<Self, DBError> {
    // let convert = |bs: &[u8]| -> [u8; 8] {
    //     bs.try_into().expect("slice with incorrect length")
    // };

    let mut sub = &bs[offset..offset + 8];
    let ts = sub.read_u64::<BigEndian>().unwrap(); // u64::from_be_bytes(convert(&bs[offset..offset+8]));
    let ts = Utc.timestamp_millis(ts as i64);

    let accuracy = TimeAccuracy::from_bytes(bs, offset + 8)?;
    let position = IntervalPosition::from_bytes(bs, offset + 9)?;

    Ok(Time(ts, accuracy, position))
  }

  pub(crate) fn start(&self) -> Self {
    if self.2 == IntervalPosition::Start {
      self.clone()
    } else {
      match self.1 {
        TimeAccuracy::Day => {
          let ts = self.0.beginning_of_day();
          Time(ts, self.1.clone(), IntervalPosition::End)
        },
        _ => unimplemented!("{:?}", self),
      }
    }
  }

  pub(crate) fn end(&self) -> Self {
    if self.2 == IntervalPosition::End {
      self.clone()
    } else {
      match self.1 {
        TimeAccuracy::Day => {
          let ts = self.0.end_of_day();
          Time(ts, self.1.clone(), IntervalPosition::End)
        },
        _ => unimplemented!("{:?}", self),
      }
    }
  }

  pub(crate) fn beginning_of_month(&self) -> Self {
    let ts = self.0.beginning_of_month();
    Time(ts, TimeAccuracy::Month, IntervalPosition::Start)
  }

  pub(crate) fn beginning_of_next_month(&self) -> Self {
    let ts = self.0.end_of_month().add(Duration::nanoseconds(1));
    Time(ts, TimeAccuracy::Month, IntervalPosition::Start)
  }

  pub(crate) fn is_beginning_of_month(&self) -> bool {
    self.0.day() == 1 && self.0.num_seconds_from_midnight() == 0 && self.0.nanosecond() == 0
  }

  pub(crate) fn add_quantum(&self) -> Self {
    match self.1 {
      TimeAccuracy::Day => {
        match self.2 {
          IntervalPosition::Start => {
            todo!()
            // let ts = self.0
            //     .end_of_day()
            //     .add(Duration::nanoseconds(1));
            // Time(ts, TimeAccuracy::Day, IntervalPosition::Start)
          },
          IntervalPosition::End => {
            let ts = self.0.add(Duration::nanoseconds(1));
            Time(ts, TimeAccuracy::Day, IntervalPosition::End)
          },
        }
      },
      _ => unimplemented!("{:?}", self),
    }
  }

  pub(crate) fn sub_quantum(&self) -> Self {
    match self.1 {
      TimeAccuracy::Day => {
        match self.2 {
          IntervalPosition::Start => {
            let ts = self.0.sub(Duration::nanoseconds(1));
            Time(ts, TimeAccuracy::Day, IntervalPosition::End)
          },
          IntervalPosition::End => {
            todo!()
            // let ts = self.0
            //     .beginning_of_day()
            //     .sub(Duration::nanoseconds(1));
            // Time(ts, TimeAccuracy::Day, IntervalPosition::End)
          },
        }
      },
      TimeAccuracy::Month => match self.2 {
        IntervalPosition::Start => {
          let ts = self.0.sub(Duration::nanoseconds(1));
          Time(ts, TimeAccuracy::Month, IntervalPosition::End)
        },
        IntervalPosition::End => {
          unimplemented!("{:?}", self)
        },
      },
      _ => unimplemented!("{:?}", self),
    }
  }
}

// #[derive(PartialEq, PartialOrd)]

impl PartialEq<Self> for Time {
  fn eq(&self, other: &Self) -> bool {
    if self.2 == other.2 {
      if self.1 == other.1 {
        return self.0 == other.0;
      }
    }
    unimplemented!("{:?} vs {:?}", self, other)
  }

  fn ne(&self, other: &Self) -> bool {
    todo!()
  }
}

impl PartialOrd<Self> for Time {
  fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
    if self.2 == other.2 {
      if self.1 == other.1 {
        return self.0.partial_cmp(&other.0);
      }
    }
    unimplemented!()
  }
}

#[derive(Debug, Clone)]
pub struct TimeInterval {
  pub(crate) from: Time,
  pub(crate) till: Time,
}

impl TimeInterval {
  pub fn new(from: &str, till: &str) -> Result<Self, DBError> {
    let from = Time::new(from)?.start();
    let till = Time::new(till)?.end();
    // TODO raise error in case from.1 != till.1
    Ok(TimeInterval { from, till })
  }
}

pub trait DateTimeNow {
  type Timezone: TimeZone;
  fn beginning_of_minute(&self) -> DateTime<Self::Timezone>;
  fn beginning_of_hour(&self) -> DateTime<Self::Timezone>;
  fn beginning_of_day(&self) -> DateTime<Self::Timezone>;
  // /// get beginning of week. the default week start day is Monday.
  // fn beginning_of_week(&self) -> DateTime<Self::Timezone>;
  // /// get beginning of week given specific week start day.
  // fn beginning_of_week_with_start_day(
  //     &self,
  //     week_start_day: &WeekStartDay,
  // ) -> DateTime<Self::Timezone>;

  fn beginning_of_month(&self) -> DateTime<Self::Timezone>;
  fn beginning_of_quarter(&self) -> DateTime<Self::Timezone>;
  fn beginning_of_year(&self) -> DateTime<Self::Timezone>;

  fn end_of_minute(&self) -> DateTime<Self::Timezone>;
  fn end_of_hour(&self) -> DateTime<Self::Timezone>;
  fn end_of_day(&self) -> DateTime<Self::Timezone>;
  // fn end_of_week(&self) -> DateTime<Self::Timezone>;
  // fn end_of_week_with_start_day(&self, week_start_day: &WeekStartDay)
  //                               -> DateTime<Self::Timezone>;
  fn end_of_month(&self) -> DateTime<Self::Timezone>;
  fn end_of_quarter(&self) -> DateTime<Self::Timezone>;
  fn end_of_year(&self) -> DateTime<Self::Timezone>;

  fn week_of_year(&self) -> u32;
}

impl<T: TimeZone> DateTimeNow for DateTime<T> {
  type Timezone = T;

  fn beginning_of_minute(&self) -> DateTime<Self::Timezone> {
    let local_date_time = self.naive_local();
    let time5 =
      NaiveDate::from_ymd(local_date_time.year(), local_date_time.month(), local_date_time.day())
        .and_hms(local_date_time.hour(), local_date_time.minute(), 0);
    self.timezone().from_local_datetime(&time5).unwrap()
  }

  fn beginning_of_hour(&self) -> DateTime<Self::Timezone> {
    let local_date_time = self.naive_local();
    let time5 =
      NaiveDate::from_ymd(local_date_time.year(), local_date_time.month(), local_date_time.day())
        .and_hms(local_date_time.hour(), 0, 0);
    self.timezone().from_local_datetime(&time5).unwrap()
  }

  fn beginning_of_day(&self) -> DateTime<Self::Timezone> {
    let local_date_time = self.naive_local();
    let time5 =
      NaiveDate::from_ymd(local_date_time.year(), local_date_time.month(), local_date_time.day())
        .and_hms(0, 0, 0);
    self.timezone().from_local_datetime(&time5).unwrap()
  }

  // fn beginning_of_week(&self) -> DateTime<Self::Timezone> {
  //     self.beginning_of_week_with_start_day(&WeekStartDay::Monday)
  // }
  //
  // fn beginning_of_week_with_start_day(
  //     &self,
  //     week_start_day: &WeekStartDay,
  // ) -> DateTime<Self::Timezone> {
  //     let prec_day = match week_start_day {
  //         WeekStartDay::Monday => self.weekday().number_from_monday() - 1,
  //         WeekStartDay::Sunday => self.weekday().num_days_from_sunday(),
  //     };
  //     let time: DateTime<T> = self.clone().sub(Duration::days(prec_day as i64));
  //     let succ_local_date_time = time.naive_local();
  //     let time5 = NaiveDate::from_ymd(
  //         succ_local_date_time.year(),
  //         succ_local_date_time.month(),
  //         succ_local_date_time.day(),
  //     )
  //         .and_hms(0, 0, 0);
  //     self.timezone().from_local_datetime(&time5).unwrap()
  // }

  fn beginning_of_month(&self) -> DateTime<Self::Timezone> {
    let local_date_time = self.naive_local();
    let time5 =
      NaiveDate::from_ymd(local_date_time.year(), local_date_time.month(), 1).and_hms(0, 0, 0);
    self.timezone().from_local_datetime(&time5).unwrap()
  }

  fn beginning_of_quarter(&self) -> DateTime<Self::Timezone> {
    let local_date_time = self.naive_local();
    let month = match local_date_time.month() {
      1..=3 => 1u32,
      4..=6 => 4u32,
      7..=9 => 7u32,
      _ => 10u32,
    };
    let time5 = NaiveDate::from_ymd(local_date_time.year(), month, 1).and_hms(0, 0, 0);
    self.timezone().from_local_datetime(&time5).unwrap()
  }

  fn beginning_of_year(&self) -> DateTime<Self::Timezone> {
    let local_date_time = self.naive_local();
    let time5 = NaiveDate::from_ymd(local_date_time.year(), 1, 1).and_hms(0, 0, 0);
    self.timezone().from_local_datetime(&time5).unwrap()
  }

  fn end_of_minute(&self) -> DateTime<Self::Timezone> {
    let local_date_time = self.naive_local();
    let time5 =
      NaiveDate::from_ymd(local_date_time.year(), local_date_time.month(), local_date_time.day())
        .and_hms_nano(local_date_time.hour(), local_date_time.minute(), 59, 999999999);
    self.timezone().from_local_datetime(&time5).unwrap()
  }

  fn end_of_hour(&self) -> DateTime<Self::Timezone> {
    let local_date_time = self.naive_local();
    let time5 =
      NaiveDate::from_ymd(local_date_time.year(), local_date_time.month(), local_date_time.day())
        .and_hms_nano(local_date_time.hour(), 59, 59, 999999999);
    self.timezone().from_local_datetime(&time5).unwrap()
  }

  fn end_of_day(&self) -> DateTime<Self::Timezone> {
    let local_date_time = self.naive_local();
    let time5 =
      NaiveDate::from_ymd(local_date_time.year(), local_date_time.month(), local_date_time.day())
        .and_hms_nano(23, 59, 59, 999999999);
    self.timezone().from_local_datetime(&time5).unwrap()
  }

  // fn end_of_week(&self) -> DateTime<Self::Timezone> {
  //     self.end_of_week_with_start_day(&WeekStartDay::Monday)
  // }
  //
  // fn end_of_week_with_start_day(
  //     &self,
  //     week_start_day: &WeekStartDay,
  // ) -> DateTime<Self::Timezone> {
  //     let succ_day = match week_start_day {
  //         WeekStartDay::Monday => 7 - self.weekday().number_from_monday(),
  //         WeekStartDay::Sunday => 7 - self.weekday().number_from_sunday(),
  //     };
  //     let time: DateTime<T> = self.clone().add(Duration::days(succ_day as i64));
  //     let succ_local_date_time = time.naive_local();
  //     let time5 = NaiveDate::from_ymd(
  //         succ_local_date_time.year(),
  //         succ_local_date_time.month(),
  //         succ_local_date_time.day(),
  //     )
  //         .and_hms_nano(23, 59, 59, 999999999);
  //     self.timezone().from_local_datetime(&time5).unwrap()
  // }

  fn end_of_month(&self) -> DateTime<Self::Timezone> {
    let local_date_time = self.naive_local();
    let (year, month) = if local_date_time.month() == Month::December.number_from_month() {
      (local_date_time.year() + 1, Month::January.number_from_month())
    } else {
      (local_date_time.year(), local_date_time.month() + 1)
    };

    let time5 = NaiveDate::from_ymd(year, month, 1).and_hms(0, 0, 0);
    self
      .timezone()
      .from_local_datetime(&time5)
      .unwrap()
      .sub(Duration::nanoseconds(1))
  }

  fn end_of_quarter(&self) -> DateTime<Self::Timezone> {
    let local_date_time = self.naive_local();
    let (year, month) = match local_date_time.month() {
      1..=3 => (local_date_time.year(), 4u32),
      4..=6 => (local_date_time.year(), 7u32),
      7..=9 => (local_date_time.year(), 10u32),
      _ => (local_date_time.year() + 1, 1u32),
    };
    let time5 = NaiveDate::from_ymd(year, month, 1).and_hms(0, 0, 0);
    self
      .timezone()
      .from_local_datetime(&time5)
      .unwrap()
      .sub(Duration::nanoseconds(1))
  }

  fn end_of_year(&self) -> DateTime<Self::Timezone> {
    let local_date_time = self.naive_local();
    let time5 =
      NaiveDate::from_ymd(local_date_time.year(), 12, 31).and_hms_nano(23, 59, 59, 999999999);
    self.timezone().from_local_datetime(&time5).unwrap()
  }

  fn week_of_year(&self) -> u32 {
    self.iso_week().week()
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn test_time() {
    let t1 = Time::new("2022-05-01").unwrap();
    let bs = t1.to_bytes();

    let t2 = Time::from_bytes(bs.as_slice(), 0).unwrap();

    assert_eq!(t1, t2);
  }
}

// Time(2022-05-01T00:00:00Z, Month, Start)
