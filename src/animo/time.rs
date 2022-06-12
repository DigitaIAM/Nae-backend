use serde::{Deserialize, Serialize};
use std::cmp::Ordering;
use std::ops::{Add, Sub};
use chrono::{Datelike, DateTime, Duration, MIN_DATETIME, NaiveDate, NaiveDateTime, NaiveTime, ParseError, Timelike, TimeZone, Utc};
use chrono::serde::ts_milliseconds;
use now::DateTimeNow;
use rust_decimal::Decimal;
use crate::animo::error::DBError;


#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
enum IntervalPosition {
    Start,
    End
}

impl IntervalPosition {
    fn to_byte(&self) -> u8 {
        match self {
            IntervalPosition::Start => 1,
            IntervalPosition::End => 2,
        }
    }

    pub(crate) fn from_bytes(bs: &[u8], offset: usize) -> Result<Self,DBError> {
        match bs[offset..offset+1] {
            [1] => Ok(IntervalPosition::End),
            [2] => Ok(IntervalPosition::Start),
            [b] => Err(format!("wrong byte {}", b).into()),
            [] => Err(format!("no byte").into()),
            [_, _, ..] => unreachable!("internal error")
        }
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
enum TimeAccuracy {
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

    pub(crate) fn from_bytes(bs: &[u8], offset: usize) -> Result<Self,DBError> {
        match bs[offset..offset+1] {
            [1] => Ok(TimeAccuracy::Year),
            [2] => Ok(TimeAccuracy::Month),
            [3] => Ok(TimeAccuracy::Day),
            [4] => Ok(TimeAccuracy::Hour),
            [5] => Ok(TimeAccuracy::Minute),
            [6] => Ok(TimeAccuracy::Second),
            [b] => Err(format!("wrong byte {}", b).into()),
            [] => Err(format!("no byte").into()),
            [_, _, ..] => unreachable!("internal error")
        }
    }
}

#[derive(Debug, Clone, Eq, Serialize, Deserialize)]
pub struct Time(
    #[serde(with = "ts_milliseconds")]
    DateTime<Utc>,
    TimeAccuracy,
    IntervalPosition
);

impl Time {

    pub(crate) fn zero() -> Self {
        Time(
            std::time::UNIX_EPOCH.into(),
            TimeAccuracy::Day,
            IntervalPosition::Start
        )
    }

    pub(crate) fn new(dt: &str) -> Result<Self,DBError> {
        let ts: DateTime<Utc> = DateTime::parse_from_rfc3339(format!("{}T00:00:00Z", dt).as_str())
            .map_err(|e| DBError::from(e.to_string()))?
            .into();

        Ok(Time(ts.beginning_of_day(), TimeAccuracy::Day, IntervalPosition::Start))
    }

    pub(crate) fn to_bytes(&self) -> Vec<u8> {
        let ts: u64 = self.0.timestamp().try_into().unwrap();

        let t = ts.to_be_bytes();   // timestamp
        let f = [self.1.to_byte()]; // accuracy
        let b = [self.2.to_byte()]; // interval position
        [t.as_slice(), f.as_slice(), b.as_slice()].concat()
    }

    pub(crate) fn from_bytes(bs: &[u8], offset: usize) -> Result<Self,DBError> {
        let convert = |bs: &[u8]| -> [u8; 8] {
            bs.try_into().expect("slice with incorrect length")
        };

        let ts = u64::from_be_bytes(convert(&bs[offset..offset+8]));
        let ts = Utc.timestamp_millis(ts as i64);

        let accuracy = TimeAccuracy::from_bytes(bs, offset+8)?;
        let position = IntervalPosition::from_bytes(bs, offset+9)?;

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
                }
                _ => unimplemented!("{:?}", self)
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
                }
                _ => unimplemented!("{:?}", self)
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
                        let ts = self.0
                            .end_of_day()
                            .add(Duration::nanoseconds(1));
                        Time(ts, TimeAccuracy::Day, IntervalPosition::Start)
                    }
                    IntervalPosition::End => {
                        let ts = self.0.add(Duration::nanoseconds(1));
                        Time(ts, TimeAccuracy::Day, IntervalPosition::End)
                    }
                }
            }
            _ => unimplemented!("{:?}", self)
        }
    }

    pub(crate) fn sub_quantum(&self) -> Self {
        match self.1 {
            TimeAccuracy::Day => {
                match self.2 {
                    IntervalPosition::Start => {
                        let ts = self.0.sub(Duration::nanoseconds(1));
                        Time(ts, TimeAccuracy::Day, IntervalPosition::End)
                    }
                    IntervalPosition::End => {
                        let ts = self.0
                            .beginning_of_day()
                            .sub(Duration::nanoseconds(1));
                        Time(ts, TimeAccuracy::Day, IntervalPosition::End)
                    }
                }
            }
            TimeAccuracy::Month => {
                match self.2 {
                    IntervalPosition::Start => {
                        let ts = self.0.sub(Duration::nanoseconds(1));
                        Time(ts, TimeAccuracy::Month, IntervalPosition::End)
                    }
                    IntervalPosition::End => {
                        unimplemented!("{:?}", self)
                    }
                }
            }
            _ => unimplemented!("{:?}", self)
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
        unimplemented!()
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
    pub fn new(from: &str, till: &str) -> Result<Self,DBError> {
        let from = Time::new(from)?.start();
        let till = Time::new(till)?.end();
        // TODO raise error in case from.1 != till.1
        Ok(TimeInterval { from, till })
    }
}