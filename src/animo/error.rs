use std::error;
use std::fmt;

#[derive(Debug, Clone, PartialEq)]
pub struct DBError {
    message: String,
}

impl From<&str> for DBError {
    fn from(msg: &str) -> DBError {
        DBError { message: msg.to_string() }
    }
}

impl From<String> for DBError {
    fn from(message: String) -> DBError {
        DBError { message }
    }
}

impl From<rocksdb::Error> for DBError {
    fn from(e: rocksdb::Error) -> DBError {
        DBError { message: e.to_string() }
    }
}

impl From<DBError> for String {
    fn from(e: DBError) -> String {
        e.message
    }
}

impl error::Error for DBError {
    fn description(&self) -> &str {
        &self.message
    }
}

impl fmt::Display for DBError {
    fn fmt(&self, formatter: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        self.message.fmt(formatter)
    }
}

pub(crate) fn convert(e: impl ToString) -> DBError {
    DBError { message: e.to_string() }
}