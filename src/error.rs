use std::error;
use std::fmt;

#[derive(Debug, Clone, PartialEq)]
pub struct Error {
    message: String,
}

impl From<&str> for Error {
    fn from(msg: &str) -> Error {
        Error { message: msg.to_string() }
    }
}

impl From<rocksdb::Error> for Error {
    fn from(e: rocksdb::Error) -> Error {
        Error { message: e.to_string() }
    }
}

impl From<Error> for String {
    fn from(e: Error) -> String {
        e.message
    }
}

impl error::Error for Error {
    fn description(&self) -> &str {
        &self.message
    }
}

impl fmt::Display for Error {
    fn fmt(&self, formatter: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        self.message.fmt(formatter)
    }
}