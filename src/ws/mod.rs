use json::JsonValue;

mod start;
pub(crate) use start::start_connection;

mod messages;
pub(crate) use messages::*;

pub(crate) mod engine_io;
pub(crate) mod socket_io;

fn error(class_name: &str, name: &str, code: u16, message: &str) -> JsonValue {
  json::object! {
    className: class_name,
    code: code,
    message: message,
    name: name,
  }
}

pub(crate) fn error_not_found(message: &str) -> JsonValue {
  error("not-found", "NotFound", 404, message)
}

pub(crate) fn error_general(message: &str) -> JsonValue {
  error("general-error", "GeneralError", 500, message)
}

