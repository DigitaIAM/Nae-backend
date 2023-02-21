use std::fmt::{Display, Formatter};
use std::result;

pub type Result<T> = std::result::Result<T, Error>;

quick_error! {
    #[derive(Debug)]
    pub enum Error {
        IOError(error: String) {
            display("IO errors: {}", error)
        }
        RequestBuilderNotCloneable {
            display("Request body must not be a stream.")
        }
        UrlError(error: String) {
            display("Unable to parse URL: {}", error)
        }
        ConnectionError(error: reqwest::Error) {
            display("Unable to connect to camera: {}", error)
            source(error)
        }
        CameraInvalidResponseBody(error: reqwest::Error) {
            display("Camera returned mangled response body: {}", error)
            source(error)
        }
        DigestAuth(error: digest_auth::Error) {
            display("{}", error)
        }
        ToStr(error: reqwest::header::ToStrError) {
            display("{}", error)
        }
        AuthHeaderMissing {
            display("The header 'www-authenticate' is missing.")
        }
        AuthenticationFailed (error: String) {
            display("Could not authenticate with camera: {}", error)
        }
        StreamInvalid(error: String) {
            display("Stream could not be resolved to a multipart form: {}", error)
        }
        ConnectionClosed {
            display("Camera closed connection")
        }
        DeviceInfoInvalid(error: crate::hik::data::device_info::DeviceInfoParseError) {
            from()
            source(error)
        }
        TriggersInvalid(error: crate::hik::data::triggers_parser::TriggerParseError) {
            from()
            source(error)
        }
        AlertInvalid(error: crate::hik::data::alert_item::AlertParseError) {
            from()
            source(error)
        }
    }
}

impl From<std::io::Error> for Error {
  fn from(e: std::io::Error) -> Self {
    Error::IOError(e.to_string())
  }
}
