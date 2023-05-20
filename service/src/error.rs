extern crate json;

use json::JsonValue;

//     400: BadRequest
//     401: NotAuthenticated
//     402: PaymentError
//     403: Forbidden
//     404: NotFound
//     405: MethodNotAllowed
//     406: NotAcceptable
//     408: Timeout
//     409: Conflict
//     411: LengthRequired
//     422: Unprocessable
//     429: TooManyRequests
//     500: GeneralError
//     501: NotImplemented
//     502: BadGateway
//     503: Unavailable

quick_error! {
  #[derive(Debug)]
  pub enum Error {
    NotAuthenticated(error: String) {
      display("{}", error)
    }
    NotFound(error: String) {
      display("{}", error)
    }
    IOError(error: String) {
      display("{}", error)
    }
    GeneralError(error: String) {
      display("{}", error)
      // from(e: crate::hik::errors::Error) -> (e.to_string())
    }
    //seems like it is not used anywhere
    // CameraError(errors: crate::hik::errors::Error) {
    //   display("{}", errors.to_string())
    // }
    CameraError(error: String) {
      display("{}", error.to_string())
    }
    NotImplemented
  }
}

impl From<Error> for std::io::Error {
  fn from(e: Error) -> std::io::Error {
    std::io::Error::new(std::io::ErrorKind::Other, e.to_string())
  }
}

impl std::convert::From<std::io::Error> for Error {
  fn from(e: std::io::Error) -> Self {
    Error::IOError(e.to_string())
  }
}

impl std::convert::From<chrono::ParseError> for Error {
  fn from(e: chrono::ParseError) -> Self {
    Error::IOError(e.to_string())
  }
}

impl std::convert::From<uuid::Error> for Error {
  fn from(e: uuid::Error) -> Self {
    Error::IOError(e.to_string())
  }
}

// impl std::convert::From<store::WHError> for Error {
//     fn from(e: store::WHError) -> Self {
//         Error::IOError(e.message())
//     }
// }

impl Error {
  fn to_code(&self) -> usize {
    match self {
      Error::NotAuthenticated(_) => 401,
      Error::NotFound(_) => 404,
      Error::NotImplemented => 501,
      _ => 500,
    }
  }

  fn to_class_name(&self) -> &str {
    match self {
      Error::NotAuthenticated(_) => "not-authenticated",
      Error::NotFound(_) => "not-found",
      Error::IOError(_) => "io-errors",
      Error::GeneralError(_) => "general-errors",
      Error::CameraError(_) => "general-errors",
      Error::NotImplemented => "not-implemented",
    }
  }

  fn to_name(&self) -> &str {
    match self {
      Error::NotAuthenticated(_) => "NotAuthenticated",
      Error::NotFound(_) => "NotFound",
      Error::IOError(_) => "IOError",
      Error::GeneralError(_) => "GeneralError",
      Error::CameraError(_) => "GeneralError",
      Error::NotImplemented => "NotImplemented",
    }
  }

  pub fn to_json(&self) -> JsonValue {
    json::object! {
      className: self.to_class_name(),
      code: self.to_code(),
      message: self.to_string(),
      name: self.to_name(),
    }
  }
}
