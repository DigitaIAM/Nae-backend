use base64::Engine;
use blake2::Digest;
use bytecheck::CheckBytes;
use rkyv::{Archive, Deserialize, Serialize};
use std::array::TryFromSliceError;
use std::fmt::{Display, Formatter};

pub mod constants;

type Hasher = blake2::Blake2s256;

pub const ID_BYTES: usize = 32;

pub const ID_MIN: ID = ID([u8::MIN; ID_BYTES]);
pub const ID_MAX: ID = ID([u8::MAX; ID_BYTES]);

// #[derive(Debug, Clone, Hash, Serialize, Deserialize, Eq, PartialEq, Copy)]
#[derive(Clone, Copy, Hash, Ord, PartialOrd, Eq)] // , serde::Serialize, serde::Deserialize
#[derive(Archive, Deserialize, Serialize, Debug, PartialEq)]
// This will generate a PartialEq impl between our unarchived and archived types
// #[archive(compare(PartialEq))]
// To use the safe API, you have to derive CheckBytes for the archived type
#[archive_attr(derive(CheckBytes, Debug))]
pub struct ID(pub [u8; ID_BYTES]);

impl ID {
  pub fn random() -> Self {
    use rand::{distributions::Alphanumeric, Rng};

    let s: String = rand::thread_rng()
      .sample_iter(&Alphanumeric)
      .take(256)
      .map(char::from)
      .collect();

    ID::from(s)
  }

  pub(crate) fn new(data: &[u8]) -> Result<Self, IDError> {
    if data.len() != ID_BYTES {
      Err(IDError::from(format!("ID require {} bytes, but got {}", ID_BYTES, data.len())))
    } else {
      let mut a = [0; ID_BYTES];
      a[..ID_BYTES].copy_from_slice(&data[..ID_BYTES]);
      Ok(ID(a))
    }
  }

  pub fn from_base64<T: AsRef<[u8]>>(input: T) -> Result<ID, IDError> {
    match base64::engine::general_purpose::URL_SAFE_NO_PAD.decode(input) {
      Ok(bs) => ID::new(bs.as_slice()),
      Err(msg) => Err(IDError::from(msg.to_string())),
    }
  }

  pub fn to_base64(&self) -> String {
    base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(self.0)
  }

  pub fn to_clear(&self) -> String {
    self.to_base64().replace(['_', '-'], "")[..12].to_string()
  }

  // TODO make `const`
  pub fn for_constant(data: &str) -> Self {
    data.into()
  }

  pub fn as_slice(&self) -> &[u8] {
    self.0.as_slice()
  }

  pub fn bytes(context: &Vec<ID>, what: &ID) -> Vec<u8> {
    let mut bs = Vec::with_capacity(ID_BYTES * (1 + context.len()));

    for id in context {
      bs.extend_from_slice(id.0.as_slice());
    }

    bs.extend_from_slice(what.0.as_slice());

    bs
  }
}

impl From<&str> for ID {
  fn from(data: &str) -> Self {
    let mut bs = [0; 32];
    bs.copy_from_slice(Hasher::digest(data).as_slice());
    ID(bs)
  }
}

impl From<String> for ID {
  fn from(data: String) -> Self {
    let mut bs = [0; 32];
    bs.copy_from_slice(Hasher::digest(data).as_slice());
    ID(bs)
  }
}

impl TryFrom<&[u8]> for ID {
  type Error = IDError;

  fn try_from(bs: &[u8]) -> Result<Self, IDError> {
    let bs = bs.try_into().map_err(|e: TryFromSliceError| IDError::from(e.to_string()))?;
    Ok(ID(bs))
  }
}

impl Display for ID {
  fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
    write!(f, "{}", self.to_base64())
  }
}

impl serde::Serialize for ID {
  fn serialize<S>(&self, s: S) -> Result<S::Ok, S::Error>
  where
    S: serde::Serializer,
  {
    s.serialize_str(self.to_base64().as_str())
  }
}

impl<'de> serde::Deserialize<'de> for ID {
  fn deserialize<D>(d: D) -> Result<Self, D::Error>
  where
    D: serde::Deserializer<'de>,
  {
    struct CustomVisitor;

    impl<'de> serde::de::Visitor<'de> for CustomVisitor {
      type Value = ID;

      fn expecting(&self, formatter: &mut Formatter) -> std::fmt::Result {
        write!(formatter, "base64-encoded ID")
      }

      fn visit_str<E>(self, v: &str) -> Result<Self::Value, E>
      where
        E: serde::de::Error,
      {
        ID::from_base64(v.as_bytes()).map_err(|e| E::custom(e.to_string()))
      }
    }

    d.deserialize_string(CustomVisitor)
  }
}

#[derive(Debug, Clone, PartialEq)]
pub struct IDError {
  message: String,
}

impl From<&str> for IDError {
  fn from(msg: &str) -> IDError {
    IDError { message: msg.to_string() }
  }
}

impl From<String> for IDError {
  fn from(message: String) -> IDError {
    IDError { message }
  }
}

impl Display for IDError {
  fn fmt(&self, formatter: &mut Formatter) -> Result<(), std::fmt::Error> {
    self.message.fmt(formatter)
  }
}

impl actix_web::ResponseError for IDError {}
