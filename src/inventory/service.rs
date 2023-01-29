use crate::animo::error::DBError;
use crate::services::{string_to_id, Data, Error, Params, Service};
use crate::store::{Report, WHError};
use crate::ws::error_general;
use crate::{
  auth, Application, ChangeTransformation, Memory, SOrganizations, Services, Transformation,
  TransformationKey, Value, ID,
};
use chrono::{DateTime, Utc};
use json::object::Object;
use json::JsonValue;
use std::sync::{Arc, RwLock};

pub(crate) struct Inventory {
  app: Application,
  path: Arc<String>,
}

impl Inventory {
  pub(crate) fn new(app: Application) -> Arc<dyn Service> {
    Arc::new(Inventory { app, path: Arc::new("inventory".to_string()) })
  }
}

impl Service for Inventory {
  fn path(&self) -> &str {
    &self.path
  }

  fn find(&self, params: Params) -> crate::services::Result {
    let oid = self.oid(&params)?;

    // let limit = self.limit(&params);
    // let skip = self.skip(&params);

    let storage = self.uuid("storage", &params)?;

    let dates = if let Some(dates) = self.date_range(&params)? {
      dates
    } else {
      return Err(Error::GeneralError("dates not defined".into()));
    };

    let report = match self.app.warehouse.database.get_report(storage, dates.0, dates.1) {
      Ok(report) => report.to_json(),
      Err(error) => return Err(Error::GeneralError(error.message())),
    };

    Ok(json::object! {
      data: JsonValue::Array(vec![report]),
      total: 1,
      "$skip": 0,
    })
  }

  fn get(&self, id: String, params: Params) -> crate::services::Result {
    Err(Error::NotImplemented)
  }

  fn create(&self, data: Data, params: Params) -> crate::services::Result {
    Err(Error::NotImplemented)
  }

  fn update(&self, id: String, data: Data, params: Params) -> crate::services::Result {
    Err(Error::NotImplemented)
  }

  fn patch(&self, id: String, data: Data, params: Params) -> crate::services::Result {
    Err(Error::NotImplemented)
  }

  fn remove(&self, id: String, params: Params) -> crate::services::Result {
    Err(Error::NotImplemented)
  }
}
