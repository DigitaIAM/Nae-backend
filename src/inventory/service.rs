use crate::animo::error::DBError;
use crate::services::{string_to_id, Data, Params};
use crate::ws::error_general;
use crate::{
  animo::memory::{ChangeTransformation, Memory, Transformation, TransformationKey, Value, ID},
  auth,
  commutator::Application,
  storage::SOrganizations,
};
use chrono::{DateTime, Utc};
use json::object::Object;
use json::JsonValue;
use service::error::Error;
use service::{Service, Services};
use std::sync::{Arc, RwLock};
use store::elements::Batch;
use store::{
  elements::{Report, ToJson},
  error::WHError,
};

pub struct Inventory {
  app: Application,
  path: Arc<String>,
}

impl Inventory {
  pub fn new(app: Application) -> Arc<dyn Service> {
    Arc::new(Inventory { app, path: Arc::new("inventory".to_string()) })
  }
}

impl Service for Inventory {
  fn path(&self) -> &str {
    &self.path
  }

  fn find(&self, params: Params) -> crate::services::Result {
    let oid = crate::services::oid(&params)?;

    // let limit = self.limit(&params);
    let skip = self.skip(&params);

    if skip != 0 {
      return     Ok(json::object! {
      data: json::array![],
      total: 0,
      "$skip": skip,
    });
    }

    // println!("FN_FIND_PARAMS: {:#?}", params);

    if params.is_array() {
      let params = params[0]["filter"].clone();

      let storage = crate::services::uuid("storage", &params)?;

      let goods = crate::services::uuid("goods", &params)?;

      let batch_id = crate::services::uuid("batch_id", &params)?;

      let batch_date: Result<DateTime<Utc>, Error> =
          if let Some(date) = &params["batch_date"].as_str() {
            Ok(DateTime::parse_from_rfc3339(date)?.into())
          } else {
            Err(service::error::Error::GeneralError(String::from("No batch_date in params for fn find")))
          };

      let batch = Batch { id: batch_id, date: batch_date? };

      let dates = if let Some(dates) = self.date_range(&params)? {
        dates
      } else {
        return Err(Error::GeneralError("dates not defined".into()));
      };

      let report = match self
          .app
          .warehouse
          .database
          .get_report_for_goods(storage, goods, &batch, dates.0, dates.1)
      {
        Ok(report) => report,
        Err(error) => return Err(Error::GeneralError(error.message())),
      };

      println!("REPORT = {report:?}");

      Ok(json::object! {
      data: report,
      total: 1,
      "$skip": 0,
      })

    } else {
      let storage = crate::services::uuid("storage", &params)?;

      let dates = if let Some(dates) = self.date_range(&params)? {
        dates
      } else {
        return Err(Error::GeneralError("dates not defined".into()));
      };

      let report = match self
          .app
          .warehouse
          .database
          .get_report_for_storage(storage, dates.0, dates.1)
      {
        Ok(report) => report.to_json(),
        Err(error) => return Err(Error::GeneralError(error.message())),
      };

      // println!("REPORT = {report:?}");

      Ok(json::object! {
      data: report,
      total: 1,
      "$skip": 0,
      })
    }
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
