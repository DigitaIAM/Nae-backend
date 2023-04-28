use crate::commutator::Application;
use crate::services::{Data, Params};
use chrono::{DateTime, Utc};
use service::error::Error;
use service::utils::json::JsonParams;
use service::Service;
use std::sync::Arc;
use store::batch::Batch;
use store::elements::ToJson;

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
    let _oid = crate::services::oid(&params)?;

    // let limit = self.limit(&params);
    let skip = self.skip(&params);

    if skip != 0 {
      return Ok(json::object! {
        data: json::array![],
        total: 0,
        "$skip": skip,
      });
    }

    // println!("FN_FIND_PARAMS: {:#?}", params);

    if params.is_array() {
      let params = self.params(&params);

      let filter = params["filter"].clone();

      let storage = filter["storage"].uuid()?;
      let goods = filter["goods"].uuid()?;

      let batch_id = filter["batch_id"].uuid()?;

      println!("1");
      let batch_date: DateTime<Utc> = filter["batch_date"].date_with_check()?;

      let batch = Batch { id: batch_id, date: batch_date };

      println!("2");
      let dates = if let Some(dates) = self.date_range(&filter)? {
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

      // println!("REPORT = {report:?}");

      Ok(json::object! {
        data: report,
        total: 1,
        "$skip": 0,
      })
    } else {
      let params = self.params(&params);

      let storage = params["storage"].uuid()?;

      let dates = if let Some(dates) = self.date_range(params)? {
        dates
      } else {
        return Err(Error::GeneralError("dates not defined".into()));
      };

      let report =
        match self.app.warehouse.database.get_report_for_storage(storage, dates.0, dates.1) {
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

  fn get(&self, _id: String, _params: Params) -> crate::services::Result {
    Err(Error::NotImplemented)
  }

  fn create(&self, _data: Data, _params: Params) -> crate::services::Result {
    Err(Error::NotImplemented)
  }

  fn update(&self, _id: String, _data: Data, _params: Params) -> crate::services::Result {
    Err(Error::NotImplemented)
  }

  fn patch(&self, _id: String, _data: Data, _params: Params) -> crate::services::Result {
    Err(Error::NotImplemented)
  }

  fn remove(&self, _id: String, _params: Params) -> crate::services::Result {
    Err(Error::NotImplemented)
  }
}
