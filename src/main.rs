use crate::commutator::{Application, Commutator};
use actix::{Actor, Addr};
use actix_cors::Cors;
use actix_web::dev::ServiceRequest;
use actix_web::http::header;
use actix_web::{http, middleware, web, App, HttpServer};
use actix_web_httpauth::extractors::bearer::{BearerAuth, Config};
use actix_web_httpauth::extractors::AuthenticationError;
use actix_web_httpauth::middleware::HttpAuthentication;

use dbase::{FieldValue, Record};
use json::JsonValue;
use lazy_static::lazy_static;
use std::collections::HashMap;
use std::fs::File;
use std::io::{Error, ErrorKind, Write};
use std::path::PathBuf;
use std::sync::{Arc, RwLock};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use std::{io, thread};
use structopt::StructOpt;
use uuid::Uuid;

mod auth;
mod commutator;
mod inventory;
mod services;
mod settings;
mod storage;
mod utils;
mod websocket;
mod ws;

mod animo;
mod api;
mod hr;
pub mod links;
mod memories;
mod text_search;
mod use_cases;
pub mod warehouse;

use crate::animo::memory::*;
use crate::animo::shared::*;
use crate::hr::services::companies::Companies;
use crate::links::GetLinks;
use crate::memories::MemoriesInFiles;
use crate::settings::Settings;
use crate::storage::organizations::Workspace;
use crate::storage::Workspaces;
use crate::warehouse::primitive_types::Decimal;
use animo::db::AnimoDB;
use animo::memory::Memory;
use inventory::service::Inventory;
use service::utils::json::JsonParams;
use service::Services;
use store::balance::BalanceForGoods;
use store::elements::ToJson;
use store::error::WHError;
use store::operations::InternalOperation;
use store::qty::Qty;
use values::constants::{_DOCUMENT, _STATUS, _UUID};

#[derive(StructOpt, Debug)]
#[structopt(name = "basic")]
struct Opt {
  #[structopt(short = "c", long = "case", default_value = "001")]
  case: String,

  /// Run mode
  #[structopt(short, long, default_value = "server")]
  mode: String,

  /// Data folder
  #[structopt(short, long, default_value = "./data", parse(from_os_str))]
  data: PathBuf,
}

async fn check_ops(app: Application) -> io::Result<()> {
  let mut count = 0;

  // let mut prev_op: Option<Op> = None;
  let mut cur_balances: HashMap<Vec<u8>, BalanceForGoods> = HashMap::new();

  let topology = &app.warehouse.database.ordered_topologies[0];

  for item in topology.db().iterator_cf(&topology.cf().unwrap(), rocksdb::IteratorMode::Start) {
    let (_, value) = item.unwrap();

    let (cur, op_balance) = topology.from_bytes(&value).unwrap();

    // if cur.goods.to_string() != "c74f7aab-bbdd-4832-8bd3-0291470e8964".to_string() {
    //   continue;
    // }

    // if !cur.dependant.is_empty() {
    //   continue;
    // }

    if cur.is_virtual() || (cur.is_receive() && !cur.is_dependent && !cur.batch.is_empty()) {
    } else {
      continue;
    }

    count += 1;

    let key = cur
      .store
      .as_bytes()
      .iter() // store
      .chain(cur.batch.to_bytes(&cur.goods).iter()) // batch
      .copied()
      .collect();

    let cur_balance = cur_balances.entry(key).or_insert(BalanceForGoods::default());

    // println!("balance before: {:?} {cur_balance:?}", cur.store);
    // println!("cur: {:#?}", cur);

    // ================================================================
    println!("OpMutation {{");
    println!("id: Uuid::from_str(\"{}\").unwrap(),", cur.id);
    println!("date: dt(\"{}\").unwrap(),", cur.date.date_naive());
    println!("store: Uuid::from_str(\"{}\").unwrap(),", cur.store);
    match cur.store_into {
      None => println!("transfer: None,"),
      Some(s) => println!("transfer: Some(Uuid::from_str(\"{}\").unwrap()),", s),
    }
    println!("goods: Uuid::from_str(\"{}\").unwrap(),", cur.goods);
    println!(
      "batch: Batch {{ id: Uuid::from_str(\"{}\").unwrap(), date: dt(\"{}\").unwrap() }},",
      cur.batch.id,
      cur.batch.date.date_naive()
    );
    println!("before: None,");
    match &cur.op {
      InternalOperation::Inventory(_, _, _) => {},
      InternalOperation::Receive(q, c) => {
        println!(
          "after: Some((InternalOperation::Receive(Qty::new(vec![Number {{ number: Decimal::try_from({}).unwrap(), name: In(Uuid::from_str(\"{}\").unwrap(), None) }}]), Cost::from(Decimal::try_from(\"{:?}\").unwrap())), false)),",
          q.inner[0].number, q.inner[0].name.uuid(), c.number()
        )
      },
      InternalOperation::Issue(q, c, m) => {
        println!(
          "after: Some((InternalOperation::Issue(Qty::new(vec![Number {{ number: Decimal::try_from({}).unwrap(), name: In(Uuid::from_str(\"{}\").unwrap(), None) }}]), Cost::from(Decimal::try_from(\"{:?}\").unwrap()), Mode::{:?}), false)),",
          q.inner[0].number, q.inner[0].name.uuid(), c.number(), m,
        )
      },
    }
    // println!("is_dependent: {},", cur.is_dependent);
    // println!("dependant: {:?}", cur.dependant);
    println!("}},");
    // ================================================================

    cur_balance.apply(&cur.op);
    // println!("balance after: {:?} {cur_balance:?}", cur.store);
    // println!("====================================================================================");

    // topology.debug().unwrap();
    // app.warehouse.database.checkpoint_topologies[0].debug().unwrap();

    // assert_eq!(cur_balance, &op_balance, "\ncount {}", count);

    // if cur_balance != &op_balance {
    //   println!("NOT_EQUAL");
    // }
  }

  println!("count {count}");

  Ok(())
}

async fn reindex(
  settings: Arc<Settings>,
  app: Application,
  com: Addr<Commutator>,
) -> io::Result<()> {
  let mut count = 0;
  for ws in app.wss.list()? {
    for doc in ws.clone().into_iter() {
      // println!("{:?} {:?}", doc.id, doc.json().unwrap());

      let ctx = &doc.mem.ctx;

      let before = JsonValue::Null;
      let mut after = doc.json().unwrap();

      // goods/2023-05-12T09:08:16.827Z
      // if after["goods"].string() != "goods/2023-05-12T09:08:16.827Z".to_string() {
      //   continue;
      // }

      count += 1;

      // inject uuid if missing
      if after[_UUID].is_null() {
        let uuid = Uuid::new_v4().to_string();
        after[_UUID] = uuid.clone().into();

        storage::memories::index_uuid(
          &doc.mem.top_folder,
          &doc.path.parent().unwrap().into(),
          uuid.as_str(),
        )?;
      } else {
        // create symlink if not exist
        storage::memories::index_uuid(
          &doc.mem.top_folder,
          &doc.path.parent().unwrap().into(),
          &after[_UUID].string(),
        )?;
      }

      // replace "status" for "_status"
      if !after["status"].is_null() {
        after[_STATUS] = after["status"].clone();
        after.remove("status");
      }

      // replace "order" for "document"
      if !after["order"].is_null() {
        after[_DOCUMENT] = after["order"].clone();
        after.remove("order");
      }

      // delete batch from document if it exists
      after.remove("batch");

      match update_qty(&app, &ws, ctx, &mut after) {
        Ok(_) => {},
        Err(_) => {
          // log::debug!("skip_update_qty");
          continue;
        },
      }

      text_search::handle_mutation(&app, ctx, &before, &after).unwrap();

      store::elements::receive_data(
        &app,
        ws.id.to_string().as_str(),
        before.clone(),
        after.clone(),
        ctx,
        &HashMap::new(),
      )
      .unwrap();

      app.links().save_links(&ws, &ctx, &after, &before).unwrap();

      storage::save(&doc.path, after.dump())?;
    }
  }

  println!("count {count}");

  Ok(())
}

fn update_qty(
  app: &Application,
  ws: &Workspace,
  ctx: &Vec<String>,
  after: &mut JsonValue,
) -> io::Result<()> {
  let params =
    json::object! {oid: ws.id.to_string().as_str(), ctx: ["uom"], name: "Кор", enrich: false };
  let uom_in = match app.service("memories").find(service::Context::local(), params.clone()) {
    Ok(mut res) => {
      let mut uom_in = String::new();
      res["data"].members().for_each(|o| {
        if &(o["name"].string()) == "Кор" {
          uom_in = o["_uuid"].string()
        }
      });
      uom_in.to_json()
    },
    Err(_) => JsonValue::Null,
  };

  // update qty structure
  let ctx_str: Vec<&str> = ctx.iter().map(|s| s.as_str()).collect();

  let qty = after["qty"].clone();

  let goods =
    |ctx_str: Vec<&str>, data: &JsonValue, params: JsonValue| -> Result<JsonValue, WHError> {
      let goods_id = match ctx_str[..] {
        ["production", "produce"] => {
          let document = match app.service("memories").get(
            service::Context::local(),
            data["document"].string(),
            params.clone(),
          ) {
            Ok(doc) => doc,
            Err(e) => {
              return Err(WHError::new(e.to_string().as_str()));
            },
          };
          // log::debug!("_doc {document:?}");

          let product = match app.service("memories").get(
            service::Context::local(),
            document["product"].string(),
            params.clone(),
          ) {
            Ok(p) => p,
            Err(e) => {
              return Err(WHError::new(e.to_string().as_str()));
            },
          };
          // log::debug!("_product {product:?}");

          let goods = product["goods"].string();

          if &goods != "" {
            goods
          } else {
            return Ok(product);
          }
        },
        _ => data["goods"].string(),
      };

      let goods =
        match app.service("memories").get(service::Context::local(), goods_id, params.clone()) {
          Ok(g) => g,
          Err(e) => {
            return Err(WHError::new(e.to_string().as_str()));
          },
        };

      Ok(goods)
    };

  if !qty.is_null() {
    match <JsonValue as TryInto<Qty>>::try_into(qty.clone()) {
      Ok(_q) => {}, // nothing to do
      Err(_) => {
        // log::debug!("change_qty {qty:?}");
        let params = json::object! {oid: ws.id.to_string().as_str(), ctx: [], enrich: false };

        let goods = match goods(ctx_str.clone(), &after, params.clone()) {
          Ok(g) => g,
          Err(e) => {
            // log::debug!("goods_error: {e:?}, after: {after:?}");
            return Err(Error::new(ErrorKind::NotFound, e.message()));
          },
        };
        // log::debug!("_goods {goods:?}");

        let uom = match app.service("memories").get(
          service::Context::local(),
          goods["uom"].string(),
          params.clone(),
        ) {
          Ok(uom) => uom,
          Err(e) => {
            // log::debug!("uom_error {e}");
            return Err(Error::new(ErrorKind::NotFound, e.to_string()));
          },
        };
        // log::debug!("_uom {uom:?}");

        match ctx_str[..] {
          ["production", "produce"] => {
            let tmp_number = if qty.is_string() {
              // after["qty"]["number"] = Decimal::from(1).into();
              qty.clone()
            } else {
              qty["number"].clone()
            };

            if qty["uom"].is_null() {
              after["qty"]["number"] = Decimal::from(1).into();
              after["qty"]["uom"] = json::object! {"number": tmp_number, "uom": uom["_uuid"].clone(), "in": uom_in.clone()};
            }
          },
          _ => {
            if qty.is_string() {
              after["qty"]["number"] = qty.clone();
            }
            if !qty["uom"].is_object() {
              after["qty"]["uom"] = uom["_uuid"].clone();
            }
          },
        }

        // log::debug!("_new_after {:?}", after);
      },
    }
  }
  Ok(())
}

async fn server(settings: Arc<Settings>, app: Application, com: Addr<Commutator>) -> io::Result<()> {
  let domain = "https://animi.ws";
  let address = "localhost"; // "127.0.0.1"
  let port = 3030;

  log::info!("starting up {address}:{port} for {domain}");

  HttpServer::new(move || {
    // let auth = HttpAuthentication::bearer(auth::validator);

    let cors = Cors::default()
      .allowed_origin(domain)
      .allow_any_origin()
      .allow_any_method()
      // .allowed_origin_fn(|origin, _req_head| {
      //   println!("origin {origin}");
      //   origin.as_bytes().ends_with(b".rust-lang.org")
      // })
      // .allowed_methods(vec!["GET", "POST"])
      // .allowed_headers(vec![http::header::AUTHORIZATION, http::header::ACCEPT])
      // .allowed_header(http::header::CONTENT_TYPE)
      .max_age(3600);

    App::new()
      .wrap(cors)
      .app_data(web::Data::new(app.clone()))
      .app_data(web::Data::new(com.clone()))
      .wrap(middleware::Logger::default())
      // .wrap(auth)
      .service(web::scope("/socket.io").service(ws::start_connection))
      .service(web::scope("/"))
      .service(
        web::scope("/v1")
          // .service(websocket::start_connection_route)
          .service(api::memory_query)
          .service(api::memory_modify),
      )
      // .route("/ws/", web::get().to(websocket))
      .default_service(web::route().to(api::not_implemented))
  })
  .bind((address, port))?
  .run()
  .await
}

async fn startup() -> io::Result<()> {
  std::env::set_var("RUST_LOG", "debug,actix_web=debug,actix_server=debug");
  env_logger::init();

  let opt = Opt::from_args();

  let settings = Arc::new(Settings::new().unwrap());
  println!("db starting up");
  let db: AnimoDB = Memory::init(settings.database.memory.clone()).unwrap();
  println!("db started up");

  println!("app starting up");
  let (mut app, events_receiver) =
    Application::new(settings.clone(), Arc::new(db), Workspaces::new("./data/companies/"))
      .await
      .map_err(|e| io::Error::new(io::ErrorKind::Unsupported, e))?;

  {
    let mut engine = app.search.write().unwrap();
    engine.load(app.wss.clone()).unwrap();
  }

  app.register(services::Authentication::new(app.clone(), "authentication"));
  app.register(services::Users::new(app.clone(), "users"));

  app.register(Companies::new(app.clone()));

  app.register(MemoriesInFiles::new(app.clone(), "memories"));
  app.register(Inventory::new(app.clone()));

  println!("app started up");

  println!("com starting up");
  let mut com = Commutator::new(app.clone(), events_receiver).start();
  println!("com started up");

  match opt.mode.as_str() {
    "reindex" => reindex(settings, app, com).await,
    "server" => server(settings, app, com).await,
    "import" => {
      match opt.case.as_str() {
        "001" => use_cases::uc_001::import(&app.db),
        "002" => use_cases::uc_002::import(&app.db),
        "003" => use_cases::uc_003::import(&app),
        "005" => use_cases::uc_005::import(&app),
        "006" => use_cases::uc_006::import(&app),
        "007" => use_cases::uc_007::import(&app),
        "008" => use_cases::uc_008::import(&app),
        "009" => use_cases::uc_009::import(&app),
        "010" => use_cases::uc_010::import(&app),
        _ => unreachable!(),
      }
      Ok(())
    },
    "report" => {
      match opt.case.as_str() {
        "001" => use_cases::uc_001::report(&app.db),
        "002" => use_cases::uc_002::report(&app.db),
        "003" => use_cases::uc_003::report(&app),
        "005" => use_cases::uc_005::report(&app),
        "006" => use_cases::uc_006::report(&app),
        "007" => use_cases::uc_007::report(&app),
        "008" => use_cases::uc_008::report(&app),
        "009" => use_cases::uc_009::report(&app.db),
        "010" => use_cases::uc_010::report(&app),
        _ => unreachable!(),
      }
      Ok(())
    },
    "delete" => match opt.case.as_str() {
      "produce" => use_cases::uc_delete::delete_produce(&app),
      "transfer" => use_cases::uc_delete::delete_transfers_for_one_goods(
        &app,
        Some("склад"),
        "Полипропилен (дроб)",
      ),
      _ => unreachable!(),
    },
    "save" => match opt.case.as_str() {
      "roll" => use_cases::uc_save::save_roll(&app),
      "cups" => use_cases::uc_save::save_half_stuff_cups(&app),
      "produced" => use_cases::uc_save::save_produced(&app),
      "file_transfer" => use_cases::uc_save::save_transfer_from_file(&app),
      "goods_transfer" => use_cases::uc_save::save_transfer_for_goods(&app),
      _ => unreachable!(),
    },
    "replace" => match opt.case.as_str() {
      "goods" => {
        use_cases::uc_replace::replace_goods(&app, "Полипропилен дробленный", "Полипропилен (дроб)")
      },
      _ => unreachable!(),
    },
    "check" => check_ops(app).await,
    _ => unreachable!(),
  }
}

// fn main() {
//     let mut rt = tokio::runtime::Runtime::new().unwrap();
//     let local = tokio::task::LocalSet::new();
//     local.block_on(&mut rt, async move {
//         tokio::task::spawn_local(async move {
//             let local = tokio::task::LocalSet::new();
//             let sys = actix_rt::System::run_in_tokio("server", &local);
//             // define your actix-web app
//             // define your actix-web server
//             sys.await;
//         });
//         // This still allows use of tokio::spawn
//     });
// }

#[actix_web::main]
async fn main() -> io::Result<()> {
  startup().await
}

#[cfg(test)]
mod tests {
  use super::*;
  use crate::animo::memory::{ChangeTransformation, Transformation, TransformationKey, Value};
  use crate::warehouse::test_util::init;
  use actix_web::web::Bytes;
  use actix_web::{test, web, App};
  use rocksdb::Direction;

  #[actix_web::test]
  async fn test_put_get() {
    let (tmp_dir, settings, db) = init();

    let app = test::init_service(
      App::new()
        .app_data(web::Data::new(db.clone()))
        .wrap(middleware::Logger::default())
        .service(api::memory_modify)
        .service(api::memory_query)
        .default_service(web::route().to(api::not_implemented)),
    )
    .await;

    let changes = vec![ChangeTransformation {
      zone: *DESC,
      context: vec!["language".into(), "label".into()].into(),
      what: "english".into(),
      into_before: Value::Nothing,
      into_after: Value::String("language".into()),
    }];

    let req = test::TestRequest::post().uri("/memory/modify").set_json(changes).to_request();

    let response = test::call_and_read_body(&app, req).await;
    assert_eq!(response, Bytes::from_static(b""));

    let keys: Vec<TransformationKey> = vec![TransformationKey {
      context: vec!["language".into(), "label".into()].into(),
      what: "english".into(),
    }];

    let req = test::TestRequest::post().uri("/memory/query").set_json(keys).to_request();

    let response: Vec<Transformation> = test::call_and_read_body_json(&app, req).await;
    assert_eq!(
      response,
      vec![Transformation {
        context: vec!["language".into(), "label".into()].into(),
        what: "english".into(),
        into: Value::String("language".into())
      }]
    );

    let req = test::TestRequest::post().uri("/memory").set_json("").to_request();

    let response = test::call_service(&app, req).await;
    assert_eq!(response.status().to_string(), "501 Not Implemented");

    // stop db and delete data folder
    db.close();
    tmp_dir.close().unwrap();
  }
}
