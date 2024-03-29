use crate::commutator::{Application, Commutator};
use actix::{Actor, Addr};
use actix_cors::Cors;
use actix_web::{middleware, web, App, HttpServer};

use chrono::Utc;
use json::JsonValue;
use std::collections::HashMap;
use std::fs::File;
use std::io;
use std::io::{Error, ErrorKind, Write};
use std::path::PathBuf;
use std::sync::Arc;
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
pub mod memories;
mod text_search;
mod use_cases;
pub mod warehouse;

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
use store::error::WHError;
use store::operations::OpMutation;
use store::qty::Qty;
use values::c;

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

  /// port
  #[structopt(short, long, default_value = "3030")]
  port: u16,
}

async fn fix_topologies(app: Application) -> io::Result<()> {
  let mut count = 0;

  // let mut prev_op: Option<Op> = None;
  let mut cur_balances: HashMap<Vec<u8>, BalanceForGoods> = HashMap::new();

  let topology = &app.warehouse.database.ordered_topologies[0];

  let time = Utc::now().to_string();
  let path = format!("./fix_logs/fix_log_{}.txt", &time);
  let mut log_file = File::create(path.clone())?;

  for item in topology.db().iterator_cf(&topology.cf().unwrap(), rocksdb::IteratorMode::Start) {
    let (_, value) = item.unwrap();

    let (cur, op_balance) = topology.from_bytes(&value).unwrap();

    // if cur.goods.to_string() != "c74f7aab-bbdd-4832-8bd3-0291470e8964".to_string() {
    //   continue;
    // }

    if !cur.dependant.is_empty() {
      continue;
    }

    // if cur.is_virtual() || (cur.is_receive() && !cur.is_dependent && !cur.batch.is_empty()) {
    // } else {
    //   continue;
    // }

    let key = cur
      .store
      .as_bytes()
      .iter() // store
      .chain(cur.batch.to_bytes(&cur.goods).iter())
      .copied()
      .collect();

    let cur_balance = cur_balances.entry(key).or_insert(BalanceForGoods::default());

    // println!("balance before: {:?} {cur_balance:?}", cur.store);
    // println!("cur: {:#?}", cur);

    // code for print ops and copy them into test
    // ================================================================
    // println!("OpMutation {{");
    // println!("id: Uuid::from_str(\"{}\").unwrap(),", cur.id);
    // println!("date: dt(\"{}\").unwrap(),", cur.date.date_naive());
    // println!("store: Uuid::from_str(\"{}\").unwrap(),", cur.store);
    // match cur.store_into {
    //   None => println!("transfer: None,"),
    //   Some(s) => println!("transfer: Some(Uuid::from_str(\"{}\").unwrap()),", s),
    // }
    // println!("goods: Uuid::from_str(\"{}\").unwrap(),", cur.goods);
    // println!(
    //   "batch: Batch {{ id: Uuid::from_str(\"{}\").unwrap(), date: dt(\"{}\").unwrap() }},",
    //   cur.batch.id,
    //   cur.batch.date.date_naive()
    // );
    // println!("before: None,");
    // match &cur.op {
    //   InternalOperation::Inventory(_, _, _) => {},
    //   InternalOperation::Receive(q, c) => {
    //     println!(
    //       "after: Some((InternalOperation::Receive(Qty::new(vec![Number {{ number: Decimal::try_from({}).unwrap(), name: In(Uuid::from_str(\"{}\").unwrap(), None) }}]), Cost::from(Decimal::try_from(\"{:?}\").unwrap())), false)),",
    //       q.inner[0].number, q.inner[0].name.uuid(), c.number()
    //     )
    //   },
    //   InternalOperation::Issue(q, c, m) => {
    //     println!(
    //       "after: Some((InternalOperation::Issue(Qty::new(vec![Number {{ number: Decimal::try_from({}).unwrap(), name: In(Uuid::from_str(\"{}\").unwrap(), None) }}]), Cost::from(Decimal::try_from(\"{:?}\").unwrap()), Mode::{:?}), false)),",
    //       q.inner[0].number, q.inner[0].name.uuid(), c.number(), m,
    //     )
    //   },
    // }
    // // println!("is_dependent: {},", cur.is_dependent);
    // // println!("dependant: {:?}", cur.dependant);
    // println!("}},");
    // ================================================================

    cur_balance.apply(&cur.op);
    // println!("balance after: {:?} {cur_balance:?}", cur.store);
    // println!("====================================================================================");

    // topology.debug().unwrap();
    // app.warehouse.database.checkpoint_topologies[0].debug().unwrap();

    // assert_eq!(cur_balance, &op_balance, "\ncount {}", count);

    if cur_balance != &op_balance {
      count += 1;
      println!("NOT_EQUAL \n{cur_balance:?} \nvs. {op_balance:?}");

      let old = format!("op {:?}\nold: balance {:?}", cur, op_balance);
      let new = format!("\nnew: balance {:?}\n\n", cur_balance);

      log_file.write_all(old.as_bytes())?;
      log_file.write_all(new.as_bytes())?;

      let next_op_date = match topology.operation_after(&cur, true) {
        Ok(res) => {
          if let Some((next_op, _)) = res {
            Some(next_op.date)
          } else {
            None
          }
        },
        Err(e) => {
          println!("check_ops ERROR: {}", e.message());
          return Err(Error::new(ErrorKind::NotFound, "check_ops ERROR"));
        },
      };

      let cur_mut = OpMutation::new(
        cur.id,
        cur.date,
        cur.store,
        cur.store_into,
        cur.goods,
        cur.batch.clone(),
        Some(cur.op.clone()),
        Some(cur.op.clone()),
      );

      for ordered_topology in app.warehouse.database.ordered_topologies.iter() {
        ordered_topology.put(&cur, cur_balance).unwrap();
      }

      for checkpoint_topology in app.warehouse.database.checkpoint_topologies.iter() {
        checkpoint_topology
          .checkpoint_update(&cur_mut, next_op_date, cur_balance)
          .unwrap();
      }
    }
  }

  println!("count {count}");

  Ok(())
}

async fn reindex(
  // settings: Arc<Settings>,
  app: Application,
  // com: Addr<Commutator>,
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
      if after[c::UUID].is_null() {
        let uuid = Uuid::new_v4().to_string();
        after[c::UUID] = uuid.clone().into();

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
          &after[c::UUID].string(),
        )?;
      }

      // replace "status" for "_status"
      if !after["status"].is_null() {
        after[c::STATUS] = after["status"].clone();
        after.remove("status");
      }

      // replace "order" for "document"
      if !after["order"].is_null() {
        after[c::DOCUMENT] = after["order"].clone();
        after.remove("order");
      }

      // delete batch from document if it exists
      // let ctx_str: Vec<&str> = ctx.iter().map(|s| s.as_str()).collect();
      // match ctx_str[..] {
      //   ["production", "material", "used"] => {},
      //   _ => {
      //     after.remove("batch");
      //   },
      // }

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

  // app.warehouse().database.ordered_topologies[0].debug().unwrap();
  // app.warehouse().database.checkpoint_topologies[0].debug().unwrap();

  Ok(())
}

fn update_qty(
  app: &Application,
  ws: &Workspace,
  ctx: &Vec<String>,
  after: &mut JsonValue,
) -> io::Result<()> {
  // update qty structure
  let qty = after["qty"].clone();

  let goods =
    |ctx_str: &Vec<&str>, data: &JsonValue, params: JsonValue| -> Result<JsonValue, WHError> {
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
      // log::debug!("__goods {goods:?}");

      Ok(goods)
    };

  if !qty.is_null() && !qty["number"].is_null() {
    let params = json::object! {oid: ws.id.to_string().as_str(), ctx: [], enrich: false };

    let ctx_str: Vec<&str> = ctx.iter().map(|s| s.as_str()).collect();

    let goods = match goods(&ctx_str, &after, params.clone()) {
      Ok(g) => g,
      Err(e) => {
        log::debug!("goods_error: {e:?}, after: {after:?}");
        return Err(Error::new(ErrorKind::NotFound, e.message()));
      },
    };

    // get whole object cause we need an uuid
    let uom = match app.service("memories").get(
      service::Context::local(),
      goods["uom"].string(),
      params.clone(),
    ) {
      Ok(uom) => uom[c::UUID].clone(),
      Err(e) => {
        // log::debug!("uom_error {e}");
        return Err(Error::new(ErrorKind::NotFound, e.to_string()));
      },
    };
    // log::debug!("_uom {uom:?}");

    let tmp_number = if qty.is_string() {
      // after["qty"]["number"] = Decimal::from(1).into();
      qty.clone()
    } else {
      qty["uom"]["number"].clone()
    };

    let box_uom = String::from("76db8665-68bf-4088-857a-cce650bac352");
    let roll_uom = String::from("3c887c88-964c-4ce2-b1f0-c7f1709e233a");

    match <JsonValue as TryInto<Qty>>::try_into(qty.clone()) {
      Ok(_q) => {
        // workaround to fix roll production uom
        match ctx_str[..] {
          ["production", "produce"] => {
            if goods["name"].string().starts_with("Пленка") {
              after["qty"]["number"] = Decimal::from(1).into();
              after["qty"]["uom"] =
                json::object! {"number": tmp_number, "uom": uom, "in": roll_uom.clone()};
              // println!("RULON1 {:?}", after["qty"]);
            }
          },
          _ => {},
        }
      }, // nothing to do
      Err(_) => {
        log::debug!("change_qty {qty:?}");
        match ctx_str[..] {
          ["production", "produce"] => {
            // if qty["uom"].is_null() {
            //   after["qty"]["number"] = Decimal::from(1).into();
            //   after["qty"]["uom"] = json::object! {"number": tmp_number, "uom": uom, "in": box_uom.clone()};
            // }

            if goods["name"].string().starts_with("Пленка") {
              // println!("RULON2");
              after["qty"]["number"] = Decimal::from(1).into();
              after["qty"]["uom"] =
                json::object! {"number": tmp_number, "uom": uom, "in": roll_uom.clone()};
            } else {
              after["qty"]["number"] = Decimal::from(1).into();
              after["qty"]["uom"] =
                json::object! {"number": tmp_number, "uom": uom, "in": box_uom.clone()};
            }
          },
          _ => {
            if qty.is_string() {
              after["qty"]["number"] = qty.clone();
            }
            if !qty["uom"].is_object() {
              after["qty"]["uom"] = uom;
            }
          },
        }
        // log::debug!("_new_after {:?}", after);
      },
    }
  }
  Ok(())
}

async fn server(
  _settings: Arc<Settings>,
  app: Application,
  com: Addr<Commutator>,
  port: u16,
) -> io::Result<()> {
  let domain = "https://animi.ws";
  // "127.0.0.1"
  let address = "localhost";
  // let port = 3030;

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
      .service(web::scope("/socket.io").service(ws::start::start_connection))
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
  let com = Commutator::new(app.clone(), events_receiver).start();
  println!("com started up");

  match opt.mode.as_str() {
    // "reindex" => reindex(settings, app, com).await,
    "reindex" => reindex(app).await,
    "server" => server(settings, app, com, opt.port).await,
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
    "create" => match opt.case.as_str() {
      "production" => use_cases::uc_create::create_production(&app),
      _ => unreachable!(),
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
      "cups" => {
        use_cases::uc_save::save_half_stuff_products(&app, use_cases::uc_save::Product::CUPS)
      },
      "caps" => {
        use_cases::uc_save::save_half_stuff_products(&app, use_cases::uc_save::Product::CAPS)
      },
      "products" => use_cases::uc_save::save_cups_and_caps(&app),
      "produced" => use_cases::uc_save::save_produced(&app),
      "file_transfer" => use_cases::uc_save::save_transfer_from_file(&app),
      "goods_transfer" => use_cases::uc_save::save_transfer_for_goods(&app),
      _ => unreachable!(),
    },
    "replace" => match opt.case.as_str() {
      "goods" => {
        use_cases::uc_replace::replace_goods(&app, "Полипропилен дробленный", "Полипропилен (дроб)")
      },
      "order" => use_cases::uc_replace::replace_storage_at_material_produced_and_used(&app),
      _ => unreachable!(),
    },
    "fix" => fix_topologies(app).await,
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
  use crate::animo::shared;
  use crate::warehouse::test_util::init;
  use actix_web::web::Bytes;
  use actix_web::{test, web, App};
  use json::object;
  use rust_decimal::Decimal;
  use service::Context;
  use store::qty::Number;
  use store::GetWarehouse;
  use values::c;
  const WID: &str = "yjmgJUmDo_kn9uxVi8s9Mj9mgGRJISxRt63wT46NyTQ";

  #[actix_web::test]
  async fn test_put_get() {
    let (tmp_dir, _, db) = init();

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
      zone: *shared::DESC,
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

  #[actix_web::test]
  async fn check_material_used_and_reindex() {
    std::env::set_var("RUST_LOG", "debug,actix_web=debug,actix_server=debug");
    env_logger::init();

    fn create(ctx: &Vec<&str>, app: &Application, data: JsonValue) -> JsonValue {
      let data = app
        .service("memories")
        .create(
          Context::local(),
          data,
          json::object! {
            oid: WID,
            ctx: ctx.clone(),
          },
        )
        .unwrap();

      data
    }

    let (tmp_dir, settings, db) = init();

    let wss = Workspaces::new(tmp_dir.path().join("companies"));

    let (mut app, _) = Application::new(Arc::new(settings), Arc::new(db), wss).await.unwrap();

    app.register(MemoriesInFiles::new(app.clone(), "memories"));
    // app.register(nae_backend::inventory::service::Inventory::new(app.clone()));

    let produce_op = vec!["production", "produce"];
    let ctx_used = vec!["production", "material", "used"];
    let produce_doc = vec!["production", "order"];

    let rolls = app
      .service("memories")
      .create(
        Context::local(),
        json::object! {
          name: "рулоны",
            code:"0",
        },
        json::object! {
          oid: WID,
          ctx: vec!["warehouse", "storage"],
        },
      )
      .unwrap();

    let extrusion = app
      .service("memories")
      .create(
        Context::local(),
        json::object! {
          name: "экструдер",
            type: "roll",
            storage: rolls[c::ID].to_string(),
        },
        json::object! {
          oid: WID,
          ctx: vec!["production", "area"],
        },
      )
      .unwrap();

    // let a1 = store(&app, "a1");

    let g1 = app
      .service("memories")
      .create(
        Context::local(),
        json::object! {
          name: "Пленка Midas",
        },
        json::object! {
          oid: WID,
          ctx: vec!["goods"],
        },
      )
      .unwrap();

    // let p1 = goods(&app, "p1");
    let p1 = app
      .service("memories")
      .create(
        Context::local(),
        json::object! {
          name: "p1",
          goods: g1[c::ID].string(),
        },
        json::object! {
          oid: WID,
          ctx: vec!["product"],
        },
      )
      .unwrap();

    let uom0 = app
      .service("memories")
      .create(
        Context::local(),
        json::object! {
          name: "Рул",
        },
        json::object! {
          oid: WID,
          ctx: vec!["uom"],
        },
      )
      .unwrap();

    let uom1 = app
      .service("memories")
      .create(
        Context::local(),
        json::object! {
          name: "кг",
        },
        json::object! {
          oid: WID,
          ctx: vec!["uom"],
        },
      )
      .unwrap();

    // create first produce document
    let produce_doc_0 = object! {
      date: "2023-01-01",
      area: extrusion[c::ID].to_string(),
      product: p1[c::ID].to_string(),
    };

    let d0 = create(&produce_doc, &app, produce_doc_0.clone());

    // create first produce operation
    let qty0: JsonValue = (&Qty::new(vec![Number::new(
      Decimal::from(1),
      uom0[c::UUID].uuid().unwrap(),
      Some(Box::new(Number::new(
        Decimal::try_from("333.3").unwrap(),
        uom1[c::UUID].uuid().unwrap(),
        None,
      ))),
    )]))
      .into();

    let produce_op_0 = object! {
      date: "2023-01-01",
      document: d0[c::ID].to_string(),
      qty: qty0.clone(),
    };

    let _ = create(&produce_op, &app, produce_op_0);
    // log::debug!("produce_data: {:#?}", r1.dump());

    // let r1_batch = Batch { id: r1["_uuid"].uuid().unwrap(), date: dt("2023-01-01").unwrap() };

    // create second produce operation
    let qty1: JsonValue = (&Qty::new(vec![Number::new(
      Decimal::from(1),
      uom0[c::UUID].uuid().unwrap(),
      Some(Box::new(Number::new(
        Decimal::try_from("444.4").unwrap(),
        uom1[c::UUID].uuid().unwrap(),
        None,
      ))),
    )]))
      .into();

    let produce_op_1 = object! {
      date: "2023-01-01",
      document: d0["_id"].to_string(),
      qty: qty1.clone(),
    };

    let _ = create(&produce_op, &app, produce_op_1);

    app.warehouse().database.ordered_topologies[0].debug().unwrap();

    // create second produce document
    let produce_doc_1 = object! {
      date: "2023-01-02",
      area: extrusion[c::ID].to_string(),
      product: p1[c::ID].to_string(),
    };
    let d1 = create(&produce_doc, &app, produce_doc_1.clone());

    // create used operation
    let used_op = object! {
      document: d1[c::ID].to_string(),
      storage: rolls[c::ID].to_string(),
      goods: g1[c::ID].string(),
      qty: qty0,
    };
    let u1 = create(&ctx_used, &app, used_op);
    log::debug!("used_data: {:#?}", u1.dump());

    app.warehouse().database.ordered_topologies[0].debug().unwrap();

    let _ = reindex(app).await;

    // app.warehouse().database.ordered_topologies[0].debug().unwrap();
    tmp_dir.close().unwrap();
  }
}
