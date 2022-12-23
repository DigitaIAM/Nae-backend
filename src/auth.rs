use crate::animo::error::DBError;
use crate::animo::memory::{ChangeTransformation, Context, TransformationKey, Value, ID};
use crate::utils::time::{now_in_millis, now_in_seconds};
use crate::{AnimoDB, Application, Memory, Settings, DESC};
use actix_web::dev::{Payload, ServiceRequest};
use actix_web::{post, web, Error, FromRequest, HttpRequest, HttpResponse, Responder};
use actix_web_httpauth::extractors::bearer::BearerAuth;
use chrono::Duration;
use jsonwebtoken::{Algorithm, DecodingKey, EncodingKey, Header, Validation};
use pbkdf2::{
  password_hash::{rand_core::OsRng, PasswordHash, PasswordHasher, PasswordVerifier, SaltString},
  Pbkdf2,
};
use std::time::{SystemTime, UNIX_EPOCH};
// use validator::{Validate, ValidationError};

const ALGORITHM: Algorithm = Algorithm::HS256;

#[derive(Debug, serde::Serialize, serde::Deserialize)]
struct Claims {
  aud: String, // Audience [optional]
  exp: u128, // Required (validate_exp defaults to true in validation). Expiration time (as UTC timestamp)
  iat: u128, // Issued at (as UTC timestamp) [optional]
  iss: String, // Issuer [optional]
  nbf: u128, // Not Before (as UTC timestamp) [optional]
  sub: String, // Subject (whom token refers to) [optional]
}

pub(crate) fn decode_token(app: &Application, token: &str) -> Result<String, DBError> {
  let key = DecodingKey::from_secret(app.settings.jwt_config.secret.as_bytes());
  match jsonwebtoken::decode::<Claims>(token, &key, &Validation::new(ALGORITHM)) {
    Ok(token) => {
      log::debug!("token {:?}", token);

      // check expiration
      let now = now_in_millis();

      log::debug!("decode_token {} vs {} = {}", now, token.claims.exp, token.claims.exp > now);

      // TODO check nbf and iat

      if token.claims.exp > now
        && &token.claims.aud == &app.settings.jwt_config.audience
        && &token.claims.iss == &app.settings.jwt_config.issuer
        && !token.claims.sub.is_empty()
      {
        let account_id = ID::from(token.claims.sub.as_str());

        let last_logout: Option<u128> =
          match app.db.value(TransformationKey::simple(account_id, "last_logout"))? {
            Value::U128(number) => {
              log::debug!("last_logout {} vs {}", number, token.claims.iat);
              Some(number)
            },
            _ => None,
          };

        match last_logout {
          Some(ts) => {
            log::debug!("last_logout {} vs {} = {}", ts, token.claims.iat, token.claims.iat > ts);
            if token.claims.iat > ts {
              Ok(token.claims.sub)
            } else {
              Err(DBError::from("Unauthorised".to_string()))
            }
          },
          None => Ok(token.claims.sub),
        }
      } else {
        Err(DBError::from("Unauthorised".to_string()))
      }
    },
    Err(e) => Err(DBError::from(e.to_string())),
  }
}

// pub(crate) async fn validator(req: ServiceRequest, credentials: BearerAuth) -> Result<ServiceRequest, actix_web::Error> {
//     eprintln!("{:?}", credentials);
//
//     let config = req
//         .app_data::<Settings>()
//         .map(|data| data.clone())
//         .unwrap();
//
//     match validate_token(config, credentials.token()) {
//         Ok(res) => {
//             if res == true {
//                 Ok(req)
//             } else {
//                 Err(actix_web::error::ErrorUnauthorized(""))
//             }
//         }
//         Err(_) => Err(actix_web::error::ErrorUnauthorized("")),
//     }
// }

#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub(crate) struct Account {
  pub(crate) id: ID,
  // first_name: String,
  // last_name: String,
  email: String,
}

impl Account {
  pub(crate) fn jwt(app: &Application, token: &str) -> Result<Self, DBError> {
    let email = decode_token(app, token)?;
    let id = ID::from(email.as_str());
    Ok(Account { id, email })
  }
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub(crate) struct SignUpRequest {
  // #[validate(email)]
  pub(crate) email: String,
  // #[validate(length(min = 6))]
  pub(crate) password: String,
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub(crate) struct LoginRequest {
  // #[validate(email)]
  pub(crate) email: String,
  // #[validate(length(min = 6))]
  pub(crate) password: String,
  pub(crate) remember_me: bool,
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub(crate) struct LoginResponse {
  token: String,
}

#[post("/logout")]
pub(crate) async fn logout(
  auth: BearerAuth,
  app: web::Data<Application>,
) -> Result<HttpResponse, Error> {
  let account =
    Account::jwt(app.get_ref(), auth.token()).map_err(actix_web::error::ErrorUnauthorized)?;

  let now = now_in_millis();

  log::debug!("logout {}", now);

  let mutation =
    vec![ChangeTransformation::create(*DESC, account.id, "last_logout", Value::U128(now))];
  app.db.modify(mutation).map_err(actix_web::error::ErrorInternalServerError)?;

  Ok(HttpResponse::Ok().json("logged out"))
}

#[post("/signup")]
pub(crate) async fn signup_post(
  app: web::Data<Application>,
  data: web::Json<SignUpRequest>,
) -> Result<HttpResponse, Error> {
  match signup_procedure(app.get_ref(), data.into_inner()) {
    Ok((_, token)) => Ok(HttpResponse::Ok().json(token)),
    Err(mgs) => Err(actix_web::error::ErrorUnauthorized(mgs)),
  }
}

pub(crate) fn signup_procedure(
  app: &Application,
  data: SignUpRequest,
) -> Result<(ID, String), String> {
  // data.validate()
  //   .map_err(|e| e.to_string())?;

  let password = data.password.as_bytes();

  let salt = SaltString::generate(&mut OsRng);
  let password_hash = match Pbkdf2.hash_password(password, &salt) {
    Ok(hash) => hash.to_string(),
    Err(e) => {
      return Err(e.to_string());
    },
  };

  let account_id = ID::from(data.email.as_str());

  let mutation = vec![
    ChangeTransformation::create(*DESC, account_id, "instance_of", ID::from("user_account").into()),
    ChangeTransformation::create(*DESC, account_id, "email", data.email.clone().into()),
    ChangeTransformation::create(*DESC, account_id, "password_hash", password_hash.into()),
  ];
  app.db.modify(mutation).map_err(|e| e.to_string())?;

  let login_data =
    LoginRequest { email: data.email.clone(), password: data.password.clone(), remember_me: false };

  login_procedure(app, login_data)
}

#[post("/login")]
pub(crate) async fn login_post(
  app: web::Data<Application>,
  data: web::Json<LoginRequest>,
) -> Result<HttpResponse, Error> {
  match login_procedure(app.get_ref(), data.0) {
    Ok((_, token)) => Ok(HttpResponse::Ok().json(LoginResponse { token })),
    Err(mgs) => Err(actix_web::error::ErrorUnauthorized(mgs)),
  }
}

pub(crate) fn login_procedure(
  app: &Application,
  data: LoginRequest,
) -> Result<(ID, String), String> {
  // data.validate()
  //   .map_err(|e| e.to_string())?;

  // find user's password hash
  let account_id = ID::from(data.email.as_str());
  let hash = match app
    .db
    .value(TransformationKey::simple(account_id, "password_hash"))
    .map_err(|e| e.to_string())?
  {
    Value::String(hash) => hash,
    _ => return Err("not found".to_string()),
  };

  let password_hash = PasswordHash::new(hash.as_str()).map_err(|e| e.to_string())?;

  if Pbkdf2.verify_password(data.password.as_bytes(), &password_hash).is_ok() {
    // JWT
    let now = now_in_millis();

    let exp = if data.remember_me {
      now + Duration::days(365).num_milliseconds() as u128
    } else {
      now + Duration::hours(1).num_milliseconds() as u128
    };

    let claims = Claims {
      aud: app.settings.jwt_config.audience.clone(),
      iss: app.settings.jwt_config.issuer.clone(),
      sub: data.email.clone(),
      iat: now,
      nbf: now,
      exp,
    };

    let key = EncodingKey::from_secret(app.settings.jwt_config.secret.as_bytes());
    let token =
      jsonwebtoken::encode(&Header::new(ALGORITHM), &claims, &key).map_err(|e| e.to_string())?;

    Ok((account_id, token))
  } else {
    Err("invalid password".to_string())
  }
}

#[post("/ping")]
pub(crate) async fn ping_post(
  auth: BearerAuth,
  app: web::Data<Application>,
) -> Result<HttpResponse, Error> {
  let account =
    Account::jwt(app.get_ref(), auth.token()).map_err(actix_web::error::ErrorUnauthorized)?;

  Ok(HttpResponse::Ok().json("pong"))
}

#[cfg(test)]
mod tests {
  use super::*;
  use crate::animo::memory::{ChangeTransformation, Transformation, TransformationKey, Value};
  use crate::api;
  use crate::warehouse::test_util::init;
  use actix_web::http::{header, StatusCode};
  use actix_web::web::Bytes;
  use actix_web::{test, web, App};
  use actix_web_httpauth::extractors::bearer::Config;
  use actix_web_httpauth::middleware::HttpAuthentication;
  use std::sync::Arc;

  #[actix_web::test]
  async fn test_register_and_login() {
    let (tmp_dir, settings, db) = init();

    let app = Application::new(Arc::new(settings), Arc::new(db));

    let server = test::init_service(
      App::new()
        .app_data(web::Data::new(app))
        .wrap(actix_web::middleware::Logger::default())
        // .wrap(HttpAuthentication::bearer(crate::auth::validator))
        .app_data(Config::default().realm("Restricted area").scope("email photo"))
        .service(signup_post)
        .service(login_post)
        .service(logout)
        .service(ping_post)
        .default_service(web::route().to(api::not_implemented)),
    )
    .await;

    // ping without token
    let req = test::TestRequest::post().uri("/ping").to_request();
    let response = test::call_service(&server, req).await;
    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);

    let signup_data = crate::auth::SignUpRequest {
      email: "tester@nae.org".to_string(),
      password: "Nae_password".to_string(),
    };

    let req = test::TestRequest::post().uri("/signup").set_json(signup_data).to_request();

    let response: LoginResponse = test::call_and_read_body_json(&server, req).await;
    let token1 = response.token;
    assert_eq!(token1.len() > 0, true);

    let req = test::TestRequest::post()
      .uri("/ping")
      .insert_header((header::AUTHORIZATION, format!("Bearer {}", token1)))
      .to_request();
    let response = test::call_and_read_body(&server, req).await;
    assert_eq!(response, Bytes::from_static(b"\"pong\""));

    let req = test::TestRequest::post()
      .uri("/logout")
      .insert_header((header::AUTHORIZATION, format!("Bearer {}", token1)))
      .to_request();
    let response = test::call_and_read_body(&server, req).await;
    assert_eq!(response, Bytes::from_static(b"\"logged out\""));

    let login_data = crate::auth::LoginRequest {
      email: "tester@nae.org".to_string(),
      password: "Nae".to_string(),
      remember_me: false,
    };

    let req = test::TestRequest::post().uri("/login").set_json(login_data).to_request();

    let response: LoginResponse = test::call_and_read_body_json(&server, req).await;
    let token2 = response.token;
    assert_eq!(token2.len() > 0, true);

    // ping with new token
    let req = test::TestRequest::post()
      .uri("/ping")
      .insert_header((header::AUTHORIZATION, format!("Bearer {}", token2)))
      .to_request();
    let response = test::call_and_read_body(&server, req).await;
    assert_eq!(response, Bytes::from_static(b"\"pong\""));

    // ping with old token
    let req = test::TestRequest::post()
      .uri("/ping")
      .insert_header((header::AUTHORIZATION, format!("Bearer {}", token1)))
      .to_request();
    let response = test::call_service(&server, req).await;
    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);

    // stop db and delete data folder
    // TODO app.close();
    tmp_dir.close().unwrap();
  }
}
