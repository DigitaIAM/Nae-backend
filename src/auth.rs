use std::time::{SystemTime, UNIX_EPOCH};
use actix_web::{post, web, Responder, HttpResponse, Error, FromRequest, HttpRequest};
use actix_web::dev::{Payload, ServiceRequest};
use actix_web_httpauth::extractors::bearer::BearerAuth;
use jsonwebtoken::{Algorithm, DecodingKey, EncodingKey, Header, Validation};
use serde::{Deserialize, Serialize};
use pbkdf2::{
    password_hash::{
        rand_core::OsRng,
        PasswordHash, PasswordHasher, PasswordVerifier, SaltString
    },
    Pbkdf2
};
use crate::animo::error::DBError;
use crate::{AnimoDB, Memory, Settings};
use crate::animo::memory::{ChangeTransformation, Context, ID, TransformationKey, Value};

const ALGORITHM: Algorithm = Algorithm::HS256;

#[derive(Debug, Serialize, Deserialize)]
struct Claims {
    aud: String,         // Audience [optional]
    exp: u128,            // Required (validate_exp defaults to true in validation). Expiration time (as UTC timestamp)
    iat: u128,            // Issued at (as UTC timestamp) [optional]
    iss: String,         // Issuer [optional]
    nbf: u128,            // Not Before (as UTC timestamp) [optional]
    sub: String,         // Subject (whom token refers to) [optional]
}

fn now_in_seconds() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system time is likely incorrect")
        .as_millis()
}

pub(crate) fn decode_token(settings: &Settings, db: &AnimoDB, token: &str) -> Result<String, DBError> {
    let key = DecodingKey::from_secret(settings.jwt_config.secret.as_bytes());
    match jsonwebtoken::decode::<Claims>(token, &key, &Validation::new(ALGORITHM)) {
        Ok(token) => {
            log::debug!("token {:?}", token);

            // check expiration
            let now = now_in_seconds();

            log::debug!("decode_token {} vs {} = {}", now, token.claims.exp, token.claims.exp > now);

            // TODO check nbf and iat

            if token.claims.exp > now
                && &token.claims.aud == &settings.jwt_config.audience
                && &token.claims.iss == &settings.jwt_config.issuer
                && !token.claims.sub.is_empty()
            {

                let account_id = ID::from(token.claims.sub.as_str());

                let last_logout: Option<u128> = match
                    db.value(TransformationKey::simple(account_id, "last_logout"))?
                {
                    Value::Number(number) => {
                        log::debug!("last_logout {} vs {}", number, token.claims.iat);
                        number.try_into().ok()
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
                    }
                    None => Ok(token.claims.sub)
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

#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct Account {
    id: ID,
    // first_name: String,
    // last_name: String,
    email: String,
}

impl Account {
    fn jwt(settings: &Settings, db: &AnimoDB, credentials: BearerAuth) -> Result<Self, DBError> {
        let email = decode_token(settings, db, credentials.token())?;
        let id = ID::from(email.as_str());
        Ok(Account { id, email })
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct SignUpRequest {
    email: String,
    password: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct LoginRequest {
    email: String,
    password: String,
    remember_me: bool,
}

#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct LoginResponse {
    token: String,
}

#[post("/logout")]
pub(crate) async fn logout(auth: BearerAuth, settings: web::Data<Settings>, db: web::Data<AnimoDB>) -> Result<HttpResponse, Error> {
    let account = Account::jwt(&settings, &db, auth)
        .map_err(actix_web::error::ErrorUnauthorized)?;

    let now = now_in_seconds();

    log::debug!("logout {}", now);

    let mutation = vec![
        ChangeTransformation::create(account.id, "last_logout", Value::Number(now.into())),
    ];
    db.modify(mutation)
        .map_err(actix_web::error::ErrorInternalServerError)?;

    Ok(HttpResponse::Ok().json("logged out"))
}

#[post("/signup")]
pub(crate) async fn signup_post(settings: web::Data<Settings>, db: web::Data<AnimoDB>, data: web::Json<SignUpRequest>) -> Result<HttpResponse, Error> {
    let password = data.password.as_bytes();

    let salt = SaltString::generate(&mut OsRng);
    let password_hash = match Pbkdf2.hash_password(password, &salt) {
        Ok(hash) => hash.to_string(),
        Err(e) => { return Err(actix_web::error::ErrorInternalServerError(e.to_string())); }
    };

    let account_id = ID::from(data.email.as_str());

    let mutation = vec![
        ChangeTransformation::create(account_id, "instance_of", ID::from("user_account").into()),
        ChangeTransformation::create(account_id, "email", data.email.clone().into()),
        ChangeTransformation::create(account_id, "password_hash", password_hash.into()),
    ];
    db.modify(mutation)
        .map_err(actix_web::error::ErrorInternalServerError)?;

    let login_data = LoginRequest {
        email: data.email.clone(),
        password: data.password.clone(),
        remember_me: false,
    };

    login_procedure(settings, db, login_data)
}

#[post("/login")]
pub(crate) async fn login_post(settings: web::Data<Settings>, db: web::Data<AnimoDB>, data: web::Json<LoginRequest>) -> Result<HttpResponse, Error> {
    login_procedure(settings, db, data.0)
}

fn login_procedure(settings: web::Data<Settings>, db: web::Data<AnimoDB>, data: LoginRequest) -> Result<HttpResponse, Error> {
    // find user's password hash
    let account_id = ID::from(data.email.as_str());
    let hash = match db.value(TransformationKey::simple(account_id, "password_hash"))
        .map_err(actix_web::error::ErrorUnauthorized)?
    {
        Value::String(hash) => hash,
        _ => return Err(actix_web::error::ErrorUnauthorized("")),
    };

    let password_hash = PasswordHash::new(hash.as_str())
        .map_err(actix_web::error::ErrorUnauthorized)?;

    if Pbkdf2.verify_password(data.password.as_bytes(), &password_hash).is_ok() {
        // JWT
        let now = now_in_seconds();

        let exp = if data.remember_me {
            now + chrono::Duration::days(365).num_milliseconds() as u128
        } else {
            now + chrono::Duration::hours(1).num_milliseconds() as u128
        };

        let claims = Claims {
            aud: settings.jwt_config.audience.clone(),
            iss: settings.jwt_config.issuer.clone(),
            sub: data.email.clone(),
            iat: now,
            nbf: now,
            exp: exp,
        };

        let key = EncodingKey::from_secret(settings.jwt_config.secret.as_bytes());
        let token = jsonwebtoken::encode(&Header::new(ALGORITHM), &claims, &key)
            .map_err(|e| actix_web::error::ErrorInternalServerError(e.to_string()) )?;

        Ok(HttpResponse::Ok().json(LoginResponse { token }))
    } else {
        Err(actix_web::error::ErrorUnauthorized(""))
    }
}

#[post("/ping")]
pub(crate) async fn ping_post(auth: BearerAuth, settings: web::Data<Settings>, db: web::Data<AnimoDB>) -> Result<HttpResponse, Error> {
    let account = Account::jwt(&settings, &db, auth)
        .map_err(actix_web::error::ErrorUnauthorized)?;

    Ok(HttpResponse::Ok().json("pong"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use actix_web::{App, test, web};
    use actix_web::http::{header, StatusCode};
    use actix_web::web::Bytes;
    use actix_web_httpauth::extractors::bearer::Config;
    use actix_web_httpauth::middleware::HttpAuthentication;
    use crate::animo::memory::{ChangeTransformation, Transformation, TransformationKey, Value};
    use crate::api;
    use crate::warehouse::test_util::init;

    #[actix_web::test]
    async fn test_register_and_login() {
        let (tmp_dir, settings, db) = init();

        let app = test::init_service(
            App::new()
                .app_data(web::Data::new(settings))
                .app_data(web::Data::new(db.clone()))
                .wrap(actix_web::middleware::Logger::default())
                // .wrap(HttpAuthentication::bearer(crate::auth::validator))
                .app_data(
                    Config::default()
                        .realm("Restricted area")
                        .scope("email photo"),
                )
                .service(signup_post)
                .service(login_post)
                .service(logout)
                .service(ping_post)
                .default_service(web::route().to(api::not_implemented))
        ).await;

        // ping without token
        let req = test::TestRequest::post()
            .uri("/ping")
            .to_request();
        let response = test::call_service(&app, req).await;
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);

        let signup_data = crate::auth::SignUpRequest {
            email: "tester@nae.org".to_string(),
            password: "Nae".to_string()
        };

        let req = test::TestRequest::post()
            .uri("/signup")
            .set_json(signup_data)
            .to_request();

        let response: LoginResponse = test::call_and_read_body_json(&app, req).await;
        let token1 = response.token;
        assert_eq!(token1.len() > 0, true);

        let req = test::TestRequest::post()
            .uri("/ping")
            .insert_header((header::AUTHORIZATION, format!("Bearer {}", token1)))
            .to_request();
        let response = test::call_and_read_body(&app, req).await;
        assert_eq!(response, Bytes::from_static(b"\"pong\""));

        let req = test::TestRequest::post()
            .uri("/logout")
            .insert_header((header::AUTHORIZATION, format!("Bearer {}", token1)))
            .to_request();
        let response = test::call_and_read_body(&app, req).await;
        assert_eq!(response, Bytes::from_static(b"\"logged out\""));

        let login_data = crate::auth::LoginRequest {
            email: "tester@nae.org".to_string(),
            password: "Nae".to_string(),
            remember_me: false,
        };

        let req = test::TestRequest::post()
            .uri("/login")
            .set_json(login_data)
            .to_request();

        let response: LoginResponse = test::call_and_read_body_json(&app, req).await;
        let token2 = response.token;
        assert_eq!(token2.len() > 0, true);

        // ping with new token
        let req = test::TestRequest::post()
            .uri("/ping")
            .insert_header((header::AUTHORIZATION, format!("Bearer {}", token2)))
            .to_request();
        let response = test::call_and_read_body(&app, req).await;
        assert_eq!(response, Bytes::from_static(b"\"pong\""));

        // ping with old token
        let req = test::TestRequest::post()
            .uri("/ping")
            .insert_header((header::AUTHORIZATION, format!("Bearer {}", token1)))
            .to_request();
        let response = test::call_service(&app, req).await;
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);

        // stop db and delete data folder
        db.close();
        tmp_dir.close().unwrap();
    }
}