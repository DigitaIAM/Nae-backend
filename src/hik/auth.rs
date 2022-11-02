use crate::hik::error::{Error, Result};
use async_trait::async_trait;
use digest_auth::{AuthContext, HttpMethod};
use reqwest::{RequestBuilder, Response, StatusCode};

#[async_trait]
pub trait WithDigestAuth {
  async fn send_with_digest_auth(&self, username: &str, password: &str) -> Result<Response>;
}

#[async_trait]
impl WithDigestAuth for RequestBuilder {
  async fn send_with_digest_auth(&self, username: &str, password: &str) -> Result<Response> {
    fn clone_request_builder(request_builder: &RequestBuilder) -> Result<RequestBuilder> {
      request_builder.try_clone().ok_or(Error::RequestBuilderNotCloneable)
    }

    // println!("1st send");
    let response = clone_request_builder(self)?.send().await.map_err(Error::ConnectionError)?;

    // println!("status {}", response.status());
    let response = match response.status() {
      StatusCode::UNAUTHORIZED => {
        let request = clone_request_builder(self)?.build().map_err(Error::ConnectionError)?;

        let path = request.url().path();
        let method = HttpMethod::from(request.method().as_str());
        let body = request.body().and_then(|b| b.as_bytes());

        let www_auth = response
          .headers()
          .get("www-authenticate")
          .ok_or(Error::AuthHeaderMissing)?
          .to_str()
          .map_err(Error::ToStr)?;

        let context = AuthContext::new_with_method(username, password, path, body, method);
        let mut promt = digest_auth::parse(www_auth).map_err(Error::DigestAuth)?;

        let respond = promt.respond(&context).map_err(|e| {
          Error::AuthenticationFailed(format!("Unable to formulate digest response: {}", e))
        })?;

        // println!("2nd send");
        clone_request_builder(self)?
          .header("Authorization", respond.to_header_string())
          .send()
          .await
          .map_err(Error::ConnectionError)?
      },
      _ => response,
    };

    // println!("response {response:?}");

    Ok(response)
  }
}

#[async_trait]
pub trait RequestGenerator {
  async fn request(&self) -> Result<RequestBuilder>;
}

pub(crate) async fn digest_auth(
  builder: impl RequestGenerator,
  username: &str,
  password: &str,
) -> Result<Response> {
  // println!("1st send");
  let response = builder.request().await?.send().await.map_err(Error::ConnectionError)?;

  // println!("status {}", response.status());
  let response = match response.status() {
    StatusCode::UNAUTHORIZED => {
      let request = builder.request().await?.build().map_err(Error::ConnectionError)?;

      let path = request.url().path();
      let method = HttpMethod::from(request.method().as_str());
      let body = request.body().and_then(|b| b.as_bytes());

      let www_auth = response
        .headers()
        .get("www-authenticate")
        .ok_or(Error::AuthHeaderMissing)?
        .to_str()
        .map_err(Error::ToStr)?;

      let context = AuthContext::new_with_method(username, password, path, body, method);
      let mut promt = digest_auth::parse(www_auth).map_err(Error::DigestAuth)?;

      let respond = promt.respond(&context).map_err(|e| {
        Error::AuthenticationFailed(format!("Unable to formulate digest response: {}", e))
      })?;

      // println!("2nd send");
      builder
        .request()
        .await?
        .header("Authorization", respond.to_header_string())
        .send()
        .await
        .map_err(Error::ConnectionError)?
    },
    _ => response,
  };

  // println!("response {response:?}");
  // println!("status {}", response.status());

  Ok(response)
}
