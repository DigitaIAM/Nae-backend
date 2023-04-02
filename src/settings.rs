use config::{Config, ConfigError, File};
use serde::Deserialize;
use std::env;
use std::path::PathBuf;

#[derive(Debug, Deserialize)]
pub(crate) struct Database {
  pub(crate) memory: PathBuf,
  pub(crate) inventory: PathBuf,
}

#[derive(Debug, Deserialize)]
pub struct JWTConfig {
  pub(crate) audience: String,
  pub(crate) issuer: String,
  pub(crate) secret: String,
}

#[derive(Debug, Deserialize)]
pub struct Settings {
  pub(crate) debug: bool,
  pub(crate) jwt_config: JWTConfig,
  pub(crate) database: Database,
}

impl Settings {
  pub fn test(folder: PathBuf) -> Settings {
    Settings {
      debug: false,
      jwt_config: JWTConfig {
        audience: "http://localhost".into(),
        issuer: "Nae".into(),
        secret: "1234567890".into(),
      },
      database: Database { memory: folder.join("memory"), inventory: folder.join("inventory") },
    }
  }

  pub(crate) fn new() -> Result<Self, ConfigError> {
    let run_mode = env::var("RUN_MODE").unwrap_or_else(|_| "development".into());

    let config = Config::builder()
      // the "default" configuration file
      .add_source(File::with_name("config/default"))
      // the current environment configuration file
      .add_source(File::with_name(&format!("config/{}", run_mode)).required(false))
      // local configuration file
      .add_source(File::with_name("config/local").required(false))
      .build()?;

    println!("debug: {:?}", config.get_bool("debug"));
    println!("memory: {:?}", config.get::<String>("database.memory"));
    println!("inventory: {:?}", config.get::<String>("database.inventory"));

    config.try_deserialize()
  }
}
