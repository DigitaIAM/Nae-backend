use config::{Config, ConfigError, File};
use serde::Deserialize;
use std::env;
use std::path::PathBuf;

#[derive(Debug, Deserialize)]
pub struct Database {
  pub memory: PathBuf,
  pub inventory: PathBuf,
  pub links: PathBuf,
  pub ftsearch: PathBuf,
}

#[derive(Debug, Deserialize)]
pub struct JWTConfig {
  pub audience: String,
  pub issuer: String,
  pub secret: String,
}

#[derive(Debug, Deserialize)]
pub struct Settings {
  pub debug: bool,
  pub jwt_config: JWTConfig,
  pub database: Database,
}

impl Settings {
  pub fn new() -> Result<Self, ConfigError> {
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
    println!("ftsearch: {:?}", config.get::<String>("database.ftsearch"));

    config.try_deserialize()
  }

  #[cfg(test)]
  pub fn test(folder: PathBuf) -> Settings {
    Settings {
      debug: false,
      jwt_config: JWTConfig {
        audience: "http://localhost".into(),
        issuer: "Nae".into(),
        secret: "1234567890".into(),
      },
      database: Database {
        memory: folder.join("memory"),
        inventory: folder.join("inventory"),
        links: folder.join("links"),
        ftsearch: folder.join("tantivy"),
      },
    }
  }
}
