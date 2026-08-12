use std::fs::File;
use std::path::Path;

use serde::Deserialize;

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    pub server: ServerConfig,
    pub redis: RedisConfig,
    pub broker: BrokerConfig,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ServerConfig {
    pub bind_address: String,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RedisConfig {
    pub url: String,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BrokerConfig {
    pub key_prefix: String,
    pub task_destination: String,
}

impl Config {
    pub fn load(path: impl AsRef<Path>) -> Result<Self, Box<dyn std::error::Error>> {
        let path = path.as_ref();
        let file = File::open(path).map_err(|error| {
            format!("cannot open configuration file {}: {error}", path.display())
        })?;
        serde_yaml::from_reader(file).map_err(|error| {
            format!(
                "cannot parse configuration file {}: {error}",
                path.display()
            )
            .into()
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_yaml_configuration() {
        let config: Config = serde_yaml::from_str(
            r#"
server:
  bind_address: 127.0.0.1:0
redis:
  url: redis://localhost:6379/
broker:
  key_prefix: test:broker
  task_destination: test-tasks
"#,
        )
        .unwrap();

        assert_eq!(config.server.bind_address, "127.0.0.1:0");
        assert_eq!(config.redis.url, "redis://localhost:6379/");
        assert_eq!(config.broker.key_prefix, "test:broker");
        assert_eq!(config.broker.task_destination, "test-tasks");
    }
}
