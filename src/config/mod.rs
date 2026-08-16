use std::fs::File;
use std::path::Path;

use serde::Deserialize;

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    pub server: Option<ServerConfig>,
    pub redis: RedisConfig,
    pub broker: BrokerConfig,
    pub worker: Option<WorkerConfig>,
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
    #[serde(default = "default_worker_membership_ttl_seconds")]
    pub worker_membership_ttl_seconds: u64,
}

fn default_worker_membership_ttl_seconds() -> u64 {
    30
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BrokerConfig {
    pub key_prefix: String,
    pub task_destination: String,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WorkerConfig {
    pub name: Option<String>,
    pub core_endpoint: String,
    #[serde(default)]
    pub resources: WorkerResourcesConfig,
    #[serde(default)]
    pub capabilities: Vec<String>,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WorkerResourcesConfig {
    #[serde(default, deserialize_with = "deserialize_cpu")]
    pub cpu: u64,
    #[serde(default, deserialize_with = "deserialize_memory")]
    pub memory: u64,
}

fn deserialize_cpu<'de, D>(deserializer: D) -> Result<u64, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let cpu = u64::deserialize(deserializer)?;
    if cpu > u64::MAX / 1000 {
        return Err(serde::de::Error::custom(
            "worker CPU exceeds the protocol millicore range",
        ));
    }
    Ok(cpu)
}

fn deserialize_memory<'de, D>(deserializer: D) -> Result<u64, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let value = String::deserialize(deserializer)?;
    parse_memory(&value).map_err(serde::de::Error::custom)
}

fn parse_memory(value: &str) -> Result<u64, String> {
    const UNITS: [(&str, u64); 7] = [
        ("TiB", 1 << 40),
        ("GiB", 1 << 30),
        ("MiB", 1 << 20),
        ("KiB", 1 << 10),
        ("TB", 1_000_000_000_000),
        ("GB", 1_000_000_000),
        ("MB", 1_000_000),
    ];
    let value = value.trim();
    for (suffix, multiplier) in UNITS {
        if let Some(number) = value.strip_suffix(suffix) {
            let number = number
                .trim()
                .parse::<u64>()
                .map_err(|_| format!("worker memory must be an integer followed by {suffix}"))?;
            return number
                .checked_mul(multiplier)
                .ok_or_else(|| "worker memory exceeds the supported range".to_owned());
        }
    }
    Err("worker memory must use one of KiB, MiB, GiB, TiB, MB, GB, or TB".to_owned())
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

    pub fn server_config(&self) -> Result<&ServerConfig, &'static str> {
        self.server
            .as_ref()
            .ok_or("server configuration is required for the server role")
    }

    pub fn worker_config(&self) -> Result<&WorkerConfig, &'static str> {
        self.worker
            .as_ref()
            .ok_or("worker configuration is required for the worker role")
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

        assert_eq!(config.server_config().unwrap().bind_address, "127.0.0.1:0");
        assert_eq!(config.redis.url, "redis://localhost:6379/");
        assert_eq!(config.redis.worker_membership_ttl_seconds, 30);
        assert_eq!(config.broker.key_prefix, "test:broker");
        assert_eq!(config.broker.task_destination, "test-tasks");
        assert!(config.worker.is_none());
    }

    #[test]
    fn parses_worker_membership_ttl() {
        let config: Config = serde_yaml::from_str(
            r#"
redis:
  url: redis://localhost:6379/
  worker_membership_ttl_seconds: 45
broker: { key_prefix: "test:broker", task_destination: "tasks" }
"#,
        )
        .unwrap();

        assert_eq!(config.redis.worker_membership_ttl_seconds, 45);
    }

    #[test]
    fn parses_configured_worker_name() {
        let config: Config = serde_yaml::from_str(
            r#"
server:
  bind_address: 127.0.0.1:0
redis:
  url: redis://localhost:6379/
broker:
  key_prefix: test:broker
  task_destination: test-tasks
worker:
  name: gpu-worker-01
  core_endpoint: http://core:50051
"#,
        )
        .unwrap();

        assert_eq!(
            config.worker_config().unwrap().name.as_deref(),
            Some("gpu-worker-01")
        );
    }

    #[test]
    fn parses_worker_resources_and_capabilities() {
        let config: Config = serde_yaml::from_str(
            r#"
server: { bind_address: "127.0.0.1:0" }
redis: { url: "redis://localhost:6379/" }
broker:
  key_prefix: test:broker
  task_destination: test-tasks
worker:
  core_endpoint: http://core:50051
  resources:
    cpu: 16
    memory: 32GiB
  capabilities: [python, linux]
"#,
        )
        .unwrap();

        let worker = config.worker_config().unwrap();
        assert_eq!(worker.resources.cpu, 16);
        assert_eq!(worker.resources.memory, 32 * (1 << 30));
        assert_eq!(worker.capabilities, ["python", "linux"]);
    }

    #[test]
    fn rejects_invalid_worker_memory() {
        let error = serde_yaml::from_str::<Config>(
            r#"
server: { bind_address: "127.0.0.1:0" }
redis: { url: "redis://localhost:6379/" }
broker:
  key_prefix: test:broker
  task_destination: test-tasks
worker:
  resources: { cpu: 4, memory: lots }
  core_endpoint: http://core:50051
"#,
        )
        .unwrap_err();

        assert!(error.to_string().contains("worker memory must use"));
    }

    #[test]
    fn parses_explicit_empty_capabilities() {
        let config: Config = serde_yaml::from_str(
            r#"
server: { bind_address: "127.0.0.1:0" }
redis: { url: "redis://localhost:6379/" }
broker:
  key_prefix: test:broker
  task_destination: test-tasks
worker:
  capabilities: []
  core_endpoint: http://core:50051
"#,
        )
        .unwrap();

        assert!(config.worker_config().unwrap().capabilities.is_empty());
    }

    #[test]
    fn rejects_cpu_that_cannot_be_represented_as_millicores() {
        let error = serde_yaml::from_str::<Config>(
            r#"
server: { bind_address: "127.0.0.1:0" }
redis: { url: "redis://localhost:6379/" }
broker:
  key_prefix: test:broker
  task_destination: test-tasks
worker:
  resources: { cpu: 18446744073709551615 }
  core_endpoint: http://core:50051
"#,
        )
        .unwrap_err();

        assert!(error.to_string().contains("millicore range"));
    }

    #[test]
    fn server_and_worker_configuration_can_be_loaded_independently() {
        let server: Config = serde_yaml::from_str(
            r#"
server: { bind_address: "0.0.0.0:50051" }
redis: { url: "redis://redis:6379/" }
broker: { key_prefix: "threadweave:broker", task_destination: "tasks" }
"#,
        )
        .unwrap();
        assert!(server.server_config().is_ok());
        assert!(server.worker_config().is_err());

        let worker: Config = serde_yaml::from_str(
            r#"
redis: { url: "redis://redis:6379/" }
broker: { key_prefix: "threadweave:broker", task_destination: "tasks" }
worker:
  core_endpoint: http://server:50051
  resources: { cpu: 2, memory: 1GiB }
  capabilities: [linux]
"#,
        )
        .unwrap();
        assert!(worker.server_config().is_err());
        assert_eq!(
            worker.worker_config().unwrap().core_endpoint,
            "http://server:50051"
        );
    }
}
