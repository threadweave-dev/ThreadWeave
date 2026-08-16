use std::fs::File;
use std::path::Path;

use serde::Deserialize;

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    pub server: ServerConfig,
    pub redis: RedisConfig,
    pub broker: BrokerConfig,
    #[serde(default)]
    pub worker: WorkerConfig,
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

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WorkerConfig {
    pub name: Option<String>,
    #[serde(default = "default_core_endpoint")]
    pub core_endpoint: String,
    #[serde(default)]
    pub resources: WorkerResourcesConfig,
    #[serde(default)]
    pub capabilities: Vec<String>,
}

fn default_core_endpoint() -> String {
    "http://127.0.0.1:50051".to_owned()
}

impl Default for WorkerConfig {
    fn default() -> Self {
        Self {
            name: None,
            core_endpoint: default_core_endpoint(),
            resources: WorkerResourcesConfig::default(),
            capabilities: Vec::new(),
        }
    }
}

#[derive(Debug, Default, Deserialize)]
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
        assert!(config.worker.name.is_none());
        assert_eq!(config.worker.core_endpoint, "http://127.0.0.1:50051");
        assert_eq!(config.worker.resources.cpu, 0);
        assert_eq!(config.worker.resources.memory, 0);
        assert!(config.worker.capabilities.is_empty());
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
"#,
        )
        .unwrap();

        assert_eq!(config.worker.name.as_deref(), Some("gpu-worker-01"));
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
  resources:
    cpu: 16
    memory: 32GiB
  capabilities: [python, linux]
"#,
        )
        .unwrap();

        assert_eq!(config.worker.resources.cpu, 16);
        assert_eq!(config.worker.resources.memory, 32 * (1 << 30));
        assert_eq!(config.worker.capabilities, ["python", "linux"]);
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
"#,
        )
        .unwrap();

        assert!(config.worker.capabilities.is_empty());
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
"#,
        )
        .unwrap_err();

        assert!(error.to_string().contains("millicore range"));
    }
}
