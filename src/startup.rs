use std::io::{self, Write};

use crate::config::Config;

const DESCRIPTION: &str = "A language-agnostic, resource-aware distributed execution engine for modern workloads. ThreadWeave orchestrates tasks, scheduling, resources, and observability while letting any language provide its own native runtime through a stable execution protocol.";

const PROTOCOL_VERSIONS: &str = "common/v1, artifacts/v1, broker/v1, execution/v1, runtime/v1";
const PROTOCOLS: [&str; 5] = [
    "common/v1",
    "artifacts/v1",
    "broker/v1",
    "execution/v1",
    "runtime/v1",
];

/// Print a human-friendly summary of the running component and its connections.
///
/// Startup information is deliberately written to stderr: stdout is reserved for
/// machine-readable lifecycle messages such as the API's `ready` event.
pub fn print_banner(config: &Config, component: &str) -> io::Result<()> {
    print_banner_to(config, component, &mut io::stderr().lock())
}

/// Print the startup information as one JSON object on stdout.
pub fn print_json(config: &Config, component: &str) -> io::Result<()> {
    print_json_to(config, component, &mut io::stdout().lock())
}

fn print_json_to(config: &Config, component: &str, output: &mut impl Write) -> io::Result<()> {
    let redis_address = address_without_credentials(&config.redis.url);
    let value = serde_json::json!({
        "type": "startup",
        "name": "ThreadWeave",
        "description": DESCRIPTION,
        "component": component,
        "program_version": env!("CARGO_PKG_VERSION"),
        "protocols": PROTOCOLS,
        "broker": {
            "transport": "redis",
            "address": redis_address,
            "key_prefix": config.broker.key_prefix,
        },
        "result_backend": {
            "transport": "redis",
            "address": redis_address,
            "key_prefix": format!("{}:results", config.broker.key_prefix),
        },
    });
    serde_json::to_writer(&mut *output, &value)?;
    writeln!(output)?;
    output.flush()
}

fn print_banner_to(config: &Config, component: &str, output: &mut impl Write) -> io::Result<()> {
    let redis_address = address_without_credentials(&config.redis.url);
    let result_prefix = format!("{}:results", config.broker.key_prefix);

    writeln!(
        output,
        r#"
  _______ _                        _ __          __
 |__   __| |                      | |\ \        / /
    | |  | |__  _ __ ___  __ _  __| |\ \  /\  / /__  __ ___   _____
    | |  | '_ \| '__/ _ \/ _` |/ _` | \ \/  \/ / _ \/ _` \ \ / / _ \
    | |  | | | | | |  __/ (_| | (_| |  \  /\  /  __/ (_| |\ V /  __/
    |_|  |_| |_|_|  \___|\__,_|\__,_|   \/  \/ \___|\__,_| \_/ \___|

 {DESCRIPTION}

 ┌──────────────────────────────────────────────────────────────────────
 │ Component       {component}
 │ Program version v{program_version}
 │ Protocols       {protocol_versions}
 │ Broker          redis · {redis_address}
 │ Result backend  redis · {redis_address} · prefix {result_prefix}
 └──────────────────────────────────────────────────────────────────────
"#,
        program_version = env!("CARGO_PKG_VERSION"),
        protocol_versions = PROTOCOL_VERSIONS,
    )
}

fn address_without_credentials(address: &str) -> String {
    let Some((scheme, remainder)) = address.split_once("://") else {
        return address.to_owned();
    };
    let Some((_, endpoint)) = remainder.rsplit_once('@') else {
        return address.to_owned();
    };
    format!("{scheme}://{endpoint}")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn config(redis_url: &str) -> Config {
        serde_yaml::from_str(&format!(
            r#"
server:
  bind_address: 127.0.0.1:0
redis:
  url: {redis_url}
broker:
  key_prefix: threadweave:broker
  task_destination: tasks
  worker_destination: workers.default
"#
        ))
        .unwrap()
    }

    #[test]
    fn banner_contains_runtime_configuration_and_versions() {
        let mut output = Vec::new();
        print_banner_to(&config("redis://localhost:6379/"), "api", &mut output).unwrap();
        let output = String::from_utf8(output).unwrap();

        assert!(output.contains("ThreadWeave orchestrates tasks"));
        assert!(output.contains("Component       api"));
        assert!(output.contains(concat!("Program version v", env!("CARGO_PKG_VERSION"))));
        assert!(output.contains(PROTOCOL_VERSIONS));
        assert!(output.contains("Broker          redis · redis://localhost:6379/"));
        assert!(output.contains(
            "Result backend  redis · redis://localhost:6379/ · prefix threadweave:broker:results"
        ));
    }

    #[test]
    fn banner_does_not_expose_redis_credentials() {
        let mut output = Vec::new();
        print_banner_to(
            &config("redis://user:secret@redis.internal:6379/"),
            "worker",
            &mut output,
        )
        .unwrap();
        let output = String::from_utf8(output).unwrap();

        assert!(output.contains("redis://redis.internal:6379/"));
        assert!(!output.contains("user:secret"));
    }

    #[test]
    fn json_output_is_structured_and_does_not_expose_credentials() {
        let mut output = Vec::new();
        print_json_to(
            &config("redis://user:secret@redis.internal:6379/"),
            "scheduler",
            &mut output,
        )
        .unwrap();
        let value: serde_json::Value = serde_json::from_slice(&output).unwrap();

        assert_eq!(value["type"], "startup");
        assert_eq!(value["name"], "ThreadWeave");
        assert_eq!(value["component"], "scheduler");
        assert_eq!(value["program_version"], env!("CARGO_PKG_VERSION"));
        assert_eq!(value["protocols"][3], "execution/v1");
        assert_eq!(value["broker"]["transport"], "redis");
        assert_eq!(value["broker"]["address"], "redis://redis.internal:6379/");
        assert_eq!(
            value["result_backend"]["key_prefix"],
            "threadweave:broker:results"
        );
        assert!(!String::from_utf8(output).unwrap().contains("user:secret"));
    }
}
