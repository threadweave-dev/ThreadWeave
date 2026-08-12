use std::path::PathBuf;

use clap::Parser;

#[derive(Debug, Parser)]
#[command(version, about)]
struct Cli {
    /// Path to the YAML configuration file.
    #[arg(short, long, default_value = "threadweave.yaml")]
    config: PathBuf,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .init();
    let cli = Cli::parse();
    let config = threadweave::config::Config::load(cli.config)?;
    threadweave::serve(config).await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn uses_default_configuration_path() {
        let cli = Cli::try_parse_from(["threadweave"]).unwrap();
        assert_eq!(cli.config, PathBuf::from("threadweave.yaml"));
    }

    #[test]
    fn accepts_a_custom_configuration_path() {
        let cli = Cli::try_parse_from(["threadweave", "--config", "/tmp/custom.yaml"]).unwrap();
        assert_eq!(cli.config, PathBuf::from("/tmp/custom.yaml"));
    }
}
