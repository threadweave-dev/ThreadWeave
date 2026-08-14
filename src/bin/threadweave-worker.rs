use std::path::PathBuf;
use std::sync::Arc;

use clap::Parser;
use threadweave::broker::RedisBroker;
use threadweave::config::Config;
use threadweave::worker::Worker;

#[derive(Parser)]
struct Cli {
    #[arg(short, long, default_value = "threadweave.yaml")]
    config: PathBuf,

    /// Emit startup information as JSON on stdout instead of a text banner on stderr.
    #[arg(long)]
    json: bool,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cli = Cli::parse();
    if cli.json {
        tracing_subscriber::fmt().with_writer(std::io::sink).init();
    } else {
        tracing_subscriber::fmt()
            .with_writer(std::io::stderr)
            .init();
    }
    let config = Config::load(cli.config)?;
    if cli.json {
        threadweave::startup::print_json(&config, "worker")?;
    } else {
        threadweave::startup::print_banner(&config, "worker")?;
    }
    let broker = Arc::new(RedisBroker::new(
        &config.redis.url,
        &config.broker.key_prefix,
    )?);
    Worker::new(broker, config.broker.worker_destination)
        .run()
        .await?;
    Ok(())
}
