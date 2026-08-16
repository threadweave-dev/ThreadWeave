use std::path::PathBuf;
use std::sync::Arc;

use clap::Parser;
use threadweave::broker::RedisBroker;
use threadweave::config::Config;
use threadweave::scheduler::Scheduler;
use threadweave::worker_registry::WorkerRegistry;

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
        threadweave::startup::print_json(&config, "scheduler")?;
    } else {
        threadweave::startup::print_banner(&config, "scheduler")?;
    }
    let broker = Arc::new(RedisBroker::new(
        &config.redis.url,
        &config.broker.key_prefix,
    )?);
    Scheduler::new(
        broker,
        config.broker.task_destination,
        Arc::new(WorkerRegistry::default()),
    )
    .run()
    .await?;
    Ok(())
}
