use std::path::PathBuf;
use std::sync::Arc;

use clap::Parser;
use threadweave::broker::RedisBroker;
use threadweave::config::Config;
use threadweave::scheduler::Scheduler;

#[derive(Parser)]
struct Cli {
    #[arg(short, long, default_value = "threadweave.yaml")]
    config: PathBuf,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .init();
    let config = Config::load(Cli::parse().config)?;
    let broker = Arc::new(RedisBroker::new(
        &config.redis.url,
        &config.broker.key_prefix,
    )?);
    Scheduler::new(
        broker,
        config.broker.task_destination,
        config.broker.worker_destination,
    )
    .run()
    .await?;
    Ok(())
}
