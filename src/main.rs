use std::path::PathBuf;

use clap::{Parser, Subcommand};
use threadweave::config::Config;

#[derive(Parser)]
#[command(name = "threadweave")]
struct Cli {
    #[command(subcommand)]
    role: Role,
}

#[derive(Subcommand)]
enum Role {
    /// Run the ThreadWeave control plane and public gRPC API.
    Server(Options),
    /// Run a ThreadWeave execution-plane worker.
    Worker(Options),
}

#[derive(clap::Args)]
struct Options {
    #[arg(short, long, default_value = "threadweave.yaml")]
    config: PathBuf,
    /// Emit startup information as JSON on stdout instead of a text banner on stderr.
    #[arg(long)]
    json: bool,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let (component, options, is_server) = match Cli::parse().role {
        Role::Server(options) => ("server", options, true),
        Role::Worker(options) => ("worker", options, false),
    };
    if options.json {
        tracing_subscriber::fmt().with_writer(std::io::sink).init();
    } else {
        tracing_subscriber::fmt()
            .with_writer(std::io::stderr)
            .init();
    }
    let config = Config::load(options.config)?;
    if options.json {
        threadweave::startup::print_json(&config, component)?;
    } else {
        threadweave::startup::print_banner(&config, component)?;
    }
    if is_server {
        threadweave::startup::run_server(config).await
    } else {
        threadweave::startup::run_worker(config).await
    }
}
