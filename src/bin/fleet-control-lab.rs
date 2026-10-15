//! Qualification entry point; normal users use fleet.
use clap::Parser;
#[derive(Parser)]
struct Args {
    #[command(subcommand)]
    command: fleet::control::admin::Command,
}
#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .with_writer(std::io::stderr)
        .init();
    fleet::control::admin::run(Args::parse().command).await
}
