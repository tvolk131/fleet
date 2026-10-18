#![allow(dead_code)] // Shared fixture functions are also used by the integration-test target.
mod garage;
mod garage_native;
mod host;
mod multihost;
mod rolling;
mod support;
mod tart;
mod vm_services;
use clap::{Parser, Subcommand};
use std::path::PathBuf;
#[derive(Parser)]
#[command(
    about = "Isolated Fleet release qualification; never installs or upgrades a normal service"
)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}
#[derive(Subcommand)]
enum Commands {
    Garage(garage::Options),
    GarageNative(garage_native::Options),
    VmServices(tart::Options),
    Host,
    BuildUpgradeFixtures {
        #[arg(long)]
        output: PathBuf,
    },
    RollingUpgrade {
        #[arg(long)]
        home: PathBuf,
        #[arg(long)]
        baseline: PathBuf,
        #[arg(long)]
        candidate: PathBuf,
        #[arg(long)]
        unsupported: PathBuf,
    },
    Checkpoint(tart::Options),
    Portable(tart::Options),
    NormalMultihost(multihost::Options),
    NormalTart(tart::Options),
}
fn main() {
    let result = match Cli::parse().command {
        Commands::Garage(options) => garage::run(options),
        Commands::GarageNative(options) => garage_native::run(options),
        Commands::VmServices(options) => vm_services::run(options),
        Commands::Host => host::stdin(),
        Commands::BuildUpgradeFixtures { output } => rolling::build(&output),
        Commands::RollingUpgrade {
            home,
            baseline,
            candidate,
            unsupported,
        } => rolling::run(&home, &baseline, &candidate, &unsupported),
        Commands::Checkpoint(options) => tart::checkpoint(options),
        Commands::Portable(options) => tart::portable(options),
        Commands::NormalMultihost(options) => multihost::run(options),
        Commands::NormalTart(options) => tart::normal(options),
    };
    if let Err(e) = result {
        eprintln!("{e:#}");
        std::process::exit(1);
    }
}
