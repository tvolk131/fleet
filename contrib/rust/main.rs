#![allow(dead_code)] // Shared fixture functions are also used by the integration-test target.
mod garage;
mod garage_native;
mod host;
mod managed_vm;
mod multihost;
mod rolling;
mod support;
mod vm;
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
    VmServices(vm::Options),
    ManagedVm(vm::Options),
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
    Checkpoint(vm::Options),
    Portable(vm::Options),
    NormalMultihost(multihost::Options),
    NormalVm(vm::Options),
}
fn main() {
    let result = match Cli::parse().command {
        Commands::Garage(options) => garage::run(options),
        Commands::GarageNative(options) => garage_native::run(options),
        Commands::VmServices(options) => vm_services::run(options),
        Commands::ManagedVm(options) => managed_vm::run(options),
        Commands::Host => host::stdin(),
        Commands::BuildUpgradeFixtures { output } => rolling::build(&output),
        Commands::RollingUpgrade {
            home,
            baseline,
            candidate,
            unsupported,
        } => rolling::run(&home, &baseline, &candidate, &unsupported),
        Commands::Checkpoint(options) => vm::checkpoint(options),
        Commands::Portable(options) => vm::portable(options),
        Commands::NormalMultihost(options) => multihost::run(options),
        Commands::NormalVm(options) => vm::normal(options),
    };
    if let Err(e) = result {
        eprintln!("{e:#}");
        std::process::exit(1);
    }
}
