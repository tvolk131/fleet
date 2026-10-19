//! The Virtualization.framework runner: `fleet-runner-vz <instance dir> [--lock-fd N]
//! [--restore PATH]`. Started only by the `vz` driver. A separate executable, so the
//! virtualization entitlement is never granted to the rest of Fleet.

#[cfg(target_os = "macos")]
fn main() -> anyhow::Result<()> {
    let args = fleet::driver::runner::Args::parse(std::env::args_os().skip(1))?;
    fleet::driver::vz::run(args)
}

#[cfg(not(target_os = "macos"))]
fn main() -> anyhow::Result<()> {
    anyhow::bail!("the vz driver runs only on macOS")
}
