//! The Virtualization.framework driver. The supervising side ([`VzBackend`]) only lays out
//! instance directories; everything that touches the framework runs in the `fleet-runner-vz`
//! executable ([`run`]), which must be signed with `com.apple.security.virtualization`
//! (`contrib/vz.entitlements`, applied by [`sign_runner`]).
//!
//! An image bundle is a directory with an `image.json` ([`Manifest`]) naming files beside it.
//! [`VzBackend::prepare`] copies the bundle into the instance directory as APFS clones, so a
//! running instance never depends on its image again and the copies cost no space until written.

mod vm;

use std::{
    ffi::CString,
    fs,
    os::unix::ffi::OsStrExt,
    path::{Path, PathBuf},
    process::Command,
};

use anyhow::{Context, bail, ensure};
use serde::{Deserialize, Serialize};

use super::{Backend, Capabilities, Error, GuestOs, InstanceSpec, Result};

pub use vm::run;

pub const MANIFEST: &str = "image.json";
/// Recorded at the first prepare; an instance's disk always comes from one image.
const SOURCE: &str = "source.json";
pub const DISK: &str = "disk.img";

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "guest", rename_all = "snake_case", deny_unknown_fields)]
pub enum Manifest {
    Linux {
        kernel: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        initrd: Option<String>,
        cmdline: String,
        /// Without one, the guest gets a blank disk of the instance's size.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        disk: Option<String>,
    },
    Macos {
        /// `VZMacHardwareModel.dataRepresentation`.
        hardware_model: String,
        /// The auxiliary storage (NVRAM) created when macOS was installed.
        aux: String,
        disk: String,
    },
}

impl Manifest {
    pub fn read(bundle: &Path) -> anyhow::Result<Self> {
        let path = bundle.join(MANIFEST);
        let manifest: Self = serde_json::from_slice(
            &fs::read(&path).with_context(|| format!("reading {}", path.display()))?,
        )
        .with_context(|| format!("invalid {}", path.display()))?;
        for name in manifest.files() {
            ensure!(
                !name.is_empty()
                    && name != MANIFEST
                    && !name.contains('/')
                    && !name.starts_with('.'),
                "image file {name:?} must be a plain name inside the bundle"
            );
        }
        Ok(manifest)
    }

    pub fn guest(&self) -> GuestOs {
        match self {
            Self::Linux { .. } => GuestOs::Linux,
            Self::Macos { .. } => GuestOs::Macos,
        }
    }

    fn files(&self) -> Vec<&str> {
        match self {
            Self::Linux {
                kernel,
                initrd,
                disk,
                ..
            } => [Some(kernel), initrd.as_ref(), disk.as_ref()]
                .into_iter()
                .flatten()
                .map(String::as_str)
                .collect(),
            Self::Macos {
                hardware_model,
                aux,
                disk,
            } => vec![hardware_model, aux, disk],
        }
    }

    fn disk(&self) -> Option<&str> {
        match self {
            Self::Linux { disk, .. } => disk.as_deref(),
            Self::Macos { disk, .. } => Some(disk),
        }
    }
}

pub struct VzBackend {
    /// The signed `fleet-runner-vz` executable.
    pub runner: PathBuf,
}

impl Backend for VzBackend {
    fn capabilities(&self) -> Capabilities {
        Capabilities {
            driver: "vz".into(),
            guests: vec![GuestOs::Linux, GuestOs::Macos],
            // Apple's licence and the framework both cap concurrent macOS guests at two.
            max_macos_guests: 2,
            save_restore: true,
            configurable_mtu: true,
        }
    }

    fn prepare(&self, spec: &InstanceSpec, dir: &Path) -> Result<()> {
        let image = fs::canonicalize(&spec.image)
            .with_context(|| format!("image {}", spec.image.display()))?;
        let manifest = Manifest::read(&image)?;
        if manifest.guest() != spec.guest {
            return Err(Error::Invalid(format!(
                "image {} is a {:?} guest, the spec asks for {:?}",
                image.display(),
                manifest.guest(),
                spec.guest
            )));
        }
        let source = dir.join(SOURCE);
        match fs::read(&source) {
            Ok(recorded) => {
                let recorded: PathBuf = serde_json::from_slice(&recorded)?;
                if recorded != image {
                    return Err(Error::Invalid(format!(
                        "instance {} was created from {}; delete it to change its image",
                        spec.id,
                        recorded.display()
                    )));
                }
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                // Disk first and the record last, so an interrupted prepare starts over.
                for name in manifest.files() {
                    let to = dir.join(if Some(name) == manifest.disk() {
                        DISK
                    } else {
                        name
                    });
                    let _ = fs::remove_file(&to);
                    clone(&image.join(name), &to)?;
                }
                if manifest.disk().is_none() {
                    let _ = fs::remove_file(dir.join(DISK));
                    fs::File::create(dir.join(DISK))?;
                }
                // The instance's own manifest names its own disk.
                let mut copy = manifest.clone();
                match &mut copy {
                    Manifest::Linux { disk, .. } => *disk = Some(DISK.into()),
                    Manifest::Macos { disk, .. } => *disk = DISK.into(),
                }
                super::write_json(&dir.join(MANIFEST), &copy)?;
                super::write_json(&source, &image)?;
            }
            Err(e) => return Err(e.into()),
        }
        grow(&dir.join(DISK), spec.disk_gb)?;
        Ok(())
    }

    fn runner(&self, dir: &Path) -> Command {
        let mut command = Command::new(&self.runner);
        command.arg(dir);
        command
    }
}

/// A copy-on-write clone: instant and free until either side is written.
fn clone(from: &Path, to: &Path) -> anyhow::Result<()> {
    let (src, dst) = (
        CString::new(from.as_os_str().as_bytes())?,
        CString::new(to.as_os_str().as_bytes())?,
    );
    if unsafe { libc::clonefile(src.as_ptr(), dst.as_ptr(), 0) } != 0 {
        bail!(
            "cloning {} to {}: {}",
            from.display(),
            to.display(),
            std::io::Error::last_os_error()
        );
    }
    Ok(())
}

/// Disks only grow: shrinking one would cut off the guest's filesystem.
fn grow(disk: &Path, gb: u32) -> Result<()> {
    let want = u64::from(gb) << 30;
    let file = fs::OpenOptions::new().write(true).open(disk)?;
    let have = file.metadata()?.len();
    if have > want {
        return Err(Error::Invalid(format!(
            "the disk is already {} GiB; disks cannot shrink to {gb} GiB",
            have.div_ceil(1 << 30)
        )));
    }
    file.set_len(want)?;
    Ok(())
}

/// Ad-hoc signs `runner` with the virtualization entitlement, which the framework requires.
/// Signing needs no identity or network access; it is repeated only when the binary changed.
pub fn sign_runner(runner: &Path) -> anyhow::Result<()> {
    let entitlements = std::env::temp_dir().join(format!(
        "fleet-vz-entitlements-{}.plist",
        std::process::id()
    ));
    fs::write(&entitlements, ENTITLEMENTS)?;
    let status = Command::new("/usr/bin/codesign")
        .args(["--force", "--sign", "-", "--entitlements"])
        .arg(&entitlements)
        .arg(runner)
        .output();
    let _ = fs::remove_file(&entitlements);
    let output = status?;
    ensure!(
        output.status.success(),
        "codesign {}: {}",
        runner.display(),
        String::from_utf8_lossy(&output.stderr)
    );
    Ok(())
}

const ENTITLEMENTS: &str = include_str!("../../../contrib/vz.entitlements");

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct TartConfig {
    os: String,
    hardware_model: Option<String>,
}

/// Turns a Tart VM directory (`config.json`, `disk.img`, `nvram.bin`) into an image bundle,
/// cloning rather than copying the disk.
pub fn import_tart(tart: &Path, bundle: &Path) -> anyhow::Result<()> {
    use base64::Engine;
    let config: TartConfig = serde_json::from_slice(&fs::read(tart.join("config.json"))?)
        .context("invalid Tart config.json")?;
    ensure!(
        config.os == "darwin",
        "only macOS Tart images can be imported, not {:?}",
        config.os
    );
    let model = base64::engine::general_purpose::STANDARD
        .decode(
            config
                .hardware_model
                .context("Tart config has no hardwareModel")?,
        )
        .context("invalid hardwareModel")?;
    ensure!(!bundle.exists(), "{} already exists", bundle.display());
    let staging = bundle.with_extension("importing");
    let _ = fs::remove_dir_all(&staging);
    fs::create_dir_all(&staging)?;
    fs::write(staging.join("hardware-model.bin"), model)?;
    clone(&tart.join("nvram.bin"), &staging.join("aux.img"))?;
    clone(&tart.join("disk.img"), &staging.join(DISK))?;
    super::write_json(
        &staging.join(MANIFEST),
        &Manifest::Macos {
            hardware_model: "hardware-model.bin".into(),
            aux: "aux.img".into(),
            disk: DISK.into(),
        },
    )?;
    fs::rename(&staging, bundle)?;
    Ok(())
}
