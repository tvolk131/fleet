//! The runner process: `fleet driver-runner <backend> <instance dir>`. One per VM, detached from
//! the worker. It owns the guest's network card for the VM's whole life, serves the control
//! protocol on `runner.sock`, and records why it ended in `exit.json`.

use std::{
    os::{
        fd::{AsRawFd, FromRawFd, OwnedFd, RawFd},
        unix::net::{UnixListener, UnixStream},
    },
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
};

use anyhow::{Context, Result, bail};

use super::{
    EXIT, Exit, InstanceSpec, Phase, RUNNER_LOCK, SPEC,
    protocol::{self, Request, Response},
    write_json,
};

/// The descriptor number the driver hands the held runner lock to.
pub const LOCK_FD: RawFd = 3;

/// A backend's VM inside the runner process. Every method may be called from any thread.
pub trait Vm: Send + Sync + 'static {
    /// Boots the guest, or restores it from `restore`, with `card` as its only network card.
    /// Returns once the guest is running.
    fn start(&self, spec: &InstanceSpec, card: OwnedFd, restore: Option<&Path>) -> Result<()>;
    /// Asks the guest to stop. Returning does not mean it has.
    fn request_stop(&self) -> Result<()>;
    fn connect_guest(&self, port: u32) -> Result<OwnedFd>;
    /// Saves to `to` and stops the guest, after which `wait` returns `Exit::Saved`.
    fn save(&self, to: &Path) -> Result<()>;
    /// Blocks until the guest has stopped.
    fn wait(&self) -> Exit;
}

pub struct Args {
    pub dir: PathBuf,
    pub lock_fd: Option<RawFd>,
    pub restore: Option<PathBuf>,
}

/// Runs one VM to completion. Errors before the guest starts are recorded as `Exit::Failed`.
pub fn run(args: Args, vm: impl Vm) -> Result<()> {
    let _lock = hold_lock(&args)?;
    let result = serve(&args, Arc::new(vm));
    let exit = match result {
        Ok(exit) => exit,
        Err(e) => Exit::Failed {
            error: format!("{e:#}"),
        },
    };
    if let Ok(socket) = super::socket_path(&args.dir) {
        let _ = std::fs::remove_file(socket);
    }
    write_json(&args.dir.join(EXIT), &exit)?;
    match exit {
        Exit::Failed { error } => bail!(error),
        _ => Ok(()),
    }
}

fn serve(args: &Args, vm: Arc<impl Vm>) -> Result<Exit> {
    let spec: InstanceSpec = serde_json::from_slice(
        &std::fs::read(args.dir.join(SPEC)).context("reading the instance spec")?,
    )?;
    let (guest_end, host_end) = card_pair()?;
    vm.start(&spec, guest_end, args.restore.as_deref())?;

    let socket = super::socket_path(&args.dir)?;
    let _ = std::fs::remove_file(&socket);
    let listener = UnixListener::bind(&socket)?;
    let state = Arc::new(State {
        host_end,
        stopping: AtomicBool::new(false),
        generation: spec.generation,
    });
    std::thread::spawn({
        let vm = vm.clone();
        move || {
            for stream in listener.incoming().flatten() {
                let (vm, state) = (vm.clone(), state.clone());
                std::thread::spawn(move || {
                    let _ = handle(&*vm, &state, stream);
                });
            }
        }
    });
    Ok(vm.wait())
}

struct State {
    /// The runner's own copy, kept until exit; borrowers get duplicates.
    host_end: OwnedFd,
    stopping: AtomicBool,
    generation: u64,
}

fn handle(vm: &impl Vm, state: &State, mut stream: UnixStream) -> Result<()> {
    let request: Request = protocol::read_line(&mut stream)?;
    let reply = |stream: &mut UnixStream, result: Result<Response>| {
        let response = result.unwrap_or_else(|e| Response::Error {
            message: format!("{e:#}"),
        });
        protocol::write_line(stream, &response)
    };
    match request {
        Request::Status => reply(
            &mut stream,
            Ok(Response::Status {
                phase: if state.stopping.load(Ordering::Relaxed) {
                    Phase::Stopping
                } else {
                    Phase::Running
                },
                pid: std::process::id(),
                generation: state.generation,
            }),
        ),
        Request::Stop => {
            state.stopping.store(true, Ordering::Relaxed);
            reply(&mut stream, vm.request_stop().map(|()| Response::Ok))
        }
        Request::NetworkCard => {
            let fd = state.host_end.try_clone()?;
            send_with_fd(&mut stream, &fd)
        }
        Request::Guest { port } => match vm.connect_guest(port) {
            Ok(fd) => send_with_fd(&mut stream, &fd),
            Err(e) => reply(&mut stream, Err(e)),
        },
        Request::Save { path } => {
            state.stopping.store(true, Ordering::Relaxed);
            reply(&mut stream, vm.save(&path).map(|()| Response::Ok))
        }
    }
}

fn send_with_fd(stream: &mut UnixStream, fd: &OwnedFd) -> Result<()> {
    protocol::write_line(stream, &Response::Fd)?;
    protocol::send_fd(stream, fd.as_raw_fd())?;
    // On macOS a socket descriptor still in flight can be discarded if the sender closes its
    // copy first: about one in five guest streams arrived already closed. Keep ours open until
    // the client has received it and hung up.
    stream.set_read_timeout(Some(std::time::Duration::from_secs(30)))?;
    let _ = std::io::Read::read(stream, &mut [0u8; 1]);
    Ok(())
}

/// The guest's network card: a datagram socketpair, one datagram per Ethernet frame.
pub fn card_pair() -> Result<(OwnedFd, OwnedFd)> {
    let mut fds = [0; 2];
    if unsafe { libc::socketpair(libc::AF_UNIX, libc::SOCK_DGRAM, 0, fds.as_mut_ptr()) } != 0 {
        bail!("socketpair: {}", std::io::Error::last_os_error());
    }
    let pair = unsafe { (OwnedFd::from_raw_fd(fds[0]), OwnedFd::from_raw_fd(fds[1])) };
    for fd in [&pair.0, &pair.1] {
        unsafe { libc::fcntl(fd.as_raw_fd(), libc::F_SETFD, libc::FD_CLOEXEC) };
        // Apple recommends a receive buffer several times the send buffer for frame sockets.
        for (option, size) in [(libc::SO_SNDBUF, 1 << 20), (libc::SO_RCVBUF, 4 << 20)] {
            let size: libc::c_int = size;
            unsafe {
                libc::setsockopt(
                    fd.as_raw_fd(),
                    libc::SOL_SOCKET,
                    option,
                    (&size as *const libc::c_int).cast(),
                    size_of::<libc::c_int>() as libc::socklen_t,
                )
            };
        }
    }
    Ok(pair)
}

/// Holds the runner lock for the process lifetime: the one the driver handed over, or a fresh
/// one when started by hand.
fn hold_lock(args: &Args) -> Result<OwnedFd> {
    let fd = match args.lock_fd {
        Some(fd) => unsafe { OwnedFd::from_raw_fd(fd) },
        None => std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(args.dir.join(RUNNER_LOCK))?
            .into(),
    };
    unsafe { libc::fcntl(fd.as_raw_fd(), libc::F_SETFD, libc::FD_CLOEXEC) };
    // Succeeds immediately when this open file already holds the lock.
    if unsafe { libc::flock(fd.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) } != 0 {
        bail!("another runner holds {}", args.dir.display());
    }
    Ok(fd)
}
