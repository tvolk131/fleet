//! The guest agent: a daemon inside the VM, listening on vsock port [`PORT`], through which
//! the host runs guest commands. Each connection carries one request line:
//!
//! - `{"kind":"shutdown"}` shuts the guest down;
//! - `{"kind":"spawn","argv":[...]}` runs a process, then carries its stdio as frames.
//!
//! A frame is a tag byte, a big-endian `u32` length and that many bytes. The host sends
//! [`STDIN`] frames, an empty one closing the process's stdin. The agent sends [`STDOUT`] and
//! [`STDERR`] frames and finally one [`EXIT`] frame holding the exit code as a big-endian `i32`
//! (128 + the signal for a killed process). If the host disconnects first, the agent kills the
//! process's whole group.
//!
//! The agent adds no authority: the host already controls the VM, and the agent runs as the
//! guest's automatically logged-in user.

use std::{
    io::{Read, Write},
    os::{
        fd::{AsRawFd, FromRawFd, OwnedFd},
        unix::process::{CommandExt, ExitStatusExt},
    },
    process::{Command, Stdio},
    sync::{Arc, Mutex},
};

use anyhow::{Result, bail, ensure};
use serde::{Deserialize, Serialize};

/// Must match `fleet::driver::AGENT_PORT`.
pub const PORT: u32 = 1024;
pub const STDIN: u8 = 0;
pub const STDOUT: u8 = 1;
pub const STDERR: u8 = 2;
pub const EXIT: u8 = 3;
const MAX_FRAME: usize = 1 << 20;
const MAX_LINE: usize = 1 << 20;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum AgentRequest {
    Shutdown,
    Spawn { argv: Vec<String> },
}

pub fn write_frame(out: &mut impl Write, tag: u8, data: &[u8]) -> std::io::Result<()> {
    let mut frame = Vec::with_capacity(5 + data.len());
    frame.push(tag);
    frame.extend_from_slice(&(data.len() as u32).to_be_bytes());
    frame.extend_from_slice(data);
    out.write_all(&frame)
}

/// The next frame, or `None` at a clean end of stream.
pub fn read_frame(input: &mut impl Read) -> Result<Option<(u8, Vec<u8>)>> {
    let mut header = [0u8; 5];
    match input.read_exact(&mut header) {
        Ok(()) => {}
        Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(None),
        Err(e) => return Err(e.into()),
    }
    let len = u32::from_be_bytes(header[1..].try_into().unwrap()) as usize;
    ensure!(len <= MAX_FRAME, "frame of {len} bytes exceeds {MAX_FRAME}");
    let mut data = vec![0u8; len];
    input.read_exact(&mut data)?;
    Ok(Some((header[0], data)))
}

/// Reads the request line byte by byte, so no frame bytes are consumed with it.
fn read_request(input: &mut impl Read) -> Result<AgentRequest> {
    let mut line = Vec::new();
    let mut byte = [0u8];
    loop {
        ensure!(
            input.read(&mut byte)? == 1,
            "connection closed before a request"
        );
        if byte[0] == b'\n' {
            break;
        }
        line.push(byte[0]);
        ensure!(line.len() <= MAX_LINE, "request line too long");
    }
    Ok(serde_json::from_slice(&line)?)
}

/// Serves connections from `listener` (a listening stream socket) forever.
pub fn serve(listener: OwnedFd) -> Result<()> {
    loop {
        let fd = unsafe {
            libc::accept(
                listener.as_raw_fd(),
                std::ptr::null_mut(),
                std::ptr::null_mut(),
            )
        };
        if fd < 0 {
            let error = std::io::Error::last_os_error();
            if error.kind() == std::io::ErrorKind::Interrupted {
                continue;
            }
            return Err(error.into());
        }
        let stream = unsafe { std::fs::File::from_raw_fd(fd) };
        std::thread::spawn(move || {
            if let Err(error) = connection(stream) {
                eprintln!("fleet-guest agent: {error:#}");
            }
        });
    }
}

fn connection(stream: std::fs::File) -> Result<()> {
    handle(
        stream,
        |argv| {
            let mut command = Command::new(&argv[0]);
            command.args(&argv[1..]);
            command
        },
        shutdown,
    )
}

/// Serves one connection, building each process with `command` (given a non-empty argv) and
/// shutting down with `shutdown`. A test double that runs its "guest" on the host uses this to
/// confine the processes it starts.
pub fn handle(
    mut stream: std::fs::File,
    command: impl FnOnce(&[String]) -> Command,
    shutdown: impl FnOnce() -> Result<()>,
) -> Result<()> {
    match read_request(&mut stream)? {
        AgentRequest::Shutdown => shutdown(),
        AgentRequest::Spawn { argv } => {
            ensure!(!argv.is_empty(), "empty argv");
            let program = argv[0].clone();
            spawn(stream, &program, command(&argv))
        }
    }
}

fn shutdown() -> Result<()> {
    let mut command = if unsafe { libc::geteuid() } == 0 {
        Command::new("/sbin/shutdown")
    } else {
        let mut sudo = Command::new("/usr/bin/sudo");
        sudo.args(["-n", "/sbin/shutdown"]);
        sudo
    };
    let status = command.args(["-h", "now"]).stdin(Stdio::null()).status()?;
    ensure!(status.success(), "shutdown failed: {status}");
    Ok(())
}

fn spawn(stream: std::fs::File, program: &str, mut command: Command) -> Result<()> {
    let out = Arc::new(Mutex::new(stream.try_clone()?));
    let mut child = match command
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .process_group(0)
        .spawn()
    {
        Ok(child) => child,
        Err(error) => {
            let mut out = out.lock().unwrap();
            write_frame(
                &mut *out,
                STDERR,
                format!("fleet-guest: {program}: {error}\n").as_bytes(),
            )?;
            write_frame(&mut *out, EXIT, &127i32.to_be_bytes())?;
            return Ok(());
        }
    };
    let pid = child.id() as libc::pid_t;
    let pumps = [
        (child.stdout.take().unwrap().into(), STDOUT),
        (child.stderr.take().unwrap().into(), STDERR),
    ]
    .map(|(source, tag): (OwnedFd, u8)| {
        let out = out.clone();
        std::thread::spawn(move || {
            let mut source = std::fs::File::from(source);
            let mut buf = vec![0u8; 64 << 10];
            loop {
                match source.read(&mut buf) {
                    Ok(0) | Err(_) => return,
                    Ok(n) => {
                        if write_frame(&mut *out.lock().unwrap(), tag, &buf[..n]).is_err() {
                            return;
                        }
                    }
                }
            }
        })
    });
    // Host to process. A disconnect before the process ends kills its group.
    let mut stdin = child.stdin.take();
    std::thread::spawn(move || {
        let mut stream = stream;
        loop {
            match read_frame(&mut stream) {
                Ok(Some((STDIN, data))) if data.is_empty() => drop(stdin.take()),
                Ok(Some((STDIN, data))) => {
                    if let Some(pipe) = stdin.as_mut()
                        && pipe.write_all(&data).is_err()
                    {
                        stdin = None;
                    }
                }
                Ok(Some(_)) | Ok(None) | Err(_) => {
                    unsafe { libc::kill(-pid, libc::SIGKILL) };
                    return;
                }
            }
        }
    });
    let status = child.wait()?;
    // Output still buffered in the pipes belongs before the exit frame. A descendant that
    // keeps a pipe open must not hold the exit back: give the pumps a moment, not forever.
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(2);
    for pump in pumps {
        while !pump.is_finished() && std::time::Instant::now() < deadline {
            std::thread::sleep(std::time::Duration::from_millis(10));
        }
    }
    let code = status
        .code()
        .unwrap_or_else(|| 128 + status.signal().unwrap_or(0));
    write_frame(&mut *out.lock().unwrap(), EXIT, &code.to_be_bytes())?;
    Ok(())
}

/// The host side: runs `argv` in the guest over `stream`, bridging this process's stdio,
/// and returns the guest process's exit code.
pub fn client(
    stream: std::os::unix::net::UnixStream,
    argv: &[String],
    mut stdin: impl Read + Send + 'static,
    mut stdout: impl Write,
    mut stderr: impl Write,
) -> Result<i32> {
    let mut writer = stream.try_clone()?;
    let mut line = serde_json::to_vec(&AgentRequest::Spawn {
        argv: argv.to_vec(),
    })?;
    line.push(b'\n');
    writer.write_all(&line)?;
    std::thread::spawn(move || {
        let mut buf = vec![0u8; 64 << 10];
        loop {
            match stdin.read(&mut buf) {
                Ok(0) | Err(_) => {
                    let _ = write_frame(&mut writer, STDIN, &[]);
                    return;
                }
                Ok(n) => {
                    if write_frame(&mut writer, STDIN, &buf[..n]).is_err() {
                        return;
                    }
                }
            }
        }
    });
    let mut reader = stream;
    loop {
        match read_frame(&mut reader)? {
            Some((STDOUT, data)) => {
                stdout.write_all(&data)?;
                stdout.flush()?;
            }
            Some((STDERR, data)) => {
                stderr.write_all(&data)?;
                stderr.flush()?;
            }
            Some((EXIT, data)) => {
                ensure!(data.len() == 4, "malformed exit frame");
                return Ok(i32::from_be_bytes(data.try_into().unwrap()));
            }
            Some((tag, _)) => bail!("unexpected frame {tag}"),
            None => bail!("the guest agent disconnected before the process exited"),
        }
    }
}

/// A listening vsock socket on `port`, accepting from the host.
pub fn listen_vsock(port: u32) -> Result<OwnedFd> {
    #[cfg(target_os = "linux")]
    let (family, addr) = {
        let mut addr: libc::sockaddr_vm = unsafe { std::mem::zeroed() };
        addr.svm_family = libc::AF_VSOCK as libc::sa_family_t;
        addr.svm_port = port;
        addr.svm_cid = libc::VMADDR_CID_ANY;
        (libc::AF_VSOCK, addr)
    };
    // <sys/vsock.h>; the libc crate does not define it for Apple targets.
    #[cfg(target_os = "macos")]
    let (family, addr) = {
        #[repr(C)]
        struct SockaddrVm {
            len: u8,
            family: u8,
            reserved: u16,
            port: u32,
            cid: u32,
        }
        const AF_VSOCK: libc::c_int = 40;
        (
            AF_VSOCK,
            SockaddrVm {
                len: size_of::<SockaddrVm>() as u8,
                family: AF_VSOCK as u8,
                reserved: 0,
                port,
                cid: u32::MAX,
            },
        )
    };
    let fd = unsafe { libc::socket(family, libc::SOCK_STREAM, 0) };
    ensure!(fd >= 0, "vsock socket: {}", std::io::Error::last_os_error());
    let socket = unsafe { OwnedFd::from_raw_fd(fd) };
    let bound = unsafe {
        libc::bind(
            fd,
            (&addr as *const _ as *const libc::sockaddr).cast(),
            size_of_val(&addr) as libc::socklen_t,
        )
    };
    ensure!(
        bound == 0 && unsafe { libc::listen(fd, 64) } == 0,
        "vsock port {port}: {}",
        std::io::Error::last_os_error()
    );
    Ok(socket)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::os::unix::net::{UnixListener, UnixStream};

    fn agent() -> std::path::PathBuf {
        use std::sync::atomic::{AtomicU64, Ordering};
        static N: AtomicU64 = AtomicU64::new(0);
        let path = std::env::temp_dir().join(format!(
            "fleet-agent-{}-{}.sock",
            std::process::id(),
            N.fetch_add(1, Ordering::Relaxed)
        ));
        let _ = std::fs::remove_file(&path);
        let listener = UnixListener::bind(&path).unwrap();
        std::thread::spawn(move || serve(listener.into()));
        path
    }

    fn run(path: &std::path::Path, argv: &[&str], input: &'static [u8]) -> (i32, Vec<u8>, Vec<u8>) {
        let (mut out, mut err) = (Vec::new(), Vec::new());
        let argv: Vec<String> = argv.iter().map(|s| s.to_string()).collect();
        let code = client(
            UnixStream::connect(path).unwrap(),
            &argv,
            input,
            &mut out,
            &mut err,
        )
        .unwrap();
        (code, out, err)
    }

    #[test]
    fn carries_stdio_and_exit_codes() {
        let path = agent();
        let (code, out, err) = run(
            &path,
            &["/bin/sh", "-c", "cat; echo oops >&2; exit 7"],
            b"hello from the host",
        );
        assert_eq!(
            (code, out.as_slice(), err.as_slice()),
            (7, &b"hello from the host"[..], &b"oops\n"[..])
        );
        // Large output arrives whole and before the exit frame.
        let (code, out, _) = run(&path, &["/bin/sh", "-c", "head -c 3000000 /dev/zero"], b"");
        assert_eq!((code, out.len()), (0, 3_000_000));
        let (code, _, err) = run(&path, &["/nonexistent/program"], b"");
        assert_eq!(code, 127);
        assert!(String::from_utf8_lossy(&err).contains("/nonexistent/program"));
        let (code, _, _) = run(&path, &["/bin/sh", "-c", "kill -9 $$"], b"");
        assert_eq!(code, 128 + 9);
    }

    #[test]
    fn host_disconnect_kills_the_process_group() {
        let path = agent();
        let marker =
            std::env::temp_dir().join(format!("fleet-agent-marker-{}", std::process::id()));
        let _ = std::fs::remove_file(&marker);
        let mut stream = UnixStream::connect(&path).unwrap();
        let script = format!("(sleep 2; touch {}) & sleep 30", marker.display());
        let mut line = serde_json::to_vec(&AgentRequest::Spawn {
            argv: vec!["/bin/sh".into(), "-c".into(), script],
        })
        .unwrap();
        line.push(b'\n');
        stream.write_all(&line).unwrap();
        std::thread::sleep(std::time::Duration::from_millis(300));
        drop(stream);
        std::thread::sleep(std::time::Duration::from_secs(3));
        assert!(
            !marker.exists(),
            "a descendant outlived the host's disconnect"
        );
    }
}
