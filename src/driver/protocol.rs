//! The runner's control protocol over `runner.sock`: one JSON request line, one JSON response
//! line, and for `NetworkCard` and `Guest` a descriptor sent after the response with
//! `SCM_RIGHTS`. The client hangs up once it holds the descriptor; the runner keeps its own copy
//! open until then (see `runner::send_with_fd`). The same protocol serves every backend.

use std::{
    io::{Read, Write},
    os::{
        fd::{AsRawFd, FromRawFd, OwnedFd, RawFd},
        unix::net::UnixStream,
    },
    path::{Path, PathBuf},
    time::Duration,
};

use anyhow::{Context, Result, bail};
use serde::{Deserialize, Serialize};

use super::Phase;

const MAX_LINE: usize = 64 << 10;
const TIMEOUT: Duration = Duration::from_secs(30);

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "op", rename_all = "snake_case")]
pub enum Request {
    Status,
    /// Ask the guest to stop. The driver forces it after its own deadline.
    Stop,
    NetworkCard,
    Guest {
        port: u32,
    },
    /// Save to `path`, then stop. The runner exits with `Exit::Saved`.
    Save {
        path: PathBuf,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "result", rename_all = "snake_case")]
pub enum Response {
    Ok,
    /// `fd` responses are followed by one byte carrying the descriptor.
    Fd,
    Status {
        phase: Phase,
        pid: u32,
        generation: u64,
    },
    Error {
        message: String,
    },
}

/// One request/response exchange. A runner that is not listening is an error.
pub fn call(socket: &Path, request: &Request) -> Result<(Response, Option<OwnedFd>)> {
    let mut stream = UnixStream::connect(socket)
        .with_context(|| format!("connecting to {}", socket.display()))?;
    stream.set_read_timeout(Some(TIMEOUT))?;
    stream.set_write_timeout(Some(TIMEOUT))?;
    write_line(&mut stream, request)?;
    let response: Response = read_line(&mut stream)?;
    Ok(match response {
        Response::Fd => (Response::Ok, Some(recv_fd(&stream)?)),
        other => (other, None),
    })
}

pub fn write_line<T: Serialize>(stream: &mut UnixStream, value: &T) -> Result<()> {
    let mut line = serde_json::to_vec(value)?;
    line.push(b'\n');
    stream.write_all(&line)?;
    Ok(())
}

/// Reads exactly one line without buffering past it, so a descriptor sent right after the
/// line is not consumed by an ordinary read (which would discard it).
pub fn read_line<T: serde::de::DeserializeOwned>(stream: &mut UnixStream) -> Result<T> {
    let mut line = Vec::new();
    let mut byte = [0u8];
    loop {
        if stream.read(&mut byte)? == 0 {
            bail!("connection closed before a complete line");
        }
        if byte[0] == b'\n' {
            break;
        }
        line.push(byte[0]);
        if line.len() > MAX_LINE {
            bail!("line exceeds {MAX_LINE} bytes");
        }
    }
    Ok(serde_json::from_slice(&line)?)
}

pub fn send_fd(stream: &UnixStream, fd: RawFd) -> Result<()> {
    let mut byte = *b"F";
    let mut iov = libc::iovec {
        iov_base: byte.as_mut_ptr().cast(),
        iov_len: 1,
    };
    let mut space = vec![0u8; unsafe { libc::CMSG_SPACE(size_of::<RawFd>() as u32) } as usize];
    unsafe {
        let mut msg: libc::msghdr = std::mem::zeroed();
        msg.msg_iov = &mut iov;
        msg.msg_iovlen = 1;
        msg.msg_control = space.as_mut_ptr().cast();
        msg.msg_controllen = space.len() as _;
        let cmsg = libc::CMSG_FIRSTHDR(&msg);
        (*cmsg).cmsg_level = libc::SOL_SOCKET;
        (*cmsg).cmsg_type = libc::SCM_RIGHTS;
        (*cmsg).cmsg_len = libc::CMSG_LEN(size_of::<RawFd>() as u32) as _;
        std::ptr::write_unaligned(libc::CMSG_DATA(cmsg).cast::<RawFd>(), fd);
        if libc::sendmsg(stream.as_raw_fd(), &msg, 0) != 1 {
            bail!("sending descriptor: {}", std::io::Error::last_os_error());
        }
    }
    Ok(())
}

pub fn recv_fd(stream: &UnixStream) -> Result<OwnedFd> {
    let mut byte = [0u8];
    let mut iov = libc::iovec {
        iov_base: byte.as_mut_ptr().cast(),
        iov_len: 1,
    };
    let mut space = vec![0u8; unsafe { libc::CMSG_SPACE(size_of::<RawFd>() as u32) } as usize];
    unsafe {
        let mut msg: libc::msghdr = std::mem::zeroed();
        msg.msg_iov = &mut iov;
        msg.msg_iovlen = 1;
        msg.msg_control = space.as_mut_ptr().cast();
        msg.msg_controllen = space.len() as _;
        if libc::recvmsg(stream.as_raw_fd(), &mut msg, 0) != 1 {
            bail!("receiving descriptor: {}", std::io::Error::last_os_error());
        }
        let cmsg = libc::CMSG_FIRSTHDR(&msg);
        if cmsg.is_null()
            || (*cmsg).cmsg_level != libc::SOL_SOCKET
            || (*cmsg).cmsg_type != libc::SCM_RIGHTS
        {
            bail!("no descriptor in reply");
        }
        let fd = std::ptr::read_unaligned(libc::CMSG_DATA(cmsg).cast::<RawFd>());
        let fd = OwnedFd::from_raw_fd(fd);
        // Received descriptors are not close-on-exec by default.
        libc::fcntl(fd.as_raw_fd(), libc::F_SETFD, libc::FD_CLOEXEC);
        Ok(fd)
    }
}
