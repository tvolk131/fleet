//! PID 1 of the Linux test guest for the driver conformance suite (`src/driver/conformance.rs`):
//!
//! - vsock port 1234 serves one request per connection: `poweroff` powers off, anything else is
//!   answered `echo#N: <request>`, where N counts answers since boot (kept across save/restore,
//!   because it lives in guest memory);
//! - eth0 carries a 0x88b5 frame every 250 ms, and each 0x88b5 frame received is answered with
//!   `echo: ` followed by its payload;
//! - vsock port 1024 is the agent port: `{"kind":"shutdown"}` powers off, unless the kernel
//!   command line contains `fleet_test.ignore_stop`. With `fleet_test.agent` the real
//!   `fleet_guest::agent` serves it instead, and `/init cat` and `/init exit N` are commands for it
//!   to run.

use std::{
    io::{Read, Write},
    os::fd::{AsRawFd, FromRawFd, OwnedFd},
    sync::atomic::{AtomicU64, Ordering},
    time::Duration,
};

const ECHO_PORT: u32 = 1234;
const AGENT_PORT: u32 = 1024;
const ETHERTYPE: u16 = 0x88b5;

static ANSWERS: AtomicU64 = AtomicU64::new(0);

fn log(message: impl AsRef<str>) {
    let _ = writeln!(std::io::stdout().lock(), "[test-guest] {}", message.as_ref());
}

fn check(ret: libc::c_int, what: &str) -> std::io::Result<libc::c_int> {
    if ret < 0 {
        let error = std::io::Error::last_os_error();
        log(format!("{what}: {error}"));
        return Err(error);
    }
    Ok(ret)
}

fn mount(source: &str, target: &str, fstype: &str) {
    let c = |s: &str| std::ffi::CString::new(s).unwrap();
    let (s, t, f) = (c(source), c(target), c(fstype));
    let _ = check(
        unsafe { libc::mount(s.as_ptr(), t.as_ptr(), f.as_ptr(), 0, std::ptr::null()) },
        &format!("mount {target}"),
    );
}

fn power_off() -> ! {
    unsafe {
        libc::sync();
        libc::reboot(libc::RB_POWER_OFF);
    }
    unreachable!("reboot(RB_POWER_OFF) returned");
}

fn ifreq(name: &str) -> libc::ifreq {
    let mut req: libc::ifreq = unsafe { std::mem::zeroed() };
    for (dst, src) in req.ifr_name.iter_mut().zip(name.bytes()) {
        *dst = src as libc::c_char;
    }
    req
}

fn network() -> std::io::Result<()> {
    let ctl = unsafe {
        OwnedFd::from_raw_fd(check(
            libc::socket(libc::AF_INET, libc::SOCK_DGRAM, 0),
            "socket",
        )?)
    };
    let fd = ctl.as_raw_fd();
    let mut req = ifreq("eth0");
    check(
        unsafe { libc::ioctl(fd, libc::SIOCGIFFLAGS as libc::Ioctl, &mut req) },
        "SIOCGIFFLAGS",
    )?;
    unsafe { req.ifr_ifru.ifru_flags |= libc::IFF_UP as libc::c_short };
    check(
        unsafe { libc::ioctl(fd, libc::SIOCSIFFLAGS as libc::Ioctl, &req) },
        "SIOCSIFFLAGS",
    )?;
    let mut hw = ifreq("eth0");
    check(
        unsafe { libc::ioctl(fd, libc::SIOCGIFHWADDR as libc::Ioctl, &mut hw) },
        "SIOCGIFHWADDR",
    )?;
    let mut mac = [0u8; 6];
    for (dst, src) in mac.iter_mut().zip(unsafe { hw.ifr_ifru.ifru_hwaddr.sa_data }) {
        *dst = src as u8;
    }
    let mut index = ifreq("eth0");
    check(
        unsafe { libc::ioctl(fd, libc::SIOCGIFINDEX as libc::Ioctl, &mut index) },
        "SIOCGIFINDEX",
    )?;
    let ifindex = unsafe { index.ifr_ifru.ifru_ifindex };

    let protocol = ETHERTYPE.to_be();
    let raw = unsafe {
        OwnedFd::from_raw_fd(check(
            libc::socket(libc::AF_PACKET, libc::SOCK_RAW, protocol as i32),
            "AF_PACKET",
        )?)
    };
    let mut addr: libc::sockaddr_ll = unsafe { std::mem::zeroed() };
    addr.sll_family = libc::AF_PACKET as u16;
    addr.sll_protocol = protocol;
    addr.sll_ifindex = ifindex;
    check(
        unsafe {
            libc::bind(
                raw.as_raw_fd(),
                (&addr as *const libc::sockaddr_ll).cast(),
                size_of::<libc::sockaddr_ll>() as u32,
            )
        },
        "bind AF_PACKET",
    )?;
    log("eth0 up");

    let raw = std::sync::Arc::new(raw);
    let sender = raw.clone();
    std::thread::spawn(move || {
        for n in 1u64.. {
            send(&sender, &[0xff; 6], &mac, format!("guest frame {n}").as_bytes());
            std::thread::sleep(Duration::from_millis(250));
        }
    });
    let mut buf = vec![0u8; 65536];
    loop {
        let mut from: libc::sockaddr_ll = unsafe { std::mem::zeroed() };
        let mut len = size_of::<libc::sockaddr_ll>() as libc::socklen_t;
        let n = unsafe {
            libc::recvfrom(
                raw.as_raw_fd(),
                buf.as_mut_ptr().cast(),
                buf.len(),
                0,
                (&mut from as *mut libc::sockaddr_ll).cast(),
                &mut len,
            )
        };
        if n < 0 {
            return Err(std::io::Error::last_os_error());
        }
        // Packet sockets also see this guest's own transmissions.
        if from.sll_pkttype == libc::PACKET_OUTGOING || n < 14 {
            continue;
        }
        let n = n as usize;
        let mut payload = b"echo: ".to_vec();
        payload.extend_from_slice(&buf[14..n]);
        let to: [u8; 6] = buf[6..12].try_into().unwrap();
        send(&raw, &to, &mac, &payload);
    }
}

fn send(raw: &OwnedFd, to: &[u8; 6], from: &[u8; 6], payload: &[u8]) {
    let mut frame = Vec::with_capacity(14 + payload.len());
    frame.extend_from_slice(to);
    frame.extend_from_slice(from);
    frame.extend_from_slice(&ETHERTYPE.to_be_bytes());
    frame.extend_from_slice(payload);
    unsafe { libc::send(raw.as_raw_fd(), frame.as_ptr().cast(), frame.len(), 0) };
}

fn listen(port: u32) -> std::io::Result<OwnedFd> {
    let listener = unsafe {
        OwnedFd::from_raw_fd(check(
            libc::socket(libc::AF_VSOCK, libc::SOCK_STREAM, 0),
            "AF_VSOCK",
        )?)
    };
    let mut addr: libc::sockaddr_vm = unsafe { std::mem::zeroed() };
    addr.svm_family = libc::AF_VSOCK as u16;
    addr.svm_port = port;
    addr.svm_cid = libc::VMADDR_CID_ANY;
    check(
        unsafe {
            libc::bind(
                listener.as_raw_fd(),
                (&addr as *const libc::sockaddr_vm).cast(),
                size_of::<libc::sockaddr_vm>() as u32,
            )
        },
        "bind vsock",
    )?;
    check(unsafe { libc::listen(listener.as_raw_fd(), 16) }, "listen")?;
    Ok(listener)
}

/// Accepts connections on `port` and passes each one's first read to `serve`.
fn serve(port: u32, serve: fn(&str, &mut std::fs::File)) -> std::io::Result<()> {
    let listener = listen(port)?;
    log(format!("vsock port {port} listening"));
    loop {
        let fd = unsafe {
            libc::accept(
                listener.as_raw_fd(),
                std::ptr::null_mut(),
                std::ptr::null_mut(),
            )
        };
        if fd < 0 {
            continue;
        }
        std::thread::spawn(move || {
            let mut stream = unsafe { std::fs::File::from_raw_fd(fd) };
            let mut buf = [0u8; 4096];
            let n = stream.read(&mut buf).unwrap_or(0);
            serve(String::from_utf8_lossy(&buf[..n]).trim(), &mut stream);
        });
    }
}

fn echo(request: &str, stream: &mut std::fs::File) {
    if request == "poweroff" {
        let _ = stream.write_all(b"powering off");
        power_off();
    }
    let n = ANSWERS.fetch_add(1, Ordering::SeqCst) + 1;
    let _ = stream.write_all(format!("echo#{n}: {request}").as_bytes());
}

fn agent(request: &str, _: &mut std::fs::File) {
    if request != r#"{"kind":"shutdown"}"# {
        log(format!("agent: unknown request {request:?}"));
        return;
    }
    let cmdline = std::fs::read_to_string("/proc/cmdline").unwrap_or_default();
    if cmdline.split_whitespace().any(|w| w == "fleet_test.ignore_stop") {
        log("agent: ignoring shutdown request");
        return;
    }
    log("agent: shutting down");
    power_off();
}

fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    match args.first().map(String::as_str) {
        Some("cat") => {
            let _ = std::io::copy(&mut std::io::stdin(), &mut std::io::stdout());
            std::process::exit(0);
        }
        Some("exit") => std::process::exit(args.get(1).and_then(|c| c.parse().ok()).unwrap_or(1)),
        _ => {}
    }
    mount("devtmpfs", "/dev", "devtmpfs");
    mount("proc", "/proc", "proc");
    mount("sysfs", "/sys", "sysfs");
    log("up");
    std::thread::spawn(|| {
        if let Err(error) = network() {
            log(format!("network failed: {error}"));
        }
    });
    let cmdline = std::fs::read_to_string("/proc/cmdline").unwrap_or_default();
    if cmdline.split_whitespace().any(|w| w == "fleet_test.agent") {
        std::thread::spawn(|| {
            let served = fleet_guest::agent::listen_vsock(AGENT_PORT).and_then(fleet_guest::agent::serve);
            log(format!("fleet-guest agent ended: {served:?}"));
        });
    } else {
        std::thread::spawn(|| serve(AGENT_PORT, agent));
    }
    if let Err(error) = serve(ECHO_PORT, echo) {
        log(format!("echo port failed: {error}"));
    }
    loop {
        std::thread::sleep(Duration::from_secs(3600));
    }
}
