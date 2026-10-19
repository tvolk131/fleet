//! PID 1 for the vz spike guest. No shell, no userland: it exercises exactly what the spike checks.
//!
//! - prints a heartbeat with a monotonic counter to the console (continuity across save/restore);
//! - brings eth0 up and sends a 0x88b5 frame every 2s, logging frames the host sends back;
//! - answers on vsock port 1234: "poweroff" powers the guest off, anything else is echoed.

use std::{
    io::{Read, Write},
    os::fd::{FromRawFd, OwnedFd, AsRawFd},
    sync::atomic::{AtomicU64, Ordering},
    time::Duration,
};

static TICKS: AtomicU64 = AtomicU64::new(0);

fn log(msg: impl AsRef<str>) {
    let mut out = std::io::stdout().lock();
    let _ = writeln!(out, "[spike-init tick={}] {}", TICKS.load(Ordering::Relaxed), msg.as_ref());
}

fn check(ret: libc::c_int, what: &str) -> std::io::Result<libc::c_int> {
    if ret < 0 {
        let e = std::io::Error::last_os_error();
        log(format!("{what}: {e}"));
        Err(e)
    } else {
        Ok(ret)
    }
}

fn mount(source: &str, target: &str, fstype: &str) {
    let c = |s: &str| std::ffi::CString::new(s).unwrap();
    let (s, t, f) = (c(source), c(target), c(fstype));
    let _ = check(
        unsafe { libc::mount(s.as_ptr(), t.as_ptr(), f.as_ptr(), 0, std::ptr::null()) },
        &format!("mount {target}"),
    );
}

fn uptime() -> String {
    std::fs::read_to_string("/proc/uptime")
        .ok()
        .and_then(|s| s.split_whitespace().next().map(str::to_owned))
        .unwrap_or_default()
}

fn ifreq(name: &str) -> libc::ifreq {
    let mut req: libc::ifreq = unsafe { std::mem::zeroed() };
    for (dst, src) in req.ifr_name.iter_mut().zip(name.bytes()) {
        *dst = src as libc::c_char;
    }
    req
}

fn network() -> std::io::Result<()> {
    let ctl = unsafe { OwnedFd::from_raw_fd(check(libc::socket(libc::AF_INET, libc::SOCK_DGRAM, 0), "socket")?) };
    let mut req = ifreq("eth0");
    check(unsafe { libc::ioctl(ctl.as_raw_fd(), libc::SIOCGIFFLAGS as libc::Ioctl, &mut req) }, "SIOCGIFFLAGS")?;
    unsafe { req.ifr_ifru.ifru_flags |= libc::IFF_UP as libc::c_short };
    check(unsafe { libc::ioctl(ctl.as_raw_fd(), libc::SIOCSIFFLAGS as libc::Ioctl, &req) }, "SIOCSIFFLAGS")?;
    let mut hw = ifreq("eth0");
    check(unsafe { libc::ioctl(ctl.as_raw_fd(), libc::SIOCGIFHWADDR as libc::Ioctl, &mut hw) }, "SIOCGIFHWADDR")?;
    let mac: Vec<u8> = unsafe { hw.ifr_ifru.ifru_hwaddr.sa_data[..6].iter().map(|b| *b as u8).collect() };
    let mut mtu = ifreq("eth0");
    check(unsafe { libc::ioctl(ctl.as_raw_fd(), libc::SIOCGIFMTU as libc::Ioctl, &mut mtu) }, "SIOCGIFMTU")?;
    let mut index = ifreq("eth0");
    check(unsafe { libc::ioctl(ctl.as_raw_fd(), libc::SIOCGIFINDEX as libc::Ioctl, &mut index) }, "SIOCGIFINDEX")?;
    let ifindex = unsafe { index.ifr_ifru.ifru_ifindex };
    log(format!(
        "eth0 up: mac {} mtu {} ifindex {ifindex}",
        mac.iter().map(|b| format!("{b:02x}")).collect::<Vec<_>>().join(":"),
        unsafe { mtu.ifr_ifru.ifru_mtu }
    ));

    let proto = (0x88b5u16).to_be();
    let raw = unsafe { OwnedFd::from_raw_fd(check(libc::socket(libc::AF_PACKET, libc::SOCK_RAW, proto as i32), "AF_PACKET")?) };
    let mut addr: libc::sockaddr_ll = unsafe { std::mem::zeroed() };
    addr.sll_family = libc::AF_PACKET as u16;
    addr.sll_protocol = proto;
    addr.sll_ifindex = ifindex;
    check(
        unsafe { libc::bind(raw.as_raw_fd(), &addr as *const _ as *const _, size_of::<libc::sockaddr_ll>() as u32) },
        "bind AF_PACKET",
    )?;

    let rx = raw.as_raw_fd();
    std::thread::spawn(move || {
        let mut buf = [0u8; 65536];
        loop {
            let n = unsafe { libc::recv(rx, buf.as_mut_ptr().cast(), buf.len(), 0) };
            if n <= 0 {
                log("frame recv failed");
                return;
            }
            let n = n as usize;
            log(format!("frame from host len={n}: {}", String::from_utf8_lossy(&buf[14.min(n)..n])));
        }
    });

    let mut n = 0u64;
    loop {
        n += 1;
        let mut frame = vec![0xff; 6];
        frame.extend_from_slice(&mac);
        frame.extend_from_slice(&0x88b5u16.to_be_bytes());
        frame.extend_from_slice(format!("guest frame {n} uptime {}", uptime()).as_bytes());
        unsafe { libc::send(raw.as_raw_fd(), frame.as_ptr().cast(), frame.len(), 0) };
        std::thread::sleep(Duration::from_secs(2));
    }
}

fn vsock() -> std::io::Result<()> {
    let fd = check(unsafe { libc::socket(libc::AF_VSOCK, libc::SOCK_STREAM, 0) }, "AF_VSOCK")?;
    let listener = unsafe { OwnedFd::from_raw_fd(fd) };
    let mut addr: libc::sockaddr_vm = unsafe { std::mem::zeroed() };
    addr.svm_family = libc::AF_VSOCK as u16;
    addr.svm_port = 1234;
    addr.svm_cid = libc::VMADDR_CID_ANY;
    check(
        unsafe { libc::bind(fd, &addr as *const _ as *const _, size_of::<libc::sockaddr_vm>() as u32) },
        "bind vsock",
    )?;
    check(unsafe { libc::listen(fd, 16) }, "listen vsock")?;
    log("vsock listening on port 1234");
    loop {
        let conn = check(unsafe { libc::accept(listener.as_raw_fd(), std::ptr::null_mut(), std::ptr::null_mut()) }, "accept")?;
        let mut stream = unsafe { std::fs::File::from_raw_fd(conn) };
        let mut buf = [0u8; 4096];
        let n = stream.read(&mut buf).unwrap_or(0);
        let msg = String::from_utf8_lossy(&buf[..n]).trim().to_owned();
        log(format!("vsock request: {msg:?}"));
        if msg == "poweroff" {
            let _ = stream.write_all(b"powering off");
            let _ = stream.flush();
            unsafe {
                libc::sync();
                libc::reboot(libc::RB_POWER_OFF);
            }
        }
        let _ = stream.write_all(
            format!("guest-echo tick={} uptime={}: {msg}", TICKS.load(Ordering::Relaxed), uptime()).as_bytes(),
        );
    }
}

fn main() {
    mount("devtmpfs", "/dev", "devtmpfs");
    mount("proc", "/proc", "proc");
    mount("sysfs", "/sys", "sysfs");
    log(format!("up, kernel {}", std::fs::read_to_string("/proc/version").unwrap_or_default().trim()));
    std::thread::spawn(|| {
        if let Err(e) = network() {
            log(format!("network failed: {e}"));
        }
    });
    std::thread::spawn(|| {
        if let Err(e) = vsock() {
            log(format!("vsock failed: {e}"));
        }
    });
    loop {
        std::thread::sleep(Duration::from_secs(1));
        let t = TICKS.fetch_add(1, Ordering::Relaxed) + 1;
        if t % 5 == 0 {
            log(format!("heartbeat uptime {}", uptime()));
        }
    }
}
