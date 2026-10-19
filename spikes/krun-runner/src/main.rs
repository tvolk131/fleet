//! Throwaway libkrun runner spike: the same checks as spikes/vz-runner, minus save/restore, which
//! libkrun does not have. See docs/VM_DRIVERS.md, "Spikes".
//!
//! krun-runner-spike run <dir> --kernel K --initrd I [--cpus N] [--memory-mb M]
//! krun-runner-spike ctl <dir> <command...>
//!
//! `krun_start_enter` takes over the process and never returns, so the control socket and the
//! network card's host end are served by threads started before it. Control commands: status,
//! stop (shutdown eventfd), kill (exit the process), vsock PORT TEXT, netsend TEXT, netstats.

use std::{
    ffi::{CString, c_char},
    io::{BufRead, BufReader, Read, Write},
    os::{
        fd::{FromRawFd, IntoRawFd, OwnedFd, RawFd},
        unix::net::{UnixListener, UnixStream},
    },
    path::{Path, PathBuf},
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};

use anyhow::{Context, Result, bail};

unsafe extern "C" {
    fn krun_init_log(target_fd: i32, level: u32, style: u32, options: u32) -> i32;
    fn krun_create_ctx() -> i32;
    fn krun_set_vm_config(ctx: u32, vcpus: u8, ram_mib: u32) -> i32;
    fn krun_set_kernel(
        ctx: u32,
        kernel: *const c_char,
        format: u32,
        initramfs: *const c_char,
        cmdline: *const c_char,
    ) -> i32;
    fn krun_disable_implicit_init(ctx: u32) -> i32;
    fn krun_disable_implicit_vsock(ctx: u32) -> i32;
    fn krun_add_vsock(ctx: u32, tsi_features: u32) -> i32;
    fn krun_add_vsock_port2(ctx: u32, port: u32, path: *const c_char, listen: bool) -> i32;
    fn krun_add_net_unixgram(
        ctx: u32,
        path: *const c_char,
        fd: i32,
        mac: *const u8,
        features: u32,
        flags: u32,
    ) -> i32;
    fn krun_set_console_output(ctx: u32, path: *const c_char) -> i32;
    fn krun_get_shutdown_eventfd(ctx: u32) -> i32;
    fn krun_start_enter(ctx: u32) -> i32;
}

const KRUN_KERNEL_FORMAT_RAW: u32 = 0;
const GUEST_MAC: [u8; 6] = [0x02, 0xf1, 0xee, 0x70, 0x00, 0x02];

fn main() -> Result<()> {
    let args: Vec<String> = std::env::args().skip(1).collect();
    match args.first().map(String::as_str) {
        Some("run") => run(&args[1..]),
        Some("ctl") => {
            let dir = PathBuf::from(args.get(1).context("ctl <dir> <command>")?);
            let mut stream = UnixStream::connect(dir.join("control.sock"))?;
            writeln!(stream, "{}", args[2..].join(" "))?;
            let mut reply = String::new();
            stream.read_to_string(&mut reply)?;
            print!("{reply}");
            Ok(())
        }
        _ => bail!("usage: run <dir> --kernel K --initrd I [...] | ctl <dir> <command>"),
    }
}

fn check(ret: i32, what: &str) -> Result<i32> {
    if ret < 0 {
        bail!("{what}: {}", std::io::Error::from_raw_os_error(-ret));
    }
    Ok(ret)
}

fn c(s: impl AsRef<str>) -> CString {
    CString::new(s.as_ref()).unwrap()
}

struct Nic {
    host: OwnedFd,
    paused: std::sync::atomic::AtomicBool,
    frames_in: AtomicU64,
    bytes_in: AtomicU64,
    largest_in: AtomicU64,
    frames_out: AtomicU64,
    log: Mutex<std::fs::File>,
}

fn socketpair() -> Result<(OwnedFd, OwnedFd)> {
    let mut fds = [0; 2];
    if unsafe { libc::socketpair(libc::AF_UNIX, libc::SOCK_DGRAM, 0, fds.as_mut_ptr()) } != 0 {
        bail!("socketpair: {}", std::io::Error::last_os_error());
    }
    for fd in fds {
        for (opt, size) in [(libc::SO_SNDBUF, 1 << 20), (libc::SO_RCVBUF, 4 << 20)] {
            let size: libc::c_int = size;
            unsafe {
                libc::setsockopt(fd, libc::SOL_SOCKET, opt, &size as *const _ as *const _, 4)
            };
        }
    }
    Ok(unsafe { (OwnedFd::from_raw_fd(fds[0]), OwnedFd::from_raw_fd(fds[1])) })
}

fn mac(b: &[u8]) -> String {
    b.iter().map(|x| format!("{x:02x}")).collect::<Vec<_>>().join(":")
}

fn nic_reader(nic: Arc<Nic>) {
    use std::os::fd::AsRawFd;
    let mut buf = vec![0u8; 65536];
    loop {
        if nic.paused.load(Ordering::Relaxed) {
            std::thread::sleep(Duration::from_millis(50));
            continue;
        }
        let n = unsafe { libc::recv(nic.host.as_raw_fd(), buf.as_mut_ptr().cast(), buf.len(), 0) };
        if n <= 0 {
            let _ = writeln!(nic.log.lock().unwrap(), "recv ended: {n}");
            return;
        }
        let n = n as usize;
        let count = nic.frames_in.fetch_add(1, Ordering::Relaxed) + 1;
        nic.bytes_in.fetch_add(n as u64, Ordering::Relaxed);
        nic.largest_in.fetch_max(n as u64, Ordering::Relaxed);
        if count <= 50 || count % 1000 == 0 {
            let ethertype = if n >= 14 { u16::from_be_bytes([buf[12], buf[13]]) } else { 0 };
            let payload = if ethertype == 0x88b5 {
                String::from_utf8_lossy(&buf[14..n]).into_owned()
            } else {
                String::new()
            };
            let _ = writeln!(
                nic.log.lock().unwrap(),
                "frame #{count} len={n} src={} type={ethertype:#06x} {payload}",
                mac(&buf[6..12.min(n)])
            );
        }
    }
}

fn run(args: &[String]) -> Result<()> {
    let mut it = args.iter();
    let dir = PathBuf::from(it.next().context("run <dir>")?);
    let (mut kernel, mut initrd, mut cpus, mut memory) = (String::new(), String::new(), 2u8, 512u32);
    while let Some(flag) = it.next() {
        let mut value = || it.next().context("missing flag value");
        match flag.as_str() {
            "--kernel" => kernel = value()?.clone(),
            "--initrd" => initrd = value()?.clone(),
            "--cpus" => cpus = value()?.parse()?,
            "--memory-mb" => memory = value()?.parse()?,
            other => bail!("unknown flag {other}"),
        }
    }
    std::fs::create_dir_all(&dir)?;
    let dir = dir.canonicalize()?;
    let started = Instant::now();

    let (guest_end, host_end) = socketpair()?;
    let nic = Arc::new(Nic {
        host: host_end,
        paused: std::sync::atomic::AtomicBool::new(false),
        frames_in: AtomicU64::new(0),
        bytes_in: AtomicU64::new(0),
        largest_in: AtomicU64::new(0),
        frames_out: AtomicU64::new(0),
        log: Mutex::new(
            std::fs::OpenOptions::new().create(true).append(true).open(dir.join("net.log"))?,
        ),
    });
    std::thread::spawn({
        let nic = nic.clone();
        move || nic_reader(nic)
    });

    let ctx = unsafe {
        krun_init_log(2, 2, 0, 0);
        check(krun_create_ctx(), "create_ctx")? as u32
    };
    let shutdown = unsafe {
        check(krun_set_vm_config(ctx, cpus, memory), "set_vm_config")?;
        check(krun_disable_implicit_init(ctx), "disable_implicit_init")?;
        check(
            krun_set_kernel(
                ctx,
                c(&kernel).as_ptr(),
                KRUN_KERNEL_FORMAT_RAW,
                c(&initrd).as_ptr(),
                c("console=hvc0 rdinit=/init panic=-1").as_ptr(),
            ),
            "set_kernel",
        )?;
        check(krun_set_console_output(ctx, c(dir.join("console.log").to_string_lossy()).as_ptr()), "console")?;
        // An explicit vsock device with no TSI: Fleet owns networking, not libkrun's socket hijacking.
        check(krun_disable_implicit_vsock(ctx), "disable_implicit_vsock")?;
        check(krun_add_vsock(ctx, 0), "add_vsock")?;
        let port_path = dir.join("vsock-1234.sock");
        let _ = std::fs::remove_file(&port_path);
        check(krun_add_vsock_port2(ctx, 1234, c(port_path.to_string_lossy()).as_ptr(), true), "vsock port")?;
        check(
            krun_add_net_unixgram(ctx, std::ptr::null(), guest_end.into_raw_fd(), GUEST_MAC.as_ptr(), 0, 0),
            "add_net_unixgram",
        )?;
        check(krun_get_shutdown_eventfd(ctx), "shutdown_eventfd")?
    };

    let socket = dir.join("control.sock");
    let _ = std::fs::remove_file(&socket);
    let listener = UnixListener::bind(&socket)?;
    std::thread::spawn({
        let nic = nic.clone();
        let dir = dir.clone();
        move || {
            for stream in listener.incoming().flatten() {
                let reply = handle(&dir, &nic, shutdown, started, &stream)
                    .unwrap_or_else(|e| format!("error: {e:#}"));
                let mut stream = stream;
                let _ = writeln!(stream, "{reply}");
            }
        }
    });

    eprintln!("entering VM after {:?}, pid {}", started.elapsed(), std::process::id());
    // Only returns on a configuration error; otherwise libkrun calls exit() when the guest stops.
    let ret = unsafe { krun_start_enter(ctx) };
    bail!("krun_start_enter returned {ret}")
}

fn handle(dir: &Path, nic: &Nic, shutdown: RawFd, started: Instant, stream: &UnixStream) -> Result<String> {
    use std::os::fd::AsRawFd;
    let mut line = String::new();
    BufReader::new(stream).read_line(&mut line)?;
    let mut words = line.trim().splitn(3, ' ');
    let t = Instant::now();
    let result = match words.next().unwrap_or("") {
        "status" => format!("running for {:?}", started.elapsed()),
        "stop" => {
            let one: u64 = 1;
            let n = unsafe { libc::write(shutdown, &one as *const _ as *const _, 8) };
            format!("shutdown eventfd written ({n})")
        }
        "kill" => {
            let _ = writeln!(&*stream, "exiting");
            std::process::exit(137)
        }
        "vsock" => {
            let port: u32 = words.next().context("vsock PORT TEXT")?.parse()?;
            let mut s = UnixStream::connect(dir.join(format!("vsock-{port}.sock")))?;
            s.set_read_timeout(Some(Duration::from_secs(5)))?;
            s.write_all(words.collect::<Vec<_>>().join(" ").as_bytes())?;
            let mut buf = [0u8; 4096];
            let n = s.read(&mut buf)?;
            String::from_utf8_lossy(&buf[..n]).into_owned()
        }
        "netsend" => {
            let mut frame = GUEST_MAC.to_vec();
            frame.extend_from_slice(&[0x02, 0xf1, 0xee, 0x70, 0x00, 0xfe]);
            frame.extend_from_slice(&0x88b5u16.to_be_bytes());
            frame.extend_from_slice(words.collect::<Vec<_>>().join(" ").as_bytes());
            let n = unsafe { libc::send(nic.host.as_raw_fd(), frame.as_ptr().cast(), frame.len(), 0) };
            if n < 0 {
                bail!("send: {}", std::io::Error::last_os_error());
            }
            nic.frames_out.fetch_add(1, Ordering::Relaxed);
            "sent".into()
        }
        "netpause" | "netresume" => {
            nic.paused.store(line.trim() == "netpause", Ordering::Relaxed);
            line.trim().into()
        }
        "netstats" => format!(
            "in: {} frames, {} bytes, largest {}; out: {} frames",
            nic.frames_in.load(Ordering::Relaxed),
            nic.bytes_in.load(Ordering::Relaxed),
            nic.largest_in.load(Ordering::Relaxed),
            nic.frames_out.load(Ordering::Relaxed)
        ),
        other => bail!("unknown command {other:?}"),
    };
    Ok(format!("{result}  ({:?})", t.elapsed()))
}
