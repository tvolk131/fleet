//! Throwaway Virtualization.framework runner spike. See docs/VM_DRIVERS.md, "Spikes".
//!
//! vz-runner-spike run <dir> --kernel K --initrd I [--cpus N] [--memory-mb M] [--mtu N] [--restore]
//! vz-runner-spike ctl <dir> <command...>
//!
//! One process owns one VM. `<dir>` holds control.sock, console.log, net.log, machine-id and
//! state.vzvmsave. Control commands: status, stop, kill, pause, resume, save, vsock PORT TEXT,
//! netsend TEXT, netstats.

use std::{
    io::{BufRead, BufReader, Read, Write},
    os::{
        fd::{AsRawFd, FromRawFd, OwnedFd, RawFd},
        unix::net::{UnixListener, UnixStream},
    },
    path::{Path, PathBuf},
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
        mpsc,
    },
    time::{Duration, Instant},
};

use anyhow::{Context, Result, bail};
use block2::RcBlock;
use dispatch2::{DispatchQueue, DispatchRetained};
use objc2::{AllocAnyThread, rc::Retained};
use objc2_foundation::{NSArray, NSData, NSError, NSFileHandle, NSString, NSURL};
use objc2_virtualization::*;

fn main() -> Result<()> {
    let args: Vec<String> = std::env::args().skip(1).collect();
    match args.first().map(String::as_str) {
        Some("run") => run(&args[1..]),
        Some("grab") => {
            let dir = PathBuf::from(args.get(1).context("grab <dir> <seconds>")?);
            let seconds: u64 = args.get(2).map_or(Ok(3), |s| s.parse())?;
            grab(&dir, seconds)
        }
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

fn ns(s: &str) -> Retained<NSString> {
    NSString::from_str(s)
}

fn url(path: &Path) -> Retained<NSURL> {
    NSURL::fileURLWithPath(&ns(&path.to_string_lossy()))
}

fn describe(err: *mut NSError) -> String {
    if err.is_null() {
        return "unknown error".into();
    }
    let err = unsafe { &*err };
    format!(
        "{} (domain {}, code {})",
        err.localizedDescription(),
        err.domain(),
        err.code()
    )
}

/// Virtualization objects are not Send; every use happens on the VM's serial queue.
struct OnQueue<T>(T);
unsafe impl<T> Send for OnQueue<T> {}
unsafe impl<T> Sync for OnQueue<T> {}

struct Vm {
    queue: DispatchRetained<DispatchQueue>,
    vm: OnQueue<Retained<VZVirtualMachine>>,
}

impl Vm {
    fn sync<R: Send>(&self, f: impl FnOnce(&VZVirtualMachine) -> R + Send) -> R {
        let mut out = None;
        let vm = OnQueue(&*self.vm.0);
        let slot = OnQueue(&mut out);
        self.queue.exec_sync(move || {
            let (vm, slot) = (vm, slot);
            *slot.0 = Some(f(vm.0));
        });
        out.unwrap()
    }

    /// Runs an async VZ call with an error-only completion handler and waits for it.
    fn call(
        &self,
        what: &str,
        f: impl FnOnce(&VZVirtualMachine, &block2::DynBlock<dyn Fn(*mut NSError)>) + Send,
    ) -> Result<()> {
        let (tx, rx) = mpsc::channel::<Result<(), String>>();
        let block = RcBlock::new(move |err: *mut NSError| {
            let _ = tx.send(if err.is_null() { Ok(()) } else { Err(describe(err)) });
        });
        let block = OnQueue(block);
        self.sync(move |vm| {
            let block = block;
            f(vm, &block.0)
        });
        match rx.recv_timeout(Duration::from_secs(120)) {
            Ok(Ok(())) => Ok(()),
            Ok(Err(e)) => bail!("{what}: {e}"),
            Err(_) => bail!("{what}: no completion within 120s"),
        }
    }

    fn state(&self) -> &'static str {
        let state = self.sync(|vm| unsafe { vm.state() });
        match state {
            VZVirtualMachineState::Stopped => "stopped",
            VZVirtualMachineState::Running => "running",
            VZVirtualMachineState::Paused => "paused",
            VZVirtualMachineState::Error => "error",
            VZVirtualMachineState::Starting => "starting",
            VZVirtualMachineState::Pausing => "pausing",
            VZVirtualMachineState::Resuming => "resuming",
            VZVirtualMachineState::Stopping => "stopping",
            VZVirtualMachineState::Saving => "saving",
            VZVirtualMachineState::Restoring => "restoring",
            _ => "unknown",
        }
    }

    fn vsock(&self, port: u32, message: &str) -> Result<String> {
        let (tx, rx) = mpsc::channel::<Result<RawFd, String>>();
        let block = RcBlock::new(move |conn: *mut VZVirtioSocketConnection, err: *mut NSError| {
            let _ = tx.send(if conn.is_null() {
                Err(describe(err))
            } else {
                // The connection closes its descriptor when released; keep our own copy.
                Ok(unsafe { libc::dup((*conn).fileDescriptor()) })
            });
        });
        let block = OnQueue(block);
        self.sync(move |vm| {
            let block = block;
            let devices = unsafe { vm.socketDevices() };
            let device = devices.objectAtIndex(0);
            let device = device
                .downcast::<VZVirtioSocketDevice>()
                .expect("virtio socket device");
            unsafe { device.connectToPort_completionHandler(port, &block.0) };
        });
        let fd = match rx.recv_timeout(Duration::from_secs(10)) {
            Ok(Ok(fd)) if fd >= 0 => fd,
            Ok(Ok(_)) => bail!("dup failed"),
            Ok(Err(e)) => bail!("vsock connect: {e}"),
            Err(_) => bail!("vsock connect: timed out"),
        };
        let mut stream = unsafe { UnixStream::from_raw_fd(fd) };
        stream.set_read_timeout(Some(Duration::from_secs(5)))?;
        stream.write_all(message.as_bytes())?;
        let mut buf = [0u8; 4096];
        let n = stream.read(&mut buf)?;
        Ok(String::from_utf8_lossy(&buf[..n]).into_owned())
    }
}

struct Options {
    dir: PathBuf,
    kernel: PathBuf,
    initrd: PathBuf,
    cpus: usize,
    memory_mb: u64,
    mtu: Option<isize>,
    restore: bool,
}

fn parse(args: &[String]) -> Result<Options> {
    let mut it = args.iter();
    let dir = PathBuf::from(it.next().context("run <dir>")?);
    let mut o = Options {
        dir,
        kernel: PathBuf::new(),
        initrd: PathBuf::new(),
        cpus: 2,
        memory_mb: 512,
        mtu: None,
        restore: false,
    };
    while let Some(flag) = it.next() {
        let mut value = || it.next().context("missing flag value");
        match flag.as_str() {
            "--kernel" => o.kernel = value()?.into(),
            "--initrd" => o.initrd = value()?.into(),
            "--cpus" => o.cpus = value()?.parse()?,
            "--memory-mb" => o.memory_mb = value()?.parse()?,
            "--mtu" => o.mtu = Some(value()?.parse()?),
            "--restore" => o.restore = true,
            other => bail!("unknown flag {other}"),
        }
    }
    Ok(o)
}

/// Host end of the guest's only network card: one datagram is one Ethernet frame.
struct Nic {
    /// The runner keeps the host end for the VM's lifetime; a worker borrows a duplicate.
    host: std::sync::atomic::AtomicI32,
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
    for (fd, snd, rcv) in [(fds[0], 1 << 20, 4 << 20), (fds[1], 1 << 20, 4 << 20)] {
        for (opt, size) in [(libc::SO_SNDBUF, snd), (libc::SO_RCVBUF, rcv)] {
            let size: libc::c_int = size;
            unsafe {
                libc::setsockopt(
                    fd,
                    libc::SOL_SOCKET,
                    opt,
                    &size as *const _ as *const _,
                    size_of::<libc::c_int>() as _,
                )
            };
        }
    }
    Ok(unsafe { (OwnedFd::from_raw_fd(fds[0]), OwnedFd::from_raw_fd(fds[1])) })
}

fn nic_reader(nic: Arc<Nic>) {
    let mut buf = vec![0u8; 65536];
    loop {
        let fd = nic.host.load(Ordering::Relaxed);
        if fd < 0 {
            let _ = writeln!(nic.log.lock().unwrap(), "host end closed; reader exits");
            return;
        }
        if nic.paused.load(Ordering::Relaxed) {
            std::thread::sleep(Duration::from_millis(50));
            continue;
        }
        let mut pfd = libc::pollfd { fd, events: libc::POLLIN, revents: 0 };
        if unsafe { libc::poll(&mut pfd, 1, 100) } <= 0 {
            continue;
        }
        let n = unsafe { libc::recv(fd, buf.as_mut_ptr().cast(), buf.len(), libc::MSG_DONTWAIT) };
        if n <= 0 {
            let _ = writeln!(
                nic.log.lock().unwrap(),
                "recv ended: {n} {}",
                std::io::Error::last_os_error()
            );
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
                mac(&buf[6..12])
            );
        }
    }
}

fn mac(b: &[u8]) -> String {
    b.iter().map(|x| format!("{x:02x}")).collect::<Vec<_>>().join(":")
}

const GUEST_MAC: [u8; 6] = [0x02, 0xf1, 0xee, 0x70, 0x00, 0x01];

fn nic_send(nic: &Nic, text: &str) -> Result<()> {
    let mut frame = Vec::new();
    frame.extend_from_slice(&GUEST_MAC);
    frame.extend_from_slice(&[0x02, 0xf1, 0xee, 0x70, 0x00, 0xfe]);
    frame.extend_from_slice(&0x88b5u16.to_be_bytes());
    frame.extend_from_slice(text.as_bytes());
    let n = unsafe { libc::send(nic.host.load(Ordering::Relaxed), frame.as_ptr().cast(), frame.len(), 0) };
    if n < 0 {
        bail!("send: {}", std::io::Error::last_os_error());
    }
    nic.frames_out.fetch_add(1, Ordering::Relaxed);
    Ok(())
}

fn configure(o: &Options, guest_nic: OwnedFd) -> Result<Retained<VZVirtualMachineConfiguration>> {
    use std::os::fd::IntoRawFd;
    unsafe {
        let config = VZVirtualMachineConfiguration::new();

        let boot = VZLinuxBootLoader::initWithKernelURL(VZLinuxBootLoader::alloc(), &url(&o.kernel));
        boot.setInitialRamdiskURL(Some(&url(&o.initrd)));
        boot.setCommandLine(&ns("console=hvc0 rdinit=/init panic=-1"));
        config.setBootLoader(Some(&boot));

        // Save/restore requires a platform with a stable machine identifier.
        let id_path = o.dir.join("machine-id");
        let identifier = match std::fs::read(&id_path) {
            Ok(bytes) => VZGenericMachineIdentifier::initWithDataRepresentation(
                VZGenericMachineIdentifier::alloc(),
                &NSData::with_bytes(&bytes),
            )
            .context("stored machine identifier is invalid")?,
            Err(_) => {
                let id = VZGenericMachineIdentifier::new();
                std::fs::write(&id_path, id.dataRepresentation().to_vec())?;
                id
            }
        };
        let platform = VZGenericPlatformConfiguration::new();
        platform.setMachineIdentifier(&identifier);
        config.setPlatform(&platform);

        config.setCPUCount(o.cpus);
        config.setMemorySize(o.memory_mb << 20);

        let console = VZFileSerialPortAttachment::initWithURL_append_error(
            VZFileSerialPortAttachment::alloc(),
            &url(&o.dir.join("console.log")),
            true,
        )
        .map_err(|e| anyhow::anyhow!("console: {}", describe(Retained::as_ptr(&e) as *mut _)))?;
        let serial = VZVirtioConsoleDeviceSerialPortConfiguration::new();
        serial.setAttachment(Some(&console));
        config.setSerialPorts(&NSArray::from_retained_slice(&[Retained::into_super(serial)]));

        let handle = NSFileHandle::initWithFileDescriptor_closeOnDealloc(
            NSFileHandle::alloc(),
            guest_nic.into_raw_fd(),
            true,
        );
        let attachment = VZFileHandleNetworkDeviceAttachment::initWithFileHandle(
            VZFileHandleNetworkDeviceAttachment::alloc(),
            &handle,
        );
        if let Some(mtu) = o.mtu {
            attachment.setMaximumTransmissionUnit(mtu);
        }
        eprintln!("network card MTU: {}", attachment.maximumTransmissionUnit());
        let net = VZVirtioNetworkDeviceConfiguration::new();
        net.setAttachment(Some(&attachment));
        let mac_address = VZMACAddress::initWithString(VZMACAddress::alloc(), &ns(&mac(&GUEST_MAC)))
            .context("MAC address")?;
        net.setMACAddress(&mac_address);
        config.setNetworkDevices(&NSArray::from_retained_slice(&[Retained::into_super(net)]));

        config.setSocketDevices(&NSArray::from_retained_slice(&[Retained::into_super(
            VZVirtioSocketDeviceConfiguration::new(),
        )]));
        config.setEntropyDevices(&NSArray::from_retained_slice(&[Retained::into_super(
            VZVirtioEntropyDeviceConfiguration::new(),
        )]));

        config
            .validateWithError()
            .map_err(|e| anyhow::anyhow!("invalid configuration: {}", describe(Retained::as_ptr(&e) as *mut _)))?;
        match config.validateSaveRestoreSupportWithError() {
            Ok(()) => eprintln!("save/restore: supported by this configuration"),
            Err(e) => eprintln!(
                "save/restore: NOT supported: {}",
                describe(Retained::as_ptr(&e) as *mut _)
            ),
        }
        Ok(config)
    }
}

fn run(args: &[String]) -> Result<()> {
    let o = parse(args)?;
    std::fs::create_dir_all(&o.dir)?;
    let (guest_end, host_end) = socketpair()?;
    let nic = Arc::new(Nic {
        host: std::sync::atomic::AtomicI32::new(std::os::fd::IntoRawFd::into_raw_fd(host_end)),
        paused: std::sync::atomic::AtomicBool::new(false),
        frames_in: AtomicU64::new(0),
        bytes_in: AtomicU64::new(0),
        largest_in: AtomicU64::new(0),
        frames_out: AtomicU64::new(0),
        log: Mutex::new(
            std::fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(o.dir.join("net.log"))?,
        ),
    });
    std::thread::spawn({
        let nic = nic.clone();
        move || nic_reader(nic)
    });

    let config = configure(&o, guest_end)?;
    let queue = DispatchQueue::new("io.fleet.vz-spike", None);
    let vm = {
        let config = OnQueue(config);
        let mut out = None;
        let slot = OnQueue(&mut out);
        let own = OnQueue(queue.clone());
        queue.exec_sync(move || {
            let (config, slot, own) = (config, slot, own);
            *slot.0 = Some(OnQueue(unsafe {
                VZVirtualMachine::initWithConfiguration_queue(
                    VZVirtualMachine::alloc(),
                    &config.0,
                    &own.0,
                )
            }));
        });
        out.unwrap()
    };
    let vm = Arc::new(Vm { queue, vm });

    let started = Instant::now();
    let save_path = o.dir.join("state.vzvmsave");
    if o.restore {
        let path = OnQueue(url(&save_path));
        vm.call("restore", move |vm, done| {
            let path = path;
            unsafe { vm.restoreMachineStateFromURL_completionHandler(&path.0, done) }
        })?;
        eprintln!("restored in {:?}, state {}", started.elapsed(), vm.state());
        vm.call("resume", |vm, done| unsafe { vm.resumeWithCompletionHandler(done) })?;
    } else {
        vm.call("start", |vm, done| unsafe { vm.startWithCompletionHandler(done) })?;
    }
    eprintln!("running after {:?}, pid {}", started.elapsed(), std::process::id());

    let socket = o.dir.join("control.sock");
    let _ = std::fs::remove_file(&socket);
    let listener = UnixListener::bind(&socket)?;
    std::thread::spawn({
        let vm = vm.clone();
        let nic = nic.clone();
        move || {
            for stream in listener.incoming().flatten() {
                let reply = handle(&vm, &nic, &save_path, &stream)
                    .unwrap_or_else(|e| format!("error: {e:#}"));
                let mut stream = stream;
                let _ = writeln!(stream, "{reply}");
            }
        }
    });

    // No delegate yet: poll, which is also what an adopting supervisor would do.
    let mut last = "";
    loop {
        let state = vm.state();
        if state != last {
            eprintln!("[{:>8.3}s] state {state}", started.elapsed().as_secs_f64());
            last = state;
        }
        match state {
            "stopped" => return Ok(()),
            "error" => bail!("VM entered the error state"),
            _ => std::thread::sleep(Duration::from_millis(100)),
        }
    }
}

fn handle(vm: &Vm, nic: &Nic, save: &Path, stream: &UnixStream) -> Result<String> {
    let mut line = String::new();
    BufReader::new(stream).read_line(&mut line)?;
    let mut words = line.trim().splitn(3, ' ');
    let started = Instant::now();
    let result = match words.next().unwrap_or("") {
        "status" => format!("{}", vm.state()),
        "stop" => {
            vm.sync(|vm| unsafe { vm.requestStopWithError() })
                .map_err(|e| anyhow::anyhow!(describe(Retained::as_ptr(&e) as *mut _)))?;
            "stop requested".into()
        }
        "kill" => {
            vm.call("stop", |vm, done| unsafe { vm.stopWithCompletionHandler(done) })?;
            "stopped".into()
        }
        "pause" => {
            vm.call("pause", |vm, done| unsafe { vm.pauseWithCompletionHandler(done) })?;
            "paused".into()
        }
        "resume" => {
            vm.call("resume", |vm, done| unsafe { vm.resumeWithCompletionHandler(done) })?;
            "resumed".into()
        }
        "save" => {
            if vm.state() == "running" {
                vm.call("pause", |vm, done| unsafe { vm.pauseWithCompletionHandler(done) })?;
            }
            let _ = std::fs::remove_file(save);
            let path = OnQueue(url(save));
            vm.call("save", move |vm, done| {
                let path = path;
                unsafe { vm.saveMachineStateToURL_completionHandler(&path.0, done) }
            })?;
            let size = std::fs::metadata(save)?.len();
            format!("saved {} ({} MiB), VM left paused", save.display(), size >> 20)
        }
        "vsock" => {
            let port = words.next().context("vsock PORT TEXT")?.parse()?;
            vm.vsock(port, words.collect::<Vec<_>>().join(" ").as_str())?
        }
        "netsend" => {
            nic_send(nic, &words.collect::<Vec<_>>().join(" "))?;
            "sent".into()
        }
        "netpause" => {
            nic.paused.store(true, Ordering::Relaxed);
            "reader paused".into()
        }
        "netresume" => {
            nic.paused.store(false, Ordering::Relaxed);
            "reader resumed".into()
        }
        "netclose" => {
            let fd = nic.host.swap(-1, Ordering::Relaxed);
            unsafe { libc::close(fd) };
            "host end closed".into()
        }
        "netgrab" => {
            // Simulates a restarted worker taking over the network card from the runner.
            nic.paused.store(true, Ordering::Relaxed);
            send_fd(stream, nic.host.load(Ordering::Relaxed))?;
            let mut done = String::new();
            BufReader::new(stream).read_line(&mut done)?;
            nic.paused.store(false, Ordering::Relaxed);
            format!("worker returned the card: {}", done.trim())
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
    Ok(format!("{result}  ({:?})", started.elapsed()))
}

fn send_fd(stream: &UnixStream, fd: RawFd) -> Result<()> {
    unsafe {
        let mut byte = [b'F'];
        let mut iov = libc::iovec { iov_base: byte.as_mut_ptr().cast(), iov_len: 1 };
        let mut space = vec![0u8; libc::CMSG_SPACE(size_of::<RawFd>() as u32) as usize];
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
        if libc::sendmsg(stream.as_raw_fd(), &msg, 0) < 0 {
            bail!("sendmsg: {}", std::io::Error::last_os_error());
        }
    }
    Ok(())
}

fn recv_fd(stream: &UnixStream) -> Result<OwnedFd> {
    unsafe {
        let mut byte = [0u8];
        let mut iov = libc::iovec { iov_base: byte.as_mut_ptr().cast(), iov_len: 1 };
        let mut space = vec![0u8; libc::CMSG_SPACE(size_of::<RawFd>() as u32) as usize];
        let mut msg: libc::msghdr = std::mem::zeroed();
        msg.msg_iov = &mut iov;
        msg.msg_iovlen = 1;
        msg.msg_control = space.as_mut_ptr().cast();
        msg.msg_controllen = space.len() as _;
        if libc::recvmsg(stream.as_raw_fd(), &mut msg, 0) <= 0 {
            bail!("recvmsg: {}", std::io::Error::last_os_error());
        }
        let cmsg = libc::CMSG_FIRSTHDR(&msg);
        if cmsg.is_null() || (*cmsg).cmsg_type != libc::SCM_RIGHTS {
            bail!("no descriptor received");
        }
        Ok(OwnedFd::from_raw_fd(std::ptr::read_unaligned(libc::CMSG_DATA(cmsg).cast::<RawFd>())))
    }
}

/// A separate process standing in for a restarted worker: borrow the card, use it, hand it back.
fn grab(dir: &Path, seconds: u64) -> Result<()> {
    let mut stream = UnixStream::connect(dir.join("control.sock"))?;
    writeln!(stream, "netgrab")?;
    let fd = recv_fd(&stream)?;
    println!("worker pid {} holds the card as fd {}", std::process::id(), fd.as_raw_fd());
    let mut frame = GUEST_MAC.to_vec();
    frame.extend_from_slice(&[0x02, 0xf1, 0xee, 0x70, 0x00, 0xfd]);
    frame.extend_from_slice(&0x88b5u16.to_be_bytes());
    frame.extend_from_slice(format!("hello from worker pid {}", std::process::id()).as_bytes());
    unsafe { libc::send(fd.as_raw_fd(), frame.as_ptr().cast(), frame.len(), 0) };
    let deadline = Instant::now() + Duration::from_secs(seconds);
    let (mut frames, mut buf) = (0, vec![0u8; 65536]);
    while Instant::now() < deadline {
        let mut pfd = libc::pollfd { fd: fd.as_raw_fd(), events: libc::POLLIN, revents: 0 };
        if unsafe { libc::poll(&mut pfd, 1, 100) } > 0 {
            let n = unsafe { libc::recv(fd.as_raw_fd(), buf.as_mut_ptr().cast(), buf.len(), 0) };
            if n > 14 && buf[12..14] == [0x88, 0xb5] {
                frames += 1;
                println!("worker got: {}", String::from_utf8_lossy(&buf[14..n as usize]));
            }
        }
    }
    drop(fd);
    writeln!(stream, "{frames} frames read, descriptor closed")?;
    let mut reply = String::new();
    stream.read_to_string(&mut reply)?;
    print!("{reply}");
    Ok(())
}
