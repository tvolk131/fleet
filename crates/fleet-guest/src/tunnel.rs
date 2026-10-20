//! Bounded, half-close preserving streams over the private guest-agent channel.
//! No keys, host addresses, or authorization decisions live in the guest.
use anyhow::{Context, Result, bail, ensure};
use serde::{Deserialize, Serialize};
use std::{
    collections::{BTreeMap, BTreeSet, VecDeque},
    net::SocketAddr,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, DuplexStream},
    net::{TcpListener, TcpStream},
    sync::{Semaphore, mpsc, oneshot},
    task::JoinSet,
};

pub const READY: &[u8] = b"fleet-service-channel-1\n";
pub const LIMIT: usize = 64;
const CHUNK: usize = 16 * 1024;
const WINDOW: usize = 4;
pub const DEADLINE: Duration = Duration::from_secs(10);

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    pub routes: Vec<SocketAddr>,
    pub targets: Vec<SocketAddr>,
}
impl Config {
    pub fn validate(&self) -> Result<()> {
        ensure!(
            self.routes.len() <= 64 && self.targets.len() <= 256,
            "guest service port limit exceeded"
        );
        let mut ports = BTreeSet::new();
        for address in self.routes.iter().chain(&self.targets) {
            ensure!(
                address.ip().is_loopback() && address.port() != 0 && ports.insert(address.port()),
                "guest services require distinct nonzero loopback ports"
            );
        }
        Ok(())
    }
}

#[derive(Debug)]
enum Frame {
    Open(u64, u32),
    Opened(u64),
    Data(u64, Vec<u8>),
    Credit(u64),
    Fin(u64),
    Close(u64),
    Heartbeat,
}
impl Frame {
    async fn read(r: &mut (impl AsyncRead + Unpin)) -> Result<Self> {
        let kind = r.read_u8().await?;
        let id = r.read_u64().await?;
        ensure!(id != 0 && id != u64::MAX, "reserved stream ID");
        Ok(match kind {
            1 => Self::Open(id, r.read_u32().await?),
            2 => Self::Opened(id),
            3 => {
                let len = r.read_u32().await? as usize;
                ensure!((1..=CHUNK).contains(&len), "invalid service frame length");
                let mut bytes = vec![0; len];
                r.read_exact(&mut bytes).await?;
                Self::Data(id, bytes)
            }
            4 => Self::Credit(id),
            5 => Self::Fin(id),
            6 => Self::Close(id),
            7 if id == 1 => Self::Heartbeat,
            _ => bail!("unknown service frame"),
        })
    }
    async fn write(self, w: &mut (impl AsyncWrite + Unpin)) -> Result<()> {
        let (kind, id) = match &self {
            Self::Open(id, _) => (1, *id),
            Self::Opened(id) => (2, *id),
            Self::Data(id, _) => (3, *id),
            Self::Credit(id) => (4, *id),
            Self::Fin(id) => (5, *id),
            Self::Close(id) => (6, *id),
            Self::Heartbeat => (7, 1),
        };
        w.write_u8(kind).await?;
        w.write_u64(id).await?;
        match self {
            Self::Open(_, target) => w.write_u32(target).await?,
            Self::Data(_, data) => {
                w.write_u32(data.len() as u32).await?;
                w.write_all(&data).await?;
            }
            _ => (),
        }
        w.flush().await?;
        Ok(())
    }
}

enum Event {
    Open {
        target: u32,
        reply: oneshot::Sender<DuplexStream>,
    },
    Consumed(u64),
}
#[derive(Clone)]
pub struct Handle {
    events: mpsc::Sender<Event>,
}
impl Handle {
    pub async fn open(&self, target: u32) -> Result<DuplexStream> {
        tokio::time::timeout(DEADLINE, async {
            let (reply, receive) = oneshot::channel();
            self.events
                .send(Event::Open { target, reply })
                .await
                .context("guest channel closed")?;
            receive.await.context("guest stream refused")
        })
        .await
        .context("guest stream open timed out")?
    }
}
enum Data {
    Bytes(Vec<u8>),
    Fin,
}
struct Stream {
    input: mpsc::Sender<Data>,
    credit: Arc<Semaphore>,
    outstanding: Arc<AtomicUsize>,
    allowance: usize,
    fin: bool,
    opening: Option<(oneshot::Sender<DuplexStream>, DuplexStream)>,
    task: tokio::task::AbortHandle,
}
impl Drop for Stream {
    fn drop(&mut self) {
        self.task.abort();
    }
}

pub type Incoming = mpsc::Receiver<(u32, DuplexStream)>;
pub struct Channel {
    events: mpsc::Receiver<Event>,
    sender: mpsc::Sender<Event>,
    incoming: mpsc::Sender<(u32, DuplexStream)>,
    /// Host uses even IDs; the guest uses odd IDs. Neither side can replay IDs.
    host: bool,
    remote_targets: u32,
}
impl Channel {
    pub fn new(host: bool, remote_targets: usize) -> (Handle, Incoming, Self) {
        let (sender, events) = mpsc::channel(16);
        let (incoming, receive) = mpsc::channel(LIMIT);
        (
            Handle {
                events: sender.clone(),
            },
            receive,
            Self {
                events,
                sender,
                incoming,
                host,
                remote_targets: remote_targets as u32,
            },
        )
    }
    pub async fn run(
        self,
        read: impl AsyncRead + Unpin + Send + 'static,
        write: impl AsyncWrite + Unpin + Send + 'static,
    ) -> Result<()> {
        self.run_until_idle(read, write, DEADLINE).await
    }
    async fn run_until_idle(
        mut self,
        mut read: impl AsyncRead + Unpin + Send + 'static,
        mut write: impl AsyncWrite + Unpin + Send + 'static,
        idle: Duration,
    ) -> Result<()> {
        let (out, mut output) = mpsc::channel::<Frame>(16);
        // A dedicated reader makes frame decoding cancellation safe across select!.
        let (wire, mut frames) = mpsc::channel(1);
        let mut io = JoinSet::new();
        io.spawn(async move {
            loop {
                let frame = Frame::read(&mut read).await?;
                if wire.send(frame).await.is_err() {
                    return Ok::<_, anyhow::Error>(());
                }
            }
        });
        io.spawn(async move {
            while let Some(frame) = output.recv().await {
                frame.write(&mut write).await?;
            }
            Ok(())
        });
        let mut streams = BTreeMap::<u64, Stream>::new();
        let mut tasks = JoinSet::<(u64, bool)>::new();
        let mut last_local = if self.host { 0 } else { u64::MAX };
        let mut last_remote = if self.host { u64::MAX } else { 0 };
        let mut expiry = tokio::time::interval(Duration::from_millis(200));
        let mut received = tokio::time::Instant::now();
        let mut sent_heartbeat = received;
        // Never await output capacity while decoding input: two busy peers
        // would otherwise both stop reading while waiting for each other's pipe.
        let mut pending = VecDeque::new();
        loop {
            ensure!(pending.len() <= 1024, "service control queue exhausted");
            tokio::select! {
                permit = out.reserve(), if !pending.is_empty() => {
                    let frame=pending.pop_front().expect("pending control");
                    let opened=if let Frame::Opened(id)=frame {Some(id)} else {None};
                    permit?.send(frame);
                    if let Some(id)=opened && let Some(s)=streams.get(&id) { s.credit.add_permits(WINDOW); }
                }
                _ = expiry.tick() => {
                    // Guest stdin can stay open after its host command dies.
                    // Expire that orphan even when the guest has no active streams.
                    ensure!(received.elapsed() < idle, "guest channel heartbeat expired");
                    if sent_heartbeat.elapsed() >= idle / 5 {
                        pending.push_back(Frame::Heartbeat);
                        sent_heartbeat = tokio::time::Instant::now();
                    }
                    let abandoned: Vec<_> = streams.iter().filter(|(_,s)| s.opening.as_ref().is_some_and(|(reply,_)| reply.is_closed())).map(|(id,_)| *id).collect();
                    for id in abandoned { streams.remove(&id); pending.push_back(Frame::Close(id)); }
                }
                result = io.join_next() => { result.context("guest I/O stopped")???; bail!("guest channel closed"); }
                Some(result) = tasks.join_next(), if !tasks.is_empty() => {
                    if let Ok((id, clean)) = result && streams.remove(&id).is_some() && !clean { pending.push_back(Frame::Close(id)); }
                }
                Some(event) = self.events.recv() => match event {
                    Event::Open {target, reply} => {
                        if streams.len() == LIMIT || reply.is_closed() { continue; }
                        let id = if last_local == u64::MAX {1} else {last_local.checked_add(2).context("stream IDs exhausted")?};
                        ensure!(id != u64::MAX, "stream IDs exhausted");
                        last_local = id;
                        let (stream, socket) = stream(id, false, &mut tasks, out.clone(), self.sender.clone());
                        let mut stream = stream;
                        stream.opening = Some((reply, socket));
                        streams.insert(id, stream);
                        pending.push_back(Frame::Open(id,target));
                    }
                    Event::Consumed(id) => if let Some(s) = streams.get_mut(&id) {
                        ensure!(s.allowance < WINDOW, "invalid local stream credit");
                        s.allowance += 1;
                        pending.push_back(Frame::Credit(id));
                    },
                },
                Some(frame) = frames.recv() => {
                    received = tokio::time::Instant::now();
                    if matches!(frame, Frame::Heartbeat) { continue; }
                    if let Frame::Open(id, target) = frame {
                        ensure!(id % 2 == u64::from(self.host) && (last_remote == u64::MAX || id > last_remote), "replayed or foreign stream ID");
                        last_remote = id;
                        if target >= self.remote_targets || streams.len() == LIMIT {
                            pending.push_back(Frame::Close(id));
                            continue;
                        }
                        let (s, socket) = stream(id, false, &mut tasks, out.clone(), self.sender.clone());
                        if self.incoming.try_send((target, socket)).is_err() { pending.push_back(Frame::Close(id)); continue; }
                        streams.insert(id,s);
                        pending.push_back(Frame::Opened(id));
                        continue;
                    }
                    let id = match &frame { Frame::Opened(id) | Frame::Data(id,_) | Frame::Credit(id) | Frame::Fin(id) | Frame::Close(id) => *id, Frame::Open(..) | Frame::Heartbeat => unreachable!() };
                    let Some(s) = streams.get_mut(&id) else {
                        // Late frames after cancellation are harmless; invented future IDs are not.
                        let limit = if id % 2 == u64::from(!self.host) {last_local} else {last_remote};
                        ensure!(limit != u64::MAX && id <= limit, "unknown stream ID");
                        continue;
                    };
                    match frame {
                        Frame::Opened(_) => {
                            let (reply, socket) = s.opening.take().context("duplicate stream acknowledgement")?;
                            s.credit.add_permits(WINDOW);
                            if reply.send(socket).is_err() { streams.remove(&id); pending.push_back(Frame::Close(id)); }
                        }
                        Frame::Close(_) => { streams.remove(&id); }
                        Frame::Data(_, data) => {
                            ensure!(s.opening.is_none() && !s.fin && s.allowance > 0, "stream exceeded receive window or sent after FIN");
                            s.allowance -= 1;
                            // WINDOW data frames plus one FIN always fit; peer cannot allocate an unbounded queue.
                            ensure!(s.input.try_send(Data::Bytes(data)).is_ok(), "stream reader closed");
                        }
                        Frame::Credit(_) => {
                            ensure!(s.opening.is_none() && s.outstanding.try_update(Ordering::SeqCst, Ordering::SeqCst, |n| n.checked_sub(1)).is_ok(), "unearned stream credit");
                            s.credit.add_permits(1);
                        }
                        Frame::Fin(_) => {
                            ensure!(s.opening.is_none() && !s.fin, "duplicate or premature FIN");
                            s.fin = true;
                            ensure!(s.input.try_send(Data::Fin).is_ok(), "stream reader closed");
                        }
                        Frame::Open(..) | Frame::Heartbeat => unreachable!(),
                    }
                }
            }
            // Completed/cancelled stream tasks must never accumulate under churn.
            while let Some(result) = tasks.try_join_next() {
                if let Ok((id, clean)) = result
                    && streams.remove(&id).is_some()
                    && !clean
                {
                    pending.push_back(Frame::Close(id));
                }
            }
        }
    }
}

fn stream(
    id: u64,
    ready: bool,
    tasks: &mut JoinSet<(u64, bool)>,
    out: mpsc::Sender<Frame>,
    events: mpsc::Sender<Event>,
) -> (Stream, DuplexStream) {
    let (socket, pump) = tokio::io::duplex(WINDOW * CHUNK);
    let (mut read, mut write) = tokio::io::split(pump);
    let (input, mut bytes) = mpsc::channel(WINDOW + 1);
    let credit = Arc::new(Semaphore::new(if ready { WINDOW } else { 0 }));
    let outstanding = Arc::new(AtomicUsize::new(0));
    let c = credit.clone();
    let o = outstanding.clone();
    let task = tasks.spawn(async move {
        let send = async {
            let mut data = vec![0; CHUNK];
            loop {
                // Also gate FIN until OPENED has been queued on the channel.
                let permit = c.acquire().await?;
                let count = read.read(&mut data).await?;
                if count == 0 {
                    drop(permit);
                    out.send(Frame::Fin(id)).await?;
                    break;
                }
                permit.forget();
                o.fetch_add(1, Ordering::SeqCst);
                out.send(Frame::Data(id, data[..count].to_vec())).await?;
            }
            Ok::<_, anyhow::Error>(())
        };
        let receive = async {
            while let Some(data) = bytes.recv().await {
                match data {
                    Data::Bytes(data) => {
                        write.write_all(&data).await?;
                        events.send(Event::Consumed(id)).await?;
                    }
                    Data::Fin => {
                        write.shutdown().await?;
                        return Ok::<_, anyhow::Error>(());
                    }
                }
            }
            bail!("stream closed")
        };
        // FIN is graceful; CLOSE is a reset. Sending CLOSE after a successful
        // FIN would abort the peer's still-draining receive buffer and lose bytes.
        let result = tokio::try_join!(send, receive);
        let clean = result.is_ok();
        (id, clean)
    });
    (
        Stream {
            input,
            credit,
            outstanding,
            allowance: WINDOW,
            fin: false,
            opening: None,
            task,
        },
        socket,
    )
}

pub async fn guest(config: Config) -> Result<()> {
    config.validate()?;
    let mut bound = Vec::new();
    for (index, address) in config.routes.iter().enumerate() {
        bound.push((index as u32, TcpListener::bind(address).await?));
    }
    let mut stdout = tokio::io::stdout();
    stdout.write_all(READY).await?;
    stdout.flush().await?;
    let (handle, mut incoming, channel) = Channel::new(false, config.targets.len());
    let mut tasks = JoinSet::new();
    tasks.spawn(channel.run(tokio::io::stdin(), stdout));
    let slots = Arc::new(Semaphore::new(LIMIT));
    for (index, listener) in bound {
        let handle = handle.clone();
        let slots = slots.clone();
        tasks.spawn(async move {
            let mut bridges = JoinSet::new();
            loop {
                tokio::select! {
                    result = listener.accept() => {
                        let (mut tcp, _) = result?;
                        let Ok(permit) = slots.clone().try_acquire_owned() else {continue;};
                        tcp.set_nodelay(true)?;
                        let handle = handle.clone();
                        bridges.spawn(async move { let _permit = permit; if let Ok(mut stream) = handle.open(index).await { let _ = tokio::io::copy_bidirectional(&mut tcp, &mut stream).await; } });
                    }
                    _ = bridges.join_next(), if !bridges.is_empty() => (),
                }
            }
            #[allow(unreachable_code)] Ok::<_,anyhow::Error>(())
        });
    }
    let mut bridges = JoinSet::new();
    loop {
        tokio::select! {
            result = tasks.join_next() => { result.context("guest service task stopped")???; bail!("guest service task stopped"); }
            Some((index, mut stream)) = incoming.recv() => {
                let target = config.targets[index as usize];
                let Ok(permit) = slots.clone().try_acquire_owned() else {continue;};
                bridges.spawn(async move { let _permit = permit;
                    let _=async { let mut tcp=tokio::time::timeout(DEADLINE,TcpStream::connect(target)).await??; tcp.set_nodelay(true)?; tokio::io::copy_bidirectional(&mut stream,&mut tcp).await?; Ok::<_,anyhow::Error>(()) }.await;
                });
            }
            _ = bridges.join_next(), if !bridges.is_empty() => (),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn orphaned_channel_expires_even_if_the_host_keeps_both_pipes_open() {
        let (local, _silent_peer) = tokio::io::duplex(4096);
        let (read, write) = tokio::io::split(local);
        let (_, _, channel) = Channel::new(false, 0);
        let error = tokio::time::timeout(
            Duration::from_secs(2),
            channel.run_until_idle(read, write, Duration::from_millis(100)),
        )
        .await
        .unwrap()
        .unwrap_err();
        assert!(error.to_string().contains("heartbeat expired"));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn multiplexed_large_streams_preserve_half_close_in_both_directions() {
        let (a, b) = tokio::io::duplex(4096);
        let (ar, aw) = tokio::io::split(a);
        let (br, bw) = tokio::io::split(b);
        let (host, mut hi, hc) = Channel::new(true, 1);
        let (guest, mut gi, gc) = Channel::new(false, 1);
        let mut tasks = JoinSet::new();
        tasks.spawn(hc.run(ar, aw));
        tasks.spawn(gc.run(br, bw));
        let scenario = async {
            let mut echoes = JoinSet::new();
            for _ in 0..8 {
                let h = host.clone();
                let g = guest.clone();
                echoes.spawn(async move {
                    roundtrip(h).await;
                    roundtrip(g).await;
                });
            }
            let mut servers = JoinSet::new();
            for _ in 0..16 {
                let socket =
                    tokio::select! { Some((0,s)) = hi.recv() => s, Some((0,s)) = gi.recv() => s };
                servers.spawn(async move {
                    let (mut read, mut write) = tokio::io::split(socket);
                    tokio::io::copy(&mut read, &mut write).await.unwrap();
                    write.shutdown().await.unwrap();
                });
            }
            while let Some(r) = echoes.join_next().await {
                r.unwrap();
            }
            while let Some(r) = servers.join_next().await {
                r.unwrap();
            }
        };
        tokio::select! { r=tasks.join_next() => panic!("channel ended: {r:?}"), r=tokio::time::timeout(Duration::from_secs(30),scenario) => r.unwrap() };
    }
    async fn roundtrip(h: Handle) {
        let mut socket = h.open(0).await.unwrap();
        let data = vec![0x81; 1024 * 1024];
        let mut actual = Vec::new();
        let (mut read, mut write) = tokio::io::split(&mut socket);
        tokio::try_join!(
            async {
                write.write_all(&data).await?;
                write.shutdown().await
            },
            async { read.read_to_end(&mut actual).await }
        )
        .unwrap();
        assert_eq!(actual, data);
    }
    async fn attack(frames: Vec<Frame>, expected: &str) {
        let (a, mut b) = tokio::io::duplex(4096);
        let (r, w) = tokio::io::split(a);
        let (_, mut incoming, channel) = Channel::new(true, 1);
        let task = tokio::spawn(channel.run(r, w));
        let mut held = Vec::new();
        for frame in frames {
            frame.write(&mut b).await.unwrap();
            if let Ok(Some((_, stream))) =
                tokio::time::timeout(Duration::from_millis(10), incoming.recv()).await
            {
                held.push(stream);
            }
        }
        let error = tokio::time::timeout(Duration::from_secs(2), task)
            .await
            .unwrap()
            .unwrap()
            .unwrap_err();
        assert!(error.to_string().contains(expected), "{error:#}");
    }
    #[tokio::test]
    async fn guest_cannot_spoof_host_streams_replay_or_invent_credit() {
        attack(vec![Frame::Open(2, 0)], "foreign").await;
        attack(vec![Frame::Open(1, 0), Frame::Open(1, 0)], "replayed").await;
        attack(vec![Frame::Open(1, 0), Frame::Credit(1)], "unearned").await;
        attack(vec![Frame::Fin(100)], "unknown stream").await;
        attack(vec![Frame::Open(u64::MAX, 0)], "reserved").await;
        attack(
            vec![Frame::Open(1, 0), Frame::Fin(1), Frame::Data(1, vec![1])],
            "after FIN",
        )
        .await;
    }
    #[tokio::test]
    async fn route_indices_are_host_authorized_and_frame_allocations_are_bounded() {
        let (a, mut b) = tokio::io::duplex(4096);
        let (r, w) = tokio::io::split(a);
        let (_, mut incoming, c) = Channel::new(true, 1);
        let task = tokio::spawn(c.run(r, w));
        Frame::Open(1, u32::MAX).write(&mut b).await.unwrap();
        assert!(matches!(
            Frame::read(&mut b).await.unwrap(),
            Frame::Close(1)
        ));
        assert!(incoming.try_recv().is_err());
        Frame::Open(3, 0).write(&mut b).await.unwrap();
        assert!(matches!(
            Frame::read(&mut b).await.unwrap(),
            Frame::Opened(3)
        ));
        let _stream = incoming.recv().await.unwrap();
        b.write_u8(3).await.unwrap();
        b.write_u64(3).await.unwrap();
        b.write_u32(u32::MAX).await.unwrap();
        let e = tokio::time::timeout(Duration::from_secs(2), task)
            .await
            .unwrap()
            .unwrap()
            .unwrap_err();
        assert!(e.to_string().contains("frame length"));
    }
    #[tokio::test]
    async fn guest_cannot_exceed_the_per_stream_window() {
        let (a, mut b) = tokio::io::duplex(2 * 1024 * 1024);
        let (r, w) = tokio::io::split(a);
        let (_, mut incoming, c) = Channel::new(true, 1);
        let task = tokio::spawn(c.run(r, w));
        Frame::Open(1, 0).write(&mut b).await.unwrap();
        let _held = incoming.recv().await.unwrap();
        for _ in 0..32 {
            if Frame::Data(1, vec![0; CHUNK]).write(&mut b).await.is_err() {
                break;
            }
        }
        let e = tokio::time::timeout(Duration::from_secs(2), task)
            .await
            .unwrap()
            .unwrap()
            .unwrap_err();
        assert!(e.to_string().contains("receive window"), "{e:#}");
    }
}
