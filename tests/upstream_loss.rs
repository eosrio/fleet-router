//! Losing an upstream SHiP node, two ways.
//!
//! - **Silent loss** (cable pulled, VM frozen, firewall drop): the TCP
//!   connection stays open with nothing arriving. A blackhole proxy in front of
//!   one mock node models that: once frozen, it stops forwarding in both
//!   directions but keeps both sockets open. Sessions on it must fail over.
//! - **Clean restart**: nodeos closes its WebSockets with a Close frame and
//!   comes back. The monitor must reconnect, not give up on the node.
//!
//! Run: cargo test --test upstream_loss -- --nocapture
use std::io::Write;
use std::process::{Child, Command};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use futures::{SinkExt, StreamExt};
use mock_ship::{MockShipConfig, MockShipServer};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::time::{sleep, timeout};
use tokio_tungstenite::connect_async;
use tokio_tungstenite::tungstenite::Message;

fn free_port() -> u16 {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    listener.local_addr().unwrap().port()
}

struct Router {
    child: Child,
    listen_port: u16,
    metrics_port: u16,
    _config: tempfile::NamedTempFile,
}

impl Drop for Router {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

impl Router {
    /// Start the router with default limits (so `idle_timeout_ms` stays 0,
    /// as in most deployments) and the given servers.
    fn start(servers: serde_json::Value) -> Self {
        let listen_port = free_port();
        let metrics_port = free_port();
        let config = serde_json::json!({
            "listen_address": "127.0.0.1",
            "listen_port": listen_port,
            "upstream_reconnect_ms": 500,
            "upstream_monitoring_ms": 1000,
            "upstream_status_ms": 1000,
            "metrics_address": "127.0.0.1",
            "metrics_port": metrics_port,
            "servers": servers,
        });
        let mut file = tempfile::NamedTempFile::new().unwrap();
        file.write_all(config.to_string().as_bytes()).unwrap();
        file.flush().unwrap();
        let child = Command::new(env!("CARGO_BIN_EXE_fleet-router"))
            .arg("run")
            .arg("--config")
            .arg(file.path())
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .spawn()
            .expect("failed to spawn fleet-router");
        Router {
            child,
            listen_port,
            metrics_port,
            _config: file,
        }
    }

    /// The value of `fleet_router_upstream_up` for `endpoint`, if reported.
    async fn upstream_up(&self, endpoint: &str) -> Option<u8> {
        let mut stream = TcpStream::connect(("127.0.0.1", self.metrics_port))
            .await
            .ok()?;
        stream
            .write_all(b"GET /metrics HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
            .await
            .ok()?;
        let mut body = String::new();
        stream.read_to_string(&mut body).await.ok()?;
        let needle = format!("endpoint=\"{endpoint}\"");
        body.lines()
            .find(|line| line.starts_with("fleet_router_upstream_up{") && line.contains(&needle))
            .and_then(|line| line.rsplit(' ').next())
            .and_then(|value| value.parse().ok())
    }

    /// Wait up to `wait` for `fleet_router_upstream_up` to equal `value`.
    async fn wait_for_up(&self, endpoint: &str, value: u8, wait: Duration) -> bool {
        let deadline = Instant::now() + wait;
        while Instant::now() < deadline {
            if self.upstream_up(endpoint).await == Some(value) {
                return true;
            }
            sleep(Duration::from_millis(100)).await;
        }
        false
    }
}

/// Copy bytes one way until either side closes. While `frozen` is set, bytes
/// read are dropped and nothing is written, but the sockets stay open.
async fn pump(
    mut from: tokio::net::tcp::OwnedReadHalf,
    mut to: tokio::net::tcp::OwnedWriteHalf,
    frozen: Arc<AtomicBool>,
) {
    let mut buf = vec![0u8; 64 * 1024];
    loop {
        let n = match from.read(&mut buf).await {
            Ok(0) | Err(_) => return,
            Ok(n) => n,
        };
        if frozen.load(Ordering::Acquire) {
            continue;
        }
        if to.write_all(&buf[..n]).await.is_err() {
            return;
        }
    }
}

/// A TCP proxy to `target` that can be frozen. Returns its `host:port`.
async fn blackhole_proxy(target: String, frozen: Arc<AtomicBool>) -> String {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    tokio::spawn(async move {
        loop {
            let Ok((inbound, _)) = listener.accept().await else {
                return;
            };
            if frozen.load(Ordering::Acquire) {
                // A frozen node answers nothing new either: hold the socket.
                tokio::spawn(async move {
                    let _held = inbound;
                    sleep(Duration::from_secs(3600)).await;
                });
                continue;
            }
            let Ok(outbound) = TcpStream::connect(&target).await else {
                continue;
            };
            let (in_read, in_write) = inbound.into_split();
            let (out_read, out_write) = outbound.into_split();
            tokio::spawn(pump(in_read, out_write, frozen.clone()));
            tokio::spawn(pump(out_read, in_write, frozen.clone()));
        }
    });
    addr
}

fn mock_config() -> MockShipConfig {
    MockShipConfig {
        head_block: 1_000_000,
        lib_block: 999_990,
        blocks_to_stream: 0,
        block_delay: Some(Duration::from_millis(5)),
        ..Default::default()
    }
}

fn blocks_request(start: u32, end: u32) -> Vec<u8> {
    let mut buf = vec![1u8];
    buf.extend_from_slice(&start.to_le_bytes());
    buf.extend_from_slice(&end.to_le_bytes());
    buf.extend_from_slice(&u32::MAX.to_le_bytes());
    buf.extend_from_slice(&[0, 0, 0, 0, 0]);
    buf
}

/// Read the next block number, or `None` after `wait` without one.
async fn next_block<S>(reader: &mut S, wait: Duration) -> Option<u32>
where
    S: futures::Stream<Item = Result<Message, tokio_tungstenite::tungstenite::Error>> + Unpin,
{
    let deadline = Instant::now() + wait;
    loop {
        let left = deadline.checked_duration_since(Instant::now())?;
        match timeout(left, reader.next()).await {
            Ok(Some(Ok(Message::Binary(d)))) if d.len() >= 78 && d[0] == 1 && d[73] == 1 => {
                return Some(u32::from_le_bytes(d[74..78].try_into().unwrap()));
            }
            Ok(Some(Ok(Message::Close(frame)))) => panic!("session closed: {frame:?}"),
            Ok(Some(Ok(_))) => continue,
            _ => return None,
        }
    }
}

#[tokio::test]
async fn session_fails_over_when_its_upstream_goes_silent() {
    let mock1 = MockShipServer::new(mock_config()).await;
    let mock2 = MockShipServer::new(mock_config()).await;
    let frozen = Arc::new(AtomicBool::new(false));
    let silent_ep = blackhole_proxy(mock1.endpoint(), frozen.clone()).await;
    let backup_ep = mock2.endpoint();
    let (h1, h2) = (mock1.start(), mock2.start());

    // The proxied node is preferred so the session lands there.
    let router = Router::start(serde_json::json!([
        { "name": "silent", "endpoint": silent_ep, "enabled": true },
        { "name": "backup", "endpoint": backup_ep, "enabled": true, "priority": 1 }
    ]));
    assert!(
        router
            .wait_for_up(&silent_ep, 1, Duration::from_secs(10))
            .await
    );
    assert!(
        router
            .wait_for_up(&backup_ep, 1, Duration::from_secs(10))
            .await
    );

    let (ws, _) = connect_async(format!("ws://127.0.0.1:{}", router.listen_port))
        .await
        .unwrap();
    let (mut writer, mut reader) = ws.split();
    let _abi = reader.next().await.unwrap().unwrap();
    writer
        .send(Message::Binary(blocks_request(100, 900_000).into()))
        .await
        .unwrap();
    let mut last = 0;
    for _ in 0..50 {
        last = next_block(&mut reader, Duration::from_secs(5))
            .await
            .expect("no blocks before the cable pull");
    }

    // The node goes silent: no FIN, no RST, no WebSocket close.
    frozen.store(true, Ordering::Release);
    let mut previous_block_at = Instant::now();
    let mut longest_pause = Duration::ZERO;

    // The stream must resume on the backup at the next block: no gap, no
    // duplicate, within a few seconds.
    for _ in 0..100 {
        let Some(block) = next_block(&mut reader, Duration::from_secs(30)).await else {
            panic!("session stalled for 30 s after its upstream went silent (last block {last})");
        };
        assert_eq!(block, last + 1, "gap or duplicate after the cable pull");
        last = block;
        // The largest pause between two blocks is how long the client waited.
        let now = Instant::now();
        longest_pause = longest_pause.max(now - previous_block_at);
        previous_block_at = now;
    }
    println!("streamed to block {last} across the cable pull; longest pause {longest_pause:?}");
    assert!(
        longest_pause < Duration::from_secs(4),
        "the client waited {longest_pause:?} for the next block"
    );

    drop(router);
    h1.shutdown().await;
    h2.shutdown().await;
}

#[tokio::test]
async fn monitor_reconnects_after_an_upstream_closes_cleanly() {
    let mock = MockShipServer::new(mock_config()).await;
    let endpoint = mock.endpoint();
    let handle = mock.start();

    let router = Router::start(serde_json::json!([
        { "name": "node", "endpoint": endpoint, "enabled": true }
    ]));
    assert!(
        router
            .wait_for_up(&endpoint, 1, Duration::from_secs(10))
            .await
    );

    // nodeos closing its sockets cleanly, as on a restart. The node accepts
    // connections again right away.
    handle.close_sessions();
    sleep(Duration::from_millis(200)).await;
    assert!(
        router
            .wait_for_up(&endpoint, 1, Duration::from_secs(10))
            .await,
        "the upstream stayed offline after a clean close; the monitor gave up on it"
    );

    // And it serves new sessions again.
    let (ws, _) = connect_async(format!("ws://127.0.0.1:{}", router.listen_port))
        .await
        .unwrap();
    let (mut writer, mut reader) = ws.split();
    let _abi = reader.next().await.unwrap().unwrap();
    writer
        .send(Message::Binary(blocks_request(10, 900_000).into()))
        .await
        .unwrap();
    assert_eq!(
        next_block(&mut reader, Duration::from_secs(5)).await,
        Some(10)
    );

    drop(router);
    handle.shutdown().await;
}

#[tokio::test]
async fn a_quiet_upstream_that_answers_pings_keeps_its_session() {
    let mock = MockShipServer::new(mock_config()).await;
    let endpoint = mock.endpoint();
    let handle = mock.start();
    let router = Router::start(serde_json::json!([
        { "name": "node", "endpoint": endpoint, "enabled": true }
    ]));
    assert!(
        router
            .wait_for_up(&endpoint, 1, Duration::from_secs(10))
            .await
    );

    // One block in flight and no acknowledgement: the node has nothing to
    // send, which is normal flow control, not a dead node.
    let (ws, _) = connect_async(format!("ws://127.0.0.1:{}", router.listen_port))
        .await
        .unwrap();
    let (mut writer, mut reader) = ws.split();
    let _abi = reader.next().await.unwrap().unwrap();
    let mut request = blocks_request(10, 900_000);
    request[9..13].copy_from_slice(&1u32.to_le_bytes()); // max_messages_in_flight
    writer.send(Message::Binary(request.into())).await.unwrap();
    assert_eq!(
        next_block(&mut reader, Duration::from_secs(5)).await,
        Some(10)
    );

    // Twice the default liveness timeout of quiet.
    assert_eq!(next_block(&mut reader, Duration::from_secs(8)).await, None);
    assert_eq!(
        router.upstream_up(&endpoint).await,
        Some(1),
        "a quiet node that answers pings must not be marked offline"
    );

    // Acknowledging resumes the stream on the same session.
    let mut ack = vec![2u8];
    ack.extend_from_slice(&1u32.to_le_bytes());
    writer.send(Message::Binary(ack.into())).await.unwrap();
    assert_eq!(
        next_block(&mut reader, Duration::from_secs(5)).await,
        Some(11)
    );

    drop(router);
    handle.shutdown().await;
}
