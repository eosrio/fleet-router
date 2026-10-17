//! Shared harness for end-to-end tests: the router as a child process, config
//! builders, metrics and `/coverage` polling, and WebSocket reads that never
//! wait forever.
//!
//! Include it with `mod common;`. Each test binary uses only part of it.
#![allow(dead_code)]

use std::io::Write;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use futures::StreamExt;
use mock_ship::client::{BlockView, StatusView};
use serde_json::{json, Value};
use tempfile::NamedTempFile;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::time::{sleep, timeout, timeout_at};
use tokio_tungstenite::tungstenite::protocol::frame::coding::CloseCode;
use tokio_tungstenite::tungstenite::protocol::CloseFrame;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::{connect_async, MaybeTlsStream, WebSocketStream};

pub type WsStream = WebSocketStream<MaybeTlsStream<TcpStream>>;
pub type WsWriter = futures::stream::SplitSink<WsStream, Message>;
pub type WsReader = futures::stream::SplitStream<WsStream>;

/// Launches tried before giving up on finding free ports.
const START_ATTEMPTS: usize = 5;
/// How long a launched router has to accept TCP on its listen ports.
const START_TIMEOUT: Duration = Duration::from_secs(10);
/// How long the metrics port may lag the listen ports before the launch counts
/// as a lost port race (the router keeps running without metrics then).
const METRICS_GRACE: Duration = Duration::from_secs(2);
const POLL: Duration = Duration::from_millis(50);
const HTTP_TIMEOUT: Duration = Duration::from_secs(2);

/// A running router. Dropping it kills the process.
pub struct RouterProcess {
    child: Child,
    /// The first listener's port (the only one in single-listener mode).
    pub listen_port: u16,
    /// Every listener's port, in config order.
    pub listener_ports: Vec<u16>,
    pub metrics_port: Option<u16>,
    _cfg: NamedTempFile,
}

impl RouterProcess {
    /// Start the router with `cfg`.
    ///
    /// A `listen_port` (top level, or on each entry of `listeners`) that is 0
    /// or absent gets a free port, and so does a `metrics_port` key that is 0.
    /// stdout and stderr go to null. When the router exits early or never opens
    /// its metrics port (another process took a port in between), it is
    /// relaunched on fresh ports. Panics unless the listen ports accept TCP
    /// within 10 s.
    pub async fn start(cfg: Value) -> Self {
        let mut last_error = String::new();
        for _ in 0..START_ATTEMPTS {
            let mut cfg = cfg.clone();
            let (listener_ports, metrics_port) = assign_ports(&mut cfg).await;

            let mut file = NamedTempFile::new().expect("create temp config");
            file.write_all(cfg.to_string().as_bytes())
                .expect("write temp config");
            file.flush().expect("flush temp config");

            let child = Command::new(env!("CARGO_BIN_EXE_fleet-router"))
                .arg("--config")
                .arg(file.path())
                .stdin(Stdio::null())
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .spawn()
                .expect("spawn fleet-router");
            let mut router = Self {
                child,
                listen_port: listener_ports[0],
                listener_ports,
                metrics_port,
                _cfg: file,
            };
            match router.wait_ready().await {
                Ok(()) => return router,
                Err(e) => last_error = e,
            }
        }
        panic!("fleet-router did not start after {START_ATTEMPTS} attempts: {last_error}");
    }

    pub fn ws_url(&self) -> String {
        format!("ws://127.0.0.1:{}", self.listen_port)
    }

    /// The metrics port; panics when the config has none.
    pub fn metrics(&self) -> u16 {
        self.metrics_port.expect("config has no metrics_port")
    }

    async fn wait_ready(&mut self) -> Result<(), String> {
        let deadline = Instant::now() + START_TIMEOUT;
        let mut listening_since: Option<Instant> = None;
        loop {
            if let Ok(Some(status)) = self.child.try_wait() {
                return Err(format!("fleet-router exited with {status}"));
            }
            if listening_since.is_none() {
                let mut all_open = true;
                for port in &self.listener_ports {
                    all_open &= port_open(*port).await;
                }
                if all_open {
                    listening_since = Some(Instant::now());
                }
            }
            if let Some(since) = listening_since {
                match self.metrics_port {
                    None => return Ok(()),
                    Some(port) if port_open(port).await => return Ok(()),
                    Some(port) if since.elapsed() > METRICS_GRACE => {
                        return Err(format!("metrics port {port} never opened"));
                    }
                    Some(_) => {}
                }
            }
            if Instant::now() > deadline {
                panic!(
                    "fleet-router did not listen on {:?} within {START_TIMEOUT:?}",
                    self.listener_ports
                );
            }
            sleep(POLL).await;
        }
    }
}

impl Drop for RouterProcess {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

fn wants_port(value: Option<&Value>) -> bool {
    value.is_none_or(|v| v.is_null() || v.as_u64() == Some(0))
}

/// Fill in the ports `start` assigns, returning the listener ports and the
/// metrics port. The probe sockets stay bound until every port is chosen, so
/// two roles never get the same port.
async fn assign_ports(cfg: &mut Value) -> (Vec<u16>, Option<u16>) {
    let mut held = Vec::new();
    let mut fresh = async || {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind 127.0.0.1:0");
        let port = listener.local_addr().expect("local addr").port();
        held.push(listener);
        port
    };

    let mut listener_ports = Vec::new();
    if let Some(listeners) = cfg.get_mut("listeners").and_then(Value::as_array_mut) {
        for listener in listeners {
            if wants_port(listener.get("listen_port")) {
                listener["listen_port"] = json!(fresh().await);
            }
            listener_ports.push(port_of(&listener["listen_port"]));
        }
    } else {
        if wants_port(cfg.get("listen_port")) {
            cfg["listen_port"] = json!(fresh().await);
        }
        listener_ports.push(port_of(&cfg["listen_port"]));
    }

    let metrics_port = match cfg.get("metrics_port") {
        None => None,
        Some(value) if wants_port(Some(value)) => {
            let port = fresh().await;
            cfg["metrics_port"] = json!(port);
            Some(port)
        }
        Some(value) => Some(port_of(value)),
    };
    drop(held);
    (listener_ports, metrics_port)
}

fn port_of(value: &Value) -> u16 {
    value
        .as_u64()
        .and_then(|p| u16::try_from(p).ok())
        .unwrap_or_else(|| panic!("not a port: {value}"))
}

async fn port_open(port: u16) -> bool {
    matches!(
        timeout(
            Duration::from_millis(200),
            TcpStream::connect(("127.0.0.1", port))
        )
        .await,
        Ok(Ok(_))
    )
}

/// A single-listener config on 127.0.0.1 with monitoring, status and
/// reconnect at 200 ms and metrics on. Ports are filled in by
/// [`RouterProcess::start`].
pub fn base_config(servers: &[Value]) -> Value {
    json!({
        "listen_address": "127.0.0.1",
        "listen_port": 0,
        "upstream_reconnect_ms": 200,
        "upstream_monitoring_ms": 200,
        "upstream_status_ms": 200,
        "metrics_address": "127.0.0.1",
        "metrics_port": 0,
        "servers": servers,
    })
}

/// An enabled live server.
pub fn server(name: &str, endpoint: &str) -> Value {
    json!({ "name": name, "endpoint": endpoint, "enabled": true })
}

/// An enabled frozen range server, with an optional declared `[begin, end)`.
pub fn frozen(name: &str, endpoint: &str, range: Option<(u32, u32)>) -> Value {
    let mut entry = server(name, endpoint);
    entry["role"] = json!("frozen");
    if let Some((begin, end)) = range {
        entry["range"] = json!({ "begin": begin, "end": end });
    }
    entry
}

/// One HTTP GET on 127.0.0.1: the status code and the body, or `None` when the
/// request fails or takes longer than 2 s.
pub async fn http_get(port: u16, path: &str) -> Option<(u16, String)> {
    let mut stream = timeout(HTTP_TIMEOUT, TcpStream::connect(("127.0.0.1", port)))
        .await
        .ok()?
        .ok()?;
    let request = format!("GET {path} HTTP/1.1\r\nHost: 127.0.0.1\r\nConnection: close\r\n\r\n");
    stream.write_all(request.as_bytes()).await.ok()?;
    let mut raw = Vec::new();
    timeout(HTTP_TIMEOUT, stream.read_to_end(&mut raw))
        .await
        .ok()?
        .ok()?;
    let text = String::from_utf8_lossy(&raw);
    let (head, body) = text.split_once("\r\n\r\n")?;
    let status = head.split_whitespace().nth(1)?.parse().ok()?;
    Some((status, body.to_string()))
}

/// Poll `/metrics` every 50 ms until it contains `needle`. Panics after
/// `within`, showing the lines for the needle's metric name.
pub async fn wait_metric(port: u16, needle: &str, within: Duration) {
    let deadline = Instant::now() + within;
    let mut last = String::new();
    loop {
        if let Some((_, body)) = http_get(port, "/metrics").await {
            if body.contains(needle) {
                return;
            }
            last = body;
        }
        if Instant::now() > deadline {
            let name = needle.split(['{', ' ']).next().unwrap_or(needle);
            let related: Vec<&str> = last.lines().filter(|l| l.starts_with(name)).collect();
            panic!("no `{needle}` in /metrics within {within:?}; {name} lines: {related:#?}");
        }
        sleep(POLL).await;
    }
}

/// The value of the series written exactly as `name_and_labels` (labels in
/// the order the router renders them), or `None` when it is absent.
pub async fn metric_value(port: u16, name_and_labels: &str) -> Option<f64> {
    let (_, body) = http_get(port, "/metrics").await?;
    body.lines().find_map(|line| {
        let rest = line.strip_prefix(name_and_labels)?.strip_prefix(' ')?;
        rest.split_whitespace().next()?.parse().ok()
    })
}

/// Poll `/coverage` every 50 ms until `pred` holds for its JSON. Panics after
/// `within`, showing the last body.
pub async fn wait_coverage(port: u16, pred: impl Fn(&Value) -> bool, within: Duration) {
    let deadline = Instant::now() + within;
    let mut last = String::new();
    loop {
        if let Some((_, body)) = http_get(port, "/coverage").await {
            if serde_json::from_str::<Value>(&body).is_ok_and(|v| pred(&v)) {
                return;
            }
            last = body;
        }
        if Instant::now() > deadline {
            panic!("/coverage did not match within {within:?}; last body: {last}");
        }
        sleep(POLL).await;
    }
}

/// Connect a WebSocket client, failing after 5 s.
pub async fn ws_connect(url: &str) -> (WsWriter, WsReader) {
    let (ws, _) = timeout(Duration::from_secs(5), connect_async(url))
        .await
        .unwrap_or_else(|_| panic!("connect to {url} timed out"))
        .unwrap_or_else(|e| panic!("connect to {url} failed: {e}"));
    ws.split()
}

/// The next message. Panics when none arrives within `within`, or when the
/// socket ends or fails first.
pub async fn read_msg(r: &mut WsReader, within: Duration) -> Message {
    match timeout(within, r.next()).await {
        Ok(Some(Ok(msg))) => msg,
        Ok(Some(Err(e))) => panic!("WebSocket read failed: {e}"),
        Ok(None) => panic!("WebSocket stream ended"),
        Err(_) => panic!("no message within {within:?}"),
    }
}

/// Collect up to `n` block results within `within` in total.
///
/// The ABI text, status results, pings and pongs are skipped; any other binary
/// frame panics. Returns early with the Close frame when one arrives (code
/// 1005 when it carried none), or without one when the time is up or the
/// socket drops.
pub async fn read_blocks(
    r: &mut WsReader,
    n: usize,
    within: Duration,
) -> (Vec<BlockView>, Option<CloseFrame>) {
    let deadline = tokio::time::Instant::now() + within;
    let mut blocks = Vec::with_capacity(n);
    while blocks.len() < n {
        let item = match timeout_at(deadline, r.next()).await {
            Ok(Some(item)) => item,
            Ok(None) | Err(_) => break,
        };
        match item {
            Ok(Message::Binary(data)) => match BlockView::parse(&data) {
                Some(block) => blocks.push(block),
                None if StatusView::parse(&data).is_some() => {}
                None => panic!(
                    "unexpected binary frame ({} bytes): {:02x?}",
                    data.len(),
                    &data[..data.len().min(16)]
                ),
            },
            Ok(Message::Close(frame)) => {
                let frame = frame.unwrap_or(CloseFrame {
                    code: CloseCode::Status,
                    reason: "".into(),
                });
                return (blocks, Some(frame));
            }
            Ok(_) => {}
            Err(_) => break,
        }
    }
    (blocks, None)
}
