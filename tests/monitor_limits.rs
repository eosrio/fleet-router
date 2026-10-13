//! Monitor limits: connect/read timeouts, offline-on-read-error, and a single
//! in-flight `get_status_request` (Spring 1.2.3 drops sessions past 100 queued).
//!
//! Run: cargo test --test monitor_limits
use std::io::{Read, Write};
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use mock_ship::{MockShipConfig, MockShipServer, StatusFault};
use tokio::net::TcpListener;
use tokio::time::sleep;

fn find_binary() -> String {
    env!("CARGO_BIN_EXE_fleet-router").to_string()
}

fn free_port() -> u16 {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    listener.local_addr().unwrap().port()
}

fn write_config(json: &serde_json::Value) -> tempfile::NamedTempFile {
    let mut f = tempfile::NamedTempFile::new().unwrap();
    f.write_all(json.to_string().as_bytes()).unwrap();
    f.flush().unwrap();
    f
}

struct ChildGuard(Child);
impl Drop for ChildGuard {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

struct Router {
    // Process handle; dropped (and killed) with the fixture.
    _child: ChildGuard,
    _config: tempfile::NamedTempFile,
}

fn spawn_router(json: &serde_json::Value) -> Router {
    let config = write_config(json);
    let child = Command::new(find_binary())
        .arg("run")
        .arg("--config")
        .arg(config.path())
        .spawn()
        .expect("failed to spawn fleet-router");
    Router {
        _child: ChildGuard(child),
        _config: config,
    }
}

fn router_config(
    listen_port: u16,
    metrics_port: u16,
    endpoint: &str,
    extra: serde_json::Value,
) -> serde_json::Value {
    let mut cfg = serde_json::json!({
        "listen_address": "127.0.0.1",
        "listen_port": listen_port,
        "upstream_reconnect_ms": 1000,
        "upstream_monitoring_ms": 5000,
        "upstream_status_ms": 1000,
        "metrics_address": "127.0.0.1",
        "metrics_port": metrics_port,
        "servers": [
            { "name": "mock", "endpoint": endpoint, "enabled": true }
        ]
    });
    if let (Some(obj), Some(extra_obj)) = (cfg.as_object_mut(), extra.as_object()) {
        for (k, v) in extra_obj {
            obj.insert(k.clone(), v.clone());
        }
    }
    cfg
}

fn http_client() -> reqwest::Client {
    reqwest::Client::builder()
        .timeout(Duration::from_secs(2))
        .build()
        .unwrap()
}

async fn get_status(client: &reqwest::Client, url: &str) -> Option<u16> {
    client
        .get(url)
        .send()
        .await
        .ok()
        .map(|r| r.status().as_u16())
}

async fn wait_health(client: &reqwest::Client, base: &str) -> bool {
    let url = format!("{base}/health");
    let start = Instant::now();
    while start.elapsed() < Duration::from_secs(8) {
        if get_status(client, &url).await == Some(200) {
            return true;
        }
        sleep(Duration::from_millis(50)).await;
    }
    false
}

/// Run `fleet-router config test` and return its combined output.
/// Fails if the process is still running after `limit`.
async fn config_test(path: &std::path::Path, limit: Duration) -> (bool, String) {
    let mut child = Command::new(find_binary())
        .arg("config")
        .arg("test")
        .arg(path)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("failed to spawn fleet-router config test");
    let mut stdout = child.stdout.take().unwrap();
    let mut stderr = child.stderr.take().unwrap();
    let out_thread = std::thread::spawn(move || {
        let mut buf = Vec::new();
        stdout.read_to_end(&mut buf).ok();
        String::from_utf8_lossy(&buf).into_owned()
    });
    let err_thread = std::thread::spawn(move || {
        let mut buf = Vec::new();
        stderr.read_to_end(&mut buf).ok();
        String::from_utf8_lossy(&buf).into_owned()
    });

    let start = Instant::now();
    let status = loop {
        match child.try_wait() {
            Ok(Some(status)) => break status,
            Ok(None) if start.elapsed() > limit => {
                let _ = child.kill();
                let _ = child.wait();
                let out = out_thread.join().unwrap_or_default();
                let err = err_thread.join().unwrap_or_default();
                panic!("config test did not exit within {limit:?}\nstdout:\n{out}\nstderr:\n{err}");
            }
            Ok(None) => sleep(Duration::from_millis(30)).await,
            Err(e) => panic!("wait failed: {e}"),
        }
    };
    let out = out_thread.join().unwrap_or_default();
    let err = err_thread.join().unwrap_or_default();
    (status.success(), format!("{out}{err}"))
}

/// Accept TCP connections and hold them open without sending a byte, so a
/// WebSocket handshake blocks until the caller's timeout.
async fn blackhole() -> (String, Arc<AtomicUsize>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = listener.local_addr().unwrap().to_string();
    let accepts = Arc::new(AtomicUsize::new(0));
    let counter = accepts.clone();
    tokio::spawn(async move {
        while let Ok((stream, _)) = listener.accept().await {
            counter.fetch_add(1, Ordering::Relaxed);
            tokio::spawn(async move {
                let _stream = stream;
                std::future::pending::<()>().await;
            });
        }
    });
    tokio::task::yield_now().await;
    (endpoint, accepts)
}

/// Complete the WebSocket handshake, then send nothing.
async fn silent_websocket() -> String {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = listener.local_addr().unwrap().to_string();
    tokio::spawn(async move {
        let (stream, _) = listener.accept().await.expect("accept");
        let ws = tokio_tungstenite::accept_async(stream)
            .await
            .expect("handshake");
        let _ws = ws;
        std::future::pending::<()>().await;
    });
    tokio::task::yield_now().await;
    endpoint
}

#[tokio::test]
async fn monitor_connect_timeout_retries_and_stays_offline() {
    let (endpoint, accepts) = blackhole().await;
    let listen_port = free_port();
    let metrics_port = free_port();
    let cfg = router_config(
        listen_port,
        metrics_port,
        &endpoint,
        serde_json::json!({
            "handshake_timeout_ms": 400,
            "upstream_reconnect_ms": 200,
            "upstream_status_ms": 1000
        }),
    );
    let _router = spawn_router(&cfg);
    let client = http_client();
    let base = format!("http://127.0.0.1:{metrics_port}");
    assert!(
        wait_health(&client, &base).await,
        "health endpoint did not come up while an upstream connect was hanging"
    );

    let start = Instant::now();
    while accepts.load(Ordering::Relaxed) < 2 && start.elapsed() < Duration::from_secs(6) {
        // The process must stay responsive between retries.
        assert_eq!(
            get_status(&client, &format!("{base}/health")).await,
            Some(200),
            "router stopped answering /health during connect timeout"
        );
        sleep(Duration::from_millis(50)).await;
    }
    assert!(
        accepts.load(Ordering::Relaxed) >= 2,
        "monitor connect did not time out and retry (accepts = {})",
        accepts.load(Ordering::Relaxed)
    );
    assert_eq!(
        get_status(&client, &format!("{base}/ready")).await,
        Some(503),
        "hung upstream should stay offline"
    );
}

#[tokio::test]
async fn read_error_marks_upstream_offline() {
    // ABI is ~61 KiB. The limit sits above that and below the fault frame, so
    // a successful status (online) is followed by a monitor read error.
    let mock = MockShipServer::new(MockShipConfig {
        head_block: 1000,
        lib_block: 990,
        status_fault: Some(StatusFault::Oversized {
            after_replies: 1,
            frame_bytes: 256 * 1024,
            delay: Duration::from_millis(800),
        }),
        ..Default::default()
    })
    .await;
    let endpoint = mock.endpoint();
    let session = tokio::spawn(async move {
        mock.handle_one_connection().await;
    });

    let listen_port = free_port();
    let metrics_port = free_port();
    let cfg = router_config(
        listen_port,
        metrics_port,
        &endpoint,
        serde_json::json!({
            "handshake_timeout_ms": 2000,
            "upstream_reconnect_ms": 2000,
            "upstream_status_ms": 5000,
            "max_message_bytes": 96 * 1024
        }),
    );
    let _router = spawn_router(&cfg);
    let client = http_client();
    let base = format!("http://127.0.0.1:{metrics_port}");
    let ready = format!("{base}/ready");
    assert!(
        wait_health(&client, &base).await,
        "metrics server did not start"
    );

    let start = Instant::now();
    let mut saw_online = false;
    while start.elapsed() < Duration::from_secs(10) {
        if get_status(&client, &ready).await == Some(200) {
            saw_online = true;
            break;
        }
        sleep(Duration::from_millis(40)).await;
    }
    assert!(
        saw_online,
        "upstream never became ready before the fault frame"
    );

    let start = Instant::now();
    let mut saw_offline = false;
    while start.elapsed() < Duration::from_secs(8) {
        if get_status(&client, &ready).await == Some(503) {
            saw_offline = true;
            break;
        }
        sleep(Duration::from_millis(40)).await;
    }
    assert!(
        saw_offline,
        "read error left the upstream online (/ready stayed 200)"
    );

    // The mock handles a single session and then goes away, so a reconnect
    // cannot mark the upstream online again.
    sleep(Duration::from_millis(400)).await;
    assert_eq!(get_status(&client, &ready).await, Some(503));
    session.abort();
}

#[tokio::test]
async fn one_status_request_in_flight_when_replies_stall() {
    let status_requests = Arc::new(AtomicUsize::new(0));
    let max_per_session = Arc::new(AtomicUsize::new(0));
    let mock = MockShipServer::new(MockShipConfig {
        head_block: 1000,
        lib_block: 990,
        status_fault: Some(StatusFault::Ignore),
        status_requests: Some(status_requests.clone()),
        max_status_per_session: Some(max_per_session.clone()),
        ..Default::default()
    })
    .await;
    let endpoint = mock.endpoint();
    let handle = mock.start();

    let listen_port = free_port();
    let metrics_port = free_port();
    let cfg = router_config(
        listen_port,
        metrics_port,
        &endpoint,
        serde_json::json!({
            "handshake_timeout_ms": 2000,
            "upstream_reconnect_ms": 200,
            "upstream_status_ms": 250
        }),
    );
    let _router = spawn_router(&cfg);
    let client = http_client();
    let base = format!("http://127.0.0.1:{metrics_port}");
    assert!(
        wait_health(&client, &base).await,
        "metrics server did not start"
    );

    let start = Instant::now();
    while status_requests.load(Ordering::Relaxed) < 2 && start.elapsed() < Duration::from_secs(8) {
        sleep(Duration::from_millis(40)).await;
    }
    let total = status_requests.load(Ordering::Relaxed);
    let max_session = max_per_session.load(Ordering::Relaxed);
    assert!(
        total >= 2,
        "expected the monitor to send a status request and retry after the reply timeout, saw {total}"
    );
    assert_eq!(
        max_session, 1,
        "queued {max_session} status requests on one session (total {total}); Spring 1.2.3 allows at most one in flight"
    );
    assert_eq!(
        get_status(&client, &format!("{base}/ready")).await,
        Some(503),
        "missing status replies should leave the upstream offline"
    );
    handle.shutdown().await;
}

#[tokio::test]
async fn config_test_times_out_on_silent_handshake() {
    let (endpoint, _accepts) = blackhole().await;
    let cfg = serde_json::json!({
        "listen_address": "127.0.0.1",
        "listen_port": free_port(),
        "upstream_reconnect_ms": 1000,
        "upstream_monitoring_ms": 1000,
        "upstream_status_ms": 1000,
        "handshake_timeout_ms": 400,
        "servers": [
            { "name": "hung", "endpoint": endpoint, "enabled": true }
        ]
    });
    let file = write_config(&cfg);
    let (_ok, output) = config_test(file.path(), Duration::from_secs(8)).await;
    assert!(
        output.contains("Websocket connect timed out"),
        "expected a connect timeout, got:\n{output}"
    );
    assert!(
        output.contains("No valid servers"),
        "timed-out upstream should not count as valid:\n{output}"
    );
}

#[tokio::test]
async fn config_test_times_out_waiting_for_abi() {
    let endpoint = silent_websocket().await;
    let cfg = serde_json::json!({
        "listen_address": "127.0.0.1",
        "listen_port": free_port(),
        "upstream_reconnect_ms": 1000,
        "upstream_monitoring_ms": 1000,
        "upstream_status_ms": 1000,
        "handshake_timeout_ms": 400,
        "servers": [
            { "name": "silent", "endpoint": endpoint, "enabled": true }
        ]
    });
    let file = write_config(&cfg);
    let (_ok, output) = config_test(file.path(), Duration::from_secs(8)).await;
    assert!(
        output.contains("Timed out waiting for the ABI"),
        "expected a read timeout, got:\n{output}"
    );
}

#[tokio::test]
async fn config_test_accepts_live_upstream() {
    let mock = MockShipServer::new(MockShipConfig {
        head_block: 10,
        lib_block: 9,
        ..Default::default()
    })
    .await;
    let endpoint = mock.endpoint();
    let _handle = mock.start();
    let cfg = serde_json::json!({
        "listen_address": "127.0.0.1",
        "listen_port": free_port(),
        "upstream_reconnect_ms": 1000,
        "upstream_monitoring_ms": 1000,
        "upstream_status_ms": 1000,
        "handshake_timeout_ms": 2000,
        "servers": [
            { "name": "mock", "endpoint": endpoint, "enabled": true }
        ]
    });
    let file = write_config(&cfg);
    let (ok, output) = config_test(file.path(), Duration::from_secs(8)).await;
    assert!(
        output.contains("ABI Received"),
        "expected a successful ABI read, got:\n{output}"
    );
    assert!(ok, "live upstream should count as valid:\n{output}");
}
