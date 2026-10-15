/// End-to-End Integration Tests
///
/// These tests boot fleet-router as a child process against mock SHiP servers
/// and verify the full proxy flow: client → fleet-router → upstream → fleet-router → client.
///
/// Run: cargo test --test e2e_proxy -- --nocapture
use std::io::Write;
use std::process::{Child, Command};
use std::sync::atomic::AtomicU64;
use std::sync::Arc;
use std::time::Duration;

use futures::{SinkExt, StreamExt};
use mock_ship::{MockShipConfig, MockShipServer};
use tokio::time::sleep;
use tokio_tungstenite::connect_async;
use tokio_tungstenite::tungstenite::Message;

// ---------------------------------------------------------------------------
// Test harness
// ---------------------------------------------------------------------------

/// A running fleet-router instance with its config and child process.
struct FleetRouterProcess {
    child: Child,
    listen_port: u16,
    _config_file: tempfile::NamedTempFile,
}

impl FleetRouterProcess {
    /// Start fleet-router with a generated config pointing to the given upstream endpoints.
    async fn start(upstream_endpoints: &[String], status_ms: u64) -> Self {
        Self::launch(upstream_endpoints, status_ms, 1000, None, None).await
    }

    /// Start fleet-router and also bind the HTTP metrics endpoint.
    async fn start_with_metrics(
        upstream_endpoints: &[String],
        status_ms: u64,
        reconnect_ms: u64,
    ) -> (Self, u16) {
        let metrics_port = {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            listener.local_addr().unwrap().port()
        };
        let router = Self::launch(
            upstream_endpoints,
            status_ms,
            reconnect_ms,
            Some(metrics_port),
            None,
        )
        .await;
        (router, metrics_port)
    }

    /// Start fleet-router with shared block-failure quarantine and metrics.
    async fn start_with_quarantine(
        upstream_endpoints: &[String],
        status_ms: u64,
        quarantine: serde_json::Value,
    ) -> (Self, u16) {
        let metrics_port = {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            listener.local_addr().unwrap().port()
        };
        let router = Self::launch(
            upstream_endpoints,
            status_ms,
            500,
            Some(metrics_port),
            Some(quarantine),
        )
        .await;
        (router, metrics_port)
    }

    async fn launch(
        upstream_endpoints: &[String],
        status_ms: u64,
        reconnect_ms: u64,
        metrics_port: Option<u16>,
        quarantine: Option<serde_json::Value>,
    ) -> Self {
        // Find a free port for fleet-router to listen on
        let listen_port = {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            listener.local_addr().unwrap().port()
        };

        // Build config JSON
        let servers: Vec<serde_json::Value> = upstream_endpoints
            .iter()
            .enumerate()
            .map(|(i, ep)| {
                serde_json::json!({
                    "name": format!("mock-upstream-{}", i + 1),
                    "endpoint": ep,
                    "enabled": true
                })
            })
            .collect();

        let mut config = serde_json::json!({
            "listen_address": "127.0.0.1",
            "listen_port": listen_port,
            "upstream_reconnect_ms": reconnect_ms,
            "upstream_monitoring_ms": 2000,
            "upstream_status_ms": status_ms,
            "servers": servers
        });
        if let Some(policy) = quarantine {
            config["block_failure_quarantine"] = policy;
        }
        if let Some(port) = metrics_port {
            config["metrics_address"] = serde_json::json!("127.0.0.1");
            config["metrics_port"] = serde_json::json!(port);
        }

        // Write config to a temp file
        let mut config_file = tempfile::NamedTempFile::new().expect("Failed to create temp file");
        config_file
            .write_all(config.to_string().as_bytes())
            .expect("Failed to write config");
        config_file.flush().unwrap();

        // Find the fleet-router binary
        let binary = Self::find_binary();

        // Start fleet-router
        let child = Command::new(&binary)
            .arg("--config")
            .arg(config_file.path())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .spawn()
            .unwrap_or_else(|e| panic!("Failed to start fleet-router at {:?}: {}", binary, e));

        let instance = Self {
            child,
            listen_port,
            _config_file: config_file,
        };

        // Wait for fleet-router to start listening
        instance.wait_for_ready().await;
        instance
    }

    fn find_binary() -> String {
        // Cargo builds the binary before running this integration test and sets
        // CARGO_BIN_EXE_<name> to its absolute path, with the platform's
        // executable extension (e.g. `.exe` on Windows).
        env!("CARGO_BIN_EXE_fleet-router").to_string()
    }

    fn ws_url(&self) -> String {
        format!("ws://127.0.0.1:{}", self.listen_port)
    }

    /// Wait until fleet-router is accepting connections.
    async fn wait_for_ready(&self) {
        let addr = format!("127.0.0.1:{}", self.listen_port);
        for attempt in 0..50 {
            if tokio::net::TcpStream::connect(&addr).await.is_ok() {
                return;
            }
            if attempt % 10 == 0 && attempt > 0 {
                eprintln!(
                    "[e2e] Waiting for fleet-router on port {} (attempt {})",
                    self.listen_port, attempt
                );
            }
            sleep(Duration::from_millis(100)).await;
        }
        panic!(
            "fleet-router did not start listening on port {} within 5s",
            self.listen_port
        );
    }
}

impl Drop for FleetRouterProcess {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

// ---------------------------------------------------------------------------
// WebSocket helpers
// ---------------------------------------------------------------------------

type WsWriter = futures::stream::SplitSink<
    tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>,
    Message,
>;
type WsReader = futures::stream::SplitStream<
    tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>,
>;

async fn ws_connect(url: &str) -> (WsWriter, WsReader) {
    let (ws, _) = connect_async(url).await.expect("WS connect failed");
    ws.split()
}

async fn read_one(reader: &mut WsReader) -> Message {
    reader
        .next()
        .await
        .expect("Stream ended unexpectedly")
        .expect("Read error")
}

fn build_status_request() -> Vec<u8> {
    vec![0u8]
}

fn build_blocks_request(start: u32, end: u32, max_in_flight: u32) -> Vec<u8> {
    let mut buf = vec![1u8];
    buf.extend_from_slice(&start.to_le_bytes());
    buf.extend_from_slice(&end.to_le_bytes());
    buf.extend_from_slice(&max_in_flight.to_le_bytes());
    buf.push(0); // have_positions
    buf.push(0); // irreversible_only
    buf.push(0); // fetch_block
    buf.push(0); // fetch_traces
    buf.push(0); // fetch_deltas
    buf
}

fn build_blocks_request_v1(
    start: u32,
    end: u32,
    max_in_flight: u32,
    fetch_finality: bool,
) -> Vec<u8> {
    let mut buf = vec![3u8]; // get_blocks_request_v1
    buf.extend_from_slice(&start.to_le_bytes());
    buf.extend_from_slice(&end.to_le_bytes());
    buf.extend_from_slice(&max_in_flight.to_le_bytes());
    buf.push(0); // have_positions
    buf.push(0); // irreversible_only
    buf.push(0); // fetch_block
    buf.push(0); // fetch_traces
    buf.push(0); // fetch_deltas
    buf.push(u8::from(fetch_finality));
    buf
}

async fn wait_for_metric(port: u16, needle: &str) {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let addr = format!("127.0.0.1:{port}");
    for _ in 0..150 {
        if let Ok(mut stream) = tokio::net::TcpStream::connect(&addr).await {
            let req = b"GET /metrics HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n";
            if stream.write_all(req).await.is_ok() {
                let mut buf = Vec::new();
                if stream.read_to_end(&mut buf).await.is_ok()
                    && String::from_utf8_lossy(&buf).contains(needle)
                {
                    return;
                }
            }
        }
        sleep(Duration::from_millis(100)).await;
    }
    panic!("timed out waiting for metric containing {needle}");
}

fn build_ack_request(num_messages: u32) -> Vec<u8> {
    let mut buf = vec![2u8];
    buf.extend_from_slice(&num_messages.to_le_bytes());
    buf
}

// ===========================================================================
// E2E TEST 1: Client connects through proxy and receives ABI
// ===========================================================================

#[tokio::test]
async fn e2e_proxy_receives_abi() {
    // Start mock upstream
    let mock = MockShipServer::new(MockShipConfig::default()).await;
    let mock_endpoint = mock.endpoint();
    let handle = mock.start();

    // Start fleet-router pointing at the mock
    let router = FleetRouterProcess::start(&[mock_endpoint], 2000).await;

    // Give fleet-router time to connect to upstream and receive ABI
    sleep(Duration::from_secs(2)).await;

    // Connect client through the proxy
    let (_writer, mut reader) = ws_connect(&router.ws_url()).await;

    // First message should be ABI JSON forwarded from upstream
    let msg = read_one(&mut reader).await;
    assert!(
        matches!(&msg, Message::Text(_)),
        "Expected Text ABI frame through proxy, got: {:?}",
        msg
    );

    if let Message::Text(text) = msg {
        let abi: serde_json::Value =
            serde_json::from_str(&text).expect("Proxied ABI is not valid JSON");
        assert_eq!(abi["version"], "eosio::abi/1.1");
    }

    drop(_writer);
    drop(reader);
    drop(router);
    handle.shutdown().await;
}

// ===========================================================================
// E2E TEST 2: Client sends status request through proxy
// ===========================================================================

#[tokio::test]
async fn e2e_proxy_status_request() {
    let config = MockShipConfig {
        head_block: 5000,
        lib_block: 4990,
        ..Default::default()
    };
    let mock = MockShipServer::new(config).await;
    let mock_endpoint = mock.endpoint();
    let handle = mock.start();

    let router = FleetRouterProcess::start(&[mock_endpoint], 2000).await;
    sleep(Duration::from_secs(2)).await;

    let (mut writer, mut reader) = ws_connect(&router.ws_url()).await;

    // Read ABI
    let _ = read_one(&mut reader).await;

    // Send status request through proxy
    writer
        .send(Message::Binary(build_status_request().into()))
        .await
        .unwrap();

    // Read proxied status result
    let msg = read_one(&mut reader).await;
    assert!(matches!(&msg, Message::Binary(_)), "Expected binary status");

    if let Message::Binary(data) = msg {
        // Should be deserialized and re-serialized by fleet-router,
        // but the variant index and head block should be preserved
        assert_eq!(data[0], 0, "Variant should be 0 (status_result_v0)");
        let head = u32::from_le_bytes(data[1..5].try_into().unwrap());
        assert_eq!(head, 5000, "Head block should be 5000");
    }

    drop(writer);
    drop(reader);
    drop(router);
    handle.shutdown().await;
}

// ===========================================================================
// E2E TEST 3: Client receives blocks through proxy
// ===========================================================================

#[tokio::test]
async fn e2e_proxy_block_streaming() {
    let config = MockShipConfig {
        head_block: 1000,
        lib_block: 990,
        blocks_to_stream: 0,
        ..Default::default()
    };
    let mock = MockShipServer::new(config).await;
    let mock_endpoint = mock.endpoint();
    let handle = mock.start();

    let router = FleetRouterProcess::start(&[mock_endpoint], 2000).await;
    sleep(Duration::from_secs(2)).await;

    let (mut writer, mut reader) = ws_connect(&router.ws_url()).await;

    // Read ABI
    let _ = read_one(&mut reader).await;

    // Request 5 blocks
    writer
        .send(Message::Binary(build_blocks_request(100, 105, 5).into()))
        .await
        .unwrap();

    let mut received: Vec<u32> = Vec::new();
    for _ in 0..5 {
        let msg = read_one(&mut reader).await;
        if let Message::Binary(data) = msg {
            assert_eq!(data[0], 1, "Expected blocks_result_v0");
            assert_eq!(data[73], 1, "this_block should be present");
            let block_num = u32::from_le_bytes(data[74..78].try_into().unwrap());
            received.push(block_num);
        }
    }

    assert_eq!(received, vec![100, 101, 102, 103, 104]);

    drop(writer);
    drop(reader);
    drop(router);
    handle.shutdown().await;
}

// ===========================================================================
// E2E TEST 4: Multiple clients through the same proxy
// ===========================================================================

#[tokio::test]
async fn e2e_proxy_multiple_clients() {
    let config = MockShipConfig {
        head_block: 2000,
        lib_block: 1990,
        blocks_to_stream: 0,
        ..Default::default()
    };
    let mock = MockShipServer::new(config).await;
    let mock_endpoint = mock.endpoint();
    let handle = mock.start();

    let router = FleetRouterProcess::start(&[mock_endpoint], 2000).await;
    sleep(Duration::from_secs(2)).await;

    // Connect 3 clients simultaneously
    let mut clients = Vec::new();
    for _ in 0..3 {
        let (writer, mut reader) = ws_connect(&router.ws_url()).await;
        // Each client should receive ABI
        let msg = read_one(&mut reader).await;
        assert!(matches!(msg, Message::Text(_)), "Expected ABI");
        clients.push((writer, reader));
    }

    // Each client sends a status request
    for (writer, reader) in clients.iter_mut() {
        writer
            .send(Message::Binary(build_status_request().into()))
            .await
            .unwrap();
        let msg = read_one(reader).await;
        if let Message::Binary(data) = msg {
            let head = u32::from_le_bytes(data[1..5].try_into().unwrap());
            assert_eq!(head, 2000);
        }
    }

    drop(clients);
    drop(router);
    handle.shutdown().await;
}

// ===========================================================================
// E2E TEST 5: Multiple upstreams (load balancing)
// ===========================================================================

#[tokio::test]
async fn e2e_proxy_multiple_upstreams() {
    // Start 2 mock upstreams with different head blocks
    let mock1 = MockShipServer::new(MockShipConfig {
        head_block: 3000,
        lib_block: 2990,
        blocks_to_stream: 0,
        ..Default::default()
    })
    .await;
    let mock2 = MockShipServer::new(MockShipConfig {
        head_block: 4000,
        lib_block: 3990,
        blocks_to_stream: 0,
        ..Default::default()
    })
    .await;

    let ep1 = mock1.endpoint();
    let ep2 = mock2.endpoint();
    let h1 = mock1.start();
    let h2 = mock2.start();

    let router = FleetRouterProcess::start(&[ep1, ep2], 2000).await;
    sleep(Duration::from_secs(2)).await;

    // Connect multiple clients — they should be distributed across upstreams
    let mut heads_seen = std::collections::HashSet::new();
    for _ in 0..6 {
        let (mut writer, mut reader) = ws_connect(&router.ws_url()).await;
        let _ = read_one(&mut reader).await; // ABI
        writer
            .send(Message::Binary(build_status_request().into()))
            .await
            .unwrap();
        let msg = read_one(&mut reader).await;
        if let Message::Binary(data) = msg {
            let head = u32::from_le_bytes(data[1..5].try_into().unwrap());
            heads_seen.insert(head);
        }
        drop(writer);
        drop(reader);
        // Small delay to let the connection fully close
        sleep(Duration::from_millis(100)).await;
    }

    // We should have seen at least one of the two head block values
    assert!(
        !heads_seen.is_empty(),
        "Should have received at least one status response"
    );
    // Ideally both upstreams are used, but with only 6 connections
    // the load balancer might not distribute perfectly.
    // At minimum, every head we saw should be one of our mock values.
    for &head in &heads_seen {
        assert!(
            head == 3000 || head == 4000,
            "Unexpected head block {}: expected 3000 or 4000",
            head
        );
    }

    println!("  Heads seen from load balancing: {:?}", heads_seen);

    drop(router);
    h1.shutdown().await;
    h2.shutdown().await;
}

// ===========================================================================
// E2E TEST 6: Flow control through proxy (credits/ack)
// ===========================================================================

#[tokio::test]
async fn e2e_proxy_flow_control() {
    let config = MockShipConfig {
        head_block: 1000,
        lib_block: 990,
        blocks_to_stream: 0,
        ..Default::default()
    };
    let mock = MockShipServer::new(config).await;
    let mock_endpoint = mock.endpoint();
    let handle = mock.start();

    let router = FleetRouterProcess::start(&[mock_endpoint], 2000).await;
    sleep(Duration::from_secs(2)).await;

    let (mut writer, mut reader) = ws_connect(&router.ws_url()).await;

    // Read ABI
    let _ = read_one(&mut reader).await;

    // Request blocks with max_in_flight=2
    writer
        .send(Message::Binary(build_blocks_request(10, 100, 2).into()))
        .await
        .unwrap();

    // Read 2 blocks
    let mut blocks = Vec::new();
    for _ in 0..2 {
        let msg = read_one(&mut reader).await;
        if let Message::Binary(data) = msg {
            blocks.push(u32::from_le_bytes(data[74..78].try_into().unwrap()));
        }
    }
    assert_eq!(blocks, vec![10, 11]);

    // Send ack for 3 more
    writer
        .send(Message::Binary(build_ack_request(3).into()))
        .await
        .unwrap();

    // Should get 3 more blocks
    for expected in [12, 13, 14] {
        let msg = read_one(&mut reader).await;
        if let Message::Binary(data) = msg {
            let num = u32::from_le_bytes(data[74..78].try_into().unwrap());
            assert_eq!(num, expected);
        }
    }

    drop(writer);
    drop(reader);
    drop(router);
    handle.shutdown().await;
}

// ===========================================================================
// E2E TEST 7: Load balancing distribution
// ===========================================================================

#[tokio::test]
async fn e2e_proxy_load_balancing_distribution() {
    // Start 2 upstreams with different head blocks to fingerprint them
    let mock1 = MockShipServer::new(MockShipConfig {
        head_block: 3000,
        lib_block: 2990,
        blocks_to_stream: 0,
        ..Default::default()
    })
    .await;
    let mock2 = MockShipServer::new(MockShipConfig {
        head_block: 4000,
        lib_block: 3990,
        blocks_to_stream: 0,
        ..Default::default()
    })
    .await;

    let ep1 = mock1.endpoint();
    let ep2 = mock2.endpoint();
    let h1 = mock1.start();
    let h2 = mock2.start();

    let router = FleetRouterProcess::start(&[ep1, ep2], 2000).await;
    sleep(Duration::from_secs(2)).await;

    // Hold all connections open so the load balancer sees actual connection counts.
    // This forces it to distribute across upstreams (least-connections algorithm).
    let num_clients: u32 = 20;
    let mut clients = Vec::new();

    for _ in 0..num_clients {
        let (mut writer, mut reader) = ws_connect(&router.ws_url()).await;
        let _ = read_one(&mut reader).await; // ABI
        writer
            .send(Message::Binary(build_status_request().into()))
            .await
            .unwrap();
        let msg = read_one(&mut reader).await;
        let head = if let Message::Binary(data) = msg {
            u32::from_le_bytes(data[1..5].try_into().unwrap())
        } else {
            0
        };
        clients.push((writer, reader, head));
        // Small delay so fleet-router registers the connection before next one
        sleep(Duration::from_millis(50)).await;
    }

    let count_3000 = clients.iter().filter(|(_, _, h)| *h == 3000).count() as u32;
    let count_4000 = clients.iter().filter(|(_, _, h)| *h == 4000).count() as u32;

    println!(
        "  Distribution: upstream-3000={}, upstream-4000={} (of {})",
        count_3000, count_4000, num_clients
    );

    // Drop all clients
    drop(clients);

    // Both upstreams must have received at least 25% of connections
    let min_expected = num_clients / 4;
    assert!(
        count_3000 >= min_expected,
        "Upstream 3000 got {} connections (expected >= {})",
        count_3000,
        min_expected
    );
    assert!(
        count_4000 >= min_expected,
        "Upstream 4000 got {} connections (expected >= {})",
        count_4000,
        min_expected
    );

    drop(router);
    h1.shutdown().await;
    h2.shutdown().await;
}

// ===========================================================================
// E2E TEST 8: Failover on upstream shutdown
// ===========================================================================

#[tokio::test]
async fn e2e_proxy_failover() {
    // Start 2 upstreams
    let mock1 = MockShipServer::new(MockShipConfig {
        head_block: 5000,
        lib_block: 4990,
        blocks_to_stream: 0,
        ..Default::default()
    })
    .await;
    let mock2 = MockShipServer::new(MockShipConfig {
        head_block: 6000,
        lib_block: 5990,
        blocks_to_stream: 0,
        ..Default::default()
    })
    .await;

    let ep1 = mock1.endpoint();
    let ep2 = mock2.endpoint();
    let h1 = mock1.start();
    let h2 = mock2.start();

    let router = FleetRouterProcess::start(&[ep1, ep2], 2000).await;
    sleep(Duration::from_secs(2)).await;

    // Verify both upstreams are initially reachable
    let mut seen_before = std::collections::HashSet::new();
    for _ in 0..6 {
        let (mut w, mut r) = ws_connect(&router.ws_url()).await;
        let _ = read_one(&mut r).await;
        w.send(Message::Binary(build_status_request().into()))
            .await
            .unwrap();
        if let Message::Binary(data) = read_one(&mut r).await {
            seen_before.insert(u32::from_le_bytes(data[1..5].try_into().unwrap()));
        }
        drop(w);
        drop(r);
        sleep(Duration::from_millis(50)).await;
    }
    println!("  Before failover: heads seen = {:?}", seen_before);

    // Kill upstream 1 (head=5000)
    h1.shutdown().await;
    println!("  Upstream 5000 shut down");

    // Wait for fleet-router to detect the failure
    sleep(Duration::from_secs(3)).await;

    // New connections should all go to surviving upstream (head=6000)
    let mut successes = 0;
    for _ in 0..5 {
        if let Ok((ws, _)) = connect_async(&router.ws_url()).await {
            let (mut writer, mut reader) = ws.split();
            let _ = read_one(&mut reader).await; // ABI
            writer
                .send(Message::Binary(build_status_request().into()))
                .await
                .unwrap();
            if let Message::Binary(data) = read_one(&mut reader).await {
                let head = u32::from_le_bytes(data[1..5].try_into().unwrap());
                assert_eq!(
                    head, 6000,
                    "After failover, should route to surviving upstream"
                );
                successes += 1;
            }
            drop(writer);
            drop(reader);
        }
        sleep(Duration::from_millis(100)).await;
    }

    println!(
        "  After failover: {} successful connections to upstream 6000",
        successes
    );
    assert!(
        successes >= 3,
        "Expected at least 3 successful connections after failover, got {}",
        successes
    );

    drop(router);
    h2.shutdown().await;
}

// ===========================================================================
// E2E TEST 9: Sustained block streaming (verify order, no gaps)
// ===========================================================================

#[tokio::test]
async fn e2e_proxy_sustained_streaming() {
    let total_blocks = 500u32;
    let num_clients = 3;

    let mock = MockShipServer::new(MockShipConfig {
        head_block: total_blocks + 100,
        lib_block: total_blocks + 90,
        blocks_to_stream: 0, // unlimited
        ..Default::default()
    })
    .await;
    let mock_endpoint = mock.endpoint();
    let handle = mock.start();

    let router = FleetRouterProcess::start(&[mock_endpoint], 2000).await;
    sleep(Duration::from_secs(2)).await;

    // Spawn concurrent clients that each stream a range of blocks
    let mut join_handles = Vec::new();
    for client_id in 0..num_clients {
        let url = router.ws_url();
        let start_block = 1u32;
        let blocks = total_blocks;
        let h = tokio::spawn(async move {
            let (mut writer, mut reader) = ws_connect(&url).await;
            let _ = read_one(&mut reader).await; // ABI

            writer
                .send(Message::Binary(
                    build_blocks_request(start_block, start_block + blocks, blocks).into(),
                ))
                .await
                .unwrap();

            let mut received = Vec::with_capacity(blocks as usize);
            for _ in 0..blocks {
                let msg = read_one(&mut reader).await;
                if let Message::Binary(data) = msg {
                    assert_eq!(data[0], 1, "Expected blocks_result_v0 variant");
                    if data[73] == 1 {
                        // this_block present
                        let block_num = u32::from_le_bytes(data[74..78].try_into().unwrap());
                        received.push(block_num);
                    }
                }
            }

            // Verify ordering: blocks should arrive in sequence
            for window in received.windows(2) {
                assert!(
                    window[1] > window[0],
                    "Client {}: blocks out of order: {} followed by {}",
                    client_id,
                    window[0],
                    window[1]
                );
            }

            (client_id, received.len())
        });
        join_handles.push(h);
    }

    let start = std::time::Instant::now();
    let mut total_received = 0;
    for h in join_handles {
        let (client_id, count) = h.await.unwrap();
        println!("  Client {}: received {} blocks", client_id, count);
        total_received += count;
    }
    let elapsed = start.elapsed();

    println!(
        "  Sustained: {} total blocks across {} clients in {:.2}s",
        total_received,
        num_clients,
        elapsed.as_secs_f64()
    );

    assert_eq!(
        total_received,
        (num_clients * total_blocks as usize),
        "Not all blocks received"
    );

    drop(router);
    handle.shutdown().await;
}

// ===========================================================================
// E2E TEST 10: Concurrent client scaling (50 clients × 2 upstreams)
// ===========================================================================

#[tokio::test]
async fn e2e_proxy_concurrent_scaling() {
    let num_clients = 50;
    let blocks_per_client = 20u32;

    // 2 upstreams with data payloads
    let mock1 = MockShipServer::new(MockShipConfig {
        head_block: 7000,
        lib_block: 6990,
        blocks_to_stream: 0,
        block_data_size: 1024, // 1KB per block
        ..Default::default()
    })
    .await;
    let mock2 = MockShipServer::new(MockShipConfig {
        head_block: 8000,
        lib_block: 7990,
        blocks_to_stream: 0,
        block_data_size: 1024,
        ..Default::default()
    })
    .await;

    let ep1 = mock1.endpoint();
    let ep2 = mock2.endpoint();
    let h1 = mock1.start();
    let h2 = mock2.start();

    let router = FleetRouterProcess::start(&[ep1, ep2], 2000).await;
    sleep(Duration::from_secs(2)).await;

    let total_bytes = Arc::new(AtomicU64::new(0));
    let success_count = Arc::new(std::sync::atomic::AtomicU32::new(0));

    let mut join_handles = Vec::new();
    for client_id in 0..num_clients {
        let url = router.ws_url();
        let bytes_counter = total_bytes.clone();
        let successes = success_count.clone();
        let h = tokio::spawn(async move {
            let (mut writer, mut reader) = ws_connect(&url).await;
            let _ = read_one(&mut reader).await; // ABI

            writer
                .send(Message::Binary(
                    build_blocks_request(1, 1 + blocks_per_client, blocks_per_client).into(),
                ))
                .await
                .unwrap();

            for _ in 0..blocks_per_client {
                let msg = read_one(&mut reader).await;
                if let Message::Binary(d) = msg {
                    bytes_counter.fetch_add(d.len() as u64, std::sync::atomic::Ordering::Relaxed);
                } else {
                    panic!("Client {} expected binary, got {:?}", client_id, msg);
                }
            }

            successes.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        });
        join_handles.push(h);
    }

    let start = std::time::Instant::now();
    for h in join_handles {
        h.await.unwrap();
    }
    let elapsed = start.elapsed();

    let total = total_bytes.load(std::sync::atomic::Ordering::Relaxed);
    let completed = success_count.load(std::sync::atomic::Ordering::Relaxed);

    println!(
        "  Scaling: {}/{} clients completed, {} blocks each, {:.1} KB total in {:.2}s",
        completed,
        num_clients,
        blocks_per_client,
        total as f64 / 1024.0,
        elapsed.as_secs_f64()
    );

    assert_eq!(
        completed, num_clients as u32,
        "Not all clients completed successfully"
    );

    drop(router);
    h1.shutdown().await;
    h2.shutdown().await;
}

// ===========================================================================
// E2E TEST 11: Range-aware failover routing
// ===========================================================================

/// Two upstreams with non-overlapping trace ranges:
/// - Upstream A: blocks 1..5001 (head=5000)  — disconnects after 3 blocks
/// - Upstream B: blocks 5001..10001 (head=10000)
///
/// Client requests block 500 (in A's range). After A disconnects,
/// failover should see that block ~503 is still in A's range and route
/// to another upstream covering that range. Since only A covers it and A
/// will accept new connections, the client should reconnect to A.
/// Crucially, it should NOT failover to B (which doesn't cover block 503).
///
/// We verify this by checking that the head_block in responses is always 5000.
#[tokio::test]
async fn e2e_proxy_range_aware_routing() {
    // Upstream A: covers blocks 1..5001, disconnects after 3 blocks
    let config_a = MockShipConfig {
        head_block: 5000,
        lib_block: 4990,
        blocks_to_stream: 0,
        disconnect_after: Some(3),
        trace_begin_block: 1,
        trace_end_block: 5001,
        ..Default::default()
    };

    // Upstream B: covers blocks 5001..10001 (does NOT cover block 500)
    let config_b = MockShipConfig {
        head_block: 10000,
        lib_block: 9990,
        blocks_to_stream: 0,
        trace_begin_block: 5001,
        trace_end_block: 10001,
        ..Default::default()
    };

    let mock_a = MockShipServer::new(config_a).await;
    let mock_b = MockShipServer::new(config_b).await;
    let ep_a = mock_a.endpoint();
    let ep_b = mock_b.endpoint();
    let ha = mock_a.start();
    let hb = mock_b.start();

    // Start router with fast status polling (500ms)
    let router = FleetRouterProcess::start(&[ep_a, ep_b], 500).await;

    // Wait for monitoring loop to populate trace ranges
    sleep(Duration::from_secs(3)).await;

    let (mut writer, mut reader) = ws_connect(&router.ws_url()).await;
    let _abi = read_one(&mut reader).await;

    // Request blocks starting at 500 (in A's range only)
    writer
        .send(Message::Binary(build_blocks_request(500, 600, 10).into()))
        .await
        .unwrap();

    // Read blocks — A will disconnect after 3, then failover should reconnect to A (not B)
    let mut heads_seen = std::collections::HashSet::new();
    let mut blocks_received = 0u32;
    let timeout = Duration::from_secs(10);
    let start = std::time::Instant::now();

    loop {
        if start.elapsed() > timeout || blocks_received >= 10 {
            break;
        }
        let msg = tokio::select! {
            msg = reader.next() => {
                match msg {
                    Some(Ok(m)) => m,
                    _ => break,
                }
            }
            _ = sleep(Duration::from_secs(5)) => break,
        };

        match msg {
            Message::Binary(data) => {
                if data.len() >= 5 {
                    let head = u32::from_le_bytes(data[1..5].try_into().unwrap());
                    heads_seen.insert(head);
                    blocks_received += 1;
                }
            }
            Message::Text(_) => continue, // ABI from reconnection
            _ => break,
        }
    }

    println!(
        "  Range routing: heads seen = {:?}, blocks = {}",
        heads_seen, blocks_received
    );

    // All blocks should come from upstream A (head=5000), never from B (head=10000)
    assert!(
        !heads_seen.contains(&10000),
        "Should NOT route to upstream B (head=10000) — it doesn't cover block 500"
    );
    assert!(
        blocks_received >= 3,
        "Should receive at least 3 blocks from upstream A (got {})",
        blocks_received
    );

    drop(writer);
    drop(reader);
    drop(router);
    ha.shutdown().await;
    hb.shutdown().await;
}

// ===========================================================================
// E2E TEST 12: Failover routes to range-valid upstream
// ===========================================================================

/// Three upstreams:
/// - Upstream A: blocks 1..5001 (head=5000)  — will be shut down
/// - Upstream B: blocks 8001..12001 (head=12000) — does NOT cover the requested range
/// - Upstream C: blocks 1..6001 (head=6000) — DOES cover the range, failover target
///
/// Client requests blocks in range covered by A and C. When A goes down,
/// failover should route to C (not B).
#[tokio::test]
async fn e2e_proxy_failover_to_range_valid() {
    let config_a = MockShipConfig {
        head_block: 5000,
        lib_block: 4990,
        blocks_to_stream: 0,
        disconnect_after: Some(5), // Disconnect after 5 blocks to trigger failover
        trace_begin_block: 1,
        trace_end_block: 5001,
        ..Default::default()
    };

    // Upstream B does NOT cover low block range
    let config_b = MockShipConfig {
        head_block: 12000,
        lib_block: 11990,
        blocks_to_stream: 0,
        trace_begin_block: 8001,
        trace_end_block: 12001,
        ..Default::default()
    };

    // Upstream C covers same range as A
    let config_c = MockShipConfig {
        head_block: 6000,
        lib_block: 5990,
        blocks_to_stream: 0,
        trace_begin_block: 1,
        trace_end_block: 6001,
        ..Default::default()
    };

    let mock_a = MockShipServer::new(config_a).await;
    let mock_b = MockShipServer::new(config_b).await;
    let mock_c = MockShipServer::new(config_c).await;
    let ep_a = mock_a.endpoint();
    let ep_b = mock_b.endpoint();
    let ep_c = mock_c.endpoint();
    let ha = mock_a.start();
    let hb = mock_b.start();
    let hc = mock_c.start();

    let router = FleetRouterProcess::start(&[ep_a, ep_b, ep_c], 500).await;

    // Wait for monitoring to populate trace ranges
    sleep(Duration::from_secs(3)).await;

    let (mut writer, mut reader) = ws_connect(&router.ws_url()).await;
    let _abi = read_one(&mut reader).await;

    // Request blocks starting at 100 — should initially go to A or C (both cover this range)
    writer
        .send(Message::Binary(build_blocks_request(100, 1000, 100).into()))
        .await
        .unwrap();

    // Read blocks until failover happens (A disconnects after 5)
    let mut heads_seen = std::collections::HashSet::new();
    let mut blocks_received = 0u32;
    let timeout = Duration::from_secs(10);
    let start = std::time::Instant::now();

    loop {
        if start.elapsed() > timeout {
            break;
        }
        let msg = tokio::select! {
            msg = reader.next() => {
                match msg {
                    Some(Ok(m)) => m,
                    _ => break,
                }
            }
            _ = sleep(Duration::from_secs(5)) => break,
        };

        match msg {
            Message::Binary(data) => {
                if data.len() >= 5 {
                    let head = u32::from_le_bytes(data[1..5].try_into().unwrap());
                    heads_seen.insert(head);
                    blocks_received += 1;
                }
                // Send acks to keep flow going
                if blocks_received.is_multiple_of(10) {
                    let _ = writer
                        .send(Message::Binary(build_ack_request(10).into()))
                        .await;
                }
                if blocks_received >= 20 {
                    break;
                }
            }
            Message::Text(_) => {
                // ABI from new upstream after failover
                continue;
            }
            _ => break,
        }
    }

    println!(
        "  Failover: heads seen = {:?}, blocks = {}",
        heads_seen, blocks_received
    );

    // After failover, we should see head from C (6000) or A (5000), NOT from B (12000)
    assert!(
        !heads_seen.contains(&12000),
        "Failover should NOT route to upstream B (head=12000) which doesn't cover block range 100+"
    );

    // We should have received blocks from at least one valid upstream
    assert!(
        blocks_received >= 5,
        "Should have received at least 5 blocks (got {})",
        blocks_received
    );

    drop(writer);
    drop(reader);
    drop(router);
    ha.shutdown().await;
    hb.shutdown().await;
    hc.shutdown().await;
}

fn finality_end_metric(endpoint: &str, server: &str, end: u32) -> String {
    format!(
        "fleet_router_upstream_finality_data_end_block{{endpoint=\"{endpoint}\",server=\"{server}\"}} {end}"
    )
}

/// Read up to `n` block-result heads, skipping ABI text frames.
async fn read_block_heads(reader: &mut WsReader, n: usize) -> Vec<u32> {
    let mut heads = Vec::new();
    let start = std::time::Instant::now();
    while heads.len() < n && start.elapsed() < Duration::from_secs(10) {
        let msg = match tokio::time::timeout(Duration::from_secs(5), reader.next()).await {
            Ok(Some(Ok(msg))) => msg,
            _ => break,
        };
        match msg {
            Message::Binary(data) if data.len() >= 5 && (data[0] == 1 || data[0] == 2) => {
                heads.push(u32::from_le_bytes(data[1..5].try_into().unwrap()));
            }
            Message::Text(_) => continue,
            _ => break,
        }
    }
    heads
}

// ===========================================================================
// E2E TEST 13: Finality-data requests follow the v1 status range
// ===========================================================================

/// Two upstreams with the same trace range and disjoint finality ranges:
/// - B (head 9000) is the only node up when the client connects, so the session
///   starts there. Its finality range does not cover block 100.
/// - A (head 5000) comes up afterwards and does cover block 100.
///
/// A `get_blocks_request_v1` with `fetch_finality_data` must move to A.
/// A later client that does not ask for finality data stays on least-connections
/// (B), which is the v0 behaviour.
#[tokio::test]
async fn e2e_proxy_routes_finality_requests_by_v1_range() {
    let config_a = MockShipConfig {
        head_block: 5000,
        lib_block: 4990,
        blocks_to_stream: 0,
        trace_begin_block: 1,
        trace_end_block: 20_001,
        emit_status_v1: true,
        finality_data_begin_block: 1,
        finality_data_end_block: 10_001,
        ..Default::default()
    };
    let config_b = MockShipConfig {
        head_block: 9000,
        lib_block: 8990,
        blocks_to_stream: 0,
        trace_begin_block: 1,
        trace_end_block: 20_001,
        emit_status_v1: true,
        finality_data_begin_block: 10_001,
        finality_data_end_block: 20_001,
        ..Default::default()
    };

    // Bind A but do not accept yet, so the router can only mark B online.
    let mock_a = MockShipServer::new(config_a).await;
    let mock_b = MockShipServer::new(config_b).await;
    let ep_a = mock_a.endpoint();
    let ep_b = mock_b.endpoint();
    let hb = mock_b.start();

    let (router, metrics_port) =
        FleetRouterProcess::start_with_metrics(&[ep_b.clone(), ep_a.clone()], 100, 200).await;

    wait_for_metric(
        metrics_port,
        &finality_end_metric(&ep_b, "mock-upstream-1", 20_001),
    )
    .await;

    let (mut writer, mut reader) = ws_connect(&router.ws_url()).await;
    let _abi = read_one(&mut reader).await;

    // A accepts the monitor handshake that has been waiting, then reports v1.
    let ha = mock_a.start();
    wait_for_metric(
        metrics_port,
        &finality_end_metric(&ep_a, "mock-upstream-2", 10_001),
    )
    .await;

    writer
        .send(Message::Binary(
            build_blocks_request_v1(100, 200, 5, true).into(),
        ))
        .await
        .unwrap();

    let heads = read_block_heads(&mut reader, 3).await;
    println!("  Finality route: heads = {:?}", heads);
    assert!(
        !heads.is_empty(),
        "expected blocks from the upstream whose finality range covers block 100"
    );
    assert!(
        heads.iter().all(|head| *head == 5000),
        "fetch_finality_data request must use upstream A (head 5000), got {heads:?}"
    );

    // A now has the live client. A new client is least-connections onto B, and a
    // request that does not ask for finality data must stay there.
    let (mut writer_b, mut reader_b) = ws_connect(&router.ws_url()).await;
    let _abi_b = read_one(&mut reader_b).await;
    writer_b
        .send(Message::Binary(build_blocks_request(100, 200, 5).into()))
        .await
        .unwrap();
    let heads_b = read_block_heads(&mut reader_b, 3).await;
    println!("  Non-finality route: heads = {:?}", heads_b);
    assert!(
        !heads_b.is_empty(),
        "expected blocks from the least-loaded upstream"
    );
    assert!(
        heads_b.iter().all(|head| *head == 9000),
        "request without fetch_finality_data keeps v0 routing (head 9000), got {heads_b:?}"
    );

    drop(writer);
    drop(reader);
    drop(writer_b);
    drop(reader_b);
    drop(router);
    ha.shutdown().await;
    hb.shutdown().await;
}

// ===========================================================================
// E2E TEST 14: Multi-listener network isolation, failover, and observability
// ===========================================================================

/// Single `fleet-router` process configured with two listeners (`eos` and `wax`):
/// - `eos` listener has two redundant mock SHiP nodes (head=5000 and head=5500,
///   first node disconnects after 3 blocks to trigger failover within `eos`).
/// - `wax` listener has one mock SHiP node (head=9000).
///
/// Verifies:
/// 1. Clients on the `eos` port only ever receive blocks from `eos` upstreams
///    (5000 / 5500) and fail over within the `eos` pool, never crossing to `wax`.
/// 2. Clients on the `wax` port only ever receive blocks from `wax` (9000).
/// 3. `/ready`, `/ready/eos`, and `/ready/wax` report 200 OK, `/ready/unknown`
///    reports 404, and `/metrics` carries `listener="eos"` and `listener="wax"`.
#[tokio::test]
async fn e2e_proxy_multi_listener_network_isolation() {
    let eos_mock_1 = MockShipServer::new(MockShipConfig {
        head_block: 5000,
        lib_block: 4990,
        blocks_to_stream: 0,
        disconnect_after: Some(3),
        ..Default::default()
    })
    .await;
    let eos_mock_2 = MockShipServer::new(MockShipConfig {
        head_block: 5500,
        lib_block: 5490,
        blocks_to_stream: 0,
        ..Default::default()
    })
    .await;
    let wax_mock_1 = MockShipServer::new(MockShipConfig {
        head_block: 9000,
        lib_block: 8990,
        blocks_to_stream: 0,
        ..Default::default()
    })
    .await;

    let eos_ep_1 = eos_mock_1.endpoint();
    let eos_ep_2 = eos_mock_2.endpoint();
    let wax_ep_1 = wax_mock_1.endpoint();

    let h_eos_1 = eos_mock_1.start();
    let h_eos_2 = eos_mock_2.start();
    let h_wax_1 = wax_mock_1.start();

    let eos_port = {
        let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        l.local_addr().unwrap().port()
    };
    let wax_port = {
        let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        l.local_addr().unwrap().port()
    };
    let metrics_port = {
        let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        l.local_addr().unwrap().port()
    };

    let config = serde_json::json!({
        "listen_address": "127.0.0.1",
        "upstream_reconnect_ms": 200,
        "upstream_monitoring_ms": 1000,
        "upstream_status_ms": 200,
        "metrics_address": "127.0.0.1",
        "metrics_port": metrics_port,
        "listeners": [
            {
                "name": "eos",
                "listen_port": eos_port,
                "servers": [
                    { "name": "eos-1", "endpoint": eos_ep_1, "enabled": true },
                    { "name": "eos-2", "endpoint": eos_ep_2, "enabled": true }
                ]
            },
            {
                "name": "wax",
                "listen_port": wax_port,
                "servers": [
                    { "name": "wax-1", "endpoint": wax_ep_1, "enabled": true }
                ]
            }
        ]
    });

    let mut config_file = tempfile::NamedTempFile::new().unwrap();
    config_file
        .write_all(config.to_string().as_bytes())
        .unwrap();
    config_file.flush().unwrap();

    let binary = FleetRouterProcess::find_binary();
    let child = Command::new(&binary)
        .arg("--config")
        .arg(config_file.path())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .spawn()
        .expect("Failed to spawn multi-listener fleet-router");

    let router = FleetRouterProcess {
        child,
        listen_port: eos_port,
        _config_file: config_file,
    };
    router.wait_for_ready().await;

    // Wait for all three upstreams across both listeners to report online in /metrics
    wait_for_metric(
        metrics_port,
        &format!(
            "fleet_router_upstream_up{{listener=\"eos\",endpoint=\"{eos_ep_1}\",server=\"eos-1\"}} 1"
        ),
    )
    .await;
    wait_for_metric(
        metrics_port,
        &format!(
            "fleet_router_upstream_up{{listener=\"eos\",endpoint=\"{eos_ep_2}\",server=\"eos-2\"}} 1"
        ),
    )
    .await;
    wait_for_metric(
        metrics_port,
        &format!(
            "fleet_router_upstream_up{{listener=\"wax\",endpoint=\"{wax_ep_1}\",server=\"wax-1\"}} 1"
        ),
    )
    .await;

    // Verify /ready, /ready/eos, /ready/wax, and /ready/unknown
    let http = reqwest::Client::new();
    let ready_resp = http
        .get(format!("http://127.0.0.1:{metrics_port}/ready"))
        .send()
        .await
        .unwrap();
    assert_eq!(ready_resp.status().as_u16(), 200);

    let eos_ready = http
        .get(format!("http://127.0.0.1:{metrics_port}/ready/eos"))
        .send()
        .await
        .unwrap();
    assert_eq!(eos_ready.status().as_u16(), 200);

    let wax_ready = http
        .get(format!("http://127.0.0.1:{metrics_port}/ready/wax"))
        .send()
        .await
        .unwrap();
    assert_eq!(wax_ready.status().as_u16(), 200);

    let unknown_ready = http
        .get(format!("http://127.0.0.1:{metrics_port}/ready/telos"))
        .send()
        .await
        .unwrap();
    assert_eq!(unknown_ready.status().as_u16(), 404);

    // Stream 8 blocks from the `eos` port (triggers failover after 3 if routed to eos-1)
    let (mut eos_writer, mut eos_reader) = ws_connect(&format!("ws://127.0.0.1:{eos_port}")).await;
    let _ = read_one(&mut eos_reader).await;
    eos_writer
        .send(Message::Binary(build_blocks_request(1, 20, 20).into()))
        .await
        .unwrap();
    let eos_heads = read_block_heads(&mut eos_reader, 8).await;
    assert_eq!(eos_heads.len(), 8);
    assert!(
        eos_heads.iter().all(|h| *h == 5000 || *h == 5500),
        "eos listener must only stream from eos upstreams (5000/5500), got {eos_heads:?}"
    );

    // Stream 5 blocks from the `wax` port
    let (mut wax_writer, mut wax_reader) = ws_connect(&format!("ws://127.0.0.1:{wax_port}")).await;
    let _ = read_one(&mut wax_reader).await;
    wax_writer
        .send(Message::Binary(build_blocks_request(1, 20, 20).into()))
        .await
        .unwrap();
    let wax_heads = read_block_heads(&mut wax_reader, 5).await;
    assert_eq!(wax_heads.len(), 5);
    assert!(
        wax_heads.iter().all(|h| *h == 9000),
        "wax listener must only stream from wax upstream (9000), got {wax_heads:?}"
    );

    drop(eos_writer);
    drop(eos_reader);
    drop(wax_writer);
    drop(wax_reader);
    drop(router);
    h_eos_1.shutdown().await;
    h_eos_2.shutdown().await;
    h_wax_1.shutdown().await;
}

// ===========================================================================
// Corrupted SHiP log entry (nodeos zlib error) — see /workspace/shipcorrupt
// ===========================================================================

/// Read `this_block.block_num` from up to `n` block frames until timeout/close.
///
/// The second value is `None` on a silent stall. A close frame yields
/// `(code, reason)`; any other end yields `(None, "")`.
async fn read_this_blocks(
    reader: &mut WsReader,
    n: usize,
    wait: Duration,
) -> (Vec<u32>, Option<(Option<u16>, String)>) {
    let mut blocks = Vec::new();
    while blocks.len() < n {
        match tokio::time::timeout(wait, reader.next()).await {
            Ok(Some(Ok(Message::Binary(d)))) if d.len() >= 78 && d[0] == 1 && d[73] == 1 => {
                blocks.push(u32::from_le_bytes(d[74..78].try_into().unwrap()));
            }
            Ok(Some(Ok(Message::Binary(_)))) | Ok(Some(Ok(Message::Text(_)))) => continue,
            Ok(Some(Ok(Message::Ping(_)))) | Ok(Some(Ok(Message::Pong(_)))) => continue,
            Ok(Some(Ok(Message::Close(frame)))) => {
                let closed = frame
                    .map(|frame| (Some(u16::from(frame.code)), frame.reason.to_string()))
                    .unwrap_or((None, String::new()));
                return (blocks, Some(closed));
            }
            Ok(_) => return (blocks, Some((None, String::new()))),
            Err(_) => return (blocks, None),
        }
    }
    (blocks, None)
}

fn corrupt_mock(block: u32, hits: Arc<std::sync::atomic::AtomicUsize>) -> MockShipConfig {
    MockShipConfig {
        head_block: 1000,
        lib_block: 990,
        blocks_to_stream: 0,
        corrupt_block: Some(block),
        corrupt_hits: Some(hits),
        ..Default::default()
    }
}

/// A healthy backup exists: the session must fail over and resume exactly at
/// the corrupted block with no gap or duplicate. Passes on v0.4.0.
#[tokio::test]
async fn e2e_proxy_corrupt_block_fails_over_to_healthy_upstream() {
    let hits = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let bad = MockShipServer::new(corrupt_mock(115, hits.clone())).await;
    let good = MockShipServer::new(MockShipConfig {
        head_block: 1000,
        lib_block: 990,
        blocks_to_stream: 0,
        ..Default::default()
    })
    .await;
    let (ep_bad, ep_good) = (bad.endpoint(), good.endpoint());
    // Only the corrupt node is up when the client connects, so the session
    // starts there; the healthy backup comes online before the request.
    let hb = bad.start();
    let router = FleetRouterProcess::start(&[ep_bad, ep_good], 500).await;
    sleep(Duration::from_secs(2)).await;
    let (mut w, mut r) = ws_connect(&router.ws_url()).await;
    let _ = read_one(&mut r).await; // ABI
    let hg = good.start();
    sleep(Duration::from_secs(3)).await;

    w.send(Message::Binary(build_blocks_request(100, 200, 1000).into()))
        .await
        .unwrap();
    let (blocks, _) = read_this_blocks(&mut r, 100, Duration::from_secs(5)).await;
    let n = hits.load(std::sync::atomic::Ordering::Relaxed);
    assert_eq!(n, 1, "session should hit the corrupt block exactly once");
    assert_eq!(
        blocks,
        (100..200).collect::<Vec<_>>(),
        "gap/dup across the corrupt block after failover"
    );

    drop(router);
    hb.shutdown().await;
    hg.shutdown().await;
}

/// Only the corrupted upstream exists. The session must stop re-dialing that
/// block, then close the client with 1011 instead of stalling.
#[tokio::test]
async fn e2e_proxy_corrupt_block_only_upstream_is_bounded() {
    let hits = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let bad = MockShipServer::new(corrupt_mock(115, hits.clone())).await;
    let ep = bad.endpoint();
    let hb = bad.start();
    let router = FleetRouterProcess::start(&[ep], 1000).await;
    sleep(Duration::from_secs(2)).await;

    let (mut w, mut r) = ws_connect(&router.ws_url()).await;
    let _ = read_one(&mut r).await;
    w.send(Message::Binary(build_blocks_request(100, 200, 1000).into()))
        .await
        .unwrap();
    let (blocks, closed) = read_this_blocks(&mut r, 100, Duration::from_secs(8)).await;
    let n = hits.load(std::sync::atomic::Ordering::Relaxed);
    println!(
        "  delivered up to {:?}, closed={closed:?}, corrupt hits={n}",
        blocks.last()
    );
    assert_eq!(blocks.last(), Some(&114));
    let (code, reason) = closed.expect("client should get a Close instead of a silent stall");
    assert_eq!(code, Some(1011), "close code, reason={reason}");
    assert_eq!(reason, "block 115 unavailable on all upstreams");
    assert!(
        n <= 5,
        "upstream re-dialed {n} times for the same bad block"
    );
    assert!(n >= 1, "the corrupt block should be observed at least once");

    drop(router);
    hb.shutdown().await;
}

/// With quarantine armed after one strike, a later session that asks for the
/// bad block is served by the healthy upstream and does not touch the corrupt
/// log entry. After the TTL, that upstream is eligible again.
#[tokio::test]
async fn e2e_proxy_quarantine_skips_corrupt_upstream_until_ttl() {
    let hits = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let bad = MockShipServer::new(corrupt_mock(115, hits.clone())).await;
    let good = MockShipServer::new(MockShipConfig {
        head_block: 1000,
        lib_block: 990,
        blocks_to_stream: 0,
        ..Default::default()
    })
    .await;
    let (ep_bad, ep_good) = (bad.endpoint(), good.endpoint());
    let hb = bad.start();
    let (router, metrics_port) = FleetRouterProcess::start_with_quarantine(
        &[ep_bad.clone(), ep_good.clone()],
        500,
        serde_json::json!({
            "strikes": 1,
            "window_secs": 300,
            "ttl_secs": 8,
            "range": 1
        }),
    )
    .await;
    let bad_labels = format!("endpoint=\"{ep_bad}\",server=\"mock-upstream-1\"");
    let good_up =
        format!("fleet_router_upstream_up{{endpoint=\"{ep_good}\",server=\"mock-upstream-2\"}} 1");
    wait_for_metric(
        metrics_port,
        &format!("fleet_router_upstream_up{{{bad_labels}}} 1"),
    )
    .await;

    // Only the corrupt node is online, so the first session starts there.
    let (mut w1, mut r1) = ws_connect(&router.ws_url()).await;
    let _ = read_one(&mut r1).await;
    let hg = good.start();
    wait_for_metric(metrics_port, &good_up).await;

    w1.send(Message::Binary(build_blocks_request(100, 130, 1000).into()))
        .await
        .unwrap();
    let (blocks, _) = read_this_blocks(&mut r1, 20, Duration::from_secs(5)).await;
    assert!(
        blocks.contains(&114) && blocks.contains(&115),
        "first session should fail over past the corrupt block, got {blocks:?}"
    );
    assert_eq!(
        hits.load(std::sync::atomic::Ordering::Relaxed),
        1,
        "first session should strike the corrupt upstream once"
    );
    wait_for_metric(
        metrics_port,
        &format!("fleet_router_upstream_block_failures_total{{{bad_labels}}} 1"),
    )
    .await;
    wait_for_metric(
        metrics_port,
        &format!("fleet_router_upstream_quarantined{{{bad_labels}}} 1"),
    )
    .await;
    wait_for_metric(
        metrics_port,
        &format!("fleet_router_upstream_quarantine_block{{{bad_labels}}} 115"),
    )
    .await;
    // Still online: quarantine is not an offline mark.
    let body = fetch_metrics(metrics_port).await;
    assert!(
        body.contains(&format!("fleet_router_upstream_up{{{bad_labels}}} 1")),
        "quarantine must leave the upstream online:\n{body}"
    );

    // Second session asks for the quarantined block and must not pay a failover.
    let (mut w2, mut r2) = ws_connect(&router.ws_url()).await;
    let _ = read_one(&mut r2).await;
    w2.send(Message::Binary(build_blocks_request(115, 125, 100).into()))
        .await
        .unwrap();
    let (blocks2, _) = read_this_blocks(&mut r2, 5, Duration::from_secs(5)).await;
    assert_eq!(
        blocks2,
        (115..120).collect::<Vec<_>>(),
        "second session should stream the quarantined block from the healthy upstream"
    );
    assert_eq!(
        hits.load(std::sync::atomic::Ordering::Relaxed),
        1,
        "second session must not hit the corrupt upstream"
    );

    // TTL is 2s from the strike. Wait until the gauge drops, then a new session
    // with the healthy node still busy should land on the corrupt upstream again.
    wait_for_metric(
        metrics_port,
        &format!("fleet_router_upstream_quarantined{{{bad_labels}}} 0"),
    )
    .await;
    let (mut w3, mut r3) = ws_connect(&router.ws_url()).await;
    let _ = read_one(&mut r3).await;
    w3.send(Message::Binary(build_blocks_request(115, 125, 100).into()))
        .await
        .unwrap();
    let _ = read_this_blocks(&mut r3, 3, Duration::from_secs(5)).await;
    let n = hits.load(std::sync::atomic::Ordering::Relaxed);
    assert!(
        n >= 2,
        "after the quarantine TTL the corrupt upstream should be selected again, hits={n}"
    );

    drop(w1);
    drop(r1);
    drop(w2);
    drop(r2);
    drop(w3);
    drop(r3);
    drop(router);
    hb.shutdown().await;
    hg.shutdown().await;
}

async fn fetch_metrics(port: u16) -> String {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let mut stream = tokio::net::TcpStream::connect(format!("127.0.0.1:{port}"))
        .await
        .expect("metrics connect");
    stream
        .write_all(b"GET /metrics HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
        .await
        .unwrap();
    let mut buf = Vec::new();
    stream.read_to_end(&mut buf).await.unwrap();
    String::from_utf8_lossy(&buf).into_owned()
}
