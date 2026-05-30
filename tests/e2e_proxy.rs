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

        let config = serde_json::json!({
            "listen_address": "127.0.0.1",
            "listen_port": listen_port,
            "upstream_reconnect_ms": 1000,
            "upstream_monitoring_ms": 2000,
            "upstream_status_ms": status_ms,
            "servers": servers
        });

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
        // Try debug build first, then release
        let debug_path = "target/debug/fleet-router";
        let release_path = "target/release/fleet-router";
        if std::path::Path::new(debug_path).exists() {
            debug_path.to_string()
        } else if std::path::Path::new(release_path).exists() {
            release_path.to_string()
        } else {
            panic!(
                "fleet-router binary not found. Run `cargo build` first.\n  Checked: {} and {}",
                debug_path, release_path
            );
        }
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
