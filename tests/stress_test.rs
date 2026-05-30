/// Stress tests for fleet-router with data-carrying block payloads.
///
/// Mock-based tests run in CI, Docker-based tests require:
///   cd docker && docker compose -f docker-compose.test.yml up --build
///
/// Run mock tests:   cargo test --test stress_test
/// Run Docker tests: cargo test --test stress_test -- --ignored
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;

use futures::{SinkExt, StreamExt};
use mock_ship::{MockShipConfig, MockShipServer};
use tokio_tungstenite::connect_async;
use tokio_tungstenite::tungstenite::Message;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

async fn ws_connect(
    url: &str,
) -> (
    futures::stream::SplitSink<
        tokio_tungstenite::WebSocketStream<
            tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
        >,
        Message,
    >,
    futures::stream::SplitStream<
        tokio_tungstenite::WebSocketStream<
            tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
        >,
    >,
) {
    let (ws, _) = connect_async(url).await.expect("WS connect failed");
    ws.split()
}

async fn read_one(
    reader: &mut futures::stream::SplitStream<
        tokio_tungstenite::WebSocketStream<
            tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
        >,
    >,
) -> Message {
    reader
        .next()
        .await
        .expect("Stream ended unexpectedly")
        .expect("Read error")
}

/// Build a get_blocks_request_v0 with configurable fetch flags.
fn build_blocks_request_with_data(
    start: u32,
    end: u32,
    max_in_flight: u32,
    fetch_block: bool,
    fetch_traces: bool,
    fetch_deltas: bool,
) -> Vec<u8> {
    let mut buf = vec![1u8]; // varuint32(1) = get_blocks_request_v0
    buf.extend_from_slice(&start.to_le_bytes());
    buf.extend_from_slice(&end.to_le_bytes());
    buf.extend_from_slice(&max_in_flight.to_le_bytes());
    buf.push(0); // have_positions: empty array
    buf.push(0); // irreversible_only
    buf.push(if fetch_block { 1 } else { 0 });
    buf.push(if fetch_traces { 1 } else { 0 });
    buf.push(if fetch_deltas { 1 } else { 0 });
    buf
}

fn build_ack_request(num_messages: u32) -> Vec<u8> {
    let mut buf = vec![2u8];
    buf.extend_from_slice(&num_messages.to_le_bytes());
    buf
}

/// Decode a varuint32 from a byte slice, returning (value, bytes_consumed).
fn decode_varuint32(data: &[u8]) -> (u32, usize) {
    let mut val: u32 = 0;
    let mut shift = 0;
    for (i, &byte) in data.iter().enumerate() {
        val |= ((byte & 0x7f) as u32) << shift;
        shift += 7;
        if byte & 0x80 == 0 {
            return (val, i + 1);
        }
    }
    (val, data.len())
}

// ===========================================================================
// TEST 1: Data-carrying blocks through mock (CI-safe)
// ===========================================================================

#[tokio::test]
async fn stress_block_data_present_mock() {
    let data_size = 4096; // 4KB per payload
    let config = MockShipConfig {
        head_block: 5000,
        lib_block: 4990,
        blocks_to_stream: 0,
        block_data_size: data_size,
        ..Default::default()
    };
    let server = MockShipServer::new(config).await;
    let url = server.ws_url();
    let server_handle = server.start();

    let (mut writer, mut reader) = ws_connect(&url).await;

    // Read ABI
    let _ = read_one(&mut reader).await;

    // Request 5 blocks with all data flags
    let start = 100u32;
    let count = 5u32;
    writer
        .send(Message::Binary(
            build_blocks_request_with_data(start, start + count, count, true, true, true).into(),
        ))
        .await
        .unwrap();

    for i in 0..count {
        let msg = read_one(&mut reader).await;
        let data = match msg {
            Message::Binary(d) => d,
            other => panic!("Expected Binary, got {:?}", other),
        };

        // Variant 1 = blocks_result_v0
        assert_eq!(data[0], 1);

        // this_block present at offset 73
        assert_eq!(data[73], 1, "this_block should be present");

        // this_block block_num
        let block_num = u32::from_le_bytes(data[74..78].try_into().unwrap());
        assert_eq!(block_num, start + i);

        // After the envelope: prev_block (present, 1 + 36 bytes),
        // then block, traces, deltas optionals
        // prev_block flag at 110, data at 111..147
        let mut offset = 110; // prev_block flag
        assert_eq!(data[offset], 1, "prev_block should be present");
        offset += 1 + 36; // skip prev_block

        // block optional — should be present with data
        assert_eq!(data[offset], 1, "block data should be present");
        offset += 1;
        let (block_len, consumed) = decode_varuint32(&data[offset..]);
        assert_eq!(block_len as usize, data_size, "block data size mismatch");
        offset += consumed + block_len as usize;

        // traces optional
        assert_eq!(data[offset], 1, "traces data should be present");
        offset += 1;
        let (traces_len, consumed) = decode_varuint32(&data[offset..]);
        assert_eq!(traces_len as usize, data_size, "traces data size mismatch");
        offset += consumed + traces_len as usize;

        // deltas optional
        assert_eq!(data[offset], 1, "deltas data should be present");
        offset += 1;
        let (deltas_len, _consumed) = decode_varuint32(&data[offset..]);
        assert_eq!(deltas_len as usize, data_size, "deltas data size mismatch");
    }

    server_handle.shutdown().await;
}

// ===========================================================================
// TEST 2: Large payload throughput (mock, CI-safe)
// ===========================================================================

#[tokio::test]
async fn stress_large_payload_throughput_mock() {
    let block_count = 200u32;
    let data_size = 262_144; // 256KB per payload
    let config = MockShipConfig {
        head_block: 10000,
        lib_block: 9990,
        blocks_to_stream: 0,
        block_data_size: data_size,
        ..Default::default()
    };
    let server = MockShipServer::new(config).await;
    let url = server.ws_url();
    let server_handle = server.start();

    let (mut writer, mut reader) = ws_connect(&url).await;
    let _ = read_one(&mut reader).await; // ABI

    let max_in_flight = 50u32;
    writer
        .send(Message::Binary(
            build_blocks_request_with_data(1, 1 + block_count, max_in_flight, true, true, true)
                .into(),
        ))
        .await
        .unwrap();

    let start = Instant::now();
    let mut total_bytes: u64 = 0;
    let mut credits_remaining = max_in_flight;

    for i in 0..block_count {
        let msg = read_one(&mut reader).await;
        if let Message::Binary(d) = msg {
            total_bytes += d.len() as u64;
            // Verify sequential block number
            let block_num = u32::from_le_bytes(d[74..78].try_into().unwrap());
            assert_eq!(block_num, 1 + i, "Out-of-order block");
        } else {
            panic!("Expected binary at block {}", i);
        }

        credits_remaining -= 1;
        if credits_remaining == 0 && i + 1 < block_count {
            writer
                .send(Message::Binary(build_ack_request(max_in_flight).into()))
                .await
                .unwrap();
            credits_remaining = max_in_flight;
        }
    }

    let elapsed = start.elapsed();
    let mb = total_bytes as f64 / (1024.0 * 1024.0);
    let mbps = mb / elapsed.as_secs_f64();
    println!(
        "  Throughput: {:.1} MB in {:.2}s = {:.1} MB/s ({} blocks × {}KB data)",
        mb,
        elapsed.as_secs_f64(),
        mbps,
        block_count,
        data_size / 1024
    );

    // Sanity: we should have received substantial data
    // 200 blocks × 3 payloads × 256KB = ~150MB minimum
    assert!(
        total_bytes > 100_000_000,
        "Expected >100MB total, got {} bytes",
        total_bytes
    );

    server_handle.shutdown().await;
}

// ===========================================================================
// TEST 3: Concurrent heavy clients (mock, CI-safe)
// ===========================================================================

#[tokio::test]
async fn stress_concurrent_heavy_clients_mock() {
    let num_clients = 10;
    let blocks_per_client = 50u32;
    let data_size = 8192; // 8KB per payload

    let config = MockShipConfig {
        head_block: 10000,
        lib_block: 9990,
        blocks_to_stream: 0,
        block_data_size: data_size,
        ..Default::default()
    };
    let server = MockShipServer::new(config).await;
    let url = server.ws_url();
    let server_handle = server.start();

    let total_bytes = Arc::new(AtomicU64::new(0));
    let mut client_handles = Vec::new();

    for client_id in 0..num_clients {
        let url = url.clone();
        let bytes_counter = total_bytes.clone();
        let handle = tokio::spawn(async move {
            let (mut writer, mut reader) = ws_connect(&url).await;
            let _ = read_one(&mut reader).await; // ABI

            let start = (client_id * blocks_per_client) + 1;
            let end = start + blocks_per_client;
            let max_in_flight = blocks_per_client;

            writer
                .send(Message::Binary(
                    build_blocks_request_with_data(start, end, max_in_flight, true, true, true)
                        .into(),
                ))
                .await
                .unwrap();

            let mut received = 0u32;
            for _ in 0..blocks_per_client {
                let msg = read_one(&mut reader).await;
                if let Message::Binary(d) = msg {
                    bytes_counter.fetch_add(d.len() as u64, Ordering::Relaxed);
                    received += 1;
                }
            }
            assert_eq!(
                received, blocks_per_client,
                "Client {} didn't receive all blocks",
                client_id
            );
        });
        client_handles.push(handle);
    }

    let start = Instant::now();
    for h in client_handles {
        h.await.unwrap();
    }
    let elapsed = start.elapsed();

    let total = total_bytes.load(Ordering::Relaxed);
    let mb = total as f64 / (1024.0 * 1024.0);
    println!(
        "  {} clients × {} blocks = {:.1} MB in {:.2}s",
        num_clients,
        blocks_per_client,
        mb,
        elapsed.as_secs_f64()
    );

    server_handle.shutdown().await;
}

// ===========================================================================
// Docker tests — target fleet-router proxy (port 9000) + load generator API
// ===========================================================================

fn router_endpoint() -> String {
    // Fleet router load-balances across SHiP peer nodes
    std::env::var("ROUTER_ENDPOINT").unwrap_or_else(|_| "ws://127.0.0.1:9100".to_string())
}

fn loadgen_url() -> String {
    std::env::var("LOADGEN_URL").unwrap_or_else(|_| "http://127.0.0.1:3333".to_string())
}

/// Start load generator, run test, stop load generator
async fn with_load<F, Fut>(tps: u32, test_fn: F)
where
    F: FnOnce() -> Fut,
    Fut: std::future::Future<Output = ()>,
{
    let base = loadgen_url();
    let client = reqwest::Client::new();

    // Start load
    let res = client
        .post(format!("{}/start", base))
        .json(&serde_json::json!({"tps": tps}))
        .send()
        .await
        .expect("Failed to contact load generator");
    assert!(
        res.status().is_success() || res.status().as_u16() == 409,
        "Load generator /start failed: {}",
        res.status()
    );

    // Wait for some transactions to land
    tokio::time::sleep(std::time::Duration::from_secs(3)).await;

    // Run the actual test
    test_fn().await;

    // Stop load
    let _ = client.post(format!("{}/stop", base)).send().await;

    // Print final status
    if let Ok(res) = client.get(format!("{}/status", base)).send().await {
        if let Ok(body) = res.text().await {
            println!("  Load generator status: {}", body);
        }
    }
}

// ===========================================================================
// TEST 4: Data-carrying blocks through fleet-router (Docker)
// ===========================================================================

#[tokio::test]
#[ignore] // Requires: docker compose -f docker/docker-compose.test.yml up --build
async fn stress_block_data_via_router_docker() {
    with_load(10, || async {
        let url = router_endpoint();
        let (mut writer, mut reader) = ws_connect(&url).await;
        let _ = read_one(&mut reader).await; // ABI

        // Request a range that should include recently-generated blocks
        // Use end_block=0xFFFFFFFF to stream latest blocks
        let max_in_flight = 50u32;
        let count = 50u32;

        // Get current head by requesting status first
        writer
            .send(Message::Binary(vec![0u8].into())) // get_status_request_v0
            .await
            .unwrap();
        let status_msg = read_one(&mut reader).await;
        let status_data = match status_msg {
            Message::Binary(d) => d,
            other => panic!("Expected Binary status, got {:?}", other),
        };
        // Head block_num is at offset 1 (after variant byte)
        let head = u32::from_le_bytes(status_data[1..5].try_into().unwrap());
        println!("  Router reports head block: {}", head);

        // Request recent blocks with data
        let start = if head > count { head - count } else { 1 };
        writer
            .send(Message::Binary(
                build_blocks_request_with_data(
                    start,
                    start + count,
                    max_in_flight,
                    true,
                    true,
                    true,
                )
                .into(),
            ))
            .await
            .unwrap();

        let mut total_bytes: u64 = 0;
        let mut blocks_with_data = 0u32;
        let mut max_block_size: usize = 0;
        let data_threshold = 400;

        for _ in 0..count {
            let msg = read_one(&mut reader).await;
            if let Message::Binary(d) = msg {
                total_bytes += d.len() as u64;
                if d.len() > data_threshold {
                    blocks_with_data += 1;
                }
                if d.len() > max_block_size {
                    max_block_size = d.len();
                }
            }
        }

        println!(
            "  Router: {} blocks, {:.1} KB total, {} with data (>{} bytes), max: {} bytes",
            count,
            total_bytes as f64 / 1024.0,
            blocks_with_data,
            data_threshold,
            max_block_size
        );

        assert!(
            blocks_with_data > 0,
            "Expected data blocks through router (max was {} bytes)",
            max_block_size
        );
    })
    .await;
}

// ===========================================================================
// TEST 5: Concurrent heavy clients through fleet-router (Docker)
// ===========================================================================

#[tokio::test]
#[ignore]
async fn stress_concurrent_clients_via_router_docker() {
    with_load(20, || async {
        let url = router_endpoint();
        let num_clients = 10;
        let blocks_per_client = 30u32;

        let total_bytes = Arc::new(AtomicU64::new(0));
        let mut handles = Vec::new();

        for client_id in 0..num_clients {
            let url = url.clone();
            let bytes_counter = total_bytes.clone();
            let handle = tokio::spawn(async move {
                let (mut writer, mut reader) = ws_connect(&url).await;
                let _ = read_one(&mut reader).await; // ABI

                writer
                    .send(Message::Binary(
                        build_blocks_request_with_data(
                            2,
                            2 + blocks_per_client,
                            blocks_per_client,
                            true,
                            true,
                            true,
                        )
                        .into(),
                    ))
                    .await
                    .unwrap();

                for _j in 0..blocks_per_client {
                    let msg = read_one(&mut reader).await;
                    if let Message::Binary(d) = msg {
                        bytes_counter.fetch_add(d.len() as u64, Ordering::Relaxed);
                    } else {
                        panic!("Client {} expected binary, got {:?}", client_id, msg);
                    }
                }
            });
            handles.push(handle);
        }

        let start = Instant::now();
        for h in handles {
            h.await.unwrap();
        }
        let elapsed = start.elapsed();

        let total = total_bytes.load(Ordering::Relaxed);
        println!(
            "  Router: {} clients x {} blocks = {:.1} KB in {:.2}s",
            num_clients,
            blocks_per_client,
            total as f64 / 1024.0,
            elapsed.as_secs_f64()
        );
    })
    .await;
}
