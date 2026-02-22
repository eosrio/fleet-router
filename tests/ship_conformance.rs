/// SHiP Protocol Conformance Tests
///
/// These tests validate that SHiP server behavior matches the Spring v1.2.2 spec.
/// The same assertions run against both the mock-ship server (fast CI) and
/// real nodeos containers (Docker, #[ignore]).
///
/// Run mock tests:   cargo test --test ship_conformance
/// Run Docker tests: cargo test --test ship_conformance -- --ignored
use futures::{SinkExt, StreamExt};
use tokio_tungstenite::connect_async;
use tokio_tungstenite::tungstenite::Message;

// ---------------------------------------------------------------------------
// Shared helpers
// ---------------------------------------------------------------------------

/// Connect to a SHiP endpoint and return the split WebSocket.
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

/// Read exactly one message, panic if the stream ends.
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

/// Build a get_status_request_v0 (variant index 0, empty body).
fn build_status_request() -> Vec<u8> {
    vec![0u8]
}

/// Build a get_blocks_request_v0 (variant index 1).
fn build_blocks_request(start: u32, end: u32, max_in_flight: u32) -> Vec<u8> {
    let mut buf = vec![1u8]; // varuint32(1)
    buf.extend_from_slice(&start.to_le_bytes());
    buf.extend_from_slice(&end.to_le_bytes());
    buf.extend_from_slice(&max_in_flight.to_le_bytes());
    buf.push(0); // have_positions: empty array
    buf.push(0); // irreversible_only
    buf.push(0); // fetch_block
    buf.push(0); // fetch_traces
    buf.push(0); // fetch_deltas
    buf
}

/// Build a get_blocks_ack_request_v0 (variant index 2).
fn build_ack_request(num_messages: u32) -> Vec<u8> {
    let mut buf = vec![2u8]; // varuint32(2)
    buf.extend_from_slice(&num_messages.to_le_bytes());
    buf
}

// ---------------------------------------------------------------------------
// Test helpers for spawning mock servers
// ---------------------------------------------------------------------------

use mock_ship::{MockShipConfig, MockShipServer};

/// Spawn a mock server, returning its WS URL and a join handle.
async fn spawn_mock(config: MockShipConfig) -> (String, tokio::task::JoinHandle<()>) {
    let server = MockShipServer::new(config).await;
    let url = server.ws_url();
    let handle = tokio::spawn(async move {
        server.handle_one_connection().await;
    });
    (url, handle)
}

/// Get the Docker SHiP endpoint from env var, or default.
fn docker_endpoint() -> String {
    std::env::var("SHIP_ENDPOINT").unwrap_or_else(|_| "ws://127.0.0.1:8080".to_string())
}

// ===========================================================================
// CONFORMANCE TEST 1: ABI is valid JSON with required SHiP fields
// ===========================================================================

async fn assert_abi_valid(url: &str) {
    let (_writer, mut reader) = ws_connect(url).await;
    let msg = read_one(&mut reader).await;

    // Must be a Text frame
    assert!(
        matches!(&msg, Message::Text(_)),
        "First frame must be Text (ABI JSON), got: {:?}",
        msg
    );

    let text = match msg {
        Message::Text(t) => t.to_string(),
        _ => unreachable!(),
    };

    // Must be valid JSON
    let abi: serde_json::Value = serde_json::from_str(&text).expect("ABI is not valid JSON");

    // Required top-level fields
    assert_eq!(abi["version"], "eosio::abi/1.1", "ABI version mismatch");
    assert!(abi["structs"].is_array(), "Missing 'structs' array");
    assert!(abi["variants"].is_array(), "Missing 'variants' array");

    // Must have request and result variants
    let variants: Vec<&str> = abi["variants"]
        .as_array()
        .unwrap()
        .iter()
        .filter_map(|v| v["name"].as_str())
        .collect();
    assert!(
        variants.contains(&"request"),
        "Missing 'request' variant; found: {:?}",
        variants
    );
    assert!(
        variants.contains(&"result"),
        "Missing 'result' variant; found: {:?}",
        variants
    );

    // Verify request variant ordering (indices matter for binary protocol)
    let request_variant = abi["variants"]
        .as_array()
        .unwrap()
        .iter()
        .find(|v| v["name"] == "request")
        .expect("request variant not found");
    let request_types: Vec<&str> = request_variant["types"]
        .as_array()
        .unwrap()
        .iter()
        .filter_map(|t| t.as_str())
        .collect();
    assert_eq!(request_types[0], "get_status_request_v0");
    assert_eq!(request_types[1], "get_blocks_request_v0");
    assert_eq!(request_types[2], "get_blocks_ack_request_v0");

    // Verify result variant ordering
    let result_variant = abi["variants"]
        .as_array()
        .unwrap()
        .iter()
        .find(|v| v["name"] == "result")
        .expect("result variant not found");
    let result_types: Vec<&str> = result_variant["types"]
        .as_array()
        .unwrap()
        .iter()
        .filter_map(|t| t.as_str())
        .collect();
    assert_eq!(result_types[0], "get_status_result_v0");
    assert_eq!(result_types[1], "get_blocks_result_v0");
}

#[tokio::test]
async fn conformance_abi_valid_mock() {
    let (url, handle) = spawn_mock(MockShipConfig::default()).await;
    assert_abi_valid(&url).await;
    let _ = handle.await;
}

#[tokio::test]
#[ignore] // Requires Docker: SHIP_ENDPOINT=ws://127.0.0.1:8080
async fn conformance_abi_valid_docker() {
    assert_abi_valid(&docker_endpoint()).await;
}

// ===========================================================================
// CONFORMANCE TEST 2: Status request returns valid binary result
// ===========================================================================

async fn assert_status_response(url: &str) {
    let (mut writer, mut reader) = ws_connect(url).await;

    // Read ABI
    let _ = read_one(&mut reader).await;

    // Send status request
    writer
        .send(Message::Binary(build_status_request().into()))
        .await
        .unwrap();

    let msg = read_one(&mut reader).await;
    assert!(
        matches!(&msg, Message::Binary(_)),
        "Status response must be Binary, got: {:?}",
        msg
    );

    let data = match msg {
        Message::Binary(d) => d,
        _ => unreachable!(),
    };

    // Minimum size: 1 (variant) + 36 (head) + 36 (lib) + 4*4 (trace/chain fields) = 89
    assert!(
        data.len() >= 89,
        "Status result too short: {} bytes (need >= 89)",
        data.len()
    );

    // Variant index must be 0 (get_status_result_v0)
    assert_eq!(data[0], 0, "Expected result variant 0 (status_result_v0)");

    // Head block_num (u32 LE at offset 1)
    let head_block = u32::from_le_bytes(data[1..5].try_into().unwrap());
    assert!(
        head_block > 0,
        "Head block should be > 0, got {}",
        head_block
    );

    // Head block_id (32 bytes at offset 5..37)
    let head_id = &data[5..37];
    assert!(
        head_id.iter().any(|&b| b != 0),
        "Head block ID should not be all zeros"
    );

    // LIB block_num (u32 LE at offset 37)
    let lib_block = u32::from_le_bytes(data[37..41].try_into().unwrap());
    assert!(
        lib_block <= head_block,
        "LIB ({}) should be <= head ({})",
        lib_block,
        head_block
    );

    // trace_begin_block (offset 73)
    let trace_begin = u32::from_le_bytes(data[73..77].try_into().unwrap());
    // trace_end_block (offset 77)
    let trace_end = u32::from_le_bytes(data[77..81].try_into().unwrap());
    assert!(
        trace_end >= trace_begin,
        "trace_end ({}) should be >= trace_begin ({})",
        trace_end,
        trace_begin
    );

    println!(
        "  Status: head={}, lib={}, trace=[{}..{}]",
        head_block, lib_block, trace_begin, trace_end
    );
}

#[tokio::test]
async fn conformance_status_response_mock() {
    let config = MockShipConfig {
        head_block: 5000,
        lib_block: 4990,
        ..Default::default()
    };
    let (url, handle) = spawn_mock(config).await;
    assert_status_response(&url).await;
    let _ = handle.await;
}

#[tokio::test]
#[ignore]
async fn conformance_status_response_docker() {
    assert_status_response(&docker_endpoint()).await;
}

// ===========================================================================
// CONFORMANCE TEST 3: Block streaming delivers sequential block numbers
// ===========================================================================

async fn assert_block_streaming(url: &str, start: u32, count: u32, max_in_flight: u32) {
    let (mut writer, mut reader) = ws_connect(url).await;

    // Read ABI
    let _ = read_one(&mut reader).await;

    // Request blocks
    let end = start + count;
    writer
        .send(Message::Binary(
            build_blocks_request(start, end, max_in_flight).into(),
        ))
        .await
        .unwrap();

    let mut received: Vec<u32> = Vec::new();
    let mut credits_remaining = max_in_flight;

    for _ in 0..count {
        let msg = read_one(&mut reader).await;
        let data = match msg {
            Message::Binary(d) => d,
            other => panic!("Expected Binary block result, got {:?}", other),
        };

        // Variant must be 1 (get_blocks_result_v0)
        assert_eq!(data[0], 1, "Expected result variant 1 (blocks_result_v0)");

        // this_block is optional: flag at offset 73 (after variant + head + lib)
        assert_eq!(
            data[73], 1,
            "this_block optional flag should be 1 (present)"
        );

        // this_block block_num at offset 74
        let block_num = u32::from_le_bytes(data[74..78].try_into().unwrap());
        received.push(block_num);

        credits_remaining -= 1;
        if credits_remaining == 0 && received.len() < count as usize {
            // Send ack to get more credits
            writer
                .send(Message::Binary(build_ack_request(max_in_flight).into()))
                .await
                .unwrap();
            credits_remaining = max_in_flight;
        }
    }

    // Verify sequential ordering
    let expected: Vec<u32> = (start..end).collect();
    assert_eq!(
        received, expected,
        "Block numbers not sequential:\n  got:      {:?}\n  expected: {:?}",
        received, expected
    );
}

#[tokio::test]
async fn conformance_block_streaming_mock() {
    let config = MockShipConfig {
        head_block: 1000,
        lib_block: 990,
        blocks_to_stream: 0, // unlimited
        ..Default::default()
    };
    let (url, handle) = spawn_mock(config).await;
    assert_block_streaming(&url, 100, 10, 5).await;
    let _ = handle.await;
}

#[tokio::test]
#[ignore]
async fn conformance_block_streaming_docker() {
    // Real nodeos: request 5 blocks starting from block 2
    assert_block_streaming(&docker_endpoint(), 2, 5, 5).await;
}

// ===========================================================================
// CONFORMANCE TEST 4: Flow control — server respects max_messages_in_flight
// ===========================================================================

async fn assert_flow_control(url: &str) {
    let (mut writer, mut reader) = ws_connect(url).await;

    // Read ABI
    let _ = read_one(&mut reader).await;

    // Query status to learn chain state (ensures we request blocks that exist)
    writer
        .send(Message::Binary(build_status_request().into()))
        .await
        .unwrap();
    let status = match read_one(&mut reader).await {
        Message::Binary(d) => d,
        other => panic!("Expected binary status, got {:?}", other),
    };
    let head = u32::from_le_bytes(status[1..5].try_into().unwrap());
    assert!(head >= 3, "Need at least 3 blocks on chain, head={}", head);

    // Request blocks starting from 2 with max_messages_in_flight=2
    let start: u32 = 2;
    writer
        .send(Message::Binary(
            build_blocks_request(start, start + 100, 2).into(),
        ))
        .await
        .unwrap();

    // Should receive exactly 2 blocks before server pauses
    let msg1 = read_one(&mut reader).await;
    assert!(matches!(msg1, Message::Binary(_)), "Expected block 1");
    let msg2 = read_one(&mut reader).await;
    assert!(matches!(msg2, Message::Binary(_)), "Expected block 2");

    // Now send ack for 1 more
    writer
        .send(Message::Binary(build_ack_request(1).into()))
        .await
        .unwrap();

    // Should receive exactly 1 more block
    let msg3 = read_one(&mut reader).await;
    assert!(
        matches!(msg3, Message::Binary(_)),
        "Expected block 3 after ack"
    );

    // Verify block numbers are sequential
    let nums: Vec<u32> = [msg1, msg2, msg3]
        .into_iter()
        .map(|m| {
            if let Message::Binary(d) = m {
                u32::from_le_bytes(d[74..78].try_into().unwrap())
            } else {
                panic!("not binary");
            }
        })
        .collect();
    assert_eq!(
        nums,
        vec![start, start + 1, start + 2],
        "Blocks should be sequential starting from {}: got {:?}",
        start,
        nums
    );
}

#[tokio::test]
async fn conformance_flow_control_mock() {
    let config = MockShipConfig {
        head_block: 1000,
        lib_block: 990,
        blocks_to_stream: 0,
        ..Default::default()
    };
    let (url, handle) = spawn_mock(config).await;
    assert_flow_control(&url).await;
    let _ = handle.await;
}

#[tokio::test]
#[ignore]
async fn conformance_flow_control_docker() {
    assert_flow_control(&docker_endpoint()).await;
}

// ===========================================================================
// CONFORMANCE TEST 5: head and lib positions in block results are consistent
// ===========================================================================

async fn assert_block_positions_consistent(url: &str) {
    let (mut writer, mut reader) = ws_connect(url).await;

    // Read ABI
    let _ = read_one(&mut reader).await;

    // Get status first to learn current head/lib
    writer
        .send(Message::Binary(build_status_request().into()))
        .await
        .unwrap();

    let status = match read_one(&mut reader).await {
        Message::Binary(d) => d,
        other => panic!("Expected binary status, got {:?}", other),
    };

    let status_head = u32::from_le_bytes(status[1..5].try_into().unwrap());
    let status_lib = u32::from_le_bytes(status[37..41].try_into().unwrap());

    // Request 1 block
    writer
        .send(Message::Binary(build_blocks_request(2, 3, 1).into()))
        .await
        .unwrap();

    let block = match read_one(&mut reader).await {
        Message::Binary(d) => d,
        other => panic!("Expected binary block, got {:?}", other),
    };

    // Head and lib in the block result
    let block_head = u32::from_le_bytes(block[1..5].try_into().unwrap());
    let block_lib = u32::from_le_bytes(block[37..41].try_into().unwrap());

    // Head in block result should be >= head in status (chain can advance)
    assert!(
        block_head >= status_head,
        "Block result head ({}) should be >= status head ({})",
        block_head,
        status_head
    );
    // LIB should be <= head
    assert!(
        block_lib <= block_head,
        "Block result LIB ({}) should be <= head ({})",
        block_lib,
        block_head
    );
    // LIB should be >= status LIB (LIB only advances)
    assert!(
        block_lib >= status_lib,
        "Block result LIB ({}) should be >= status LIB ({})",
        block_lib,
        status_lib
    );
}

#[tokio::test]
async fn conformance_block_positions_mock() {
    let config = MockShipConfig {
        head_block: 1000,
        lib_block: 990,
        blocks_to_stream: 0,
        ..Default::default()
    };
    let (url, handle) = spawn_mock(config).await;
    assert_block_positions_consistent(&url).await;
    let _ = handle.await;
}

#[tokio::test]
#[ignore]
async fn conformance_block_positions_docker() {
    assert_block_positions_consistent(&docker_endpoint()).await;
}

// ===========================================================================
// CONFORMANCE TEST 6: Multiple status requests on same connection
// ===========================================================================

async fn assert_multiple_status_requests(url: &str) {
    let (mut writer, mut reader) = ws_connect(url).await;

    // Read ABI
    let _ = read_one(&mut reader).await;

    // Send 3 status requests in a row
    for i in 0..3 {
        writer
            .send(Message::Binary(build_status_request().into()))
            .await
            .unwrap();

        let msg = read_one(&mut reader).await;
        let data = match msg {
            Message::Binary(d) => d,
            other => panic!("Request {}: expected binary, got {:?}", i, other),
        };
        assert_eq!(data[0], 0, "Request {}: variant must be 0", i);
        let head = u32::from_le_bytes(data[1..5].try_into().unwrap());
        assert!(head > 0, "Request {}: head should be >0", i);
    }
}

#[tokio::test]
async fn conformance_multiple_status_mock() {
    let (url, handle) = spawn_mock(MockShipConfig::default()).await;
    assert_multiple_status_requests(&url).await;
    let _ = handle.await;
}

#[tokio::test]
#[ignore]
async fn conformance_multiple_status_docker() {
    assert_multiple_status_requests(&docker_endpoint()).await;
}
