/// Tier 1 Integration Tests: fleet-router against mock SHiP servers
///
/// These tests spawn lightweight mock SHiP servers in-process and verify
/// fleet-router's WebSocket proxy behavior.
use futures::{SinkExt, StreamExt};
use mock_ship::{MockShipConfig, MockShipServer};
use tokio_tungstenite::connect_async;
use tokio_tungstenite::tungstenite::Message;

/// Helper: connect a WS client and return the (writer, reader) pair.
async fn connect_client(
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
    let (ws, _) = connect_async(url).await.expect("Failed to connect");
    ws.split()
}

/// Test: Client connects to mock SHiP server and receives the ABI JSON as a text frame.
#[tokio::test]
async fn test_mock_connect_and_receive_abi() {
    let server = MockShipServer::new(MockShipConfig::default()).await;
    let url = server.ws_url();

    // Server handles one connection in background
    let server_task = tokio::spawn(async move {
        server.handle_one_connection().await;
    });

    // Client connects
    let (_writer, mut reader) = connect_client(&url).await;

    // First message should be the ABI JSON as Text
    let msg = reader.next().await.unwrap().unwrap();
    assert!(
        matches!(msg, Message::Text(_)),
        "Expected Text frame, got {:?}",
        msg
    );

    if let Message::Text(text) = msg {
        // Verify it's valid JSON with expected fields
        let abi: serde_json::Value = serde_json::from_str(&text).expect("ABI is not valid JSON");
        assert_eq!(abi["version"], "eosio::abi/1.1");
        assert!(abi["structs"].is_array());
        assert!(abi["variants"].is_array());
    }

    // Clean up
    drop(_writer);
    drop(reader);
    let _ = server_task.await;
}

/// Test: Client sends a status request (variant 0) and receives a valid status result.
#[tokio::test]
async fn test_mock_status_request() {
    let config = MockShipConfig {
        head_block: 5000,
        lib_block: 4990,
        ..Default::default()
    };
    let server = MockShipServer::new(config).await;
    let url = server.ws_url();

    let server_task = tokio::spawn(async move {
        server.handle_one_connection().await;
    });

    let (mut writer, mut reader) = connect_client(&url).await;

    // Read ABI text frame first
    let _abi = reader.next().await.unwrap().unwrap();

    // Send get_status_request_v0 (variant index 0, empty body)
    let status_request = vec![0u8]; // varuint32(0)
    writer
        .send(Message::Binary(status_request.into()))
        .await
        .unwrap();

    // Read status result
    let msg = reader.next().await.unwrap().unwrap();
    assert!(matches!(msg, Message::Binary(_)), "Expected Binary frame");

    if let Message::Binary(data) = msg {
        // First byte should be varuint32(0) = get_status_result_v0
        assert_eq!(data[0], 0, "Expected result variant 0");
        // Head block num at offset 1 (uint32_le)
        let head = u32::from_le_bytes(data[1..5].try_into().unwrap());
        assert_eq!(head, 5000);
        // LIB block num at offset 1+36 = 37 (after head block_position)
        let lib = u32::from_le_bytes(data[37..41].try_into().unwrap());
        assert_eq!(lib, 4990);
    }

    drop(writer);
    drop(reader);
    let _ = server_task.await;
}

/// Test: Client requests blocks and receives the correct block numbers.
#[tokio::test]
async fn test_mock_block_streaming() {
    let config = MockShipConfig {
        head_block: 1000,
        lib_block: 990,
        blocks_to_stream: 5,
        ..Default::default()
    };
    let server = MockShipServer::new(config).await;
    let url = server.ws_url();

    let server_task = tokio::spawn(async move {
        server.handle_one_connection().await;
    });

    let (mut writer, mut reader) = connect_client(&url).await;

    // Read ABI
    let _abi = reader.next().await.unwrap().unwrap();

    // Send get_blocks_request_v0 (variant 1):
    // start_block=100, end_block=200, max_messages_in_flight=5
    let mut request = vec![1u8]; // varuint32(1)
    request.extend_from_slice(&100u32.to_le_bytes()); // start_block_num
    request.extend_from_slice(&200u32.to_le_bytes()); // end_block_num
    request.extend_from_slice(&5u32.to_le_bytes()); // max_messages_in_flight
    request.push(0); // have_positions: empty array (varuint32 0)
    request.push(0); // irreversible_only
    request.push(0); // fetch_block
    request.push(0); // fetch_traces
    request.push(0); // fetch_deltas
    writer.send(Message::Binary(request.into())).await.unwrap();

    // Should receive exactly 5 blocks (blocks_to_stream=5)
    let mut received_blocks = Vec::new();
    for _ in 0..5 {
        let msg = reader.next().await.unwrap().unwrap();
        if let Message::Binary(data) = msg {
            // Variant should be 1 (get_blocks_result_v0)
            assert_eq!(data[0], 1);
            // this_block is at: 1 (variant) + 36 (head) + 36 (lib) + 1 (optional flag) = 74
            // but optional flag at offset 73 should be 1 (present)
            assert_eq!(data[73], 1, "this_block should be present");
            let block_num = u32::from_le_bytes(data[74..78].try_into().unwrap());
            received_blocks.push(block_num);
        }
    }

    assert_eq!(received_blocks, vec![100, 101, 102, 103, 104]);

    drop(writer);
    drop(reader);
    let _ = server_task.await;
}

/// Test: Mock server disconnects after N blocks (simulates upstream failure).
#[tokio::test]
async fn test_mock_disconnect_after() {
    let config = MockShipConfig {
        head_block: 1000,
        lib_block: 990,
        disconnect_after: Some(3),
        blocks_to_stream: 100,
        ..Default::default()
    };
    let server = MockShipServer::new(config).await;
    let url = server.ws_url();

    let server_task = tokio::spawn(async move {
        server.handle_one_connection().await;
    });

    let (mut writer, mut reader) = connect_client(&url).await;

    // Read ABI
    let _abi = reader.next().await.unwrap().unwrap();

    // Request blocks with high credit
    let mut request = vec![1u8];
    request.extend_from_slice(&100u32.to_le_bytes());
    request.extend_from_slice(&200u32.to_le_bytes());
    request.extend_from_slice(&10u32.to_le_bytes());
    request.push(0);
    request.push(0);
    request.push(0);
    request.push(0);
    request.push(0);
    writer.send(Message::Binary(request.into())).await.unwrap();

    // Should receive exactly 3 blocks then connection closes
    let mut count = 0;
    while let Some(Ok(msg)) = reader.next().await {
        if matches!(msg, Message::Binary(_)) {
            count += 1;
        }
    }

    assert_eq!(count, 3, "Expected exactly 3 blocks before disconnect");

    let _ = server_task.await;
}

/// Test: Multiple mock servers can run simultaneously (foundation for load balancing tests).
#[tokio::test]
async fn test_multiple_mock_servers() {
    let mut servers = Vec::new();
    let mut urls = Vec::new();

    for i in 0..3 {
        let config = MockShipConfig {
            head_block: 1000 + i * 100,
            lib_block: 990 + i * 100,
            ..Default::default()
        };
        let server = MockShipServer::new(config).await;
        urls.push(server.ws_url());
        servers.push(server);
    }

    // Connect to each server and verify different head blocks
    for (i, server) in servers.into_iter().enumerate() {
        let url = urls[i].clone();
        let server_task = tokio::spawn(async move {
            server.handle_one_connection().await;
        });

        let (mut writer, mut reader) = connect_client(&url).await;
        let _abi = reader.next().await.unwrap().unwrap();

        // Send status request
        writer
            .send(Message::Binary(vec![0u8].into()))
            .await
            .unwrap();

        let msg = reader.next().await.unwrap().unwrap();
        if let Message::Binary(data) = msg {
            let head = u32::from_le_bytes(data[1..5].try_into().unwrap());
            assert_eq!(head, 1000 + (i as u32) * 100);
        }

        drop(writer);
        drop(reader);
        let _ = server_task.await;
    }
}
