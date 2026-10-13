//! Client-side memory limits: oversized WebSocket frames are rejected, and the
//! per-session failover buffer closes the session once its byte cap is exceeded.
//!
//! Run: cargo test --test client_limits
use std::io::Write;
use std::process::{Child, Command};
use std::time::Duration;

use futures::{SinkExt, StreamExt};
use mock_ship::{MockShipConfig, MockShipServer};
use tokio::time::{sleep, timeout};
use tokio_tungstenite::connect_async;
use tokio_tungstenite::tungstenite::protocol::frame::coding::CloseCode;
use tokio_tungstenite::tungstenite::Message;

struct Router {
    child: Child,
    listen_port: u16,
    _config: tempfile::NamedTempFile,
}

impl Router {
    async fn start(upstream: &str, extra: serde_json::Value) -> Self {
        let listen_port = {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            listener.local_addr().unwrap().port()
        };
        let mut config = serde_json::json!({
            "listen_address": "127.0.0.1",
            "listen_port": listen_port,
            "upstream_reconnect_ms": 1000,
            "upstream_monitoring_ms": 2000,
            "upstream_status_ms": 2000,
            "servers": [{ "name": "mock", "endpoint": upstream, "enabled": true }]
        });
        for (key, value) in extra.as_object().unwrap() {
            config[key] = value.clone();
        }
        let mut file = tempfile::NamedTempFile::new().unwrap();
        file.write_all(config.to_string().as_bytes()).unwrap();
        file.flush().unwrap();

        let child = Command::new(env!("CARGO_BIN_EXE_fleet-router"))
            .arg("--config")
            .arg(file.path())
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::inherit())
            .spawn()
            .expect("failed to spawn fleet-router");
        let router = Self {
            child,
            listen_port,
            _config: file,
        };
        router.wait_listening().await;
        router
    }

    fn url(&self) -> String {
        format!("ws://127.0.0.1:{}", self.listen_port)
    }

    async fn wait_listening(&self) {
        let addr = format!("127.0.0.1:{}", self.listen_port);
        for _ in 0..50 {
            if tokio::net::TcpStream::connect(&addr).await.is_ok() {
                return;
            }
            sleep(Duration::from_millis(100)).await;
        }
        panic!("fleet-router did not listen on {addr}");
    }
}

impl Drop for Router {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

type Ws =
    tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>;

async fn connect_for_abi(url: &str) -> Ws {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        if let Ok((mut ws, _)) = connect_async(url).await {
            match timeout(Duration::from_secs(2), ws.next()).await {
                Ok(Some(Ok(Message::Text(text)))) if text.contains('{') => return ws,
                _ => {}
            }
        }
        if tokio::time::Instant::now() >= deadline {
            panic!("client did not receive an ABI from {url}");
        }
        sleep(Duration::from_millis(50)).await;
    }
}

/// A client frame larger than `max_client_message_bytes` is rejected and the
/// socket does not stay up to proxy it.
#[tokio::test]
async fn oversized_client_frame_is_rejected() {
    let mock = MockShipServer::new(MockShipConfig::default()).await;
    let endpoint = mock.endpoint();
    let _mock = mock.start();
    let limit = 1024usize;
    let router = Router::start(
        &endpoint,
        serde_json::json!({ "max_client_message_bytes": limit }),
    )
    .await;
    let mut ws = connect_for_abi(&router.url()).await;

    ws.send(Message::binary(vec![0u8; limit + 1]))
        .await
        .expect("client should be allowed to transmit the frame");

    let next = timeout(Duration::from_secs(3), ws.next())
        .await
        .expect("timed out waiting for the oversized frame to be rejected");
    match next {
        Some(Ok(Message::Binary(_))) => {
            panic!("oversized client frame was proxied")
        }
        Some(Ok(Message::Close(frame))) => {
            let code = frame.map(|f| f.code);
            assert!(
                code == Some(CloseCode::Size) || code == Some(CloseCode::Policy) || code.is_none(),
                "unexpected close for an oversized frame: {code:?}"
            );
        }
        Some(Err(_)) | None => {}
        Some(Ok(other)) => panic!("unexpected message after oversized frame: {other:?}"),
    }
}

/// A frame of exactly `max_client_message_bytes` is still forwarded.
#[tokio::test]
async fn client_frame_at_configured_limit_is_forwarded() {
    let mock = MockShipServer::new(MockShipConfig::default()).await;
    let endpoint = mock.endpoint();
    let _mock = mock.start();
    let limit = 1024usize;
    let router = Router::start(
        &endpoint,
        serde_json::json!({ "max_client_message_bytes": limit }),
    )
    .await;
    let mut ws = connect_for_abi(&router.url()).await;

    // Leading 0 is get_status_request_v0. Extra bytes are ignored by the mock,
    // which still answers with a status result when the frame is forwarded.
    ws.send(Message::binary(vec![0u8; limit]))
        .await
        .expect("frame at the configured limit should be writable");

    let next = timeout(Duration::from_secs(3), ws.next())
        .await
        .expect("timed out waiting for a forwarded status result")
        .expect("connection closed on a frame at the configured limit")
        .expect("read error on a frame at the configured limit");
    assert!(
        matches!(next, Message::Binary(_)),
        "expected the status result to be forwarded, got {next:?}"
    );
}

/// Once buffered client bytes exceed the per-session cap, the router closes
/// the socket with close code 1008 instead of queuing more frames.
#[tokio::test]
async fn pending_byte_cap_closes_session() {
    let mock = MockShipServer::new(MockShipConfig::default()).await;
    let endpoint = mock.endpoint();
    let mock_handle = mock.start();
    // Omit max_client_message_bytes so this also covers a legacy config: the
    // default client cap is 64 KiB, and each frame below is under that.
    let router = Router::start(&endpoint, serde_json::json!({})).await;
    let ws = connect_for_abi(&router.url()).await;

    // Kill the live upstream session but keep the port open so failover blocks
    // in the handshake instead of finishing (and flushing) before we overflow.
    mock_handle.stall_and_drop_sessions();

    let (mut writer, mut reader) = ws.split();
    let send = tokio::spawn(async move {
        sleep(Duration::from_millis(100)).await;
        for _ in 0..16 {
            let mut payload = vec![0u8; 16 * 1024];
            payload[0] = 9; // not a blocks request
            if timeout(
                Duration::from_millis(300),
                writer.send(Message::binary(payload)),
            )
            .await
            .is_err()
            {
                break;
            }
            sleep(Duration::from_millis(10)).await;
        }
    });

    let frame = timeout(Duration::from_secs(5), async {
        loop {
            match reader.next().await {
                Some(Ok(Message::Close(Some(frame)))) => return frame,
                Some(Ok(Message::Ping(_))) | Some(Ok(Message::Pong(_))) => continue,
                Some(Ok(other)) => panic!("unexpected message while filling the buffer: {other:?}"),
                Some(Err(err)) => panic!("client read failed before a policy close: {err}"),
                None => panic!("client stream ended without a close frame"),
            }
        }
    })
    .await
    .expect("timed out waiting for the pending-buffer close");

    send.abort();
    assert_eq!(frame.code, CloseCode::Policy);
    assert!(
        frame.reason.contains("pending"),
        "close reason should name the pending buffer, got {}",
        frame.reason
    );
}
