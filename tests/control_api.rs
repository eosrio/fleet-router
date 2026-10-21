//! End-to-end tests for the opt-in dashboard API and its control actions.
//!
//! Boots fleet-router against two mock SHiP upstreams with the dashboard and
//! a control token, streams blocks through one client session, and drives the
//! control API over HTTP.
//!
//! Run: cargo test --test control_api -- --nocapture
use std::io::Write;
use std::process::{Child, Command};
use std::time::Duration;

use futures::{SinkExt, StreamExt};
use mock_ship::{MockShipConfig, MockShipServer};
use serde_json::Value;
use tokio::time::{sleep, timeout};
use tokio_tungstenite::connect_async;
use tokio_tungstenite::tungstenite::Message;

const TOKEN: &str = "control-api-test-token";

type WsReader = futures::stream::SplitStream<
    tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>,
>;

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
    fn start(endpoints: &[String], dashboard: Value) -> Self {
        let listen_port = free_port();
        let metrics_port = free_port();
        let servers: Vec<Value> = endpoints
            .iter()
            .enumerate()
            .map(|(i, endpoint)| {
                serde_json::json!({
                    "name": format!("mock-{}", i + 1),
                    "endpoint": endpoint,
                    "enabled": true,
                    // mock-1 is preferred so the first session lands there.
                    "priority": i,
                })
            })
            .collect();
        let config = serde_json::json!({
            "listen_address": "127.0.0.1",
            "listen_port": listen_port,
            "upstream_reconnect_ms": 500,
            "upstream_monitoring_ms": 1000,
            "upstream_status_ms": 500,
            "metrics_address": "127.0.0.1",
            "metrics_port": metrics_port,
            "dashboard": dashboard,
            "servers": servers,
        });
        let mut file = tempfile::NamedTempFile::new().unwrap();
        file.write_all(config.to_string().as_bytes()).unwrap();
        file.flush().unwrap();
        let child = Command::new(env!("CARGO_BIN_EXE_fleet-router"))
            .arg("run")
            .arg("--config")
            .arg(file.path())
            .env_remove("FLEET_ROUTER_CONTROL_TOKEN")
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

    fn url(&self, path: &str) -> String {
        format!("http://127.0.0.1:{}{path}", self.metrics_port)
    }

    /// Wait until every upstream reports online.
    async fn wait_until_all_up(&self, client: &reqwest::Client, upstreams: usize) {
        for _ in 0..100 {
            if let Ok(resp) = client.get(self.url("/api/v1/status")).send().await {
                if let Ok(status) = resp.json::<Value>().await {
                    if status["totals"]["upstreams_up"] == upstreams {
                        return;
                    }
                }
            }
            sleep(Duration::from_millis(100)).await;
        }
        panic!("upstreams never all came online");
    }
}

fn mock_config(head: u32) -> MockShipConfig {
    MockShipConfig {
        head_block: head,
        lib_block: head - 10,
        blocks_to_stream: 0,
        block_delay: Some(Duration::from_millis(5)),
        ..Default::default()
    }
}

fn blocks_request(start: u32, end: u32) -> Vec<u8> {
    let mut buf = vec![1u8];
    buf.extend_from_slice(&start.to_le_bytes());
    buf.extend_from_slice(&end.to_le_bytes());
    buf.extend_from_slice(&u32::MAX.to_le_bytes()); // max_messages_in_flight
    buf.extend_from_slice(&[0, 0, 0, 0, 0]); // have_positions + four bools
    buf
}

/// Read until `n` block numbers arrive. Fails on close or a 5 s stall.
async fn read_blocks(reader: &mut WsReader, n: usize) -> Vec<u32> {
    let mut blocks = Vec::with_capacity(n);
    while blocks.len() < n {
        let msg = timeout(Duration::from_secs(5), reader.next())
            .await
            .expect("stalled waiting for blocks")
            .expect("stream ended")
            .expect("read error");
        match msg {
            Message::Binary(d) if d.len() >= 78 && d[0] == 1 && d[73] == 1 => {
                blocks.push(u32::from_le_bytes(d[74..78].try_into().unwrap()));
            }
            Message::Close(frame) => panic!("session closed unexpectedly: {frame:?}"),
            _ => {}
        }
    }
    blocks
}

fn assert_consecutive(blocks: &[u32]) {
    for pair in blocks.windows(2) {
        assert_eq!(pair[1], pair[0] + 1, "gap or duplicate in {blocks:?}");
    }
}

async fn only_session(client: &reqwest::Client, router: &Router) -> Value {
    let sessions: Value = client
        .get(router.url("/api/v1/sessions"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let list = sessions["sessions"].as_array().unwrap();
    assert_eq!(list.len(), 1, "sessions: {sessions}");
    list[0].clone()
}

/// A restart offsets one monitor's polls. Comparing its old sample to a
/// peer's newer one must not label either of two synchronized nodes lagged.
#[tokio::test]
async fn restarted_synced_nodes_do_not_alternate_dashboard_lag() {
    let config = MockShipConfig {
        block_delay: Some(Duration::from_millis(2)),
        ..MockShipConfig::live(1, 10_000)
    };
    let a = MockShipServer::new(config.clone()).await;
    let peer_polls = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let b = MockShipServer::new(MockShipConfig {
        status_requests: Some(peer_polls.clone()),
        ..config
    })
    .await;
    let (ep_a, ep_b) = (a.endpoint(), b.endpoint());
    let (a, b) = (a.start(), b.start());
    let router = Router::start(
        &[ep_a.clone(), ep_b.clone()],
        serde_json::json!({ "enabled": true }),
    );
    let http = reqwest::Client::new();
    router.wait_until_all_up(&http, 2).await;

    let (ws, _) = connect_async(format!("ws://127.0.0.1:{}", router.listen_port))
        .await
        .unwrap();
    let (mut writer, mut reader) = ws.split();
    let _abi = reader.next().await.unwrap().unwrap();
    writer
        .send(Message::Binary(blocks_request(100, 9_000).into()))
        .await
        .unwrap();
    let before = read_blocks(&mut reader, 20).await;
    assert_eq!(only_session(&http, &router).await["upstream"], ep_a);

    // Restart off the original polling boundary. The existing client must
    // fail over, and the returning node's monitor must resume polling.
    sleep(Duration::from_millis(180)).await;
    a.close_sessions();
    let after = read_blocks(&mut reader, 200).await;
    assert_consecutive(&[before, after].concat());
    assert_eq!(only_session(&http, &router).await["upstream"], ep_b);
    router.wait_until_all_up(&http, 2).await;

    // Anchor the returning monitor halfway between observed peer polls.
    // A delay measured from client reads can accidentally realign both polls.
    let previous_poll = peer_polls.load(std::sync::atomic::Ordering::Relaxed);
    timeout(Duration::from_secs(5), async {
        while peer_polls.load(std::sync::atomic::Ordering::Relaxed) == previous_poll {
            sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("peer monitor stopped polling");
    sleep(Duration::from_millis(250)).await;
    let accepted = a.sessions_accepted();
    a.close_sessions();
    timeout(Duration::from_secs(5), async {
        while a.sessions_accepted() == accepted {
            sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("restarted monitor did not reconnect");
    router.wait_until_all_up(&http, 2).await;

    let mut saw_different_samples = false;
    for step in 1..=40 {
        let head = 10_000 + step * 2;
        a.set_head(head, head - 10);
        b.set_head(head, head - 10);
        sleep(Duration::from_millis(40)).await;
        let status: Value = http
            .get(router.url("/api/v1/status"))
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        let upstreams = status["listeners"][0]["upstreams"].as_array().unwrap();
        let best = upstreams
            .iter()
            .filter_map(|u| u["chain_head_block"].as_u64())
            .max()
            .unwrap();
        saw_different_samples |= upstreams
            .iter()
            .any(|u| u["chain_head_block"].as_u64() != Some(best));
        for upstream in upstreams {
            assert_eq!(
                upstream["head_lag_blocks"], 0,
                "synchronized nodes falsely appear lagged: {status}"
            );
        }
    }
    assert!(
        saw_different_samples,
        "fixture did not offset the polling schedules"
    );
}

#[tokio::test]
async fn drain_moves_a_live_session_without_a_reconnect_and_close_ends_it() {
    let mock1 = MockShipServer::new(mock_config(1_000_000)).await;
    let mock2 = MockShipServer::new(mock_config(1_000_000)).await;
    let (ep1, ep2) = (mock1.endpoint(), mock2.endpoint());
    let (h1, h2) = (mock1.start(), mock2.start());
    let router = Router::start(
        &[ep1.clone(), ep2.clone()],
        serde_json::json!({ "enabled": true, "control_token": TOKEN }),
    );
    let http = reqwest::Client::new();
    router.wait_until_all_up(&http, 2).await;

    // A client streams blocks through the preferred upstream.
    let (ws, _) = connect_async(format!("ws://127.0.0.1:{}", router.listen_port))
        .await
        .unwrap();
    let (mut writer, mut reader) = ws.split();
    let _abi = reader.next().await.unwrap().unwrap();
    writer
        .send(Message::Binary(blocks_request(100, 900_000).into()))
        .await
        .unwrap();
    let before = read_blocks(&mut reader, 50).await;
    assert_eq!(before[0], 100);

    let session = only_session(&http, &router).await;
    assert_eq!(session["upstream"], ep1.as_str());
    assert_eq!(session["start_block"], 100);
    let id = session["id"].as_u64().unwrap();

    // Without the token the control API refuses.
    let drain_url = router.url(&format!(
        "/api/v1/listeners/default/upstreams/{}/drain",
        ep1.replace(':', "%3A")
    ));
    let resp = http.post(&drain_url).send().await.unwrap();
    assert_eq!(resp.status(), 401);

    // Drain mock-1: the session must move to mock-2 and keep streaming.
    let resp = http
        .post(&drain_url)
        .bearer_auth(TOKEN)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let upstream: Value = resp.json().await.unwrap();
    assert_eq!(upstream["status"], "disabled");
    assert!(upstream["operator_disabled"].as_bool().unwrap());

    let after = read_blocks(&mut reader, 300).await;
    let mut all = before.clone();
    all.extend(&after);
    assert_consecutive(&all);

    let session = only_session(&http, &router).await;
    assert_eq!(session["upstream"], ep2.as_str(), "session: {session}");
    assert_eq!(session["failovers"], 1);

    // Events record the operator action and the move.
    let events: Value = http
        .get(router.url("/api/v1/events"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let kinds: Vec<&str> = events["events"]
        .as_array()
        .unwrap()
        .iter()
        .filter_map(|event| event["kind"].as_str())
        .collect();
    assert!(kinds.contains(&"control"), "events: {events}");
    assert!(kinds.contains(&"session_failover"), "events: {events}");

    // Closing the session sends close code 1000.
    let resp = http
        .post(router.url(&format!("/api/v1/sessions/{id}/close")))
        .bearer_auth(TOKEN)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 202);
    let code = timeout(Duration::from_secs(5), async {
        while let Some(Ok(msg)) = reader.next().await {
            if let Message::Close(frame) = msg {
                return frame.map(|frame| u16::from(frame.code));
            }
        }
        None
    })
    .await
    .expect("no close frame");
    assert_eq!(code, Some(1000));

    drop(router);
    h1.shutdown().await;
    h2.shutdown().await;
}

#[tokio::test]
async fn dashboard_is_off_unless_enabled_and_control_is_off_without_a_token() {
    let mock1 = MockShipServer::new(mock_config(1000)).await;
    let ep1 = mock1.endpoint();
    let h1 = mock1.start();

    let router = Router::start(std::slice::from_ref(&ep1), serde_json::json!({}));
    let http = reqwest::Client::new();
    for _ in 0..100 {
        if http.get(router.url("/health")).send().await.is_ok() {
            break;
        }
        sleep(Duration::from_millis(100)).await;
    }
    let resp = http.get(router.url("/api/v1/status")).send().await.unwrap();
    assert_eq!(resp.status(), 404);
    let resp = http.get(router.url("/ui/")).send().await.unwrap();
    assert_eq!(resp.status(), 404);
    drop(router);

    let router = Router::start(
        std::slice::from_ref(&ep1),
        serde_json::json!({ "enabled": true }),
    );
    router.wait_until_all_up(&http, 1).await;
    let status: Value = http
        .get(router.url("/api/v1/status"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(status["control"]["enabled"], false);
    let resp = http
        .get(router.url("/api/v1/auth"))
        .bearer_auth(TOKEN)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 403);
    let resp = http.get(router.url("/ui/")).send().await.unwrap();
    assert_eq!(resp.status(), 200);
    assert!(resp
        .headers()
        .get("content-type")
        .unwrap()
        .to_str()
        .unwrap()
        .starts_with("text/html"));

    drop(router);
    h1.shutdown().await;
}

#[tokio::test]
async fn drain_keeps_a_history_reader_that_no_other_upstream_can_serve() {
    let archive = MockShipServer::new(mock_config(1_000_000)).await;
    let pruned = MockShipServer::new(MockShipConfig {
        trace_begin_block: 900_000,
        ..mock_config(1_000_000)
    })
    .await;
    let (archive_ep, pruned_ep) = (archive.endpoint(), pruned.endpoint());
    let (h1, h2) = (archive.start(), pruned.start());
    let router = Router::start(
        &[archive_ep.clone(), pruned_ep],
        serde_json::json!({ "enabled": true, "control_token": TOKEN }),
    );
    let http = reqwest::Client::new();
    router.wait_until_all_up(&http, 2).await;

    // A reader catching up on old history, which only the archive has.
    let (ws, _) = connect_async(format!("ws://127.0.0.1:{}", router.listen_port))
        .await
        .unwrap();
    let (mut writer, mut reader) = ws.split();
    let _abi = reader.next().await.unwrap().unwrap();
    writer
        .send(Message::Binary(blocks_request(100, 900_000).into()))
        .await
        .unwrap();
    let before = read_blocks(&mut reader, 20).await;
    assert_eq!(
        only_session(&http, &router).await["upstream"],
        archive_ep.as_str()
    );

    let resp = http
        .post(router.url(&format!(
            "/api/v1/listeners/default/upstreams/{}/drain",
            archive_ep.replace(':', "%3A")
        )))
        .bearer_auth(TOKEN)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);

    // The session stays on the archive and keeps streaming without a gap.
    let after = read_blocks(&mut reader, 100).await;
    let mut all = before.clone();
    all.extend(&after);
    assert_consecutive(&all);
    let session = only_session(&http, &router).await;
    assert_eq!(
        session["upstream"],
        archive_ep.as_str(),
        "session: {session}"
    );
    assert_eq!(session["failovers"], 0);

    // A manual move is refused for the same reason.
    let id = session["id"].as_u64().unwrap();
    let resp = http
        .post(router.url(&format!("/api/v1/sessions/{id}/move")))
        .bearer_auth(TOKEN)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 409);

    let events: Value = http
        .get(router.url("/api/v1/events"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert!(
        events["events"]
            .as_array()
            .unwrap()
            .iter()
            .any(|event| event["kind"] == "control"
                && event["message"].as_str().unwrap().contains("1 stay")),
        "events: {events}"
    );

    drop(router);
    h1.shutdown().await;
    h2.shutdown().await;
}
