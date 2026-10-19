//! One server-driven stream supplies the complete dashboard, including sessions.
mod common;

use common::{base_config, server, RouterProcess};
use futures::{SinkExt, StreamExt};
use mock_ship::client::{blocks, BlocksReq};
use mock_ship::{MockShipConfig, MockShipServer};
use serde_json::{json, Value};
use std::time::Duration;
use tokio::time::timeout;
use tokio_tungstenite::connect_async;
use tokio_tungstenite::tungstenite::Message;

struct Stream {
    response: reqwest::Response,
    pending: Vec<u8>,
}

impl Stream {
    async fn connect(http: &reqwest::Client, base: &str) -> Self {
        let response = http
            .get(format!("{base}/api/v1/dashboard/stream"))
            .send()
            .await
            .unwrap();
        assert_eq!(
            response.status(),
            200,
            "the dashboard must bootstrap from one push connection"
        );
        Self {
            response,
            pending: Vec::new(),
        }
    }

    async fn next(&mut self) -> Value {
        timeout(Duration::from_secs(4), async {
            loop {
                if let Some(end) = self.pending.windows(2).position(|pair| pair == b"\n\n") {
                    let message = String::from_utf8(self.pending[..end].to_vec()).unwrap();
                    self.pending.drain(..end + 2);
                    if let Some(data) = message.lines().find_map(|line| line.strip_prefix("data: "))
                    {
                        return serde_json::from_str(data).unwrap();
                    }
                } else {
                    let chunk = self.response.chunk().await.unwrap().expect("stream closed");
                    self.pending.extend_from_slice(&chunk);
                }
            }
        })
        .await
        .expect("no dashboard update")
    }

    async fn until(&mut self, mut check: impl FnMut(&Value) -> bool) -> Value {
        timeout(Duration::from_secs(5), async {
            loop {
                let frame = self.next().await;
                let sessions = frame["sessions"].as_array().unwrap();
                assert_eq!(frame["status"]["totals"]["sessions"], sessions.len());
                if check(&frame) {
                    return frame;
                }
            }
        })
        .await
        .expect("dashboard state did not converge")
    }
}

#[tokio::test]
async fn push_bootstraps_sessions_controls_and_reconnect_without_read_requests() {
    let mock = MockShipServer::new(MockShipConfig::live(1, 1000)).await;
    let endpoint = mock.endpoint();
    let mut cfg = base_config(&[server("primary", &endpoint)]);
    cfg["dashboard"] = json!({"enabled": true, "control_token": "dashboard-push-test-token"});
    let _mock = mock.start();
    let router = RouterProcess::start(cfg).await;
    let base = format!("http://127.0.0.1:{}", router.metrics());
    let http = reqwest::Client::new();
    let mut stream = Stream::connect(&http, &base).await;
    let bootstrap = stream.next().await;
    assert!(bootstrap["history"]["samples"].is_array());
    assert!(bootstrap["events"].is_array());
    assert!(bootstrap["revision"].as_u64().unwrap() > 0);
    let first = bootstrap["revision"].as_u64().unwrap();

    let (mut client, _) = connect_async(router.ws_url()).await.unwrap();
    client.next().await.unwrap().unwrap(); // ABI
    let opened = stream
        .until(|f| f["sessions"].as_array().unwrap().len() == 1)
        .await;
    assert!(opened["revision"].as_u64().unwrap() > first);
    assert_eq!(opened["status"]["listeners"][0]["sessions"], 1);
    client.close(None).await.unwrap();
    stream
        .until(|f| f["sessions"].as_array().unwrap().is_empty())
        .await;

    let response = http
        .post(format!(
            "{base}/api/v1/listeners/default/upstreams/{endpoint}/disable"
        ))
        .bearer_auth("dashboard-push-test-token")
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 200);
    let mut saw_control = false;
    stream
        .until(|f| {
            saw_control |= f["events"]
                .as_array()
                .unwrap()
                .iter()
                .any(|e| e["kind"] == "control");
            saw_control && f["status"]["listeners"][0]["upstreams"][0]["operator_disabled"] == true
        })
        .await;

    // Reconnection supplies state, retained history and retained events again;
    // the browser never needs separate status/sessions/history/events GETs.
    drop(stream);
    let mut stream = Stream::connect(&http, &base).await;
    let resumed = stream.next().await;
    assert!(resumed["history"]["samples"].is_array());
    assert_eq!(
        resumed["status"]["listeners"][0]["upstreams"][0]["operator_disabled"],
        true
    );
    assert!(resumed["events"]
        .as_array()
        .unwrap()
        .iter()
        .any(|e| e["kind"] == "control"));
}

#[tokio::test]
async fn failover_pushes_the_new_assignment_and_event_on_the_existing_stream() {
    let config = MockShipConfig {
        block_delay: Some(Duration::from_millis(5)),
        ..MockShipConfig::live(1, 10_000)
    };
    let primary = MockShipServer::new(config.clone()).await;
    let backup = MockShipServer::new(config).await;
    let (ep1, ep2) = (primary.endpoint(), backup.endpoint());
    let (primary, _backup) = (primary.start(), backup.start());
    let mut secondary = server("backup", &ep2);
    secondary["priority"] = json!(1);
    let mut cfg = base_config(&[server("primary", &ep1), secondary]);
    cfg["dashboard"] = json!({"enabled": true});
    let router = RouterProcess::start(cfg).await;
    let http = reqwest::Client::new();
    let mut stream =
        Stream::connect(&http, &format!("http://127.0.0.1:{}", router.metrics())).await;
    stream
        .until(|f| f["status"]["totals"]["upstreams_up"] == 2)
        .await;
    let (mut writer, mut reader) = common::ws_connect(&router.ws_url()).await;
    common::read_msg(&mut reader, Duration::from_secs(2)).await; // ABI
    writer
        .send(Message::Binary(
            blocks(&BlocksReq {
                start: 100,
                end: 9000,
                max_in_flight: u32::MAX,
                ..Default::default()
            })
            .into(),
        ))
        .await
        .unwrap();
    let (before, closed) = common::read_blocks(&mut reader, 10, Duration::from_secs(3)).await;
    assert!(closed.is_none());
    assert_eq!(before.len(), 10);
    let assigned = stream.until(|f| f["sessions"][0]["upstream"] == ep1).await;
    let id = assigned["sessions"][0]["id"].clone();
    primary.drop_sessions();
    let mut saw_event = false;
    let moved = timeout(Duration::from_secs(5), async {
        loop {
            let f = stream.next().await;
            assert_eq!(f["status"]["totals"]["sessions"], 1);
            saw_event |= f["events"]
                .as_array()
                .unwrap()
                .iter()
                .any(|e| e["kind"] == "session_failover" && e["session"] == id);
            if saw_event && f["sessions"][0]["upstream"] == ep2 {
                break f;
            }
        }
    })
    .await
    .expect("failover did not reach the dashboard");
    assert_eq!(moved["sessions"][0]["id"], id);
    assert_eq!(moved["sessions"][0]["failovers"], 1);
    let (after, closed) = common::read_blocks(&mut reader, 100, Duration::from_secs(3)).await;
    assert!(closed.is_none());
    assert_eq!(after.len(), 100);
    let nums: Vec<_> = before
        .iter()
        .chain(&after)
        .map(|b| b.this.unwrap().0)
        .collect();
    assert!(nums.windows(2).all(|pair| pair[1] == pair[0] + 1));
}
