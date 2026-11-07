//! Operator rebalancing keeps the downstream WebSockets and their cursors.
mod common;

use std::time::Duration;

use common::{base_config, server, RouterProcess};
use futures::SinkExt;
use mock_ship::client::{blocks, BlocksReq};
use mock_ship::{MockShipConfig, MockShipServer};
use serde_json::{json, Value};
use tokio::time::{sleep, timeout};
use tokio_tungstenite::tungstenite::Message;

const TOKEN: &str = "rebalance-test-token";

async fn status(http: &reqwest::Client, base: &str) -> Value {
    http.get(format!("{base}/api/v1/status"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap()
}

async fn wait_status(http: &reqwest::Client, base: &str, check: impl Fn(&Value) -> bool) -> Value {
    timeout(Duration::from_secs(15), async {
        loop {
            let value = status(http, base).await;
            if check(&value) {
                return value;
            }
            sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("router status did not converge")
}

async fn action(http: &reqwest::Client, url: &str) -> reqwest::Response {
    http.post(url).bearer_auth(TOKEN).send().await.unwrap()
}

async fn rebalance(http: &reqwest::Client, base: &str) {
    let url = format!("{base}/api/v1/listeners/default/rebalance");
    let old: Value = http
        .get(format!("{base}/api/v1/events"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let after_id = old["events"]
        .as_array()
        .unwrap()
        .last()
        .and_then(|e| e["id"].as_u64())
        .unwrap_or(0);
    assert_eq!(action(http, &url).await.status(), 202);
    timeout(Duration::from_secs(15), async {
        loop {
            let events: Value = http
                .get(format!("{base}/api/v1/events"))
                .send()
                .await
                .unwrap()
                .json()
                .await
                .unwrap();
            if let Some(event) = events["events"]
                .as_array()
                .unwrap()
                .iter()
                .rev()
                .find(|event| {
                    event["id"].as_u64().is_some_and(|id| id > after_id)
                        && event["kind"] == "control"
                        && event["message"].as_str().is_some_and(|message| {
                            message.starts_with("rebalance finished:")
                                || message.starts_with("rebalance stopped")
                        })
                })
            {
                if status(http, base).await["listeners"][0]["rebalance_running"] == false {
                    assert!(
                        event["message"]
                            .as_str()
                            .unwrap()
                            .starts_with("rebalance finished:"),
                        "{event}"
                    );
                    return;
                }
            }
            sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("rebalance did not finish");
}

async fn clients(
    router: &RouterProcess,
    count: usize,
) -> Vec<(common::WsWriter, common::WsReader, Vec<u32>)> {
    let mut clients = Vec::new();
    for _ in 0..count {
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
        let (first, closed) = common::read_blocks(&mut reader, 10, Duration::from_secs(5)).await;
        assert!(closed.is_none());
        assert_eq!(first.len(), 10);
        clients.push((
            writer,
            reader,
            first.iter().map(|block| block.this.unwrap().0).collect(),
        ));
    }
    clients
}

async fn assert_continuity_past_current_router_progress(
    http: &reqwest::Client,
    base: &str,
    clients: &mut [(common::WsWriter, common::WsReader, Vec<u32>)],
) {
    let sessions: Value = http
        .get(format!("{base}/api/v1/sessions"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let through = sessions["sessions"]
        .as_array()
        .unwrap()
        .iter()
        .filter_map(|session| session["last_block"].as_u64())
        .max()
        .unwrap() as u32
        + 5;
    for (_writer, reader, before) in clients {
        let mut last = *before.last().unwrap();
        timeout(Duration::from_secs(15), async {
            while last < through {
                let (next, closed) = common::read_blocks(reader, 20, Duration::from_secs(5)).await;
                assert!(closed.is_none());
                assert_eq!(next.len(), 20);
                for block in next {
                    let number = block.this.unwrap().0;
                    assert_eq!(number, last + 1, "gap or duplicate after block {last}");
                    last = number;
                }
            }
        })
        .await
        .expect("client did not progress beyond the rebalance");
    }
}

#[tokio::test]
async fn rebalance_restores_weighted_distribution_without_reconnecting_clients() {
    let config = MockShipConfig {
        block_delay: Some(Duration::from_millis(5)),
        ..MockShipConfig::live(1, 10_000)
    };
    let a = MockShipServer::new(config.clone()).await;
    let b = MockShipServer::new(config).await;
    let (ep_a, ep_b) = (a.endpoint(), b.endpoint());
    let (_a, _b) = (a.start(), b.start());
    let mut cfg = base_config(&[server("a", &ep_a), server("b", &ep_b)]);
    cfg["dashboard"] = json!({"enabled": true, "control_token": TOKEN});
    let router = RouterProcess::start(cfg).await;
    let base = format!("http://127.0.0.1:{}", router.metrics());
    let http = reqwest::Client::new();
    wait_status(&http, &base, |s| s["totals"]["upstreams_up"] == 2).await;
    let upstream = format!("{base}/api/v1/listeners/default/upstreams/{ep_b}");
    assert_eq!(
        action(&http, &format!("{upstream}/disable")).await.status(),
        200
    );
    let mut clients = clients(&router, 4).await;
    wait_status(&http, &base, |s| {
        s["listeners"][0]["upstreams"][0]["sessions"] == 4
    })
    .await;
    assert_eq!(
        action(&http, &format!("{upstream}/enable")).await.status(),
        200
    );

    let url = format!("{base}/api/v1/listeners/default/rebalance");
    assert_eq!(http.post(&url).send().await.unwrap().status(), 401);
    rebalance(&http, &base).await;
    let s = status(&http, &base).await;
    assert_eq!(s["listeners"][0]["upstreams"][0]["sessions"], 2);
    assert_eq!(s["listeners"][0]["upstreams"][1]["sessions"], 2);
    assert_eq!(s["totals"]["sessions"], 4);

    // A second command is a no-op when the equal-weight pool is balanced.
    let before: Value = http
        .get(format!("{base}/api/v1/sessions"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    rebalance(&http, &base).await;
    let after: Value = http
        .get(format!("{base}/api/v1/sessions"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(
        before["sessions"]
            .as_array()
            .unwrap()
            .iter()
            .map(|s| s["failovers"].as_u64().unwrap())
            .sum::<u64>(),
        after["sessions"]
            .as_array()
            .unwrap()
            .iter()
            .map(|s| s["failovers"].as_u64().unwrap())
            .sum::<u64>()
    );

    // A lower-priority backup yields to the preferred upstream.
    assert_eq!(
        http.patch(&upstream)
            .bearer_auth(TOKEN)
            .json(&json!({"priority": 1}))
            .send()
            .await
            .unwrap()
            .status(),
        200
    );
    rebalance(&http, &base).await;
    let s = status(&http, &base).await;
    assert_eq!(s["listeners"][0]["upstreams"][0]["sessions"], 4);
    assert_eq!(s["listeners"][0]["upstreams"][1]["sessions"], 0);

    // Weighted routing favors b within the same tier; four clients settle at 1:3.
    assert_eq!(
        http.patch(&upstream)
            .bearer_auth(TOKEN)
            .json(&json!({"priority": 0, "weight": 2}))
            .send()
            .await
            .unwrap()
            .status(),
        200
    );
    rebalance(&http, &base).await;
    let s = status(&http, &base).await;
    assert_eq!(s["listeners"][0]["upstreams"][0]["sessions"], 1);
    assert_eq!(s["listeners"][0]["upstreams"][1]["sessions"], 3);

    assert_continuity_past_current_router_progress(&http, &base, &mut clients).await;
}

#[tokio::test]
async fn rebalance_after_an_upstream_restarts() {
    let config = MockShipConfig {
        block_delay: Some(Duration::from_millis(5)),
        ..MockShipConfig::live(1, 10_000)
    };
    let a = MockShipServer::new(config.clone()).await;
    let b = MockShipServer::new(config.clone()).await;
    let (ep_a, ep_b) = (a.endpoint(), b.endpoint());
    let (a, _b) = (a.start(), b.start());
    let mut cfg = base_config(&[server("a", &ep_a), server("b", &ep_b)]);
    cfg["dashboard"] = json!({"enabled": true, "control_token": TOKEN});
    let router = RouterProcess::start(cfg).await;
    let base = format!("http://127.0.0.1:{}", router.metrics());
    let http = reqwest::Client::new();
    wait_status(&http, &base, |s| s["totals"]["upstreams_up"] == 2).await;
    let mut clients = clients(&router, 4).await;
    wait_status(&http, &base, |s| {
        s["listeners"][0]["upstreams"][0]["sessions"] == 2
            && s["listeners"][0]["upstreams"][1]["sessions"] == 2
    })
    .await;

    // Stop one SHiP node and its sessions. Every client must fail over to b.
    a.drop_sessions();
    a.shutdown().await;
    wait_status(&http, &base, |s| {
        s["totals"]["upstreams_up"] == 1 && s["listeners"][0]["upstreams"][1]["sessions"] == 4
    })
    .await;

    let port: u16 = ep_a.rsplit(':').next().unwrap().parse().unwrap();
    timeout(Duration::from_secs(5), async {
        while std::net::TcpListener::bind(("127.0.0.1", port)).is_err() {
            sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("the stopped mock port was not released");
    let restarted = MockShipServer::new(MockShipConfig { port, ..config }).await;
    let _restarted = restarted.start();
    wait_status(&http, &base, |s| s["totals"]["upstreams_up"] == 2).await;
    rebalance(&http, &base).await;
    let s = status(&http, &base).await;
    assert_eq!(s["listeners"][0]["upstreams"][0]["sessions"], 2);
    assert_eq!(s["listeners"][0]["upstreams"][1]["sessions"], 2);
    assert_eq!(s["totals"]["sessions"], 4);
    assert_continuity_past_current_router_progress(&http, &base, &mut clients).await;
}

#[tokio::test]
async fn rebalance_respects_priority_and_does_not_move_a_history_reader_to_a_pruned_node() {
    let a = MockShipServer::new(MockShipConfig {
        block_delay: Some(Duration::from_millis(5)),
        ..MockShipConfig::live(1, 10_000)
    })
    .await;
    let b = MockShipServer::new(MockShipConfig {
        block_delay: Some(Duration::from_millis(5)),
        ..MockShipConfig::live(3000, 10_000)
    })
    .await;
    let (ep_a, ep_b) = (a.endpoint(), b.endpoint());
    let (_a, _b) = (a.start(), b.start());
    let mut secondary = server("b", &ep_b);
    secondary["priority"] = json!(1);
    let mut cfg = base_config(&[server("a", &ep_a), secondary]);
    cfg["dashboard"] = json!({"enabled": true, "control_token": TOKEN});
    let router = RouterProcess::start(cfg).await;
    let base = format!("http://127.0.0.1:{}", router.metrics());
    let http = reqwest::Client::new();
    wait_status(&http, &base, |s| s["totals"]["upstreams_up"] == 2).await;
    let _clients = clients(&router, 2).await;
    rebalance(&http, &base).await;
    let s = status(&http, &base).await;
    assert_eq!(s["listeners"][0]["upstreams"][0]["sessions"], 2);
    assert_eq!(s["listeners"][0]["upstreams"][1]["sessions"], 0);
    assert_eq!(
        http.patch(format!("{base}/api/v1/listeners/default/upstreams/{ep_b}"))
            .bearer_auth(TOKEN)
            .json(&json!({"priority": 0}))
            .send()
            .await
            .unwrap()
            .status(),
        200
    );
    rebalance(&http, &base).await;
    let s = status(&http, &base).await;
    assert_eq!(s["listeners"][0]["upstreams"][0]["sessions"], 2);
    assert_eq!(s["listeners"][0]["upstreams"][1]["sessions"], 0);
}

#[tokio::test]
async fn rebalance_refuses_stitched_listeners() {
    let upstream = MockShipServer::new(MockShipConfig::live(1, 10_000)).await;
    let endpoint = upstream.endpoint();
    let _upstream = upstream.start();
    let mut cfg = base_config(&[server("live", &endpoint)]);
    cfg["topology"] = json!("stitched");
    cfg["chain_id"] = json!("00".repeat(32));
    cfg["dashboard"] = json!({"enabled": true, "control_token": TOKEN});
    let router = RouterProcess::start(cfg).await;
    let base = format!("http://127.0.0.1:{}", router.metrics());
    let http = reqwest::Client::new();
    let s = status(&http, &base).await;
    assert_eq!(s["listeners"][0]["rebalance_supported"], false);
    let response = action(&http, &format!("{base}/api/v1/listeners/default/rebalance")).await;
    assert_eq!(response.status(), 409);
    assert!(response
        .text()
        .await
        .unwrap()
        .contains("pool listeners only"));
}
