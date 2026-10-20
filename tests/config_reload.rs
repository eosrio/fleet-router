//! Real-process configuration transactions, port replacement and stream draining.
mod common;

use common::{base_config, server, RouterProcess};
use futures::{SinkExt, StreamExt};
use mock_ship::client::{blocks, BlocksReq};
use mock_ship::{MockShipConfig, MockShipServer};
use serde_json::{json, Value};
use std::time::Duration;
use tokio::time::{sleep, timeout};
use tokio_tungstenite::{connect_async, tungstenite::Message};

const TOKEN: &str = "config-reload-test-token";

async fn save(http: &reqwest::Client, base: &str, doc: &Value, cfg: &Value) {
    let response = http
        .put(format!("{base}/api/v1/config"))
        .bearer_auth(TOKEN)
        .json(&change(doc, Some(cfg)))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 200, "{}", response.text().await.unwrap());
}

async fn document(http: &reqwest::Client, base: &str) -> Value {
    let response = http
        .get(format!("{base}/api/v1/config"))
        .bearer_auth(TOKEN)
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 200);
    response.json().await.unwrap()
}

fn change(doc: &Value, source: Option<&Value>) -> Value {
    let mut body = json!({"expected_revision": doc["revision"], "expected_disk_revision": doc["disk_revision"]});
    if let Some(source) = source {
        body["source"] = json!(source.to_string());
    }
    body
}

async fn status(http: &reqwest::Client, base: &str, check: impl Fn(&Value) -> bool) -> Value {
    timeout(Duration::from_secs(8), async {
        loop {
            let value: Value = http
                .get(format!("{base}/api/v1/status"))
                .send()
                .await
                .unwrap()
                .json()
                .await
                .unwrap();
            if check(&value) {
                return value;
            }
            sleep(Duration::from_millis(30)).await;
        }
    })
    .await
    .expect("configuration did not converge")
}

async fn fixture() -> (
    RouterProcess,
    mock_ship::MockShipHandle,
    reqwest::Client,
    String,
) {
    let mock = MockShipServer::new(MockShipConfig::live(1, 1000)).await;
    let mut cfg = base_config(&[server("first", &mock.endpoint())]);
    cfg["dashboard"] = json!({"enabled": true, "control_token": TOKEN});
    let mock = mock.start();
    let router = RouterProcess::start(cfg).await;
    let base = format!("http://127.0.0.1:{}", router.metrics());
    (router, mock, reqwest::Client::new(), base)
}

#[tokio::test]
async fn config_is_authenticated_conflict_checked_and_saved_only_after_prepare() {
    let (_router, _mock, http, base) = fixture().await;
    assert_eq!(
        http.get(format!("{base}/api/v1/config"))
            .send()
            .await
            .unwrap()
            .status(),
        401
    );
    let doc = document(&http, &base).await;
    assert!(!doc["source"].as_str().unwrap().contains(TOKEN));
    let path = doc["path"].as_str().unwrap();
    let disk = std::fs::read_to_string(path).unwrap();
    let mut cfg: Value = serde_json::from_str(doc["source"].as_str().unwrap()).unwrap();

    // A valid config that cannot bind must not replace the file or running state.
    let occupied = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    cfg["listen_port"] = json!(occupied.local_addr().unwrap().port());
    let response = http
        .put(format!("{base}/api/v1/config"))
        .bearer_auth(TOKEN)
        .json(&change(&doc, Some(&cfg)))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 400);
    assert_eq!(std::fs::read_to_string(path).unwrap(), disk);
    assert_eq!(document(&http, &base).await["revision"], doc["revision"]);

    cfg = serde_json::from_str(doc["source"].as_str().unwrap()).unwrap();
    cfg["max_connections"] = json!(123);
    let validation: Value = http
        .post(format!("{base}/api/v1/config/validate"))
        .bearer_auth(TOKEN)
        .json(&change(&doc, Some(&cfg)))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(validation["listeners"], 1);
    assert_eq!(std::fs::read_to_string(path).unwrap(), disk);
    let response = http
        .put(format!("{base}/api/v1/config"))
        .bearer_auth(TOKEN)
        .json(&change(&doc, Some(&cfg)))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 200, "{}", response.text().await.unwrap());
    let saved: Value = serde_json::from_str(&std::fs::read_to_string(path).unwrap()).unwrap();
    assert_eq!(saved["max_connections"], 123);
    assert_eq!(
        saved["dashboard"]["control_token"], TOKEN,
        "redacted token must be preserved on save"
    );
    assert_eq!(
        http.put(format!("{base}/api/v1/config"))
            .bearer_auth(TOKEN)
            .json(&change(&doc, Some(&cfg)))
            .send()
            .await
            .unwrap()
            .status(),
        409
    );
    status(&http, &base, |s| {
        s["listeners"][0]["max_connections"] == 123
    })
    .await;

    // File edits use the exact same apply path, and malformed edits are recoverable.
    let doc = document(&http, &base).await;
    std::fs::write(path, "{invalid json").unwrap();
    assert_eq!(
        http.post(format!("{base}/api/v1/config/reload"))
            .bearer_auth(TOKEN)
            .json(&change(&doc, None))
            .send()
            .await
            .unwrap()
            .status(),
        409
    );
    let malformed = document(&http, &base).await;
    assert!(malformed["disk_error"].is_string());
    assert_eq!(
        http.post(format!("{base}/api/v1/config/reload"))
            .bearer_auth(TOKEN)
            .json(&change(&malformed, None))
            .send()
            .await
            .unwrap()
            .status(),
        400
    );
    saved_config_reload(&http, &base, path, saved).await;
}

async fn saved_config_reload(http: &reqwest::Client, base: &str, path: &str, mut cfg: Value) {
    cfg["max_connections"] = json!(234);
    std::fs::write(path, cfg.to_string()).unwrap();
    let doc = document(http, base).await;
    assert_eq!(
        http.post(format!("{base}/api/v1/config/reload"))
            .bearer_auth(TOKEN)
            .json(&change(&doc, None))
            .send()
            .await
            .unwrap()
            .status(),
        200
    );
    status(http, base, |s| s["listeners"][0]["max_connections"] == 234).await;
}

#[tokio::test]
async fn replacing_topology_preserves_old_stream_and_its_failover_pool() {
    let mock_cfg = MockShipConfig {
        block_delay: Some(Duration::from_millis(2)),
        ..MockShipConfig::live(1, 100_000)
    };
    let first = MockShipServer::new(mock_cfg.clone()).await;
    let backup = MockShipServer::new(mock_cfg.clone()).await;
    let replacement = MockShipServer::new(mock_cfg).await;
    let (first_ep, backup_ep, replacement_ep) =
        (first.endpoint(), backup.endpoint(), replacement.endpoint());
    let first = first.start();
    let _backup = backup.start();
    let _replacement = replacement.start();
    let mut b = server("backup", &backup_ep);
    b["priority"] = json!(1);
    let mut cfg = base_config(&[server("first", &first_ep), b]);
    cfg["dashboard"] = json!({"enabled": true, "control_token": TOKEN});
    let router = RouterProcess::start(cfg).await;
    let http = reqwest::Client::new();
    let base = format!("http://127.0.0.1:{}", router.metrics());
    let (mut client, _) = connect_async(router.ws_url()).await.unwrap();
    client.next().await.unwrap().unwrap();
    let (mut writer, mut reader) = client.split();
    writer
        .send(Message::Binary(
            blocks(&BlocksReq {
                start: 1,
                end: 100_000,
                max_in_flight: 100_000,
                ..Default::default()
            })
            .into(),
        ))
        .await
        .unwrap();
    let (before, closed) = common::read_blocks(&mut reader, 5, Duration::from_secs(3)).await;
    assert!(closed.is_none());
    assert_eq!(before.len(), 5);
    let doc = document(&http, &base).await;
    let mut cfg: Value = serde_json::from_str(doc["source"].as_str().unwrap()).unwrap();
    // Move from legacy single listener to a different named listener and port.
    let reservation = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let new_port = reservation.local_addr().unwrap().port();
    drop(reservation);
    cfg["servers"] = json!([]);
    cfg["listen_port"] = json!(0);
    cfg["listeners"] = json!([{"name":"new-pool", "listen_address":"127.0.0.1", "listen_port":new_port, "servers":[server("replacement", &replacement_ep)]}]);
    let response = http
        .put(format!("{base}/api/v1/config"))
        .bearer_auth(TOKEN)
        .json(&change(&doc, Some(&cfg)))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 200, "{}", response.text().await.unwrap());
    assert!(
        tokio::net::TcpStream::connect(("127.0.0.1", router.listen_port))
            .await
            .is_err(),
        "removed listener must release its port"
    );
    let view = status(&http, &base, |s| s["listeners"][0]["ready"] == true).await;
    assert_eq!(view["totals"]["sessions"], 1);
    assert_eq!(view["config"]["draining"][0]["listener"], "default");
    assert_eq!(view["config"]["draining"][0]["sessions"], 1);
    let (mut new_client, _) = connect_async(format!("ws://127.0.0.1:{new_port}"))
        .await
        .unwrap();
    new_client.next().await.unwrap().unwrap();
    // Retired streams can still fail over without closing their client socket.
    first.drop_sessions();
    let (after, closed) = common::read_blocks(&mut reader, 100, Duration::from_secs(5)).await;
    assert!(closed.is_none());
    assert_eq!(after.len(), 100);
    let nums: Vec<_> = before
        .iter()
        .chain(&after)
        .map(|b| b.this.unwrap().0)
        .collect();
    assert!(nums.windows(2).all(|p| p[1] == p[0] + 1));
    timeout(Duration::from_secs(5), async {
        loop {
            let sessions: Value = http
                .get(format!("{base}/api/v1/sessions"))
                .send()
                .await
                .unwrap()
                .json()
                .await
                .unwrap();
            if sessions["sessions"]
                .as_array()
                .unwrap()
                .iter()
                .any(|s| s["draining"] == true && s["upstream"] == backup_ep)
            {
                break;
            }
            // Keep consuming so a slow reader cannot hide the failover behind queued blocks.
            let _ = common::read_blocks(&mut reader, 50, Duration::from_secs(1)).await;
        }
    })
    .await
    .unwrap();
    writer.close().await.unwrap();
    drop(reader);
    status(&http, &base, |s| {
        s["config"]["draining"].as_array().unwrap().is_empty()
    })
    .await;
    timeout(Duration::from_secs(5), async {
        while first.connection_count().await != 0 || _backup.connection_count().await != 0 {
            sleep(Duration::from_millis(30)).await;
        }
    })
    .await
    .expect("retired monitors must stop after the last stream closes");
    new_client.close(None).await.unwrap();
}

#[tokio::test]
async fn same_port_upstream_replacement_preserves_connections_and_honors_the_new_cap() {
    let (router, _first, http, base) = fixture().await;
    let second = MockShipServer::new(MockShipConfig::live(1, 1000)).await;
    let endpoint = second.endpoint();
    let _second = second.start();
    let (mut old, _) = connect_async(router.ws_url()).await.unwrap();
    assert!(old.next().await.unwrap().unwrap().is_text());
    let doc = document(&http, &base).await;
    let mut cfg: Value = serde_json::from_str(doc["source"].as_str().unwrap()).unwrap();
    cfg["servers"] = json!([server("second", &endpoint)]);
    cfg["max_connections"] = json!(1);
    let response = http
        .put(format!("{base}/api/v1/config"))
        .bearer_auth(TOKEN)
        .json(&change(&doc, Some(&cfg)))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 200, "{}", response.text().await.unwrap());
    status(&http, &base, |s| s["listeners"][0]["ready"] == true).await;
    assert!(
        connect_async(router.ws_url()).await.is_err(),
        "draining streams count toward the new listener cap"
    );
    old.send(Message::Ping(vec![1, 2, 3].into())).await.unwrap();
    assert!(timeout(Duration::from_secs(2), old.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap()
        .is_pong());
    old.close(None).await.unwrap();
    status(&http, &base, |s| {
        s["config"]["draining"].as_array().unwrap().is_empty()
    })
    .await;
    let (mut new, _) = connect_async(router.ws_url()).await.unwrap();
    assert!(new.next().await.unwrap().unwrap().is_text());
    let sessions: Value = http
        .get(format!("{base}/api/v1/sessions"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(sessions["sessions"][0]["upstream"], endpoint);
    new.close(None).await.unwrap();
}

#[tokio::test]
async fn dashboard_address_and_token_change_without_losing_the_apply_response() {
    let (_router, _mock, http, base) = fixture().await;
    let doc = document(&http, &base).await;
    let mut cfg: Value = serde_json::from_str(doc["source"].as_str().unwrap()).unwrap();
    let reservation = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = reservation.local_addr().unwrap().port();
    drop(reservation);
    cfg["metrics_port"] = json!(port);
    cfg["dashboard"]["control_token"] = json!("replacement-control-token");
    let response = http
        .put(format!("{base}/api/v1/config"))
        .bearer_auth(TOKEN)
        .json(&change(&doc, Some(&cfg)))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 200);
    let result: Value = response.json().await.unwrap();
    assert_eq!(result["applied"], true);
    assert_eq!(result["access_changed"], true);
    let base = format!("http://127.0.0.1:{port}");
    assert_eq!(
        http.get(format!("{base}/api/v1/config"))
            .bearer_auth(TOKEN)
            .send()
            .await
            .unwrap()
            .status(),
        401
    );
    assert_eq!(
        http.get(format!("{base}/api/v1/config"))
            .bearer_auth("replacement-control-token")
            .send()
            .await
            .unwrap()
            .status(),
        200
    );
}

#[tokio::test]
async fn removing_and_readding_an_upstream_keeps_its_occupied_capacity() {
    let (router, _first, http, base) = fixture().await;
    let second = MockShipServer::new(MockShipConfig::live(1, 1000)).await;
    let endpoint = second.endpoint();
    let _second = second.start();
    let (mut old, _) = connect_async(router.ws_url()).await.unwrap();
    assert!(old.next().await.unwrap().unwrap().is_text());
    let doc = document(&http, &base).await;
    let mut cfg: Value = serde_json::from_str(doc["source"].as_str().unwrap()).unwrap();
    let mut first = cfg["servers"][0].clone();
    first["max_sessions"] = json!(1);
    let mut second = server("second", &endpoint);
    second["priority"] = json!(1);
    cfg["servers"] = json!([second.clone()]);
    save(&http, &base, &doc, &cfg).await;
    status(&http, &base, |s| s["listeners"][0]["ready"] == true).await;
    let doc = document(&http, &base).await;
    cfg["servers"] = json!([first, second]);
    save(&http, &base, &doc, &cfg).await;
    let (mut new, _) = connect_async(router.ws_url()).await.unwrap();
    assert!(new.next().await.unwrap().unwrap().is_text());
    let sessions: Value = http
        .get(format!("{base}/api/v1/sessions"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let current = sessions["sessions"]
        .as_array()
        .unwrap()
        .iter()
        .find(|s| s["draining"] == false)
        .unwrap();
    assert_eq!(
        current["upstream"], endpoint,
        "the restored preferred upstream still has its old stream and is full"
    );
    old.close(None).await.unwrap();
    new.close(None).await.unwrap();
}

#[tokio::test]
async fn existing_event_streams_obey_reloaded_dashboard_access_policy() {
    for disable in [false, true] {
        let (_router, _mock, http, base) = fixture().await;
        let doc = document(&http, &base).await;
        let mut cfg: Value = serde_json::from_str(doc["source"].as_str().unwrap()).unwrap();
        cfg["dashboard"]["allowed_hosts"] = json!(["operator.example"]);
        save(&http, &base, &doc, &cfg).await;
        let doc = document(&http, &base).await;
        let mut streams = Vec::new();
        for path in ["/api/v1/stream", "/api/v1/dashboard/stream"] {
            let mut response = http
                .get(format!("{base}{path}"))
                .header("Host", "operator.example")
                .send()
                .await
                .unwrap();
            assert_eq!(response.status(), 200);
            assert!(response.chunk().await.unwrap().is_some());
            streams.push(response);
        }
        if disable {
            cfg["dashboard"]["enabled"] = json!(false);
        } else {
            cfg["dashboard"]["allowed_hosts"] = json!([]);
        }
        save(&http, &base, &doc, &cfg).await;
        for mut response in streams {
            timeout(Duration::from_secs(3), async {
                while response.chunk().await.unwrap().is_some() {}
            })
            .await
            .expect("old SSE connection kept publishing after access was revoked");
        }
        assert_eq!(
            http.get(format!("{base}/api/v1/status"))
                .header("Host", "operator.example")
                .send()
                .await
                .unwrap()
                .status(),
            if disable { 404 } else { 403 }
        );
        assert_eq!(
            http.get(format!("{base}/health"))
                .send()
                .await
                .unwrap()
                .status(),
            200
        );
    }
}

#[tokio::test]
async fn chain_api_topology_drains_in_flight_requests_and_counts_old_connections() {
    use mock_ship::chain_api::{request, Fault, MockChainApi, RawConn};
    let (router, ship, http, base) = fixture().await;
    let chain_id = MockShipConfig::live(1, 1000).chain_id;
    let old_api = MockChainApi {
        clock: Some(ship.clock()),
        ..MockChainApi::live("old", chain_id, 1000, 990, Some(1), "v1.2.2")
    }
    .start()
    .await;
    let new_api = MockChainApi {
        clock: Some(ship.clock()),
        ..MockChainApi::live("new", chain_id, 1000, 990, Some(1), "v1.2.2")
    }
    .start()
    .await;
    let reservation = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = reservation.local_addr().unwrap();
    drop(reservation);
    let doc = document(&http, &base).await;
    let mut cfg: Value = serde_json::from_str(doc["source"].as_str().unwrap()).unwrap();
    cfg["servers"][0]["api_endpoint"] = json!(old_api.url());
    cfg["chain_api"] = json!({"listen_port":addr.port(), "probe_ms":100, "max_connections":1, "upstream_timeout_ms":5000});
    save(&http, &base, &doc, &cfg).await;
    let target = "/v1/chain/get_account";
    let query = request("POST", target, &addr.to_string(), b"{}");
    let mut old = RawConn::connect(addr).await.unwrap();
    timeout(Duration::from_secs(5), async {
        loop {
            old.send(&query).await.unwrap();
            if old.response(Duration::from_secs(2)).await.unwrap().status == 200 {
                break;
            }
            sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .unwrap();
    old_api.set_fault(Some(target), Some(Fault::Delay(Duration::from_millis(800))));
    let before = old_api.requests_to(target).len();
    old.send(&query).await.unwrap();
    timeout(Duration::from_secs(3), async {
        while old_api.requests_to(target).len() == before {
            sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    let doc = document(&http, &base).await;
    cfg["servers"][0]["api_endpoint"] = json!(new_api.url());
    save(&http, &base, &doc, &cfg).await;
    let view = status(&http, &base, |s| {
        !s["config"]["draining"].as_array().unwrap().is_empty()
    })
    .await;
    assert_eq!(view["config"]["draining"][0]["chain_api_connections"], 1);
    let mut rejected = RawConn::connect(addr).await.unwrap();
    assert!(
        rejected.closed(Duration::from_secs(2)).await,
        "old HTTP connections count toward the new cap"
    );
    let answer = old.response(Duration::from_secs(3)).await.unwrap();
    assert_eq!(answer.status, 200);
    assert_eq!(
        serde_json::from_slice::<Value>(&answer.body).unwrap()["served_by"],
        "old"
    );
    drop(old);
    status(&http, &base, |s| {
        s["config"]["draining"].as_array().unwrap().is_empty()
    })
    .await;
    let mut new = RawConn::connect(addr).await.unwrap();
    new.send(&query).await.unwrap();
    let answer = new.response(Duration::from_secs(3)).await.unwrap();
    assert_eq!(answer.status, 200);
    assert_eq!(
        serde_json::from_slice::<Value>(&answer.body).unwrap()["served_by"],
        "new"
    );
    // Removing the HTTP listener releases its port but leaves this connection usable.
    let doc = document(&http, &base).await;
    cfg.as_object_mut().unwrap().remove("chain_api");
    save(&http, &base, &doc, &cfg).await;
    assert!(tokio::net::TcpStream::connect(addr).await.is_err());
    new.send(&query).await.unwrap();
    assert_eq!(
        new.response(Duration::from_secs(3)).await.unwrap().status,
        200
    );
    drop(new);
    status(&http, &base, |s| {
        s["config"]["draining"].as_array().unwrap().is_empty()
    })
    .await;
    // The unaffected SHiP listener continues accepting.
    let (mut client, _) = connect_async(router.ws_url()).await.unwrap();
    assert!(client.next().await.unwrap().unwrap().is_text());
    client.close(None).await.unwrap();
}
