//! Listener-level tests: CORS, `get_supported_apis` synthesis, and a
//! listener served in process (shutdown drain, HEAD).

use std::collections::BTreeSet;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use hyper::header::{HeaderMap, HeaderName, HeaderValue};
use mock_ship::chain_api::{request, Fault, MockChainApi, MockChainApiHandle, RawConn};
use tokio::net::TcpListener;
use tokio::sync::broadcast;
use tokio::time::{sleep, timeout};

use super::*;
use crate::models::{build_config_db, build_state_db, ServerConfig};

const WAIT: Duration = Duration::from_secs(5);
const CHAIN: [u8; 32] = [0x5a; 32];

fn set(paths: &[&str]) -> BTreeSet<String> {
    paths.iter().map(|p| p.to_string()).collect()
}

fn headers(pairs: &[(&'static str, &'static str)]) -> HeaderMap {
    let mut map = HeaderMap::new();
    for (name, value) in pairs {
        map.append(
            HeaderName::from_static(name),
            HeaderValue::from_static(value),
        );
    }
    map
}

fn names(map: &HeaderMap) -> Vec<String> {
    map.iter()
        .map(|(n, v)| format!("{n}: {}", v.to_str().unwrap()))
        .collect()
}

// CU6
#[test]
fn cors_headers_are_the_routers_own() {
    let upstream = || {
        headers(&[
            ("content-type", "application/json"),
            ("access-control-allow-origin", "https://upstream.example"),
            ("access-control-allow-methods", "POST"),
            ("access-control-expose-headers", "x"),
            ("server", "nodeos/v1.2.2"),
        ])
    };
    // No `cors`: like a nodeos with none configured, whatever upstream sent.
    let mut proxied = upstream();
    apply_cors(&mut proxied, None);
    assert_eq!(
        names(&proxied),
        ["content-type: application/json", "server: nodeos/v1.2.2"]
    );

    // `cors` set: replaced on proxied answers, added to the router's own.
    let full = CorsHeaders::new(&ChainApiCors {
        allow_origin: "*".into(),
        allow_headers: Some("Content-Type, X-Requested-With".into()),
        max_age: Some(600),
        allow_credentials: true,
    })
    .unwrap();
    let mut proxied = upstream();
    apply_cors(&mut proxied, Some(&full));
    assert_eq!(
        names(&proxied),
        [
            "content-type: application/json",
            "server: nodeos/v1.2.2",
            "access-control-allow-origin: *",
            "access-control-allow-headers: Content-Type, X-Requested-With",
            "access-control-max-age: 600",
            "access-control-allow-credentials: true",
        ]
    );
    for generated in [
        router_error(RouterError::NotFound),
        router_error(RouterError::IllegalTarget),
        busy(Busy::NoUpstream),
        super::generated(200, OPTIONS_BODY),
    ] {
        let mut map = generated.headers().clone();
        apply_cors(&mut map, Some(&full));
        assert_eq!(map["access-control-allow-origin"], "*");
        assert_eq!(map["access-control-allow-credentials"], "true");
        assert!(map.get("access-control-allow-methods").is_none());
        assert_eq!(map["content-type"], "application/json");
        assert!(map.get("server").is_none() && map.get("date").is_none());
    }
    // Only the configured parts.
    let origin_only = CorsHeaders::new(&ChainApiCors {
        allow_origin: "https://app.example".into(),
        allow_headers: None,
        max_age: None,
        allow_credentials: false,
    })
    .unwrap();
    let mut map = upstream();
    apply_cors(&mut map, Some(&origin_only));
    let cors: Vec<String> = names(&map)
        .into_iter()
        .filter(|h| h.starts_with("access-control-"))
        .collect();
    assert_eq!(cors, ["access-control-allow-origin: https://app.example"]);
    assert!(CorsHeaders::new(&ChainApiCors {
        allow_origin: "bad\nvalue".into(),
        allow_headers: None,
        max_age: None,
        allow_credentials: false,
    })
    .is_none());
}

// CU7
#[test]
fn supported_apis_are_the_sorted_union_of_what_upstreams_list() {
    let defaults = [ApiClass::Block, ApiClass::Info, ApiClass::State];
    let live = set(&[
        "/v1/chain/get_info",
        "/v1/chain/push_transaction",
        "/v1/chain/push_block",
        "/v1/chain/get_account",
        "/v1/chain/get_block",
        "/v1/trace_api/get_block",
        "/v1/node/get_supported_apis",
        "/v1/producer/pause",
    ]);
    let frozen = set(&[
        "/v1/chain/get_block_header",
        "/v1/chain/get_block_info",
        "/v1/chain/get_raw_block",
    ]);
    let body = supported_apis_body([&live, &frozen], &defaults);
    assert_eq!(
        String::from_utf8(body).unwrap(),
        r#"{"apis":["/v1/chain/get_account","/v1/chain/get_block","/v1/chain/get_block_header","/v1/chain/get_block_info","/v1/chain/get_info","/v1/chain/get_raw_block"]}"#
    );
    // Allowed classes filter; duplicates appear once; never itself.
    let pushes = [ApiClass::Push, ApiClass::Block];
    assert_eq!(
        String::from_utf8(supported_apis_body([&live, &live, &frozen], &pushes)).unwrap(),
        r#"{"apis":["/v1/chain/get_block","/v1/chain/get_block_header","/v1/chain/get_block_info","/v1/chain/get_raw_block","/v1/chain/push_transaction"]}"#
    );
    // Nothing known: an empty list, never invented paths.
    assert_eq!(
        supported_apis_body(std::iter::empty(), &defaults),
        br#"{"apis":[]}"#
    );
    // Sorted by bytes.
    let odd = set(&["/v1/chain/get_b", "/v1/chain/get_a_b", "/v1/chain/get_a"]);
    assert_eq!(
        String::from_utf8(supported_apis_body([&odd], &defaults)).unwrap(),
        r#"{"apis":["/v1/chain/get_a","/v1/chain/get_a_b","/v1/chain/get_b"]}"#
    );
}

#[test]
fn upstream_results_follow_the_answer() {
    assert_eq!(result_of(200, None), UpstreamResult::Ok);
    assert_eq!(result_of(204, None), UpstreamResult::Ok);
    assert_eq!(
        result_of(400, Some(RetryReason::UnknownBlock)),
        UpstreamResult::UnknownBlock
    );
    assert_eq!(result_of(404, None), UpstreamResult::NotFound);
    assert_eq!(
        result_of(404, Some(RetryReason::NotFound)),
        UpstreamResult::NotFound
    );
    assert_eq!(
        result_of(503, Some(RetryReason::Busy)),
        UpstreamResult::Busy
    );
    assert_eq!(result_of(503, None), UpstreamResult::Busy);
    assert_eq!(result_of(500, None), UpstreamResult::Error);
    assert_eq!(result_of(400, None), UpstreamResult::Error);
    assert_eq!(answer_role(None), AnswerRole::None);
    assert_eq!(answer_role(Some(Role::Frozen)), AnswerRole::Frozen);
}

/// A listener's chain API served in process, with `api` as its only live
/// upstream.
struct Served {
    ctx: Arc<ChainApiCtx>,
    addr: SocketAddr,
    shutdown: broadcast::Sender<()>,
    task: tokio::task::JoinHandle<()>,
}

async fn serve(api: &MockChainApiHandle, chain_api: serde_json::Value) -> Served {
    let cfg: ServerConfig = serde_json::from_value(serde_json::json!({
        "listen_address": "127.0.0.1",
        "listen_port": 17000,
        "chain_id": "5a".repeat(32),
        "upstream_reconnect_ms": 1000,
        "upstream_monitoring_ms": 1000,
        "upstream_status_ms": 1000,
        "servers": [{
            "name": "live-1",
            "endpoint": "127.0.0.1:9",
            "enabled": true,
            "api_endpoint": api.url()
        }],
        "chain_api": chain_api
    }))
    .unwrap();
    let listener = cfg.resolved_listeners().remove(0);
    let resolved = cfg.resolved_chain_apis().remove(0).unwrap();
    let hub = FleetHub::new(
        &listener,
        build_state_db(listener.servers.clone()),
        build_config_db(listener.servers.clone()),
    );
    let ctx = ChainApiCtx::new(&resolved, hub, Duration::from_secs(1), 2_000, 0);
    let tcp = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = tcp.local_addr().unwrap();
    let (shutdown, _) = broadcast::channel(4);
    let task = ctx.spawn(tcp, &shutdown);
    // Wait for the first good probe.
    let deadline = tokio::time::Instant::now() + WAIT;
    while !ctx.directory.slots()[0].state().up {
        assert!(
            tokio::time::Instant::now() < deadline,
            "the API never came up"
        );
        sleep(Duration::from_millis(10)).await;
    }
    Served {
        ctx,
        addr,
        shutdown,
        task,
    }
}

async fn live_api() -> MockChainApiHandle {
    MockChainApi::live("live-1", CHAIN, 1000, 990, Some(1), "v1.2.2")
        .start()
        .await
}

// CA20
#[tokio::test]
async fn shutdown_finishes_requests_in_progress_and_drains_connections() {
    let api = live_api().await;
    api.set_fault(
        Some("/v1/chain/get_account"),
        Some(Fault::Delay(Duration::from_millis(600))),
    );
    let served = serve(
        &api,
        serde_json::json!({ "listen_port": 1, "probe_ms": 100 }),
    )
    .await;
    let max = served.ctx.max_connections();

    // A request in progress, and an idle kept-alive connection.
    let mut slow = RawConn::connect(served.addr).await.unwrap();
    slow.send(&request(
        "POST",
        "/v1/chain/get_account",
        "127.0.0.1",
        br#"{"account_name":"eosio"}"#,
    ))
    .await
    .unwrap();
    let mut idle = RawConn::connect(served.addr).await.unwrap();
    idle.send(&request("POST", "/v1/chain/get_info", "127.0.0.1", b""))
        .await
        .unwrap();
    assert_eq!(idle.response(WAIT).await.unwrap().status, 200);
    let deadline = tokio::time::Instant::now() + WAIT;
    while api.requests_to("/v1/chain/get_account").is_empty() {
        assert!(tokio::time::Instant::now() < deadline);
        sleep(Duration::from_millis(5)).await;
    }
    assert_eq!(served.ctx.connections.available_permits(), max - 2);

    served.shutdown.send(()).unwrap();
    timeout(WAIT, served.task).await.unwrap().unwrap();
    // The idle connection closes at once; the slow request still gets its
    // answer, then its connection closes too.
    assert!(idle.closed(Duration::from_secs(2)).await);
    let answer = slow.response(WAIT).await.unwrap();
    assert_eq!(answer.status, 200);
    assert_eq!(
        answer.body,
        mock_ship::chain_api::call_answer("live-1", "/v1/chain/get_account", 24)
    );
    assert!(slow.closed(Duration::from_secs(2)).await);
    // No new connection is accepted, and every permit came back: the drain
    // that waits for the SHiP listeners waits for these too.
    // (Windows retries a refused connect for a while: no success is enough.)
    assert!(!matches!(
        timeout(
            Duration::from_secs(1),
            tokio::net::TcpStream::connect(served.addr)
        )
        .await,
        Ok(Ok(_))
    ));
    let deadline = tokio::time::Instant::now() + WAIT;
    while served.ctx.connections.available_permits() < max {
        assert!(
            tokio::time::Instant::now() < deadline,
            "connections never drained"
        );
        sleep(Duration::from_millis(10)).await;
    }
    assert!(!served.ctx.stats.bound());
}

// HEAD reaches the upstream as HEAD; the client gets the head (its
// Content-Length included) and no body, and the connections stay sound
// although nodeos writes a body after the head.
#[tokio::test]
async fn head_requests_are_proxied_as_head() {
    let api = live_api().await;
    let served = serve(
        &api,
        serde_json::json!({ "listen_port": 1, "probe_ms": 100 }),
    )
    .await;
    let get_info_len = {
        let mut conn = RawConn::connect(api.addr()).await.unwrap();
        conn.send(&request("POST", "/v1/chain/get_info", &api.endpoint(), b""))
            .await
            .unwrap();
        conn.response(WAIT).await.unwrap().body.len()
    };
    let mut conn = RawConn::connect(served.addr).await.unwrap();
    for _ in 0..3 {
        conn.send(&request("HEAD", "/v1/chain/get_info", "127.0.0.1", b""))
            .await
            .unwrap();
        let head = conn.response_head(WAIT).await.unwrap();
        assert_eq!(head.status, 200);
        assert_eq!(
            head.header("content-length"),
            Some(get_info_len.to_string().as_str())
        );
        assert_eq!(head.header("server"), Some("nodeos/v1.2.2"));
        // The same connection carries on with a whole answer.
        conn.send(&request(
            "POST",
            "/v1/chain/get_block_info",
            "127.0.0.1",
            br#"{"block_num":5}"#,
        ))
        .await
        .unwrap();
        let block = conn.response(WAIT).await.unwrap();
        assert_eq!(
            block.body,
            mock_ship::chain_api::block_answer("live-1", 5, &mock_ship::chain_api::block_id(5))
        );
    }
    let heads: Vec<_> = api
        .requests()
        .into_iter()
        .filter(|r| r.method == "HEAD")
        .collect();
    assert_eq!(heads.len(), 3);
    assert!(heads.iter().all(|r| r.target == "/v1/chain/get_info"));
    let _ = served.shutdown.send(());
}
