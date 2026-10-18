//! Listener-level tests: CORS, `get_supported_apis` synthesis, and a
//! listener served in process (shutdown drain, HEAD, bodies, stalled
//! clients and upstreams, connects that hang, deadlines, control API
//! changes).

use std::collections::BTreeSet;
use std::convert::Infallible;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use futures::{stream, StreamExt};
use http_body_util::StreamBody;
use hyper::body::Frame;
use hyper::header::{HeaderMap, HeaderName, HeaderValue};
use mock_ship::chain_api::{
    call_answer, request, Fault, MockChainApi, MockChainApiHandle, RawConn, RawResponse,
};
use serde_json::{json, Value};
use tokio::io::AsyncWriteExt;
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

// Removing the upstream's CORS headers keeps the order of the rest: a
// relayed head is the upstream's own.
#[test]
fn stripping_upstream_cors_headers_keeps_the_order() {
    let mut relayed = headers(&[
        ("server", "nodeos/v1.2.2"),
        ("access-control-allow-origin", "*"),
        ("content-type", "application/json"),
        ("content-length", "12"),
        ("x-a", "1"),
        ("access-control-max-age", "600"),
        ("x-a", "2"),
    ]);
    apply_cors(&mut relayed, None);
    assert_eq!(
        names(&relayed),
        [
            "server: nodeos/v1.2.2",
            "content-type: application/json",
            "content-length: 12",
            "x-a: 1",
            "x-a: 2",
        ]
    );
}

#[test]
fn only_resource_accept_errors_pause_the_accept_loop() {
    use std::io::{Error, ErrorKind};
    for kind in [
        ErrorKind::ConnectionAborted,
        ErrorKind::ConnectionReset,
        ErrorKind::ConnectionRefused,
        ErrorKind::Interrupted,
    ] {
        assert!(connection_error(&Error::from(kind)), "{kind:?}");
    }
    // EMFILE, ENFILE and ENOMEM surface as other kinds.
    for kind in [
        ErrorKind::Other,
        ErrorKind::OutOfMemory,
        ErrorKind::Unsupported,
    ] {
        assert!(!connection_error(&Error::from(kind)), "{kind:?}");
    }
    assert_eq!(ACCEPT_BACKOFF, Duration::from_secs(1));
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

/// A listener's chain API served in process. Upstream connects time out
/// after 1 s; client reads (and writes with no progress) after 2 s.
struct Served {
    ctx: Arc<ChainApiCtx>,
    addr: SocketAddr,
    shutdown: broadcast::Sender<()>,
    task: tokio::task::JoinHandle<()>,
}

/// A live server entry: SHiP endpoint `127.0.0.1:<ship_port>` (never
/// dialed here) and `api_endpoint`.
fn server(name: &str, ship_port: u16, api_endpoint: &str) -> Value {
    json!({
        "name": name,
        "endpoint": format!("127.0.0.1:{ship_port}"),
        "enabled": true,
        "api_endpoint": api_endpoint
    })
}

/// Serve `chain_api` over `servers`, without waiting for any probe.
async fn serve_servers(servers: Value, chain_api: Value) -> Served {
    let cfg: ServerConfig = serde_json::from_value(json!({
        "listen_address": "127.0.0.1",
        "listen_port": 17000,
        "chain_id": "5a".repeat(32),
        "upstream_reconnect_ms": 1000,
        "upstream_monitoring_ms": 1000,
        "upstream_status_ms": 1000,
        "servers": servers,
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
    Served {
        ctx,
        addr,
        shutdown,
        task,
    }
}

/// Serve `chain_api` with `api` as the only live upstream, once it is up.
async fn serve(api: &MockChainApiHandle, chain_api: Value) -> Served {
    let served = serve_servers(json!([server("live-1", 9, &api.url())]), chain_api).await;
    wait_up(&served, 0).await;
    served
}

/// Wait for the first good probe of the server at `index`.
async fn wait_up(served: &Served, index: usize) {
    let slot = served.ctx.directory.slot(index).unwrap().clone();
    let deadline = tokio::time::Instant::now() + WAIT;
    while !slot.state().up {
        assert!(
            tokio::time::Instant::now() < deadline,
            "{} never came up",
            slot.name
        );
        sleep(Duration::from_millis(10)).await;
    }
}

/// Wait until `done`, failing with `what` after `within`.
async fn wait_for(within: Duration, what: &str, done: impl Fn() -> bool) {
    let deadline = tokio::time::Instant::now() + within;
    while !done() {
        assert!(tokio::time::Instant::now() < deadline, "{what}");
        sleep(Duration::from_millis(10)).await;
    }
}

async fn post(addr: SocketAddr, target: &str, body: &[u8]) -> RawResponse {
    mock_ship::chain_api::exchange(addr, &request("POST", target, "127.0.0.1", body), WAIT)
        .await
        .unwrap_or_else(|e| panic!("POST {target}: {e}"))
}

fn served_by(answer: &RawResponse) -> Option<String> {
    serde_json::from_slice::<Value>(&answer.body)
        .ok()?
        .get("served_by")?
        .as_str()
        .map(str::to_string)
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

// In single-port mode the chain API's log lines name the listener
// `default`, as every other log line does.
#[tokio::test]
async fn chain_api_logs_name_the_default_listener() {
    let (logs, _guard) = crate::test_log::capture();
    let api = live_api().await;
    let _served = serve(
        &api,
        serde_json::json!({ "listen_port": 1, "probe_ms": 100 }),
    )
    .await;
    let up = logs.lines_with("chain API up");
    assert_eq!(up.len(), 1, "{up:?}");
    assert!(up[0].contains(r#"listener="default""#), "{}", up[0]);
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

// A request the router answers itself has its whole body read first, as
// nodeos reads it, so the keep-alive connection carries on whatever the
// body's size. Left unread, hyper closed the connection after the answer,
// which reset a client still writing it.
#[tokio::test]
async fn router_answers_read_the_body_and_keep_the_connection() {
    let api = live_api().await;
    let served = serve(&api, json!({ "listen_port": 1, "probe_ms": 100 })).await;
    let mut body = br#"{"packed_trx":""#.to_vec();
    body.resize(300 * 1024, b'0');
    body.extend_from_slice(br#""}"#);
    for (method, target, status) in [
        // Push is not allowed by default: nodeos's 404.
        ("POST", "/v1/chain/send_transaction", 404),
        ("POST", "/v1/chain/push_block", 404),
        ("OPTIONS", "/v1/chain/get_info", 200),
        ("POST", "/v1/node/get_supported_apis", 200),
        ("POST", "/v1/chain/../chain/get_info", 400),
    ] {
        let mut conn = RawConn::connect(served.addr).await.unwrap();
        timeout(
            WAIT,
            conn.send(&request(method, target, "127.0.0.1", &body)),
        )
        .await
        .unwrap()
        .unwrap_or_else(|e| panic!("{method} {target}: {e}"));
        let answer = conn
            .response(WAIT)
            .await
            .unwrap_or_else(|e| panic!("{method} {target}: {e}"));
        assert_eq!(answer.status, status, "{method} {target}");
        assert!(answer.header("connection").is_none(), "{method} {target}");
        // The same connection carries on.
        conn.send(&request("POST", "/v1/chain/get_info", "127.0.0.1", b""))
            .await
            .unwrap_or_else(|e| panic!("after {method} {target}: {e}"));
        let next = conn
            .response(WAIT)
            .await
            .unwrap_or_else(|e| panic!("after {method} {target}: {e}"));
        assert_eq!(next.status, 200, "after {method} {target}");
    }
    assert_eq!(api.requests_to("/v1/chain/send_transaction").len(), 0);

    // Over max_body_bytes it is 413 and the connection closes, as for a
    // proxied request.
    let small = serve(
        &api,
        json!({ "listen_port": 1, "probe_ms": 100, "max_body_bytes": 1024 }),
    )
    .await;
    let mut conn = RawConn::connect(small.addr).await.unwrap();
    conn.send(&request(
        "POST",
        "/v1/chain/send_transaction",
        "127.0.0.1",
        &[b'x'; 2000],
    ))
    .await
    .unwrap();
    let answer = conn.response(WAIT).await.unwrap();
    assert_eq!(answer.status, 413);
    assert_eq!(answer.header("connection"), Some("close"));
    assert!(conn.closed(WAIT).await);
    let _ = served.shutdown.send(());
    let _ = small.shutdown.send(());
}

// A client that asks for an answer and then reads none of it is cut off
// after the read timeout (2 s here) with no progress, which gives back the
// request's in-flight permit and the upstream's slot. hyper alone never
// times a write out.
#[tokio::test]
async fn an_answer_left_unread_is_cut_off_and_gives_its_slots_back() {
    let api = live_api().await;
    let table_rows = "/v1/chain/get_table_rows";
    api.set_fault(
        Some(table_rows),
        Some(Fault::Respond {
            status: 200,
            body: vec![b'x'; 32 * 1024 * 1024].into(),
        }),
    );
    let served = serve(&api, json!({ "listen_port": 1, "probe_ms": 100 })).await;
    let slot = served.ctx.directory.slots()[0].clone();

    let socket = tokio::net::TcpSocket::new_v4().unwrap();
    socket.set_recv_buffer_size(4096).unwrap();
    let mut stalled = socket.connect(served.addr).await.unwrap();
    stalled
        .write_all(&request("POST", table_rows, "127.0.0.1", b"{}"))
        .await
        .unwrap();
    wait_for(WAIT, "the request never got an answer", || {
        !api.requests_to(table_rows).is_empty() && served.ctx.stats.in_flight() == 1
    })
    .await;
    assert_eq!(slot.in_flight(), 1);
    let held_since = std::time::Instant::now();
    wait_for(
        Duration::from_secs(8),
        "the unread answer kept its permit and its slot",
        || served.ctx.stats.in_flight() == 0 && slot.in_flight() == 0,
    )
    .await;
    assert!(held_since.elapsed() >= Duration::from_millis(1500));
    assert_eq!(served.ctx.stats.rejected.get(Rejection::Timeout), 1);

    // A client that reads gets the whole answer.
    let answer = post(served.addr, table_rows, b"{}").await;
    assert_eq!(answer.body.len(), 32 * 1024 * 1024);
    drop(stalled);
    let _ = served.shutdown.send(());
}

// Without max_response_bytes an answer of any size is relayed whole, as one
// nodeos sends it: 65 MiB of get_table_rows, with its Content-Length.
#[tokio::test]
async fn an_answer_of_any_size_is_relayed_whole_by_default() {
    let api = live_api().await;
    let table_rows = "/v1/chain/get_table_rows";
    let size = 65 * 1024 * 1024;
    api.set_fault(
        Some(table_rows),
        Some(Fault::Respond {
            status: 200,
            body: vec![b'x'; size].into(),
        }),
    );
    let served = serve(&api, json!({ "listen_port": 1, "probe_ms": 100 })).await;
    let answer = mock_ship::chain_api::exchange(
        served.addr,
        &request("POST", table_rows, "127.0.0.1", b"{}"),
        Duration::from_secs(30),
    )
    .await
    .unwrap();
    assert_eq!(answer.status, 200);
    assert_eq!(answer.header("content-length"), Some(&*size.to_string()));
    assert_eq!(answer.body.len(), size);
    let _ = served.shutdown.send(());
}

/// A live chain API that answers its probes, and answers `get_account`
/// with a body of `chunks` chunks of 64 KiB and no `Content-Length`.
async fn unframed_api(chunks: usize) -> String {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    tokio::spawn(async move {
        while let Ok((stream, _)) = listener.accept().await {
            let service = service_fn(move |request: Request<Incoming>| async move {
                let mut response = match request.uri().path() {
                    "/v1/chain/get_info" => Response::new(full(format!(
                        r#"{{"chain_id":"{}","head_block_num":1000,"last_irreversible_block_num":990,"earliest_available_block_num":1}}"#,
                        "5a".repeat(32)
                    ))),
                    "/v1/chain/get_account" => {
                        let chunk = Bytes::from(vec![b'x'; 64 * 1024]);
                        let body =
                            stream::iter((0..chunks).map(move |_| {
                                Ok::<_, upstream::BoxError>(Frame::data(chunk.clone()))
                            }));
                        Response::new(StreamBody::new(body).boxed_unsync())
                    }
                    _ => router_error(RouterError::NotFound),
                };
                response
                    .headers_mut()
                    .insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
                Ok::<_, Infallible>(response)
            });
            tokio::spawn(async move {
                let _ = http1::Builder::new()
                    .serve_connection(TokioIo::new(stream), service)
                    .await;
            });
        }
    });
    url
}

// With max_response_bytes, an answer whose Content-Length is above it gets
// 502 before anything of it is sent, and one without a length is cut off
// once it passes it, closing the connection. Either way the request's
// permit and the upstream's slot come back; a shorter answer goes through.
#[tokio::test]
async fn max_response_bytes_refuses_a_longer_answer_and_cuts_one_without_a_length() {
    let (logs, _guard) = crate::test_log::capture();
    let limit = 1024 * 1024;
    let api = live_api().await;
    let table_rows = "/v1/chain/get_table_rows";
    let config = json!({ "listen_port": 1, "probe_ms": 100, "max_response_bytes": limit });
    let served = serve(&api, config.clone()).await;
    let slot = served.ctx.directory.slots()[0].clone();
    let slots_back = || served.ctx.stats.in_flight() == 0 && slot.in_flight() == 0;

    api.set_fault(
        Some(table_rows),
        Some(Fault::Respond {
            status: 200,
            body: vec![b'x'; limit].into(),
        }),
    );
    let answer = post(served.addr, table_rows, b"{}").await;
    assert_eq!((answer.status, answer.body.len()), (200, limit));

    api.set_fault(
        Some(table_rows),
        Some(Fault::Respond {
            status: 200,
            body: vec![b'x'; limit + 1].into(),
        }),
    );
    let answer = post(served.addr, table_rows, b"{}").await;
    assert_eq!(answer.status, 502);
    assert_eq!(
        answer.text(),
        r#"{"code":502,"message":"Bad Gateway","error":{"code":502,"name":"Bad Gateway","what":"Upstream answer exceeds 1048576 bytes","details":[]}}"#
    );
    wait_for(WAIT, "the refused answer kept its slots", slots_back).await;
    let refused = logs.lines_with("longer than max_response_bytes");
    assert_eq!(refused.len(), 1, "{refused:?}");
    for field in [
        "listener=default",
        "server=live-1",
        "path=/v1/chain/get_table_rows",
    ] {
        assert!(refused[0].contains(field), "{field} in {}", refused[0]);
    }
    let _ = served.shutdown.send(());

    // 32 chunks of 64 KiB: 2 MiB with no Content-Length.
    let url = unframed_api(32).await;
    let served = serve_servers(json!([server("live-1", 9, &url)]), config).await;
    wait_up(&served, 0).await;
    let slot = served.ctx.directory.slots()[0].clone();
    let mut conn = RawConn::connect(served.addr).await.unwrap();
    conn.send(&request(
        "POST",
        "/v1/chain/get_account",
        "127.0.0.1",
        br#"{"account_name":"eosio"}"#,
    ))
    .await
    .unwrap();
    let head = conn.response_head(WAIT).await.unwrap();
    assert_eq!(head.status, 200);
    let sent = conn.pending(2 * limit, Duration::from_secs(2)).await;
    assert!(sent.len() < 2 * limit, "cut off: {} bytes", sent.len());
    wait_for(WAIT, "the cut answer kept its slots", || {
        served.ctx.stats.in_flight() == 0 && slot.in_flight() == 0
    })
    .await;
    assert_eq!(logs.lines_with("passed max_response_bytes").len(), 1);
    let _ = served.shutdown.send(());
}

// A push's answer reports a write the node may have applied: one longer than
// max_response_bytes is relayed whole, not answered 502, which would read as
// a push that failed. An exec answer commits nothing and stays bounded.
#[tokio::test]
async fn a_push_answer_longer_than_max_response_bytes_is_relayed_whole() {
    let api = live_api().await;
    let push = "/v1/chain/push_transaction";
    let exec = "/v1/chain/compute_transaction";
    let limit = 64 * 1024;
    for target in [push, exec] {
        api.set_fault(
            Some(target),
            Some(Fault::Respond {
                status: 202,
                body: vec![b'x'; 2 * limit].into(),
            }),
        );
    }
    let served = serve(
        &api,
        json!({
            "listen_port": 1,
            "probe_ms": 100,
            "allow": ["block", "info", "state", "exec", "push"],
            "max_response_bytes": limit
        }),
    )
    .await;
    let answer = post(served.addr, push, br#"{"packed_trx":"00"}"#).await;
    assert_eq!((answer.status, answer.body.len()), (202, 2 * limit));
    assert_eq!(api.requests_to(push).len(), 1, "the push was sent once");
    assert_eq!(served.ctx.stats.pushes.get(PushResult::Ok), 1);
    let answer = post(served.addr, exec, br#"{"transaction":{}}"#).await;
    assert_eq!(answer.status, 502);
    let _ = served.shutdown.send(());
}

// The 502 for an answer longer than max_response_bytes is the router's
// own: a block request it ends counts with role none, not as the live node's.
#[tokio::test]
async fn a_refused_long_answer_counts_as_the_routers_own() {
    let api = live_api().await;
    let info = "/v1/chain/get_block_info";
    let limit = 64 * 1024;
    api.set_fault(
        Some(info),
        Some(Fault::Respond {
            status: 200,
            body: vec![b'x'; 2 * limit].into(),
        }),
    );
    let served = serve(
        &api,
        json!({ "listen_port": 1, "probe_ms": 100, "max_response_bytes": limit }),
    )
    .await;
    let answer = post(served.addr, info, br#"{"block_num":995}"#).await;
    assert_eq!(answer.status, 502);
    let stats = &served.ctx.stats;
    assert_eq!(
        (
            stats.block_requests(BlockRegion::Reversible, AnswerRole::Live),
            stats.block_requests(BlockRegion::Reversible, AnswerRole::None),
        ),
        (0, 1)
    );
    let _ = served.shutdown.send(());
}

/// A live chain API that answers its probes, and answers `get_account`
/// with a head and the first bytes of a body, then sends nothing more.
async fn stalling_api() -> String {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    tokio::spawn(async move {
        while let Ok((stream, _)) = listener.accept().await {
            let service = service_fn(|request: Request<Incoming>| async move {
                let mut response = match request.uri().path() {
                    "/v1/chain/get_info" => Response::new(full(format!(
                        r#"{{"chain_id":"{}","head_block_num":1000,"last_irreversible_block_num":990,"earliest_available_block_num":1}}"#,
                        "5a".repeat(32)
                    ))),
                    "/v1/chain/get_account" => {
                        let first: Result<Frame<Bytes>, upstream::BoxError> =
                            Ok(Frame::data(Bytes::from_static(br#"{"account_name":"#)));
                        let body = stream::iter([first]).chain(stream::pending());
                        Response::new(StreamBody::new(body).boxed_unsync())
                    }
                    _ => router_error(RouterError::NotFound),
                };
                response
                    .headers_mut()
                    .insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
                Ok::<_, Infallible>(response)
            });
            tokio::spawn(async move {
                let _ = http1::Builder::new()
                    .serve_connection(TokioIo::new(stream), service)
                    .await;
            });
        }
    });
    url
}

// An upstream that sends a head and then stops sending is cut off once it
// has sent nothing for upstream_timeout_ms while the client waits: the
// client connection closes and the permit and the slot come back.
#[tokio::test]
async fn an_upstream_that_stops_sending_is_cut_off() {
    let url = stalling_api().await;
    let served = serve_servers(
        json!([server("live-1", 9, &url)]),
        json!({ "listen_port": 1, "probe_ms": 100, "upstream_timeout_ms": 300 }),
    )
    .await;
    wait_up(&served, 0).await;
    let slot = served.ctx.directory.slots()[0].clone();
    let mut conn = RawConn::connect(served.addr).await.unwrap();
    conn.send(&request(
        "POST",
        "/v1/chain/get_account",
        "127.0.0.1",
        br#"{"account_name":"eosio"}"#,
    ))
    .await
    .unwrap();
    let started = std::time::Instant::now();
    let cut = conn.response(WAIT).await.unwrap_err();
    assert_ne!(cut.kind(), std::io::ErrorKind::TimedOut, "never cut off");
    assert!(started.elapsed() < Duration::from_secs(3));
    wait_for(
        WAIT,
        "the stalled answer kept its permit and its slot",
        || served.ctx.stats.in_flight() == 0 && slot.in_flight() == 0,
    )
    .await;
    let _ = served.shutdown.send(());
}

// A push or an exec call whose connect is still pending when
// upstream_timeout_ms runs out was never written: it moves on to the next
// target, and the unreachable API is taken down. (10.255.255.1 never
// answers a SYN here; where it is refused at once the same path is taken.)
#[tokio::test]
async fn a_connect_still_pending_at_the_head_timeout_moves_on() {
    let api = live_api().await;
    let mut dark = server("dark", 9, "http://10.255.255.1:8888");
    dark["priority"] = json!(0);
    let mut live = server("live-1", 10, &api.url());
    live["priority"] = json!(1);
    // Connects time out after 1 s, heads after 300 ms.
    let served = serve_servers(
        json!([dark, live]),
        json!({
            "listen_port": 1,
            "probe_ms": 60000,
            "allow": ["block", "info", "state", "exec", "push"],
            "push_servers": ["dark", "live-1"],
            "upstream_timeout_ms": 300
        }),
    )
    .await;
    wait_up(&served, 1).await;
    let dark = served.ctx.directory.slot(0).unwrap().clone();
    for (target, body) in [
        (
            "/v1/chain/push_transaction",
            br#"{"packed_trx":"00"}"#.as_slice(),
        ),
        (
            "/v1/chain/compute_transaction",
            br#"{"transaction":{}}"#.as_slice(),
        ),
    ] {
        // As if the dark API answered its probes until now.
        dark.state().up = true;
        let answer = post(served.addr, target, body).await;
        assert_eq!(
            (answer.status, answer.body),
            (200, call_answer("live-1", target, body.len())),
            "{target}"
        );
        assert!(!dark.state().up, "{target}: the dark API is down");
    }
    assert_eq!(served.ctx.stats.pushes.get(PushResult::Ok), 1);
    assert_eq!(served.ctx.stats.pushes.get(PushResult::Ambiguous), 0);
    let _ = served.shutdown.send(());
}

// A push keeps to request_deadline_ms, slot waits included: two full push
// servers with a 500 ms queue wait each answer Busy at the 600 ms deadline,
// not after 1 s.
#[tokio::test]
async fn a_push_keeps_the_request_deadline() {
    let first = live_api().await;
    let second = MockChainApi::live("live-2", CHAIN, 1000, 990, Some(1), "v1.2.2")
        .start()
        .await;
    let served = serve_servers(
        json!([
            server("live-1", 9, &first.url()),
            server("live-2", 10, &second.url())
        ]),
        json!({
            "listen_port": 1,
            "probe_ms": 100,
            "allow": ["block", "info", "state", "push"],
            "push_servers": ["live-1", "live-2"],
            "max_in_flight_per_upstream": 1,
            "upstream_queue_ms": 500,
            "upstream_timeout_ms": 300,
            "request_deadline_ms": 600
        }),
    )
    .await;
    wait_up(&served, 0).await;
    wait_up(&served, 1).await;
    let held: Vec<_> = served
        .ctx
        .directory
        .slots()
        .iter()
        .map(|slot| slot.try_acquire().unwrap())
        .collect();
    let started = std::time::Instant::now();
    let answer = post(served.addr, "/v1/chain/push_transaction", b"{}").await;
    let took = started.elapsed();
    assert_eq!(answer.status, 503);
    assert!(took >= Duration::from_millis(550), "{took:?}");
    assert!(took < Duration::from_millis(850), "{took:?}");
    drop(held);
    let _ = served.shutdown.send(());
}

// An attempt the request's deadline cut short is not the upstream's fault:
// its timeout ends the request without taking that API down.
#[tokio::test]
async fn a_timeout_the_deadline_cut_short_keeps_the_api_up() {
    let account = "/v1/chain/get_account";
    let slow = live_api().await;
    slow.set_fault(Some(account), Some(Fault::Delay(Duration::from_secs(2))));
    let late = MockChainApi::live("live-2", CHAIN, 1000, 990, Some(1), "v1.2.2")
        .start()
        .await;
    late.set_fault(
        Some(account),
        Some(Fault::Delay(Duration::from_millis(700))),
    );
    let mut first = server("live-1", 9, &slow.url());
    first["priority"] = json!(0);
    let mut second = server("live-2", 10, &late.url());
    second["priority"] = json!(1);
    let served = serve_servers(
        json!([first, second]),
        json!({
            "listen_port": 1,
            "probe_ms": 60000,
            "upstream_timeout_ms": 1000,
            "request_deadline_ms": 1500
        }),
    )
    .await;
    wait_up(&served, 0).await;
    wait_up(&served, 1).await;
    // live-1 times out after its full 1 s; live-2 gets the 500 ms left.
    let answer = post(served.addr, account, b"{}").await;
    assert_eq!(answer.status, 503);
    assert_eq!(late.requests_to(account).len(), 1);
    let slots = served.ctx.directory.slots();
    assert!(!slots[0].state().up, "live-1 had its whole timeout");
    assert!(slots[1].state().up, "live-2 was cut short by the deadline");
    let _ = served.shutdown.send(());
}

// get_info failing over from a 500 counts as an upstream error, not as
// admission pressure.
#[tokio::test]
async fn get_info_failover_on_a_500_is_an_error_retry() {
    let primary = MockChainApi::live("live-1", CHAIN, 1001, 991, Some(1), "v1.2.2")
        .start()
        .await;
    let backup = MockChainApi::live("live-2", CHAIN, 1000, 990, Some(1), "v1.2.2")
        .start()
        .await;
    let served = serve_servers(
        json!([
            server("live-1", 9, &primary.url()),
            server("live-2", 10, &backup.url())
        ]),
        json!({ "listen_port": 1, "probe_ms": 60000 }),
    )
    .await;
    wait_up(&served, 0).await;
    wait_up(&served, 1).await;
    primary.set_fault(
        Some("/v1/chain/get_info"),
        Some(Fault::Respond {
            status: 500,
            body: Bytes::from_static(b"{}"),
        }),
    );
    let answer = post(served.addr, "/v1/chain/get_info", b"").await;
    assert_eq!(served_by(&answer).as_deref(), Some("live-2"));
    assert_eq!(served.ctx.stats.retries.get(RetryReason::Error), 1);
    assert_eq!(served.ctx.stats.retries.get(RetryReason::Busy), 0);
    let _ = served.shutdown.send(());
}

// The control API's changes to a server apply to the chain API as they do
// to SHiP sessions: a disabled or drained server gets no request, and the
// priority in effect orders the candidates.
#[tokio::test]
async fn control_api_changes_apply_to_the_chain_api() {
    let one = live_api().await;
    let two = MockChainApi::live("live-2", CHAIN, 1000, 990, Some(1), "v1.2.2")
        .start()
        .await;
    let mut first = server("live-1", 9, &one.url());
    first["priority"] = json!(0);
    let mut second = server("live-2", 10, &two.url());
    second["priority"] = json!(1);
    let served = serve_servers(
        json!([first, second]),
        json!({ "listen_port": 1, "probe_ms": 100 }),
    )
    .await;
    wait_up(&served, 0).await;
    wait_up(&served, 1).await;
    let account = "/v1/chain/get_account";
    let who = |addr| async move { served_by(&post(addr, account, b"{}").await) };
    assert_eq!(who(served.addr).await.as_deref(), Some("live-1"));

    let hub = served.ctx.hub.clone();
    let set = |change: fn(&mut crate::models::ServerState)| {
        let hub = hub.clone();
        async move {
            let mut states = hub.state.lock().await;
            change(states.get_mut("127.0.0.1:9").unwrap());
            hub.rebuild(&states);
        }
    };
    set(|s| s.operator_disabled = true).await;
    for _ in 0..3 {
        assert_eq!(who(served.addr).await.as_deref(), Some("live-2"));
    }
    assert_eq!(
        served_by(&post(served.addr, "/v1/chain/get_info", b"").await).as_deref(),
        Some("live-2")
    );
    set(|s| s.operator_disabled = false).await;
    assert_eq!(who(served.addr).await.as_deref(), Some("live-1"));
    set(|s| s.priority = 5).await;
    for _ in 0..3 {
        assert_eq!(who(served.addr).await.as_deref(), Some("live-2"));
    }
    let _ = served.shutdown.send(());
}
