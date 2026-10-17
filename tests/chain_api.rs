//! End-to-end tests of the chain API listener: the router as a process, in
//! front of mock nodeos live APIs and mock frozen range server APIs (with
//! their SHiP endpoints, so the frozen APIs can be verified).
//!
//! Live mocks answer block endpoints with `{"block_num":N,"id":..,
//! "served_by":<name>}` and other calls with a tagged answer, so each test
//! sees which upstream wrote what the client got. Probes run every 100 ms.
//!
//! The scenarios are CA1-CA22 of the design (CA20, the shutdown drain, runs
//! in process in `chain_api::tests`), plus the rule that an archived block
//! never ends on an Unknown Block from a node whose log begins above it.
//!
//! Run: cargo test --test chain_api

mod common;

use std::net::SocketAddr;
use std::time::{Duration, Instant};

use common::{base_config, http_get, start_with_chain_api, RouterProcess};
use mock_ship::chain_api::{
    block_answer, block_id, block_id_seeded, call_answer, exchange, request, Fault, MockChainApi,
    MockChainApiHandle, RawConn, RawResponse, NOT_FOUND_BODY,
};
use mock_ship::{MockShipConfig, MockShipHandle, MockShipServer};
use serde_json::{json, Value};
use tokio::time::sleep;

const WAIT: Duration = Duration::from_secs(5);
const CHAIN: [u8; 32] = [0x5a; 32];
const POLL: Duration = Duration::from_millis(50);

const RAW_BLOCK: &str = "/v1/chain/get_raw_block";
const BLOCK_INFO: &str = "/v1/chain/get_block_info";
const GET_BLOCK: &str = "/v1/chain/get_block";
const GET_INFO: &str = "/v1/chain/get_info";
const GET_ACCOUNT: &str = "/v1/chain/get_account";

const NO_UPSTREAM: &str = r#"{"code":503,"message":"Busy","error":{"code":503,"name":"Busy","what":"No chain API upstream available","details":[]}}"#;
const NO_LIVE_UPSTREAM: &str = r#"{"code":503,"message":"Busy","error":{"code":503,"name":"Busy","what":"No live chain API upstream available","details":[]}}"#;
const LOST_AFTER_SEND: &str = r#"{"code":503,"message":"Busy","error":{"code":503,"name":"Busy","what":"Upstream connection lost after the request was sent","details":[]}}"#;
const ILLEGAL_TARGET: &str = r#"{"code":400,"message":"Illegal request-target","error":{"code":0,"name":"","what":"","details":[]}}"#;

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

// ----------------------------------------------------------------------
// Fleet
// ----------------------------------------------------------------------

/// One server: its SHiP endpoint (a mock, or a port that never answers)
/// and its chain API mock.
struct Node {
    name: String,
    ship_ep: String,
    ship: Option<MockShipHandle>,
    /// Holds a SHiP port that never completes a handshake.
    _dead: Option<std::net::TcpListener>,
    api: MockChainApiHandle,
}

impl Node {
    /// A live server entry with `api_endpoint`.
    fn live(&self) -> Value {
        json!({
            "name": self.name,
            "endpoint": self.ship_ep,
            "enabled": true,
            "api_endpoint": self.api.url(),
        })
    }

    /// A frozen server entry with `api_endpoint` and a declared range.
    fn frozen(&self, range: (u32, u32)) -> Value {
        let mut entry = self.live();
        entry["role"] = json!("frozen");
        entry["range"] = json!({ "begin": range.0, "end": range.1 });
        entry
    }

    fn with(mut entry: Value, extra: Value) -> Value {
        for (key, value) in extra.as_object().unwrap() {
            entry[key] = value.clone();
        }
        entry
    }

    /// Requests the API got for `target`.
    fn hits(&self, target: &str) -> usize {
        self.api.requests_to(target).len()
    }
}

/// A live node from block `earliest` to `head`: a SHiP mock and a chain API
/// mock that share one chain clock (LIB = head - 10).
async fn live_node(name: &str, earliest: u32, head: u32) -> Node {
    let ship = MockShipServer::new(MockShipConfig {
        chain_id: CHAIN,
        ..MockShipConfig::live(earliest, head)
    })
    .await;
    let ship_ep = ship.endpoint();
    let ship = ship.start();
    let api = MockChainApi {
        clock: Some(ship.clock()),
        ..MockChainApi::live(name, CHAIN, 0, 0, Some(earliest), "v1.2.2")
    }
    .start()
    .await;
    Node {
        name: name.to_string(),
        ship_ep,
        ship: Some(ship),
        _dead: None,
        api,
    }
}

/// A live chain API whose SHiP endpoint never answers, so only its probed
/// head and LIB are known.
async fn live_api_only(
    name: &str,
    chain_id: [u8; 32],
    earliest: Option<u32>,
    head: u32,
    lib: u32,
) -> Node {
    let dead = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let ship_ep = dead.local_addr().unwrap().to_string();
    let api = MockChainApi::live(name, chain_id, head, lib, earliest, "v1.2.2")
        .start()
        .await;
    Node {
        name: name.to_string(),
        ship_ep,
        ship: None,
        _dead: Some(dead),
        api,
    }
}

/// A frozen range server: a SHiP mock for `range` and a chain API mock for
/// `blocks`, whose `last` is the SHiP head (`range.1 - 1`).
async fn frozen_node(
    name: &str,
    range: (u32, u32),
    blocks: (u32, u32),
    max_in_flight: u32,
) -> Node {
    let ship = MockShipServer::new(MockShipConfig {
        chain_id: CHAIN,
        ..MockShipConfig::frozen(range.0, range.1)
    })
    .await;
    let ship_ep = ship.endpoint();
    let ship = ship.start();
    let api = MockChainApi::frozen(name, CHAIN, blocks, range.1 - 1, max_in_flight)
        .start()
        .await;
    Node {
        name: name.to_string(),
        ship_ep,
        ship: Some(ship),
        _dead: None,
        api,
    }
}

/// A single-listener config pinned to [`CHAIN`], with a chain API probing
/// every 100 ms plus `chain_api` (which may override anything).
fn config(servers: &[Value], chain_api: Value) -> Value {
    let mut cfg = base_config(servers);
    cfg["chain_id"] = json!(hex(&CHAIN));
    let mut api = json!({ "listen_port": 0, "probe_ms": 100 });
    for (key, value) in chain_api.as_object().unwrap() {
        api[key] = value.clone();
    }
    cfg["chain_api"] = api;
    cfg
}

async fn start(cfg: Value) -> (RouterProcess, u16) {
    let (router, ports) = start_with_chain_api(cfg).await;
    (router, ports[0])
}

fn addr(port: u16) -> SocketAddr {
    SocketAddr::from(([127, 0, 0, 1], port))
}

async fn send(port: u16, method: &str, target: &str, body: &str) -> RawResponse {
    let bytes = request(
        method,
        target,
        &format!("127.0.0.1:{port}"),
        body.as_bytes(),
    );
    exchange(addr(port), &bytes, WAIT)
        .await
        .unwrap_or_else(|e| panic!("{method} {target}: {e}"))
}

async fn post(port: u16, target: &str, body: &str) -> RawResponse {
    send(port, "POST", target, body).await
}

/// The `served_by` of an answer, if it has one.
fn served_by(answer: &RawResponse) -> Option<String> {
    serde_json::from_slice::<Value>(&answer.body)
        .ok()?
        .get("served_by")?
        .as_str()
        .map(str::to_string)
}

/// Repeat a request until `name` serves it.
async fn wait_served(port: u16, target: &str, body: &str, name: &str) -> RawResponse {
    let deadline = Instant::now() + WAIT;
    loop {
        let answer = post(port, target, body).await;
        if served_by(&answer).as_deref() == Some(name) {
            return answer;
        }
        assert!(
            Instant::now() < deadline,
            "{target} {body} never served by {name}; last: {} {}",
            answer.status,
            answer.text()
        );
        sleep(POLL).await;
    }
}

/// Repeat a request until its answer satisfies `pred`.
async fn wait_answer(
    port: u16,
    target: &str,
    body: &str,
    pred: impl Fn(&RawResponse) -> bool,
) -> RawResponse {
    let deadline = Instant::now() + WAIT;
    loop {
        let answer = post(port, target, body).await;
        if pred(&answer) {
            return answer;
        }
        assert!(
            Instant::now() < deadline,
            "{target} {body}: no matching answer; last: {} {}",
            answer.status,
            answer.text()
        );
        sleep(POLL).await;
    }
}

fn raw(n: u32) -> String {
    format!(r#"{{"block_num_or_id":{n}}}"#)
}

fn ok(name: &str, n: u32) -> Vec<u8> {
    block_answer(name, n, &block_id(n))
}

/// The answer an API gives to a request sent to it directly.
async fn direct(node: &Node, target: &str, body: &str) -> RawResponse {
    let bytes = request("POST", target, &node.api.endpoint(), body.as_bytes());
    exchange(node.api.addr(), &bytes, WAIT).await.unwrap()
}

// ----------------------------------------------------------------------
// Block routing
// ----------------------------------------------------------------------

// CA1
#[tokio::test]
async fn an_irreversible_block_goes_to_its_archive_with_the_body_unchanged() {
    let archive = frozen_node("archive-a", (2, 1001), (1, 1001), 4).await;
    let live = live_node("live-1", 1, 2000).await;
    let (_router, port) = start(config(&[archive.frozen((2, 1001)), live.live()], json!({}))).await;
    wait_served(port, RAW_BLOCK, &raw(500), "archive-a").await;
    archive.api.clear_requests();
    live.api.clear_requests();

    // Unusual but valid JSON spacing and members, and headers the router
    // must not forward.
    let body = "{\"block_num_or_id\":\t500 ,  \"x\": [1, {\"y\": null}]}";
    let bytes = format!(
        "POST {RAW_BLOCK} HTTP/1.1\r\nHost: chain.example\r\nContent-Type: application/json\r\nAccept: */*\r\nUser-Agent: test/1\r\nAccept-Encoding: gzip\r\nX-Forwarded-For: 10.0.0.9\r\nCookie: a=b\r\nContent-Length: {}\r\n\r\n{body}",
        body.len()
    );
    let answer = exchange(addr(port), bytes.as_bytes(), WAIT).await.unwrap();
    assert_eq!(answer.status, 200);
    assert_eq!(answer.body, ok("archive-a", 500));
    assert!(
        answer
            .header("server")
            .is_some_and(|s| s.starts_with("mock-frozen-api/")),
        "the upstream's Server passes through: {:?}",
        answer.headers
    );
    assert!(answer.header("date").is_none());

    let seen = archive.api.requests_to(RAW_BLOCK);
    assert_eq!(seen.len(), 1);
    assert_eq!(seen[0].body, body.as_bytes(), "byte for byte");
    assert_eq!(
        seen[0].host.as_deref(),
        Some(archive.api.endpoint().as_str()),
        "Host is the API's own authority"
    );
    assert_eq!(seen[0].header("content-type"), Some("application/json"));
    assert_eq!(seen[0].header("accept"), Some("*/*"));
    assert_eq!(seen[0].header("user-agent"), Some("test/1"));
    for dropped in ["accept-encoding", "x-forwarded-for", "cookie", "expect"] {
        assert!(seen[0].header(dropped).is_none(), "{dropped} forwarded");
    }
    assert_eq!(live.hits(RAW_BLOCK), 0);
    // get_block_header and get_block_info go the same way.
    assert_eq!(
        post(port, "/v1/chain/get_block_header", &raw(501))
            .await
            .body,
        ok("archive-a", 501)
    );
    assert_eq!(
        post(port, BLOCK_INFO, r#"{"block_num":502}"#).await.body,
        ok("archive-a", 502)
    );
}

// CA2
#[tokio::test]
async fn live_first_sends_a_block_both_hold_to_the_live_node() {
    let archive = frozen_node("archive-a", (2, 1001), (1, 1001), 4).await;
    let live = live_node("live-1", 100, 2000).await;
    let (_router, port) = start(config(
        &[archive.frozen((2, 1001)), live.live()],
        json!({ "block_source": "live_first" }),
    ))
    .await;
    // Block 5 only the archive holds: once it serves it, it is verified.
    wait_served(port, RAW_BLOCK, &raw(5), "archive-a").await;
    wait_served(port, RAW_BLOCK, &raw(500), "live-1").await;
    archive.api.clear_requests();
    for n in [100, 500, 1000] {
        assert_eq!(post(port, RAW_BLOCK, &raw(n)).await.body, ok("live-1", n));
    }
    assert_eq!(archive.hits(RAW_BLOCK), 0);
}

// CA3
#[tokio::test]
async fn an_archive_unknown_block_is_retried_on_a_live_node() {
    let archive = frozen_node("archive-a", (2, 1001), (1, 1001), 4).await;
    let live = live_node("live-1", 1, 2000).await;
    let (_router, port) = start(config(&[archive.frozen((2, 1001)), live.live()], json!({}))).await;
    wait_served(port, RAW_BLOCK, &raw(500), "archive-a").await;
    archive
        .api
        .set_fault(Some(RAW_BLOCK), Some(Fault::UnknownBlock));
    archive.api.clear_requests();
    live.api.clear_requests();
    let answer = post(port, RAW_BLOCK, &raw(600)).await;
    assert_eq!(answer.status, 200);
    assert_eq!(answer.body, ok("live-1", 600), "the live node's answer");
    assert_eq!(archive.hits(RAW_BLOCK), 1);
    assert_eq!(live.hits(RAW_BLOCK), 1);
}

// CA4
#[tokio::test]
async fn a_reversible_block_gets_one_answer_from_the_highest_live_node() {
    let behind = live_node("live-behind", 1, 1000).await;
    let ahead = live_node("live-ahead", 1, 1010).await;
    let (_router, port) = start(config(&[behind.live(), ahead.live()], json!({}))).await;
    // 1005 is above the fleet LIB (1000): reversible. Only `ahead` has it.
    wait_served(port, RAW_BLOCK, &raw(1005), "live-ahead").await;
    behind.api.clear_requests();
    ahead.api.clear_requests();

    // An id of block 1005 on another branch: the node's Unknown Block is
    // the answer, after exactly one attempt.
    let forked = format!(
        r#"{{"block_num_or_id":"{}"}}"#,
        hex(&block_id_seeded(1005, 7))
    );
    let answer = post(port, GET_BLOCK, &forked).await;
    assert_eq!(answer.status, 400);
    let genuine = direct(&ahead, GET_BLOCK, &forked).await;
    assert_eq!(answer.body, genuine.body);
    assert!(answer.text().contains("3100002"));
    assert_eq!(ahead.hits(GET_BLOCK), 2, "the router's and ours");
    assert_eq!(behind.hits(GET_BLOCK), 0, "never retried");
    // By number, the same node answers.
    assert_eq!(
        post(port, GET_BLOCK, &raw(1005)).await.body,
        ok("live-ahead", 1005)
    );
}

// CA5
#[tokio::test]
async fn a_cold_start_uses_the_probed_live_lib() {
    // No SHiP endpoint answers: the fleet LIB comes from the APIs alone.
    // `late`'s log begins at 600 but its head is the highest.
    let late = live_api_only("live-late", CHAIN, Some(600), 1010, 1000).await;
    let full = live_api_only("live-full", CHAIN, Some(1), 1000, 990).await;
    let (_router, port) = start(config(&[late.live(), full.live()], json!({}))).await;
    // Block 500 is irreversible (at or below the probed LIB 1000), so the
    // node whose log holds it answers. Taken as reversible, `late` (the
    // highest head) would answer Unknown Block and that would be final.
    let answer = wait_served(port, RAW_BLOCK, &raw(500), "live-full").await;
    assert_eq!(answer.status, 200);
    for _ in 0..3 {
        assert_eq!(
            post(port, RAW_BLOCK, &raw(500)).await.body,
            ok("live-full", 500)
        );
    }
    // A reversible block goes to the highest head.
    assert_eq!(
        post(port, RAW_BLOCK, &raw(1005)).await.body,
        ok("live-late", 1005)
    );
}

// CA6
#[tokio::test]
async fn an_archive_serves_its_block_log_below_its_ship_range() {
    let archive = frozen_node("archive-a", (2, 1001), (1, 1001), 4).await;
    let live = live_node("live-1", 900, 2000).await;
    let (_router, port) = start(config(&[archive.frozen((2, 1001)), live.live()], json!({}))).await;
    // Block 1 is in the archive's block log, below its SHiP range.
    let answer = wait_served(port, RAW_BLOCK, &raw(1), "archive-a").await;
    assert_eq!(answer.body, ok("archive-a", 1));
    assert_eq!(
        post(port, BLOCK_INFO, r#"{"block_num":1}"#).await.body,
        ok("archive-a", 1)
    );
    assert_eq!(live.hits(RAW_BLOCK) + live.hits(BLOCK_INFO), 0);
}

// CA7
#[tokio::test]
async fn bodies_without_a_hint_go_unchanged_to_one_live_node() {
    let archive = frozen_node("archive-a", (2, 1001), (1, 1001), 4).await;
    let live = live_node("live-1", 1, 2000).await;
    let (_router, port) = start(config(&[archive.frozen((2, 1001)), live.live()], json!({}))).await;
    wait_served(port, RAW_BLOCK, &raw(500), "archive-a").await;
    archive.api.clear_requests();
    for body in [
        r#"{"block_num_or_id":"\u0031"}"#,
        r#"{"block_num_or_id":"5","block_num_or_id":"6"}"#,
        r#"{"block_num_or_id":"+5"}"#,
        r#"{"block_num_or_id":5.0}"#,
    ] {
        live.api.clear_requests();
        let answer = post(port, RAW_BLOCK, body).await;
        let genuine = direct(&live, RAW_BLOCK, body).await;
        assert_eq!(
            (answer.status, answer.body.clone()),
            (genuine.status, genuine.body),
            "{body}"
        );
        let seen = live.api.requests_to(RAW_BLOCK);
        assert_eq!(seen.len(), 2, "{body}: the router's and ours");
        assert_eq!(seen[0].body, body.as_bytes(), "{body}");
    }
    assert_eq!(archive.hits(RAW_BLOCK), 0, "never an archive");
}

// CA8
#[tokio::test]
async fn get_block_is_answered_by_live_nodes_only() {
    let archive = frozen_node("archive-a", (2, 1001), (1, 1001), 4).await;
    let live = live_node("live-1", 900, 2000).await;
    let (_router, port) = start(config(&[archive.frozen((2, 1001)), live.live()], json!({}))).await;
    wait_served(port, RAW_BLOCK, &raw(500), "archive-a").await;
    // The live node holds 950.
    assert_eq!(
        post(port, GET_BLOCK, &raw(950)).await.body,
        ok("live-1", 950)
    );
    // Only the archive holds 500: decoded get_block is still the live
    // node's to answer, and its Unknown Block is genuine.
    let answer = post(port, GET_BLOCK, &raw(500)).await;
    assert_eq!(answer.status, 400);
    assert_eq!(answer.body, direct(&live, GET_BLOCK, &raw(500)).await.body);
    assert_eq!(archive.hits(GET_BLOCK), 0);
    // The same block over get_raw_block comes from the archive.
    assert_eq!(
        post(port, RAW_BLOCK, &raw(500)).await.body,
        ok("archive-a", 500)
    );
}

// ----------------------------------------------------------------------
// State, push, get_info
// ----------------------------------------------------------------------

// CA9
#[tokio::test]
async fn state_calls_skip_laggards_and_stream_large_answers() {
    let fresh = live_node("live-fresh", 1, 10_000).await;
    let laggard = live_node("live-laggard", 1, 5_000).await;
    let (_router, port) = start(config(&[fresh.live(), laggard.live()], json!({}))).await;
    let body = r#"{"account_name":"eosio", "x":1}"#;
    let answer = wait_served(port, GET_ACCOUNT, body, "live-fresh").await;
    assert_eq!(
        answer.body,
        call_answer("live-fresh", GET_ACCOUNT, body.len())
    );
    laggard.api.clear_requests();
    for _ in 0..4 {
        assert_eq!(
            served_by(&post(port, GET_ACCOUNT, body).await).as_deref(),
            Some("live-fresh")
        );
    }
    let seen = fresh.api.requests_to(GET_ACCOUNT);
    assert!(seen.iter().all(|r| r.body == body.as_bytes()));
    assert_eq!(laggard.hits(GET_ACCOUNT), 0, "5000 blocks behind");

    // A 12 MiB answer arrives intact.
    let big: Vec<u8> = (0..12 * 1024 * 1024).map(|i| (i % 251) as u8).collect();
    fresh.api.set_fault(
        Some("/v1/chain/get_table_rows"),
        Some(Fault::Respond {
            status: 200,
            body: big.clone().into(),
        }),
    );
    let answer = post(port, "/v1/chain/get_table_rows", "{}").await;
    assert_eq!(answer.status, 200);
    assert_eq!(answer.body.len(), big.len());
    assert!(answer.body == big);
}

// CA10
#[tokio::test]
async fn pushes_are_denied_by_default_and_written_once_when_allowed() {
    let p1 = live_api_only("push-1", CHAIN, Some(1), 1000, 990).await;
    let p2 = live_api_only("push-2", CHAIN, Some(1), 1000, 990).await;
    let push = "/v1/chain/push_transaction";
    let trx = r#"{"signatures":[],"packed_trx":"00"}"#;

    // Default classes: push is nodeos's 404.
    {
        let (_router, port) = start(config(&[p1.live(), p2.live()], json!({}))).await;
        let answer = post(port, push, trx).await;
        assert_eq!(
            (answer.status, answer.text()),
            (404, NOT_FOUND_BODY.to_string())
        );
        assert_eq!(p1.hits(push) + p2.hits(push), 0);
    }

    // Probes once at start (probe_ms 60000), so the APIs stay up whatever
    // happens to them during the test.
    let (_router, port) = start(config(
        &[p1.live(), p2.live()],
        json!({
            "allow": ["block", "info", "state", "push"],
            "push_servers": ["push-1", "push-2"],
            "probe_ms": 60000
        }),
    ))
    .await;
    wait_answer(port, GET_INFO, "", |a| a.status == 200).await;
    // The first push server answers.
    let answer = post(port, push, trx).await;
    assert_eq!(answer.body, call_answer("push-1", push, trx.len()));
    // Lost after it was written: Busy, and never resent.
    p1.api.clear_requests();
    p2.api.clear_requests();
    p1.api.set_fault(Some(push), Some(Fault::DropAfterBody));
    let answer = post(port, push, trx).await;
    assert_eq!(
        (answer.status, answer.text()),
        (503, LOST_AFTER_SEND.to_string())
    );
    assert_eq!(p1.hits(push), 1);
    assert_eq!(p2.hits(push), 0);
    // A connection that cannot be made moves on to the next push server.
    p1.api.clear_faults();
    let Node { api, .. } = p1;
    api.shutdown().await;
    let answer = post(port, "/v1/chain/send_transaction2", trx).await;
    assert_eq!(
        answer.body,
        call_answer("push-2", "/v1/chain/send_transaction2", trx.len())
    );
    assert_eq!(p2.hits("/v1/chain/send_transaction2"), 1);
}

// CA11
#[tokio::test]
async fn get_info_comes_from_a_sticky_primary_and_fails_over() {
    let a = live_api_only("live-a", CHAIN, Some(1), 1000, 990).await;
    let b = live_api_only("live-b", CHAIN, Some(1), 1001, 991).await;
    let archive = frozen_node("archive-a", (2, 1001), (1, 1001), 4).await;
    let (_router, port) = start(config(
        &[a.live(), b.live(), archive.frozen((2, 1001))],
        json!({}),
    ))
    .await;
    // The highest head is primary.
    wait_served(port, GET_INFO, "", "live-b").await;
    // One block ahead is not enough to take over...
    a.api.set_head(1002, 992);
    sleep(Duration::from_millis(400)).await;
    for _ in 0..3 {
        assert_eq!(
            served_by(&post(port, GET_INFO, "").await).as_deref(),
            Some("live-b")
        );
    }
    // ... two are.
    a.api.set_head(1003, 993);
    wait_served(port, GET_INFO, "", "live-a").await;
    // The primary goes away: the next node answers at once.
    let Node { api, .. } = a;
    api.shutdown().await;
    let answer = post(port, GET_INFO, "").await;
    assert_eq!(served_by(&answer).as_deref(), Some("live-b"));
    // A body is passed on unchanged: the node writes its own error.
    let with_body = post(port, GET_INFO, r#"{"x":1}"#).await;
    assert_eq!(with_body.status, 400);
    assert_eq!(archive.hits(GET_INFO), 0, "never an archive");

    // An archive-only listener has no node for get_info.
    let (_only, only_port) = start(config(&[archive.frozen((2, 1001))], json!({}))).await;
    wait_served(only_port, RAW_BLOCK, &raw(5), "archive-a").await;
    let answer = post(only_port, GET_INFO, "").await;
    assert_eq!(
        (answer.status, answer.text()),
        (503, NO_LIVE_UPSTREAM.to_string())
    );
    assert_eq!(archive.hits(GET_INFO), 0);
}

// ----------------------------------------------------------------------
// Verification, chain_id, busy archives, limits
// ----------------------------------------------------------------------

// CA12
#[tokio::test]
async fn an_archive_is_used_only_once_verified_and_stays_verified() {
    let wrong = frozen_node("archive-wrong", (2, 1001), (1, 1001), 4).await;
    wrong.api.set_last_mismatch(true);
    let good = frozen_node("archive-good", (1001, 2001), (1001, 2001), 4).await;
    let live = live_node("live-1", 1, 3000).await;
    let (_router, port) = start(config(
        &[
            wrong.frozen((2, 1001)),
            good.frozen((1001, 2001)),
            live.live(),
        ],
        json!({}),
    ))
    .await;
    wait_served(port, RAW_BLOCK, &raw(1500), "archive-good").await;
    // The archive whose `last` does not match its SHiP head is never used.
    assert_eq!(
        post(port, RAW_BLOCK, &raw(500)).await.body,
        ok("live-1", 500)
    );
    assert_eq!(wrong.hits(RAW_BLOCK), 0);

    // The good archive's SHiP side goes away: it stays verified.
    let Node { ship, .. } = good;
    let ship = ship.unwrap();
    ship.drop_sessions();
    ship.shutdown().await;
    sleep(Duration::from_millis(800)).await;
    for n in [1001, 1600, 1999] {
        assert_eq!(
            post(port, RAW_BLOCK, &raw(n)).await.body,
            ok("archive-good", n)
        );
    }

    // Once the first archive's blocks match, it is verified and used.
    wrong.api.set_last_mismatch(false);
    wait_served(port, RAW_BLOCK, &raw(500), "archive-wrong").await;
}

// CA13
#[tokio::test]
async fn a_live_api_on_another_chain_is_not_used() {
    let other = live_api_only("live-other", [0x77; 32], Some(1), 2000, 1990).await;
    let right = live_api_only("live-right", CHAIN, Some(1), 1000, 990).await;
    let (_router, port) = start(config(&[other.live(), right.live()], json!({}))).await;
    wait_served(port, GET_INFO, "", "live-right").await;
    for (target, body) in [(GET_INFO, ""), (GET_ACCOUNT, "{}")] {
        assert_eq!(
            served_by(&post(port, target, body).await).as_deref(),
            Some("live-right")
        );
    }
    assert_eq!(other.hits(GET_ACCOUNT), 0);
    // Once it reports the listener's chain, it is up (and, with the higher
    // head, takes over get_info).
    other.api.set_chain_id(CHAIN);
    wait_served(port, GET_INFO, "", "live-other").await;
}

// CA14
#[tokio::test]
async fn busy_archives_are_skipped_and_their_limits_kept() {
    let first = frozen_node("archive-1", (2, 1001), (1, 1001), 4).await;
    let second = frozen_node("archive-2", (2, 1001), (1, 1001), 4).await;
    let (_router, port) = start(config(
        &[
            Node::with(first.frozen((2, 1001)), json!({ "priority": 0 })),
            Node::with(second.frozen((2, 1001)), json!({ "priority": 1 })),
        ],
        json!({}),
    ))
    .await;
    wait_served(port, RAW_BLOCK, &raw(500), "archive-1").await;
    // A 503 moves on to the replica, and the busy archive is skipped for
    // 2 s.
    first.api.set_fault(Some(RAW_BLOCK), Some(Fault::Busy));
    first.api.clear_requests();
    let busy_at = Instant::now();
    assert_eq!(
        post(port, RAW_BLOCK, &raw(500)).await.body,
        ok("archive-2", 500)
    );
    assert_eq!(first.hits(RAW_BLOCK), 1);
    first.api.clear_faults();
    assert_eq!(
        post(port, RAW_BLOCK, &raw(501)).await.body,
        ok("archive-2", 501)
    );
    assert_eq!(first.hits(RAW_BLOCK), 1, "skipped while busy");
    let rest = Duration::from_millis(2100).saturating_sub(busy_at.elapsed());
    sleep(rest).await;
    assert_eq!(
        post(port, RAW_BLOCK, &raw(502)).await.body,
        ok("archive-1", 502)
    );

    // An archive allowing one request at a time: a second one waits
    // upstream_queue_ms for its slot, then gets Busy. The archive never
    // sees more than its own limit.
    let narrow = frozen_node("archive-narrow", (2, 1001), (1, 1001), 1).await;
    let (_narrow_router, narrow_port) = start(config(
        &[narrow.frozen((2, 1001))],
        json!({ "upstream_queue_ms": 300 }),
    ))
    .await;
    wait_served(narrow_port, RAW_BLOCK, &raw(5), "archive-narrow").await;
    narrow.api.set_fault(
        Some(RAW_BLOCK),
        Some(Fault::Delay(Duration::from_millis(1200))),
    );
    let slow = tokio::spawn(async move { post(narrow_port, RAW_BLOCK, &raw(6)).await });
    let deadline = Instant::now() + WAIT;
    while narrow.api.in_flight() == 0 {
        assert!(Instant::now() < deadline, "the slow request never arrived");
        sleep(Duration::from_millis(5)).await;
    }
    let started = Instant::now();
    let refused = post(narrow_port, RAW_BLOCK, &raw(7)).await;
    let waited = started.elapsed();
    assert_eq!(
        (refused.status, refused.text()),
        (503, NO_UPSTREAM.to_string())
    );
    assert!(waited >= Duration::from_millis(250), "{waited:?}");
    assert!(waited < Duration::from_millis(1100), "{waited:?}");
    assert_eq!(slow.await.unwrap().body, ok("archive-narrow", 6));
    assert_eq!((narrow.api.peak_in_flight(), narrow.api.rejected()), (1, 0));
}

// CA15
#[tokio::test]
async fn listener_limits_answer_nodeos_shaped_errors() {
    let live = live_api_only("live-1", CHAIN, Some(1), 1000, 990).await;
    let mut cfg = config(
        &[live.live()],
        json!({ "max_body_bytes": 1024, "max_in_flight": 1, "max_connections": 3 }),
    );
    cfg["handshake_timeout_ms"] = json!(1000);
    let (_router, port) = start(cfg).await;
    wait_answer(port, GET_INFO, "", |a| a.status == 200).await;
    let too_large = r#"{"code":413,"message":"Payload Too Large","error":{"code":413,"name":"Payload Too Large","what":"Request body exceeds 1024 bytes","details":[]}}"#;

    // A declared body over the cap: 413, then the connection closes.
    let mut conn = RawConn::connect(addr(port)).await.unwrap();
    let body = "x".repeat(2000);
    conn.send(&request("POST", GET_ACCOUNT, "127.0.0.1", body.as_bytes()))
        .await
        .unwrap();
    let answer = conn.response(WAIT).await.unwrap();
    assert_eq!((answer.status, answer.text()), (413, too_large.to_string()));
    assert!(conn.closed(WAIT).await);
    // A chunked body that grows past the cap: the same.
    let mut conn = RawConn::connect(addr(port)).await.unwrap();
    let chunk = "y".repeat(800);
    conn.send(
        format!(
            "POST {GET_ACCOUNT} HTTP/1.1\r\nHost: x\r\nTransfer-Encoding: chunked\r\n\r\n320\r\n{chunk}\r\n320\r\n{chunk}\r\n0\r\n\r\n"
        )
        .as_bytes(),
    )
    .await
    .unwrap();
    let answer = conn.response(WAIT).await.unwrap();
    assert_eq!((answer.status, answer.text()), (413, too_large.to_string()));
    assert!(conn.closed(WAIT).await);
    assert_eq!(live.hits(GET_ACCOUNT), 0);

    // One request in flight: the next is Busy at once.
    live.api.set_fault(
        Some(GET_ACCOUNT),
        Some(Fault::Delay(Duration::from_millis(800))),
    );
    let slow = tokio::spawn(async move { post(port, GET_ACCOUNT, "{}").await });
    let deadline = Instant::now() + WAIT;
    while live.api.in_flight() == 0 && live.hits(GET_ACCOUNT) == 0 {
        assert!(Instant::now() < deadline);
        sleep(Duration::from_millis(5)).await;
    }
    let busy = post(port, GET_INFO, "").await;
    assert_eq!(
        (busy.status, busy.text()),
        (
            503,
            r#"{"code":503,"message":"Busy","error":{"code":503,"name":"Busy","what":"Too many requests in flight: 1","details":[]}}"#
                .to_string()
        )
    );
    assert_eq!(slow.await.unwrap().status, 200);
    live.api.clear_faults();

    // Three connections at most: a fourth is closed at once. (The earlier
    // connections are closed by now.)
    sleep(Duration::from_millis(200)).await;
    let mut open = Vec::new();
    for _ in 0..3 {
        let mut conn = RawConn::connect(addr(port)).await.unwrap();
        conn.send(&request("POST", GET_INFO, "127.0.0.1", b""))
            .await
            .unwrap();
        assert_eq!(conn.response(WAIT).await.unwrap().status, 200);
        open.push(conn);
    }
    let mut extra = RawConn::connect(addr(port)).await.unwrap();
    assert!(extra.closed(Duration::from_secs(2)).await);
    // An idle connection closes after the read timeout (1 s here), which
    // frees its slot.
    let idle_since = Instant::now();
    assert!(open[0].closed(WAIT).await);
    assert!(idle_since.elapsed() < Duration::from_secs(3));
    drop(open);
    sleep(Duration::from_millis(200)).await;
    assert_eq!(post(port, GET_INFO, "").await.status, 200);
}

// CA16
#[tokio::test]
async fn targets_options_and_cors_follow_nodeos() {
    let live = MockChainApi {
        extra_headers: vec![
            (
                "Access-Control-Allow-Origin".into(),
                "https://upstream.example".into(),
            ),
            ("Access-Control-Allow-Methods".into(), "POST".into()),
        ],
        ..MockChainApi::live("live-1", CHAIN, 1000, 990, Some(1), "v1.2.2")
    }
    .start()
    .await;
    let dead = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let node = Node {
        name: "live-1".into(),
        ship_ep: dead.local_addr().unwrap().to_string(),
        ship: None,
        _dead: Some(dead),
        api: live,
    };
    let (_router, port) = start(config(&[node.live()], json!({}))).await;
    wait_answer(port, GET_INFO, "", |a| a.status == 200).await;

    // Illegal targets, first of all.
    for line in [
        "OPTIONS * HTTP/1.1",
        "GET http://127.0.0.1/v1/chain/get_info HTTP/1.1",
        "POST /v1/chain/../chain/get_info HTTP/1.1",
    ] {
        let bytes = format!("{line}\r\nHost: x\r\nContent-Length: 0\r\n\r\n");
        let answer = exchange(addr(port), bytes.as_bytes(), WAIT).await.unwrap();
        assert_eq!(
            (answer.status, answer.text()),
            (400, ILLEGAL_TARGET.to_string()),
            "{line}"
        );
        assert_eq!(answer.header("content-type"), Some("application/json"));
        assert!(answer.header("server").is_none() && answer.header("date").is_none());
    }
    // OPTIONS on any legal target, by the router.
    let options = send(port, "OPTIONS", "/v1/chain/anything", "").await;
    assert_eq!((options.status, options.text()), (200, "{}".to_string()));
    assert!(options.header("access-control-allow-origin").is_none());
    // Without `cors`, upstream CORS headers are stripped.
    let info = post(port, GET_INFO, "").await;
    assert_eq!(info.status, 200);
    assert!(info.header("access-control-allow-origin").is_none());
    assert!(info.header("access-control-allow-methods").is_none());
    // Exact dispatch: a query string is the 404.
    let query = post(port, "/v1/chain/get_info?x=1", "").await;
    assert_eq!(
        (query.status, query.text()),
        (404, NOT_FOUND_BODY.to_string())
    );
    // Title-case header names, no Date.
    assert!(info.headers.iter().any(|(n, _)| n == "Content-Type"));
    assert!(info.header("date").is_none());

    // With `cors`, the router's headers on every answer.
    let (_cors_router, cors_port) = start(config(
        &[node.live()],
        json!({ "cors": { "allow_origin": "*", "max_age": 600 } }),
    ))
    .await;
    wait_answer(cors_port, GET_INFO, "", |a| a.status == 200).await;
    let options = send(cors_port, "OPTIONS", GET_INFO, "").await;
    assert_eq!((options.status, options.text()), (200, "{}".to_string()));
    for answer in [
        options,
        post(cors_port, GET_INFO, "").await,
        post(cors_port, "/v1/history/get_actions", "{}").await,
    ] {
        assert_eq!(answer.header("access-control-allow-origin"), Some("*"));
        assert_eq!(answer.header("access-control-max-age"), Some("600"));
        assert!(answer.header("access-control-allow-methods").is_none());
        assert!(answer.header("access-control-allow-credentials").is_none());
    }
    // nodeos checks the target before it sets its CORS headers, so its
    // "Illegal request-target" answer has none.
    let illegal = exchange(
        addr(cors_port),
        b"OPTIONS * HTTP/1.1\r\nHost: x\r\nContent-Length: 0\r\n\r\n",
        WAIT,
    )
    .await
    .unwrap();
    assert_eq!(
        (illegal.status, illegal.text()),
        (400, ILLEGAL_TARGET.to_string())
    );
    assert!(
        illegal
            .headers
            .iter()
            .all(|(name, _)| !name.to_ascii_lowercase().starts_with("access-control-")),
        "{:?}",
        illegal.headers
    );
}

// CA17
#[tokio::test]
async fn supported_apis_are_synthesized_from_the_upstreams() {
    let archive = frozen_node("archive-a", (2, 1001), (1, 1001), 4).await;
    let live = live_node("live-1", 1, 2000).await;
    live.api.set_apis(Some(
        [
            "/v1/chain/get_info",
            "/v1/chain/get_account",
            "/v1/chain/get_block",
            "/v1/chain/push_transaction",
            "/v1/chain/push_block",
            "/v1/trace_api/get_block",
            "/v1/node/get_supported_apis",
        ]
        .into_iter()
        .map(String::from)
        .collect(),
    ));
    let (_router, port) = start(config(&[archive.frozen((2, 1001)), live.live()], json!({}))).await;
    let want = r#"{"apis":["/v1/chain/get_account","/v1/chain/get_block","/v1/chain/get_block_header","/v1/chain/get_block_info","/v1/chain/get_info","/v1/chain/get_raw_block"]}"#;
    let answer = wait_answer(port, "/v1/node/get_supported_apis", "", |a| {
        a.text() == want
    })
    .await;
    assert_eq!(answer.status, 200);
    assert_eq!(answer.header("content-type"), Some("application/json"));
    assert_eq!(archive.hits("/v1/node/get_supported_apis"), 0);
    // A path the live node no longer lists is not served from it.
    assert_eq!(
        post(port, "/v1/chain/get_table_rows", "{}").await.text(),
        NO_LIVE_UPSTREAM
    );
}

// CA18
#[tokio::test]
async fn without_live_nodes_archives_still_serve_and_nothing_is_invented() {
    let archive = frozen_node("archive-a", (2, 1001), (1, 1001), 4).await;
    let (_router, port) = start(config(&[archive.frozen((2, 1001))], json!({}))).await;
    wait_served(port, RAW_BLOCK, &raw(500), "archive-a").await;
    for (target, body, want) in [
        (GET_INFO, "", NO_LIVE_UPSTREAM),
        (GET_ACCOUNT, "{}", NO_LIVE_UPSTREAM),
        // Unhinted bodies need a live node.
        (RAW_BLOCK, r#"{"block_num_or_id":"+5"}"#, NO_LIVE_UPSTREAM),
        // No API holds the block: Busy, never an invented Unknown Block.
        (RAW_BLOCK, &raw(5000), NO_UPSTREAM),
        (GET_BLOCK, &raw(500), NO_UPSTREAM),
    ] {
        let answer = post(port, target, body).await;
        assert_eq!(
            (answer.status, answer.text()),
            (503, want.to_string()),
            "{target} {body}"
        );
    }
    assert_eq!(
        post(port, BLOCK_INFO, r#"{"block_num":7}"#).await.body,
        ok("archive-a", 7)
    );
}

// The archived block rule: when a frozen range server declares or serves a
// block, a live node whose log begins above it is no candidate for the
// endpoints archives serve. If the archive cannot answer, the client gets
// Busy, never that node's Unknown Block.
#[tokio::test]
async fn an_archived_block_never_ends_on_a_late_live_nodes_unknown_block() {
    let archive = frozen_node("archive-a", (2, 1001), (1, 1001), 4).await;
    let late = live_node("live-late", 3000, 5000).await;
    let (_router, port) = start(config(&[archive.frozen((2, 1001)), late.live()], json!({}))).await;
    wait_served(port, RAW_BLOCK, &raw(500), "archive-a").await;
    // The archive is busy: Busy.
    archive.api.set_fault(None, Some(Fault::Busy));
    late.api.clear_requests();
    let answer = post(port, RAW_BLOCK, &raw(500)).await;
    assert_eq!(
        (answer.status, answer.text()),
        (503, NO_UPSTREAM.to_string())
    );
    // The archive goes away: still Busy.
    archive.api.clear_faults();
    let Node { api, .. } = archive;
    api.shutdown().await;
    sleep(Duration::from_millis(400)).await;
    for target in [RAW_BLOCK, "/v1/chain/get_block_header"] {
        let answer = post(port, target, &raw(600)).await;
        assert_eq!(
            (answer.status, answer.text()),
            (503, NO_UPSTREAM.to_string()),
            "{target}"
        );
    }
    assert_eq!(
        post(port, BLOCK_INFO, r#"{"block_num":700}"#).await.text(),
        NO_UPSTREAM
    );
    assert_eq!(late.hits(RAW_BLOCK) + late.hits(BLOCK_INFO), 0);
    // get_block is the live nodes' alone: the late node's genuine answer.
    let answer = post(port, GET_BLOCK, &raw(500)).await;
    assert_eq!(answer.status, 400);
    assert_eq!(answer.body, direct(&late, GET_BLOCK, &raw(500)).await.body);
    // Above the archive's range, the late node is a candidate again.
    let answer = post(port, RAW_BLOCK, &raw(2000)).await;
    assert_eq!(answer.status, 400);
    assert!(answer.text().contains("3100002"));
}

// The same rule for a node that does not report where its log begins (an
// EOSIO 2.0 or FIO node, say): it is still asked, but for a block an
// archive claims its Unknown Block is never the answer.
#[tokio::test]
async fn an_archived_block_never_ends_on_an_unknown_block_from_a_node_without_earliest() {
    let archive = frozen_node("archive-a", (2, 1001), (1, 1001), 4).await;
    let legacy = live_api_only("live-legacy", CHAIN, None, 5000, 4990).await;
    let (_router, port) = start(config(
        &[archive.frozen((2, 1001)), legacy.live()],
        json!({}),
    ))
    .await;
    wait_served(port, RAW_BLOCK, &raw(500), "archive-a").await;
    archive.api.set_fault(Some(RAW_BLOCK), Some(Fault::Busy));
    legacy
        .api
        .set_fault(Some(RAW_BLOCK), Some(Fault::UnknownBlock));
    legacy.api.clear_requests();
    let answer = post(port, RAW_BLOCK, &raw(500)).await;
    assert_eq!(
        (answer.status, answer.text()),
        (503, NO_UPSTREAM.to_string())
    );
    assert_eq!(legacy.hits(RAW_BLOCK), 1, "asked once");
    // Outside every claim, its Unknown Block is the answer.
    let answer = post(port, RAW_BLOCK, &raw(2000)).await;
    assert_eq!(answer.status, 400);
    assert!(answer.text().contains("3100002"));
}

// CA19
#[tokio::test]
async fn probe_churn_never_changes_the_fleet_view() {
    let archive = frozen_node("archive-a", (2, 1001), (1, 1001), 4).await;
    // The live API keeps its own clock, so its head moves while the SHiP
    // side stays still.
    let ship = MockShipServer::new(MockShipConfig {
        chain_id: CHAIN,
        ..MockShipConfig::live(1, 2000)
    })
    .await;
    let ship_ep = ship.endpoint();
    let _ship = ship.start();
    let api = MockChainApi::live("live-1", CHAIN, 2000, 1990, Some(1), "v1.2.2")
        .start()
        .await;
    let live = Node {
        name: "live-1".into(),
        ship_ep,
        ship: None,
        _dead: None,
        api,
    };
    let mut cfg = config(&[archive.frozen((2, 1001)), live.live()], json!({}));
    // The SHiP side never advances here: keep the stale check from flipping
    // the live node during the test.
    cfg["upstream_monitoring_ms"] = json!(60_000);
    let (router, port) = start(cfg).await;
    wait_served(port, RAW_BLOCK, &raw(500), "archive-a").await;
    let metrics = router.metrics();
    let generation = || async move {
        let (_, body) = http_get(metrics, "/coverage").await.unwrap();
        let v: Value = serde_json::from_str(&body).unwrap();
        v["listeners"][0]["generation"].as_u64().unwrap()
    };
    // Settled: two reads apart give the same generation.
    let deadline = Instant::now() + WAIT;
    let mut settled = generation().await;
    loop {
        sleep(Duration::from_millis(500)).await;
        let now = generation().await;
        if now == settled {
            break;
        }
        settled = now;
        assert!(Instant::now() < deadline, "the view never settled");
    }

    // Churn on the API side only: heads, a chain mismatch, failing probes,
    // busy answers, requests.
    for step in 0..6u32 {
        live.api.set_head(2000 + step * 7, 1990 + step * 7);
        archive.api.set_fault(
            Some("/v1/archive/describe"),
            (step % 2 == 0).then_some(Fault::Busy),
        );
        if step == 2 {
            live.api.set_chain_id([0x77; 32]);
        }
        if step == 4 {
            live.api.set_chain_id(CHAIN);
        }
        let _ = post(port, RAW_BLOCK, &raw(400 + step)).await;
        let _ = post(port, GET_INFO, "").await;
        sleep(Duration::from_millis(250)).await;
    }
    archive.api.clear_faults();
    assert_eq!(generation().await, settled, "a probe changed the SHiP view");
}

// CA22
#[tokio::test]
async fn priority_and_weight_steer_the_choice_within_a_tier() {
    // Large enough for blocks in several rendezvous chunks.
    let backup = frozen_node("archive-backup", (2, 20_001), (1, 20_001), 4).await;
    let preferred = frozen_node("archive-preferred", (2, 20_001), (1, 20_001), 4).await;
    let light = live_api_only("live-light", CHAIN, Some(1), 1000, 990).await;
    let heavy = live_api_only("live-heavy", CHAIN, Some(1), 1000, 990).await;
    let low = live_api_only("live-low", CHAIN, Some(1), 1000, 990).await;
    let (_router, port) = start(config(
        &[
            Node::with(backup.frozen((2, 20_001)), json!({ "priority": 1 })),
            preferred.frozen((2, 20_001)),
            Node::with(light.live(), json!({ "weight": 1 })),
            Node::with(heavy.live(), json!({ "weight": 3 })),
            Node::with(low.live(), json!({ "priority": 5, "weight": 9 })),
        ],
        json!({}),
    ))
    .await;
    // Priority within the archive tier, whatever the block.
    for n in [5, 4096 * 3, 16_000] {
        let answer = wait_served(port, RAW_BLOCK, &raw(n), "archive-preferred").await;
        assert_eq!(answer.body, ok("archive-preferred", n));
    }
    assert_eq!(backup.hits(RAW_BLOCK), 0);
    // Equal priority and load: the higher weight first; a worse priority
    // never, whatever its weight.
    wait_served(port, GET_ACCOUNT, "{}", "live-heavy").await;
    for _ in 0..5 {
        assert_eq!(
            served_by(&post(port, GET_ACCOUNT, "{}").await).as_deref(),
            Some("live-heavy")
        );
    }
    assert_eq!(low.hits(GET_ACCOUNT), 0);
    // The preferred one busy: the next priority takes over.
    heavy.api.set_fault(Some(GET_ACCOUNT), Some(Fault::Busy));
    let answer = post(port, GET_ACCOUNT, "{}").await;
    assert_eq!(served_by(&answer).as_deref(), Some("live-light"));
    assert_eq!(light.hits(GET_ACCOUNT), 1);
}

/// `fleet-router config test` on `cfg` (with a fixed chain API port: the
/// command binds nothing). Returns whether it succeeded, and its output.
async fn config_test(cfg: &Value, strict: bool) -> (bool, String) {
    let mut cfg = cfg.clone();
    cfg["listen_port"] = json!(17000);
    cfg["chain_api"]["listen_port"] = json!(18888);
    cfg["handshake_timeout_ms"] = json!(1000);
    cfg.as_object_mut().unwrap().remove("metrics_port");
    let mut file = tempfile::NamedTempFile::new().unwrap();
    std::io::Write::write_all(&mut file, cfg.to_string().as_bytes()).unwrap();
    let path = file.path().to_path_buf();
    let output = tokio::task::spawn_blocking(move || {
        let mut cmd = std::process::Command::new(env!("CARGO_BIN_EXE_fleet-router"));
        cmd.args(["config", "test"]);
        if strict {
            cmd.arg("--strict");
        }
        cmd.arg(&path).output().expect("run config test")
    })
    .await
    .unwrap();
    drop(file);
    let text = format!(
        "{}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    (output.status.success(), text)
}

/// `config test` lists each server's chain API; `--strict` fails when an
/// enabled frozen server on a chain API listener has none.
#[tokio::test]
async fn config_test_lists_the_chain_api_and_strict_needs_every_archive_api() {
    let archive = frozen_node("archive-a", (2, 1001), (1, 1001), 4).await;
    let second = frozen_node("archive-b", (1001, 2001), (1001, 2001), 4).await;
    let live = live_node("live-1", 1, 3000).await;
    let complete = config(
        &[
            archive.frozen((2, 1001)),
            second.frozen((1001, 2001)),
            live.live(),
        ],
        json!({}),
    );
    let (ok, out) = config_test(&complete, true).await;
    assert!(ok, "every archive has an API:\n{out}");
    assert!(out.contains("Chain API"), "{out}");
    assert!(out.contains(&archive.api.url()), "{out}");
    assert!(out.contains(&live.api.url()), "{out}");

    let mut without = second.frozen((1001, 2001));
    without.as_object_mut().unwrap().remove("api_endpoint");
    let partial = config(
        &[archive.frozen((2, 1001)), without, live.live()],
        json!({}),
    );
    let (ok, out) = config_test(&partial, false).await;
    assert!(ok, "without --strict it is a warning:\n{out}");
    assert!(
        out.contains("frozen server 'archive-b' has no api_endpoint"),
        "{out}"
    );
    let (ok, out) = config_test(&partial, true).await;
    assert!(!ok, "--strict fails:\n{out}");
}

// ----------------------------------------------------------------------
// Metrics, readiness and coverage
// ----------------------------------------------------------------------

/// The labels of every upstream series of `node`: its SHiP endpoint and
/// name, after the listener's name with several listeners.
fn upstream_labels(listener: Option<&str>, node: &Node) -> String {
    let labels = format!("endpoint=\"{}\",server=\"{}\"", node.ship_ep, node.name);
    match listener {
        Some(name) => format!("listener=\"{name}\",{labels}"),
        None => labels,
    }
}

/// The value of one series on `/metrics`; panics when it is absent.
async fn metric(metrics: u16, series: &str) -> f64 {
    common::metric_value(metrics, series)
        .await
        .unwrap_or_else(|| panic!("no {series} on /metrics"))
}

/// The chain API lines of `/metrics`.
async fn api_lines(metrics: u16) -> Vec<String> {
    let (_, body) = http_get(metrics, "/metrics").await.unwrap();
    body.lines()
        .filter(|l| {
            l.starts_with("fleet_router_api_") || l.starts_with("fleet_router_upstream_api_")
        })
        .map(str::to_string)
        .collect()
}

/// Poll `GET path` on the metrics port until it answers `status` with
/// exactly `body`.
async fn wait_http(metrics: u16, path: &str, status: u16, body: &str) {
    let deadline = Instant::now() + WAIT;
    loop {
        let got = http_get(metrics, path).await;
        if got.as_ref().is_some_and(|(s, b)| *s == status && b == body) {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "{path}: wanted {status} {body:?}, last {got:?}"
        );
        sleep(POLL).await;
    }
}

// CA21, one listener: the series carry no listener label.
#[tokio::test]
async fn chain_api_metrics_coverage_and_readiness_with_one_listener() {
    let archive = frozen_node("archive-a", (2, 1001), (1, 1001), 4).await;
    let live = live_node("live-1", 1, 2000).await;
    let (router, port) = start(config(&[archive.frozen((2, 1001)), live.live()], json!({}))).await;
    let metrics = router.metrics();
    wait_served(port, RAW_BLOCK, &raw(500), "archive-a").await;
    wait_served(port, GET_INFO, "", "live-1").await;
    // One answer of each kind the router writes itself.
    assert_eq!(post(port, "/v1/chain/push_block", "{}").await.status, 404);
    assert_eq!(send(port, "OPTIONS", RAW_BLOCK, "").await.status, 200);

    wait_http(
        metrics,
        "/ready?scope=api",
        200,
        "ready: 2 chain API upstream(s) up\n",
    )
    .await;

    for (series, want) in [
        (
            r#"fleet_router_api_requests_total{class="denied",code="4xx"}"#,
            1.0,
        ),
        (
            r#"fleet_router_api_requests_total{class="node",code="2xx"}"#,
            1.0,
        ),
        (
            r#"fleet_router_api_requests_total{class="push",code="2xx"}"#,
            0.0,
        ),
        (
            r#"fleet_router_api_request_duration_seconds_count{class="denied"}"#,
            1.0,
        ),
        (
            r#"fleet_router_api_request_duration_seconds_bucket{class="denied",le="+Inf"}"#,
            1.0,
        ),
        ("fleet_router_api_in_flight", 0.0),
        (
            r#"fleet_router_api_rejected_total{reason="connections"}"#,
            0.0,
        ),
        (r#"fleet_router_api_pushes_total{result="ok"}"#, 0.0),
    ] {
        assert_eq!(metric(metrics, series).await, want, "{series}");
    }
    for series in [
        r#"fleet_router_api_requests_total{class="block",code="2xx"}"#,
        r#"fleet_router_api_requests_total{class="info",code="2xx"}"#,
        r#"fleet_router_api_block_requests_total{region="irreversible",role="frozen"}"#,
    ] {
        assert!(metric(metrics, series).await >= 1.0, "{series}");
    }

    let a = upstream_labels(None, &archive);
    let v = upstream_labels(None, &live);
    for (series, want) in [
        (format!("fleet_router_upstream_api_up{{{a}}}"), 1.0),
        (format!("fleet_router_upstream_api_up{{{v}}}"), 1.0),
        (format!("fleet_router_upstream_api_verified{{{a}}}"), 1.0),
        (
            format!("fleet_router_upstream_api_blocks_begin_block{{{a}}}"),
            1.0,
        ),
        (
            format!("fleet_router_upstream_api_blocks_end_block{{{a}}}"),
            1001.0,
        ),
        (
            format!("fleet_router_upstream_api_max_in_flight{{{a}}}"),
            4.0,
        ),
        (format!("fleet_router_upstream_api_in_flight{{{a}}}"), 0.0),
        (
            format!("fleet_router_upstream_api_chain_id_mismatch{{{v}}}"),
            0.0,
        ),
        (
            format!("fleet_router_upstream_api_earliest_block{{{v}}}"),
            1.0,
        ),
    ] {
        assert_eq!(metric(metrics, &series).await, want, "{series}");
    }
    for series in [
        format!("fleet_router_upstream_api_head_block{{{v}}}"),
        format!("fleet_router_upstream_api_requests_total{{{a},result=\"ok\"}}"),
        format!("fleet_router_upstream_api_requests_total{{{v},result=\"ok\"}}"),
    ] {
        assert!(metric(metrics, &series).await >= 1.0, "{series}");
    }
    let lines = api_lines(metrics).await;
    assert!(lines.len() > 100, "{lines:#?}");
    assert!(lines.iter().all(|l| !l.contains("listener=")), "{lines:#?}");

    let (_, body) = http_get(metrics, "/coverage").await.unwrap();
    let doc: Value = serde_json::from_str(&body).unwrap();
    let api = &doc["listeners"][0]["chain_api"];
    assert_eq!(api["listen"], format!("127.0.0.1:{port}"));
    let node = &api["live"][0];
    assert_eq!(node["server"], "live-1");
    assert_eq!(node["up"], true);
    assert_eq!(node["earliest"], 1);
    assert_eq!(node["version"], "v1.2.2");
    assert!(node["head"].is_u64() && node["lib"].is_u64(), "{api}");
    assert_eq!(
        api["frozen"],
        json!([{ "server": "archive-a", "up": true, "verified": true,
                 "blocks": { "begin": 1, "end": 1001 }, "get_block": "off" }])
    );
}

// CA21 with several listeners, and CA13's readiness: a live API on another
// chain leaves the chain API unready until it reports the listener's chain.
#[tokio::test]
async fn chain_api_series_name_their_listener_and_readiness_follows_the_live_chain() {
    let node = live_api_only("live-other", [0x77; 32], Some(1), 2000, 1990).await;
    // The second listener's SHiP endpoint, which never answers.
    let dead = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let mut cfg = base_config(&[]);
    let top = cfg.as_object_mut().unwrap();
    top.remove("servers");
    top.remove("listen_port");
    cfg["listeners"] = json!([
        {
            "name": "main",
            "chain_id": hex(&CHAIN),
            "servers": [node.live()],
            "chain_api": { "listen_port": 0, "probe_ms": 100 },
        },
        {
            "name": "plain",
            "chain_id": hex(&CHAIN),
            "servers": [{
                "name": "plain-1",
                "endpoint": dead.local_addr().unwrap().to_string(),
                "enabled": true,
            }],
        },
    ]);
    let (router, ports) = start_with_chain_api(cfg).await;
    let (port, metrics) = (ports[0], router.metrics());
    let labels = upstream_labels(Some("main"), &node);

    common::wait_metric(
        metrics,
        &format!("fleet_router_upstream_api_chain_id_mismatch{{{labels}}} 1\n"),
        WAIT,
    )
    .await;
    assert_eq!(
        metric(
            metrics,
            &format!("fleet_router_upstream_api_up{{{labels}}}")
        )
        .await,
        0.0
    );
    wait_http(
        metrics,
        "/ready?scope=api",
        503,
        "not ready: main: no live chain API upstream up\n",
    )
    .await;
    wait_http(
        metrics,
        "/ready/plain?scope=api",
        503,
        "not ready (plain): no chain API configured\n",
    )
    .await;

    node.api.set_chain_id(CHAIN);
    wait_http(
        metrics,
        "/ready?scope=api",
        200,
        "ready: 1 listener(s) ready, 1 chain API upstream(s) up\n",
    )
    .await;
    wait_http(
        metrics,
        "/ready/main?scope=api",
        200,
        "ready (main): 1 chain API upstream(s) up\n",
    )
    .await;
    // The SHiP side of `main` never answers: its tip readiness is apart.
    assert_eq!(http_get(metrics, "/ready/main").await.unwrap().0, 503);

    assert_eq!(
        served_by(&post(port, GET_INFO, "").await).as_deref(),
        Some("live-other")
    );
    for (series, want) in [
        (format!("fleet_router_upstream_api_up{{{labels}}}"), 1.0),
        (
            format!("fleet_router_upstream_api_chain_id_mismatch{{{labels}}}"),
            0.0,
        ),
        (
            format!("fleet_router_upstream_api_head_block{{{labels}}}"),
            2000.0,
        ),
        (
            format!("fleet_router_upstream_api_requests_total{{{labels},result=\"ok\"}}"),
            1.0,
        ),
        (
            r#"fleet_router_api_requests_total{listener="main",class="info",code="2xx"}"#
                .to_string(),
            1.0,
        ),
        (
            r#"fleet_router_api_in_flight{listener="main"}"#.to_string(),
            0.0,
        ),
    ] {
        assert_eq!(metric(metrics, &series).await, want, "{series}");
    }
    // Only the listener with a chain API has chain API series.
    let lines = api_lines(metrics).await;
    assert!(!lines.is_empty());
    assert!(
        lines.iter().all(|l| l.contains("{listener=\"main\"")),
        "{lines:#?}"
    );

    let (_, body) = http_get(metrics, "/coverage").await.unwrap();
    let doc: Value = serde_json::from_str(&body).unwrap();
    let api = &doc["listeners"][0]["chain_api"];
    assert_eq!(api["listen"], format!("127.0.0.1:{port}"));
    assert_eq!(
        api["live"],
        json!([{ "server": "live-other", "up": true, "head": 2000, "lib": 1990,
                 "earliest": 1, "version": "v1.2.2" }])
    );
    assert_eq!(api["frozen"], json!([]));
    assert_eq!(doc["listeners"][1]["name"], "plain");
    assert_eq!(doc["listeners"][1]["chain_api"], Value::Null);
}
