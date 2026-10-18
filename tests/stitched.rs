//! End-to-end tests of a stitched listener: frozen range servers and live
//! nodes behind one router that presents them as a single SHiP node.
//!
//! The default fleet is two frozen range servers, A = `[2,1001)` and
//! B = `[1001,2001)`, and a live node C from block 1900 whose head starts at
//! 2300 (some tests advance it 10 blocks every 100 ms). `chain_id` is pinned
//! and `gap_wait_ms` is 1000. Mocks run with Spring fidelity; a frozen mock
//! holds open at the end of its range, as the archive contract requires.
//! The scenarios are S1-S25 of the design, plus seam continuity, unknown
//! requests, startup without upstreams, busy archives and `config test`.
//!
//! Each test finishes in a few seconds.
//!
//! Run: cargo test --test stitched

mod common;

use std::sync::Arc;
use std::time::{Duration, Instant};

use common::{
    base_config, frozen, http_get, metric_value, read_blocks, read_msg, server, wait_metric,
    ws_connect, RouterProcess, WsReader, WsWriter,
};
use futures::SinkExt;
use mock_ship::client::{ack, blocks, status_v0, status_v1, BlockView, BlocksReq, StatusView};
use mock_ship::{fake_block_id, MockShipConfig, MockShipHandle, MockShipServer, Observed};
use serde_json::{json, Value};
use tokio::task::JoinHandle;
use tokio::time::sleep;
use tokio_tungstenite::tungstenite::protocol::frame::coding::CloseCode;
use tokio_tungstenite::tungstenite::protocol::CloseFrame;
use tokio_tungstenite::tungstenite::Message;

const WAIT: Duration = Duration::from_secs(5);
const CHAIN_ID: [u8; 32] = [0x5a; 32];
const A_RANGE: (u32, u32) = (2, 1001);
const B_RANGE: (u32, u32) = (1001, 2001);
const C_BEGIN: u32 = 1900;
const C_HEAD: u32 = 2300;

// ----------------------------------------------------------------------
// Mocks
// ----------------------------------------------------------------------

/// A running mock upstream.
struct Up {
    ep: String,
    name: String,
    mock: Arc<MockShipHandle>,
}

impl Up {
    /// The upstream labels the router renders on per-upstream series.
    fn labels(&self) -> String {
        labels(&self.ep, &self.name)
    }

    /// `fleet_router_<series>{endpoint=..,server=..}` with optional extra
    /// labels.
    fn series(&self, series: &str, extra: &str) -> String {
        if extra.is_empty() {
            format!("fleet_router_{series}{{{}}}", self.labels())
        } else {
            format!("fleet_router_{series}{{{},{extra}}}", self.labels())
        }
    }
}

fn labels(endpoint: &str, server: &str) -> String {
    format!("endpoint=\"{endpoint}\",server=\"{server}\"")
}

fn with_chain(config: MockShipConfig) -> MockShipConfig {
    MockShipConfig {
        chain_id: CHAIN_ID,
        ..config
    }
}

fn frozen_mock(range: (u32, u32)) -> MockShipConfig {
    with_chain(MockShipConfig::frozen(range.0, range.1))
}

fn live_mock(begin: u32, head: u32) -> MockShipConfig {
    with_chain(MockShipConfig::live(begin, head))
}

/// Bind a mock without accepting yet: until [`start`], the router's monitor
/// sees it as down.
async fn bound(name: &str, config: MockShipConfig) -> (String, String, MockShipServer) {
    let server = MockShipServer::new(config).await;
    (server.endpoint(), name.to_string(), server)
}

fn start(bound: (String, String, MockShipServer)) -> Up {
    let (ep, name, server) = bound;
    Up {
        ep,
        name,
        mock: Arc::new(server.start()),
    }
}

async fn up(name: &str, config: MockShipConfig) -> Up {
    start(bound(name, config).await)
}

/// Bring a mock back on the port of one that was shut down. The port is
/// probed first: an old socket may hold it for a moment.
async fn restart(ep: &str, name: &str, config: MockShipConfig) -> Up {
    let port: u16 = ep.rsplit(':').next().unwrap().parse().unwrap();
    let deadline = Instant::now() + WAIT;
    while std::net::TcpListener::bind(("127.0.0.1", port)).is_err() {
        assert!(Instant::now() < deadline, "port {port} never came free");
        sleep(Duration::from_millis(50)).await;
    }
    up(name, MockShipConfig { port, ..config }).await
}

/// An upstream that is down for the whole test: TCP connects land in the
/// backlog of a listener that never accepts, so a WebSocket upgrade never
/// completes. Holding the listener keeps its port from being handed to
/// another test.
fn dead_endpoint() -> (String, std::net::TcpListener) {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    (listener.local_addr().unwrap().to_string(), listener)
}

/// Stop a mock: drop its sessions and close its listener.
async fn stop(up: Up) {
    up.mock.drop_sessions();
    let mock = Arc::try_unwrap(up.mock)
        .unwrap_or_else(|_| panic!("{} is still shared (a ticker?)", up.name));
    mock.shutdown().await;
}

/// Advances a live mock's head and LIB by 10 every 100 ms until dropped.
struct Ticker(JoinHandle<()>);

impl Drop for Ticker {
    fn drop(&mut self) {
        self.0.abort();
    }
}

fn tick(up: &Up) -> Ticker {
    let mock = up.mock.clone();
    Ticker(tokio::spawn(async move {
        loop {
            sleep(Duration::from_millis(100)).await;
            mock.advance(10);
        }
    }))
}

/// The default fleet: A and B frozen, C live (not ticking yet).
async fn default_fleet() -> (Up, Up, Up) {
    (
        up("a", frozen_mock(A_RANGE)).await,
        up("b", frozen_mock(B_RANGE)).await,
        up("c", live_mock(C_BEGIN, C_HEAD)).await,
    )
}

// ----------------------------------------------------------------------
// Router
// ----------------------------------------------------------------------

fn frozen_server(up: &Up) -> Value {
    frozen(&up.name, &up.ep, None)
}

fn live_server(up: &Up) -> Value {
    server(&up.name, &up.ep)
}

/// A frozen server entry with `max_sessions: 1`.
fn single_slot(up: &Up) -> Value {
    let mut s = frozen_server(up);
    s["max_sessions"] = json!(1);
    s
}

/// A stitched single-listener config with `chain_id` pinned and
/// `gap_wait_ms` 1000. Status is polled every 300 ms.
fn config(servers: Vec<Value>) -> Value {
    let mut cfg = base_config(&servers);
    cfg["chain_id"] = json!(hex(&CHAIN_ID));
    cfg["upstream_status_ms"] = json!(300);
    cfg["upstream_monitoring_ms"] = json!(300);
    cfg["stitching"] = json!({ "gap_wait_ms": 1000 });
    cfg
}

fn fleet_config(a: &Up, b: &Up, c: &Up) -> Value {
    config(vec![frozen_server(a), frozen_server(b), live_server(c)])
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

async fn metric(router: &RouterProcess, series: &str) -> f64 {
    metric_value(router.metrics(), series).await.unwrap_or(0.0)
}

/// Wait until every upstream is online and its ranges are known.
async fn wait_ready(router: &RouterProcess, ups: &[&Up]) {
    wait_ready_within(router, ups, WAIT).await;
}

/// [`wait_ready`], all upstreams within `within` of the call.
async fn wait_ready_within(router: &RouterProcess, ups: &[&Up], within: Duration) {
    let deadline = Instant::now() + within;
    let left = || deadline.saturating_duration_since(Instant::now());
    for u in ups {
        wait_metric(
            router.metrics(),
            &format!("{} 1", u.series("upstream_up", "")),
            left(),
        )
        .await;
        wait_metric(
            router.metrics(),
            &format!("{} 1", u.series("upstream_ranges_known", "")),
            left(),
        )
        .await;
    }
}

async fn wait_down(router: &RouterProcess, u: &Up) {
    wait_metric(
        router.metrics(),
        &format!("{} 0", u.series("upstream_up", "")),
        WAIT,
    )
    .await;
}

/// Poll until `pred` holds, or panic after `within`.
async fn wait_for(what: &str, within: Duration, mut pred: impl FnMut() -> bool) {
    let deadline = Instant::now() + within;
    while !pred() {
        assert!(Instant::now() < deadline, "timed out waiting for {what}");
        sleep(Duration::from_millis(20)).await;
    }
}

// ----------------------------------------------------------------------
// Client
// ----------------------------------------------------------------------

/// Connect a client and read the ABI text.
async fn client(router: &RouterProcess) -> (WsWriter, WsReader, String) {
    let (writer, mut reader) = ws_connect(&router.ws_url()).await;
    match read_msg(&mut reader, WAIT).await {
        Message::Text(abi) => (writer, reader, abi.to_string()),
        other => panic!("expected the ABI, got {other:?}"),
    }
}

async fn send(writer: &mut WsWriter, bytes: Vec<u8>) {
    writer.send(Message::Binary(bytes.into())).await.unwrap();
}

fn request(start: u32, end: u32, max_in_flight: u32) -> BlocksReq {
    BlocksReq {
        start,
        end,
        max_in_flight,
        ..BlocksReq::default()
    }
}

fn nums(blocks: &[BlockView]) -> Vec<u32> {
    blocks.iter().filter_map(BlockView::num).collect()
}

/// Read status results until one arrives (blocks are not expected).
async fn read_status(r: &mut WsReader) -> (StatusView, Vec<u8>) {
    match read_msg(r, WAIT).await {
        Message::Binary(frame) => {
            let status = StatusView::parse(&frame)
                .unwrap_or_else(|| panic!("not a status result: {:02x?}", &frame[..]));
            (status, frame.to_vec())
        }
        other => panic!("expected a status result, got {other:?}"),
    }
}

async fn status(w: &mut WsWriter, r: &mut WsReader, v1: bool) -> StatusView {
    send(w, if v1 { status_v1() } else { status_v0() }).await;
    read_status(r).await.0
}

/// Expect a Close frame within `within`; return it and how long it took.
async fn expect_close(r: &mut WsReader, within: Duration) -> (CloseFrame, Duration) {
    let started = Instant::now();
    let (got, close) = read_blocks(r, 1, within).await;
    assert!(got.is_empty(), "blocks before the close: {:?}", nums(&got));
    let close = close.unwrap_or_else(|| panic!("no close frame within {within:?}"));
    (close, started.elapsed())
}

/// What a client streaming with acks did.
#[derive(Default)]
struct Stream {
    blocks: Vec<BlockView>,
    acks: u32,
}

/// Read `n` results, acking `every` at a time once that many arrived since
/// the last ack. Asserts that the client never holds more than `window`
/// unacked results.
async fn stream_acking(
    w: &mut WsWriter,
    r: &mut WsReader,
    s: &mut Stream,
    n: usize,
    window: u32,
    every: u32,
) {
    let target = s.blocks.len() + n;
    while s.blocks.len() < target {
        let (got, close) = read_blocks(r, 1, WAIT).await;
        assert!(
            close.is_none(),
            "closed after {:?}: {close:?}",
            s.blocks.last().map(|b| b.num())
        );
        assert_eq!(
            got.len(),
            1,
            "stalled after {:?}",
            s.blocks.last().map(|b| b.num())
        );
        s.blocks.extend(got);
        let unacked = s.blocks.len() as u32 - s.acks;
        assert!(unacked <= window, "{unacked} unacked results");
        if s.blocks.len() as u32 - s.acks >= every {
            send(w, ack(every)).await;
            s.acks += every;
        }
    }
}

// ----------------------------------------------------------------------
// Mock request logs
// ----------------------------------------------------------------------

fn session_of(o: &Observed) -> u32 {
    match o {
        Observed::Accepted { session }
        | Observed::Status { session, .. }
        | Observed::Blocks { session, .. }
        | Observed::Ack { session, .. }
        | Observed::Unknown { session, .. } => *session,
    }
}

/// One get_blocks a mock received and the acks that followed it on the same
/// session, before its next request.
#[derive(Clone, Debug, PartialEq, Eq)]
struct Leg {
    session: u32,
    start: u32,
    end: u32,
    max_in_flight: u32,
    positions: Vec<(u32, [u8; 32])>,
    irreversible_only: bool,
    acks: u32,
}

fn legs(mock: &MockShipHandle) -> Vec<Leg> {
    let mut out: Vec<Leg> = Vec::new();
    for o in mock.requests() {
        match o {
            Observed::Blocks {
                session,
                start,
                end,
                max_in_flight,
                have_positions,
                irreversible_only,
                ..
            } => out.push(Leg {
                session,
                start,
                end,
                max_in_flight,
                positions: have_positions,
                irreversible_only,
                acks: 0,
            }),
            Observed::Ack { session, n } => {
                if let Some(leg) = out.iter_mut().rev().find(|l| l.session == session) {
                    leg.acks = leg.acks.wrapping_add(n);
                }
            }
            _ => {}
        }
    }
    out
}

/// Sessions a mock accepted that never asked for status: client links (the
/// router answers client status itself; only its monitor asks).
fn link_sessions(mock: &MockShipHandle) -> usize {
    let log = mock.requests();
    log.iter()
        .filter_map(|o| match o {
            Observed::Accepted { session } => Some(*session),
            _ => None,
        })
        .filter(|s| {
            !log.iter()
                .any(|o| matches!(o, Observed::Status { .. }) && session_of(o) == *s)
        })
        .count()
}

/// The credit ledger across a run of legs (in order): each leg's
/// `max_in_flight` is the client's outstanding credit when it was sent, so
/// it equals the previous leg's credit plus the acks it got minus the frames
/// it sent, plus acks folded while no link had a request. All acks are
/// accounted for, and the last leg ends with the client's own credit.
fn check_ledger(legs: &[(Leg, u32)], window: u32, acks: u32, received: u32) {
    let first = &legs[0].0;
    assert_eq!(first.max_in_flight, window, "the first leg gets the window");
    let mut folded = 0u32;
    for pair in legs.windows(2) {
        let ((prev, frames), (next, _)) = (&pair[0], &pair[1]);
        let left = prev.max_in_flight + prev.acks - frames;
        assert!(
            next.max_in_flight >= left,
            "leg {next:?} got {} but {left} was outstanding after {prev:?}",
            next.max_in_flight
        );
        folded += next.max_in_flight - left;
    }
    let forwarded: u32 = legs.iter().map(|(l, _)| l.acks).sum();
    assert_eq!(forwarded + folded, acks, "acks forwarded or folded");
    let (last, frames) = legs.last().unwrap();
    assert_eq!(
        last.max_in_flight + last.acks - frames,
        window + acks - received,
        "the last leg ends with the client's credit"
    );
}

// ----------------------------------------------------------------------
// S1, S19: one request across the whole fleet
// ----------------------------------------------------------------------

/// Run `(2, end, W=50)` with acks of 25 over frozen `segments` (each its own
/// mock) and, when `live` is set, live C. Reads `n` results and checks the
/// sequence, the seams, the legs, the ledger and head/LIB in every frame.
async fn s1_run(segments: &[(u32, u32)], live: bool, end: u32, n: usize, handoffs: f64) {
    let mut ups = Vec::new();
    for (i, range) in segments.iter().enumerate() {
        ups.push(up(&format!("f{i}"), frozen_mock(*range)).await);
    }
    let c = up("c", live_mock(C_BEGIN, C_HEAD)).await;
    let _ticker = tick(&c);
    let mut servers: Vec<Value> = ups.iter().map(frozen_server).collect();
    if live {
        servers.push(live_server(&c));
    }
    let router = RouterProcess::start(config(servers)).await;
    let mut all: Vec<&Up> = ups.iter().collect();
    if live {
        all.push(&c);
    }
    wait_ready(&router, &all).await;
    let (mut w, mut r, _) = client(&router).await;

    const WINDOW: u32 = 50;
    send(&mut w, blocks(&request(2, end, WINDOW))).await;
    let mut s = Stream::default();
    stream_acking(&mut w, &mut r, &mut s, n, WINDOW, 25).await;
    sleep(Duration::from_millis(300)).await;

    let seq = nums(&s.blocks);
    assert_eq!(
        seq,
        (2..2 + n as u32).collect::<Vec<_>>(),
        "gap or duplicate"
    );
    for pair in s.blocks.windows(2) {
        let (prev, next) = (&pair[0], &pair[1]);
        assert_eq!(
            next.prev,
            prev.this,
            "prev_block chains at {:?}",
            next.num()
        );
    }
    // S19: archive frames carry the live tip; head and LIB never go back.
    let mut last: Option<(u32, u32)> = None;
    for b in &s.blocks {
        assert!(b.lib.0 <= b.head.0, "lib above head at {:?}", b.num());
        assert_eq!(
            b.head.1,
            fake_block_id(b.head.0),
            "head id at {:?}",
            b.num()
        );
        assert_eq!(b.lib.1, fake_block_id(b.lib.0), "lib id at {:?}", b.num());
        if live && b.num().is_some_and(|n| n < 2001) {
            assert!(
                b.head.0 >= C_HEAD,
                "archive frame {:?} kept head {}",
                b.num(),
                b.head.0
            );
        }
        if let Some((head, lib)) = last {
            assert!(
                b.head.0 >= head && b.lib.0 >= lib,
                "head/LIB went back at {:?}",
                b.num()
            );
        }
        last = Some((b.head.0, b.lib.0));
    }

    // Each upstream got one request for its run, in order.
    let mut run: Vec<(Leg, u32)> = Vec::new();
    let mut expected_start = 2;
    for (u, range) in ups.iter().zip(segments) {
        let got = legs(&u.mock);
        if expected_start >= end.min(2 + n as u32) {
            assert!(got.is_empty(), "{} was asked {got:?}", u.name);
            continue;
        }
        assert_eq!(got.len(), 1, "{}: {got:?}", u.name);
        let leg_end = range.1.min(end);
        assert_eq!(
            (got[0].start, got[0].end),
            (expected_start, leg_end),
            "{}",
            u.name
        );
        assert!(
            got[0].positions.is_empty(),
            "frozen legs carry no positions"
        );
        run.push((got[0].clone(), leg_end - expected_start));
        expected_start = leg_end;
    }
    let got = legs(&c.mock);
    if live && expected_start < end {
        assert_eq!(got.len(), 1, "c: {got:?}");
        assert_eq!((got[0].start, got[0].end), (expected_start, end));
        run.push((got[0].clone(), 2 + n as u32 - expected_start));
    } else {
        assert!(got.is_empty(), "c was asked {got:?}");
    }
    check_ledger(&run, WINDOW, s.acks, s.blocks.len() as u32);

    assert_eq!(
        metric(&router, "fleet_router_handoffs_total").await,
        handoffs
    );
    assert_eq!(
        metric(&router, "fleet_router_credit_overruns_total").await,
        0.0
    );
    for u in all {
        assert_eq!(
            metric(&router, &u.series("upstream_block_failures_total", "")).await,
            0.0,
            "{} was struck",
            u.name
        );
    }
}

/// S1 + S19: `(2, u32::MAX)` runs A, B, then C, with exact credit at each
/// seam and the live tip in every archive frame.
#[tokio::test]
async fn s1_one_request_runs_across_archives_and_the_live_tier() {
    s1_run(&[A_RANGE, B_RANGE], true, u32::MAX, 2200, 2.0).await;
}

/// S1 with five archive segments.
#[tokio::test]
async fn s1_five_archive_segments_hand_off_in_order() {
    let segments = [(2, 201), (201, 401), (401, 601), (601, 801), (801, 1001)];
    let mut all = segments.to_vec();
    all.push(B_RANGE);
    s1_run(&all, true, u32::MAX, 2050, 6.0).await;
}

/// S1 with the client's end inside B: no live leg.
#[tokio::test]
async fn s1_a_client_end_inside_an_archive_ends_the_run_there() {
    s1_run(&[A_RANGE, B_RANGE], true, 1500, 1498, 1.0).await;
}

// ----------------------------------------------------------------------
// S2: status answered by the router
// ----------------------------------------------------------------------

/// S2: v0 and v1 status come from the fleet view: C's head and LIB, the
/// coverage of each log, the pinned chain_id, finality in v1 only, and a
/// head that never goes back.
#[tokio::test]
async fn s2_status_is_the_fleet_view() {
    // A's chain state starts above its traces, so the logs differ.
    let a = up(
        "a",
        MockShipConfig {
            chain_state_begin_block: Some(500),
            ..frozen_mock(A_RANGE)
        },
    )
    .await;
    let b = up("b", frozen_mock(B_RANGE)).await;
    let c = up(
        "c",
        MockShipConfig {
            finality_data_begin_block: 2100,
            ..live_mock(C_BEGIN, C_HEAD)
        },
    )
    .await;
    let router = RouterProcess::start(fleet_config(&a, &b, &c)).await;
    wait_ready(&router, &[&a, &b, &c]).await;
    let (mut w, mut r, _) = client(&router).await;

    send(&mut w, status_v0()).await;
    let (v0, frame) = read_status(&mut r).await;
    assert_eq!((frame.len(), frame[0]), (121, 0));
    assert_eq!(v0.head, (C_HEAD, fake_block_id(C_HEAD)));
    assert_eq!(v0.lib, (C_HEAD - 10, fake_block_id(C_HEAD - 10)));
    assert_eq!(v0.trace, (2, C_HEAD + 1));
    assert_eq!(v0.chain_state, (500, C_HEAD + 1));
    assert_eq!(v0.chain_id, Some(CHAIN_ID));
    assert_eq!(v0.finality, None);

    send(&mut w, status_v1()).await;
    let (v1, frame) = read_status(&mut r).await;
    assert_eq!((frame.len(), frame[0]), (129, 3));
    assert_eq!(v1.head.0, C_HEAD);
    assert_eq!(v1.trace, (2, C_HEAD + 1));
    assert_eq!(v1.finality, Some((2100, C_HEAD + 1)));
    assert_eq!(v1.chain_id, Some(CHAIN_ID));

    // The router follows the live head.
    c.mock.set_head(2400, 2395);
    let deadline = Instant::now() + WAIT;
    loop {
        let s = status(&mut w, &mut r, false).await;
        if s.head.0 == 2400 {
            assert_eq!(s.lib.0, 2395);
            break;
        }
        assert!(
            Instant::now() < deadline,
            "status never showed head 2400: {s:?}"
        );
        sleep(Duration::from_millis(50)).await;
    }

    let _ticker = tick(&c);
    let mut last = (0, 0);
    for i in 0..20 {
        let s = status(&mut w, &mut r, i % 2 == 1).await;
        assert!(s.lib.0 <= s.head.0);
        assert!(
            s.head.0 >= last.0 && s.lib.0 >= last.1,
            "{s:?} after {last:?}"
        );
        last = (s.head.0, s.lib.0);
        sleep(Duration::from_millis(40)).await;
    }
    assert!(last.0 > 2400, "the head moved while ticking: {last:?}");
    assert_eq!(
        link_sessions(&a.mock) + link_sessions(&b.mock) + link_sessions(&c.mock),
        0
    );
}

// ----------------------------------------------------------------------
// S3: a small window across a seam
// ----------------------------------------------------------------------

/// S3: W=10 acked one at a time across the A to B seam: never more than 10
/// unacked, and the request to B carries the outstanding credit.
#[tokio::test]
async fn s3_a_small_window_keeps_its_credit_across_a_seam() {
    let (a, b, c) = default_fleet().await;
    let router = RouterProcess::start(fleet_config(&a, &b, &c)).await;
    wait_ready(&router, &[&a, &b, &c]).await;
    let (mut w, mut r, _) = client(&router).await;

    send(&mut w, blocks(&request(950, 1050, 10))).await;
    let mut s = Stream::default();
    stream_acking(&mut w, &mut r, &mut s, 100, 10, 1).await;
    sleep(Duration::from_millis(300)).await;
    assert_eq!(nums(&s.blocks), (950..1050).collect::<Vec<_>>());

    let (la, lb) = (legs(&a.mock), legs(&b.mock));
    assert_eq!(la.len(), 1, "{la:?}");
    assert_eq!(lb.len(), 1, "{lb:?}");
    assert_eq!((lb[0].start, lb[0].end), (1001, 1050));
    assert!(lb[0].acks > 0, "later acks reach B");
    check_ledger(
        &[(la[0].clone(), 51), (lb[0].clone(), 49)],
        10,
        s.acks,
        s.blocks.len() as u32,
    );
}

// ----------------------------------------------------------------------
// S4: failures around seams
// ----------------------------------------------------------------------

/// S4a: A drops TCP on block 1000; its replica resumes at 1000 with no
/// duplicate, then B takes over.
#[tokio::test]
async fn s4a_a_drop_at_the_last_archive_block_resumes_on_the_replica() {
    let a = up(
        "a",
        MockShipConfig {
            corrupt_block: Some(1000),
            ..frozen_mock(A_RANGE)
        },
    )
    .await;
    let a2 = bound("a2", frozen_mock(A_RANGE)).await;
    let b = up("b", frozen_mock(B_RANGE)).await;
    let c = up("c", live_mock(C_BEGIN, C_HEAD)).await;
    let a2_server = frozen(&a2.1, &a2.0, None);
    let router = RouterProcess::start(config(vec![
        frozen_server(&a),
        a2_server,
        frozen_server(&b),
        live_server(&c),
    ]))
    .await;
    wait_ready(&router, &[&a, &b, &c]).await;
    let (mut w, mut r, _) = client(&router).await;

    // Only A is up: it serves the start. The replica comes up meanwhile.
    send(&mut w, blocks(&request(990, 1010, 5))).await;
    let (first, _) = read_blocks(&mut r, 5, WAIT).await;
    let a2 = start(a2);
    wait_ready(&router, &[&a2]).await;
    send(&mut w, ack(100)).await;
    let (rest, close) = read_blocks(&mut r, 15, WAIT).await;
    assert!(close.is_none(), "{close:?}");
    let mut seq = nums(&first);
    seq.extend(nums(&rest));
    assert_eq!(seq, (990..1010).collect::<Vec<_>>(), "gap or duplicate");

    let resumed = legs(&a2.mock);
    assert_eq!(resumed.len(), 1, "{resumed:?}");
    assert_eq!((resumed[0].start, resumed[0].end), (1000, 1001));
    assert_eq!(legs(&b.mock)[0].start, 1001);
    assert_eq!(
        metric(&router, &a.series("upstream_block_failures_total", "")).await,
        1.0,
        "the drop strikes A"
    );
    assert_eq!(
        metric(&router, "fleet_router_failovers_total{reason=\"unclean\"}").await,
        1.0
    );
}

/// S4b: B stops answering just before the handoff; its replica serves 1001
/// with no gap, and nobody is struck.
#[tokio::test]
async fn s4b_a_stalled_next_archive_is_replaced_by_its_replica() {
    let (a, b, c) = default_fleet().await;
    let b2 = up("b2", frozen_mock(B_RANGE)).await;
    let router = RouterProcess::start(config(vec![
        frozen_server(&a),
        frozen_server(&b),
        frozen_server(&b2),
        live_server(&c),
    ]))
    .await;
    wait_ready(&router, &[&a, &b, &b2, &c]).await;
    let (mut w, mut r, _) = client(&router).await;

    send(&mut w, blocks(&request(990, 1020, 5))).await;
    let (first, _) = read_blocks(&mut r, 5, WAIT).await;
    b.mock.stall_and_drop_sessions();
    wait_down(&router, &b).await;
    send(&mut w, ack(100)).await;
    let (rest, close) = read_blocks(&mut r, 25, WAIT).await;
    assert!(close.is_none(), "{close:?}");
    let mut seq = nums(&first);
    seq.extend(nums(&rest));
    assert_eq!(seq, (990..1020).collect::<Vec<_>>(), "gap or duplicate");
    assert_eq!(legs(&b2.mock).first().map(|l| l.start), Some(1001));
    for u in [&a, &b, &b2, &c] {
        assert_eq!(
            metric(&router, &u.series("upstream_block_failures_total", "")).await,
            0.0,
            "{} was struck",
            u.name
        );
    }
}

/// S4c: with no replica, a stalled B holds the session at 1001 for
/// `gap_wait_ms`, then the client is closed with 1011.
#[tokio::test]
async fn s4c_an_unavailable_archive_closes_the_client_after_the_gap_wait() {
    let (a, b, c) = default_fleet().await;
    let mut cfg = fleet_config(&a, &b, &c);
    cfg["handshake_timeout_ms"] = json!(500);
    let router = RouterProcess::start(cfg).await;
    wait_ready(&router, &[&a, &b, &c]).await;
    let (mut w, mut r, _) = client(&router).await;

    send(&mut w, blocks(&request(990, 1020, 5))).await;
    let (first, _) = read_blocks(&mut r, 5, WAIT).await;
    assert_eq!(nums(&first), (990..995).collect::<Vec<_>>());
    b.mock.stall_and_drop_sessions();
    wait_down(&router, &b).await;
    send(&mut w, ack(100)).await;
    let (rest, close) = read_blocks(&mut r, 6, WAIT).await;
    assert_eq!(nums(&rest), (995..1001).collect::<Vec<_>>());
    let held = Instant::now();
    let close = match close {
        Some(close) => close,
        None => expect_close(&mut r, WAIT).await.0,
    };
    let waited = held.elapsed();
    assert_eq!(close.code, CloseCode::Error);
    assert_eq!(
        close.reason.as_str(),
        "block 1001 unavailable on all upstreams"
    );
    assert!(
        waited >= Duration::from_millis(800) && waited < Duration::from_secs(3),
        "closed after {waited:?}"
    );
    assert_eq!(
        metric(&router, "fleet_router_hold_closes_total{why=\"offline\"}").await,
        1.0
    );
    assert_eq!(
        metric(&router, &b.series("upstream_block_failures_total", "")).await,
        0.0
    );
}

/// S4d: twenty handoffs in a row are never failures.
#[tokio::test]
async fn s4d_twenty_handoffs_strike_nobody() {
    let mut ups = Vec::new();
    for i in 0..21u32 {
        ups.push(up(&format!("s{i}"), frozen_mock((2 + 50 * i, 52 + 50 * i))).await);
    }
    let router = RouterProcess::start(config(ups.iter().map(frozen_server).collect())).await;
    let all: Vec<&Up> = ups.iter().collect();
    wait_ready(&router, &all).await;
    let (mut w, mut r, _) = client(&router).await;

    send(&mut w, blocks(&request(2, 1052, 100))).await;
    let mut s = Stream::default();
    stream_acking(&mut w, &mut r, &mut s, 1050, 100, 50).await;
    assert_eq!(nums(&s.blocks), (2..1052).collect::<Vec<_>>());
    assert_eq!(metric(&router, "fleet_router_handoffs_total").await, 20.0);
    for u in all {
        assert_eq!(
            metric(&router, &u.series("upstream_block_failures_total", "")).await,
            0.0,
            "{} was struck",
            u.name
        );
        assert_eq!(legs(&u.mock).len(), 1, "{}", u.name);
    }
    for reason in ["unclean", "clean", "idle", "stall", "offline", "violation"] {
        let series = format!("fleet_router_failovers_total{{reason=\"{reason}\"}}");
        assert_eq!(metric(&router, &series).await, 0.0, "{series}");
    }
}

/// S4e: a client that leaves while the next leg's upstream is being dialed
/// leaves no connection behind.
#[tokio::test]
async fn s4e_a_client_leaving_during_a_handoff_releases_every_link() {
    let a = up("a", frozen_mock(A_RANGE)).await;
    // B's socket is slow: its 60 KB ABI takes about three seconds.
    let b = up(
        "b",
        MockShipConfig {
            throttle_bytes_per_sec: Some(20_000),
            ..frozen_mock(B_RANGE)
        },
    )
    .await;
    let c = up("c", live_mock(C_BEGIN, C_HEAD)).await;
    let mut cfg = fleet_config(&a, &b, &c);
    cfg["stitching"]["predial_blocks"] = json!(0);
    let router = RouterProcess::start(cfg).await;
    // B's ABI alone takes about three seconds.
    wait_ready_within(&router, &[&a, &b, &c], Duration::from_secs(8)).await;
    let (mut w, mut r, _) = client(&router).await;

    send(&mut w, blocks(&request(990, 1020, 100))).await;
    let (got, _) = read_blocks(&mut r, 11, WAIT).await;
    assert_eq!(nums(&got), (990..1001).collect::<Vec<_>>());
    // The handoff to B is dialing now.
    wait_metric(
        router.metrics(),
        "fleet_router_sessions{state=\"dialing\"} 1",
        WAIT,
    )
    .await;
    w.close().await.unwrap();
    drop(r);
    let deadline = Instant::now() + WAIT;
    for u in [&a, &b, &c] {
        let series = u.series("active_connections", "");
        while metric(&router, &series).await != 0.0 {
            assert!(Instant::now() < deadline, "{series} stayed up");
            sleep(Duration::from_millis(50)).await;
        }
    }
}

// ----------------------------------------------------------------------
// S5, S6: status-only clients and staleness
// ----------------------------------------------------------------------

/// S5: status-only clients open no upstream link, and a request at 1500 goes
/// to B alone.
#[tokio::test]
async fn s5_status_only_clients_touch_no_upstream() {
    let (a, b, c) = default_fleet().await;
    let router = RouterProcess::start(fleet_config(&a, &b, &c)).await;
    wait_ready(&router, &[&a, &b, &c]).await;
    for _ in 0..50 {
        let (mut w, mut r, _) = client(&router).await;
        let s = status(&mut w, &mut r, false).await;
        assert_eq!(s.trace.0, 2);
        w.close().await.unwrap();
    }
    for u in [&a, &b, &c] {
        assert_eq!(link_sessions(&u.mock), 0, "{} got a link", u.name);
    }

    let (mut w, mut r, _) = client(&router).await;
    send(&mut w, blocks(&request(1500, 1510, 10))).await;
    let (got, _) = read_blocks(&mut r, 10, WAIT).await;
    assert_eq!(nums(&got), (1500..1510).collect::<Vec<_>>());
    assert!(legs(&a.mock).is_empty() && legs(&c.mock).is_empty());
    assert_eq!(legs(&b.mock).len(), 1);
    assert_eq!(link_sessions(&a.mock) + link_sessions(&c.mock), 0);
}

/// S6: frozen upstreams are never stale; a live node whose head does not
/// move is.
#[tokio::test]
async fn s6_frozen_upstreams_are_never_stale() {
    let (a, b, c) = default_fleet().await;
    let mut cfg = fleet_config(&a, &b, &c);
    cfg["upstream_monitoring_ms"] = json!(200);
    cfg["upstream_status_ms"] = json!(200);
    let router = RouterProcess::start(cfg).await;
    wait_ready(&router, &[&a, &b, &c]).await;
    // Twelve monitor intervals without a new head, plus timer slack.
    wait_metric(
        router.metrics(),
        &format!("{} 1", c.series("upstream_stale", "")),
        WAIT * 2,
    )
    .await;
    assert_eq!(metric(&router, &a.series("upstream_stale", "")).await, 0.0);
    assert_eq!(metric(&router, &b.series("upstream_stale", "")).await, 0.0);
}

// ----------------------------------------------------------------------
// S7: irreversible_only waits for LIB on the same socket
// ----------------------------------------------------------------------

/// S7: with `irreversible_only`, B is asked only up to LIB + 1, the session
/// waits there, and when LIB moves the same socket to B gets the next leg.
#[tokio::test]
async fn s7_irreversible_only_waits_for_lib_on_the_same_socket() {
    let a = up("a", frozen_mock(A_RANGE)).await;
    let b = up("b", frozen_mock(B_RANGE)).await;
    let c = up(
        "c",
        MockShipConfig {
            lib_block: 1500,
            ..live_mock(C_BEGIN, C_HEAD)
        },
    )
    .await;
    let router = RouterProcess::start(fleet_config(&a, &b, &c)).await;
    wait_ready(&router, &[&a, &b, &c]).await;
    let (mut w, mut r, _) = client(&router).await;

    send(
        &mut w,
        blocks(&BlocksReq {
            irreversible_only: true,
            ..request(1001, u32::MAX, 1000)
        }),
    )
    .await;
    let (got, _) = read_blocks(&mut r, 500, WAIT).await;
    assert_eq!(nums(&got), (1001..1501).collect::<Vec<_>>());
    let (more, _) = read_blocks(&mut r, 1, Duration::from_millis(500)).await;
    assert!(more.is_empty(), "a block above LIB: {:?}", nums(&more));
    wait_metric(
        router.metrics(),
        "fleet_router_sessions{state=\"waiting\"} 1",
        WAIT,
    )
    .await;

    c.mock.set_head(2300, 1800);
    let (got2, _) = read_blocks(&mut r, 300, WAIT).await;
    assert_eq!(nums(&got2), (1501..1801).collect::<Vec<_>>());
    for b in got.iter().chain(&got2) {
        assert!(
            b.num().unwrap() <= b.lib.0,
            "block {:?} above LIB {}",
            b.num(),
            b.lib.0
        );
    }
    let lb = legs(&b.mock);
    assert_eq!(lb.len(), 2, "{lb:?}");
    assert_eq!((lb[0].start, lb[0].end), (1001, 1501));
    assert_eq!((lb[1].start, lb[1].end), (1501, 1801));
    assert!(lb.iter().all(|l| l.irreversible_only));
    assert_eq!(lb[0].session, lb[1].session, "the same socket");
    assert!(legs(&c.mock).is_empty());
}

/// S7 with B at `max_sessions: 1`: the session's own link fills B, which
/// neither turns the wait at LIB into a capacity hold nor keeps the socket
/// from being reused when LIB moves.
#[tokio::test]
async fn s7_a_single_slot_archive_waits_for_lib_on_its_one_socket() {
    let a = up("a", frozen_mock(A_RANGE)).await;
    let b = up("b", frozen_mock(B_RANGE)).await;
    let c = up(
        "c",
        MockShipConfig {
            lib_block: 1500,
            ..live_mock(C_BEGIN, C_HEAD)
        },
    )
    .await;
    let cfg = config(vec![frozen_server(&a), single_slot(&b), live_server(&c)]);
    let router = RouterProcess::start(cfg).await;
    wait_ready(&router, &[&a, &b, &c]).await;
    let (mut w, mut r, _) = client(&router).await;

    send(
        &mut w,
        blocks(&BlocksReq {
            irreversible_only: true,
            ..request(1001, u32::MAX, 1000)
        }),
    )
    .await;
    let (got, _) = read_blocks(&mut r, 500, WAIT).await;
    assert_eq!(nums(&got), (1001..1501).collect::<Vec<_>>());
    wait_metric(
        router.metrics(),
        "fleet_router_sessions{state=\"waiting\"} 1",
        WAIT,
    )
    .await;
    // Longer than gap_wait_ms: a capacity hold would have closed by now.
    let (more, close) = read_blocks(&mut r, 1, Duration::from_millis(1500)).await;
    assert!(close.is_none(), "{close:?}");
    assert!(more.is_empty(), "a block above LIB: {:?}", nums(&more));

    c.mock.set_head(2300, 1800);
    let (got2, close) = read_blocks(&mut r, 300, WAIT).await;
    assert!(close.is_none(), "{close:?}");
    assert_eq!(nums(&got2), (1501..1801).collect::<Vec<_>>());
    let lb = legs(&b.mock);
    assert_eq!(lb.len(), 2, "{lb:?}");
    assert_eq!(lb[0].session, lb[1].session, "the same socket");
    assert_eq!(
        metric(&router, "fleet_router_holds_total{why=\"at_capacity\"}").await,
        0.0
    );
}

// ----------------------------------------------------------------------
// S8: positions
// ----------------------------------------------------------------------

/// S8: a live leg gets only the client's positions that upstream can know,
/// archive legs get none, and a failover to a forked replica re-delivers
/// the forked blocks (no fork block is dropped as a duplicate).
#[tokio::test]
async fn s8_positions_follow_the_upstream_and_forks_are_redelivered() {
    let (a, b, c) = default_fleet().await;
    // C2 is on a fork from 2295 and comes up later.
    let c2 = bound(
        "c2",
        MockShipConfig {
            fork: Some((2295, 7)),
            ..live_mock(C_BEGIN, C_HEAD)
        },
    )
    .await;
    let c2_server = server(&c2.1, &c2.0);
    let router = RouterProcess::start(config(vec![
        frozen_server(&a),
        frozen_server(&b),
        live_server(&c),
        c2_server,
    ]))
    .await;
    wait_ready(&router, &[&a, &b, &c]).await;

    // Archive legs never carry positions.
    let (mut w, mut r, _) = client(&router).await;
    send(
        &mut w,
        blocks(&BlocksReq {
            have_positions: vec![(850, fake_block_id(850))],
            ..request(900, 1100, 300)
        }),
    )
    .await;
    let (got, _) = read_blocks(&mut r, 200, WAIT).await;
    assert_eq!(nums(&got), (900..1100).collect::<Vec<_>>());
    assert!(legs(&a.mock).iter().all(|l| l.positions.is_empty()));
    assert!(legs(&b.mock).iter().all(|l| l.positions.is_empty()));
    drop((w, r));

    // A live leg gets the positions at or above its own first block.
    let (mut w, mut r, _) = client(&router).await;
    send(
        &mut w,
        blocks(&BlocksReq {
            have_positions: vec![(1500, fake_block_id(1500)), (2280, fake_block_id(2280))],
            ..request(2290, u32::MAX, 100)
        }),
    )
    .await;
    let (got, _) = read_blocks(&mut r, 11, WAIT).await;
    assert_eq!(nums(&got), (2290..=2300).collect::<Vec<_>>());
    let lc = legs(&c.mock);
    assert_eq!(lc.len(), 1, "{lc:?}");
    assert_eq!(
        lc[0].positions.iter().map(|p| p.0).collect::<Vec<_>>(),
        [2280],
        "positions below C's first block are dropped"
    );

    // Fail over to the forked replica.
    let c2 = start(c2);
    wait_ready(&router, &[&c2]).await;
    c.mock.drop_sessions();
    let (forked, close) = read_blocks(&mut r, 6, WAIT).await;
    assert!(close.is_none(), "{close:?}");
    assert_eq!(nums(&forked), (2295..=2300).collect::<Vec<_>>());
    for blk in &forked {
        let n = blk.num().unwrap();
        assert_eq!(blk.this.unwrap().1, mock_ship::fake_block_id_seeded(n, 7));
    }
    let l2 = legs(&c2.mock);
    assert_eq!(l2.len(), 1, "{l2:?}");
    assert_eq!(l2[0].start, 2301);
    assert_eq!(
        l2[0].positions.iter().map(|p| p.0).collect::<Vec<_>>(),
        (2291..=2300).collect::<Vec<_>>(),
        "the resume carries the reversible tail"
    );
    assert_eq!(
        metric(&router, "fleet_router_duplicates_dropped_total").await,
        0.0
    );
    assert!(metric(&router, "fleet_router_fork_redeliveries_total").await >= 1.0);
}

// ----------------------------------------------------------------------
// S9: finality
// ----------------------------------------------------------------------

/// S9: a v1 finality request runs on the archive until finality starts in
/// the fleet, then on the live node; every frame follows the request
/// version.
#[tokio::test]
async fn s9_finality_moves_the_seam_to_where_finality_starts() {
    let a = up("a", frozen_mock(A_RANGE)).await;
    let c = up(
        "c",
        MockShipConfig {
            finality_data_begin_block: 950,
            ..live_mock(900, C_HEAD)
        },
    )
    .await;
    let router = RouterProcess::start(config(vec![frozen_server(&a), live_server(&c)])).await;
    wait_ready(&router, &[&a, &c]).await;
    let (mut w, mut r, _) = client(&router).await;

    send(
        &mut w,
        blocks(&BlocksReq {
            v1: true,
            fetch_finality_data: true,
            ..request(2, 1100, 2000)
        }),
    )
    .await;
    let (got, close) = read_blocks(&mut r, 1098, WAIT).await;
    assert!(close.is_none(), "{close:?}");
    assert_eq!(nums(&got), (2..1100).collect::<Vec<_>>());
    for blk in &got {
        let n = blk.num().unwrap();
        assert_eq!(blk.variant, 2, "block {n}");
        assert_eq!(blk.finality.is_some(), n >= 950, "finality at {n}");
    }
    let la = legs(&a.mock);
    assert_eq!((la[0].start, la[0].end), (2, 950));
    let lc = legs(&c.mock);
    assert_eq!((lc[0].start, lc[0].end), (950, 1100));
    assert_eq!(metric(&router, "fleet_router_handoffs_total").await, 1.0);
    for kind in [
        "idless",
        "gap",
        "rewind",
        "missing_field",
        "credit",
        "variant",
        "discontinuity",
    ] {
        let series = a.series(
            "upstream_contract_violations_total",
            &format!("kind=\"{kind}\""),
        );
        assert_eq!(metric(&router, &series).await, 0.0, "{series}");
    }

    // A v0 client gets v0 results.
    let (mut w, mut r, _) = client(&router).await;
    send(&mut w, blocks(&request(2, 12, 10))).await;
    let (got, _) = read_blocks(&mut r, 10, WAIT).await;
    assert!(got.iter().all(|b| b.variant == 1));
}

// ----------------------------------------------------------------------
// S10: the canonical ABI without a dial
// ----------------------------------------------------------------------

/// S10: clients get the live node's ABI text, byte for byte, from the
/// router's cache: no upstream link is opened for them.
#[tokio::test]
async fn s10_the_abi_comes_from_the_cache_without_a_dial() {
    let a = up("a", frozen_mock(A_RANGE)).await;
    let b = up("b", frozen_mock(B_RANGE)).await;
    // The same ABI, laid out differently: the text identifies its source.
    let value: Value = serde_json::from_str(mock_ship::SHIP_ABI_JSON).unwrap();
    let live_abi: Arc<str> = Arc::from(serde_json::to_string_pretty(&value).unwrap());
    assert_ne!(&*live_abi, mock_ship::SHIP_ABI_JSON);
    let c = up(
        "c",
        MockShipConfig {
            abi_text: Some(live_abi.clone()),
            ..live_mock(C_BEGIN, C_HEAD)
        },
    )
    .await;
    let router = RouterProcess::start(fleet_config(&a, &b, &c)).await;
    wait_ready(&router, &[&a, &b, &c]).await;
    for _ in 0..50 {
        let (mut w, mut r, abi) = client(&router).await;
        assert_eq!(abi, &*live_abi, "the canonical ABI is the live node's");
        status(&mut w, &mut r, false).await;
        w.close().await.unwrap();
    }
    for u in [&a, &b, &c] {
        assert_eq!(link_sessions(&u.mock), 0, "{} got a link", u.name);
        assert_eq!(
            metric(&router, &u.series("upstream_abi_match", "")).await,
            1.0,
            "{}",
            u.name
        );
    }
}

// ----------------------------------------------------------------------
// S11, S12: archives that are down
// ----------------------------------------------------------------------

/// S11, first run: an archive that stops answering keeps its range in
/// status, and a request in its range holds, then closes with 1011.
#[tokio::test]
async fn s11_a_lost_archive_keeps_its_range_and_holds_requests() {
    let (a, b, c) = default_fleet().await;
    let mut cfg = fleet_config(&a, &b, &c);
    cfg["handshake_timeout_ms"] = json!(500);
    let router = RouterProcess::start(cfg).await;
    wait_ready(&router, &[&a, &b, &c]).await;
    a.mock.stall_and_drop_sessions();
    wait_down(&router, &a).await;
    let (mut w, mut r, _) = client(&router).await;
    assert_eq!(status(&mut w, &mut r, false).await.trace.0, 2, "sticky");

    send(&mut w, blocks(&request(500, 600, 100))).await;
    let (close, waited) = expect_close(&mut r, WAIT).await;
    assert_eq!(close.code, CloseCode::Error);
    assert_eq!(
        close.reason.as_str(),
        "block 500 unavailable on all upstreams"
    );
    assert!(
        waited >= Duration::from_millis(800) && waited < Duration::from_secs(3),
        "closed after {waited:?}"
    );
    assert_eq!(
        metric(&router, "fleet_router_holds_total{why=\"offline\"}").await,
        1.0
    );
}

/// S11, second run: the held request goes on when the archive returns
/// within `gap_wait_ms`.
#[tokio::test]
async fn s11_a_held_request_goes_on_when_the_archive_returns() {
    let (a, b, c) = default_fleet().await;
    let mut cfg = fleet_config(&a, &b, &c);
    cfg["handshake_timeout_ms"] = json!(500);
    cfg["stitching"]["gap_wait_ms"] = json!(6000);
    let router = RouterProcess::start(cfg).await;
    wait_ready(&router, &[&a, &b, &c]).await;
    let (ep, name) = (a.ep.clone(), a.name.clone());
    a.mock.stall_and_drop_sessions();
    wait_down(&router, &a).await;
    let (mut w, mut r, _) = client(&router).await;

    send(&mut w, blocks(&request(500, 600, 100))).await;
    wait_metric(
        router.metrics(),
        "fleet_router_sessions{state=\"holding\"} 1",
        WAIT,
    )
    .await;
    let held = Instant::now();
    stop(a).await;
    let a = restart(&ep, &name, frozen_mock(A_RANGE)).await;
    let (got, close) = read_blocks(&mut r, 100, Duration::from_secs(8)).await;
    assert!(close.is_none(), "{close:?}");
    assert_eq!(nums(&got), (500..600).collect::<Vec<_>>());
    assert!(held.elapsed() < Duration::from_secs(6), "within the wait");
    assert_eq!(legs(&a.mock).first().map(|l| l.start), Some(500));
}

/// S12a: an archive that never reported and has no declared range holds
/// status until `handshake_timeout_ms`, then the client gets 1011.
#[tokio::test]
async fn s12a_status_waits_for_an_unreported_archive() {
    let (dead, _held) = dead_endpoint();
    let b = up("b", frozen_mock(B_RANGE)).await;
    let c = up("c", live_mock(C_BEGIN, C_HEAD)).await;
    let mut cfg = config(vec![
        frozen("a", &dead, None),
        frozen_server(&b),
        live_server(&c),
    ]);
    cfg["handshake_timeout_ms"] = json!(2000);
    let router = RouterProcess::start(cfg).await;
    wait_ready(&router, &[&b, &c]).await;
    assert_eq!(
        metric(&router, "fleet_router_fleet_frozen_unreported").await,
        1.0
    );
    let (mut w, mut r, _) = client(&router).await;

    send(&mut w, status_v0()).await;
    let (close, waited) = expect_close(&mut r, WAIT).await;
    assert_eq!(close.code, CloseCode::Error);
    assert_eq!(close.reason.as_str(), "fleet status unavailable");
    assert!(
        waited >= Duration::from_millis(1800),
        "closed after {waited:?}"
    );
}

/// S12b: with a declared range, the same archive never holds status.
#[tokio::test]
async fn s12b_a_declared_range_answers_status_at_once() {
    let (dead, _held) = dead_endpoint();
    let b = up("b", frozen_mock(B_RANGE)).await;
    let c = up("c", live_mock(C_BEGIN, C_HEAD)).await;
    let mut cfg = config(vec![
        frozen("a", &dead, Some(A_RANGE)),
        frozen_server(&b),
        live_server(&c),
    ]);
    cfg["handshake_timeout_ms"] = json!(2000);
    let router = RouterProcess::start(cfg).await;
    wait_ready(&router, &[&b, &c]).await;
    let (mut w, mut r, _) = client(&router).await;
    let asked = Instant::now();
    let s = status(&mut w, &mut r, true).await;
    assert!(asked.elapsed() < Duration::from_millis(500));
    assert_eq!(s.trace.0, 2);
}

/// S12a without a status first: blocks below B's begin may be the
/// unreported archive's, so they are held like an offline archive's (and
/// closed after `gap_wait_ms`), never delivered id-less from B.
#[tokio::test]
async fn s12a_blocks_an_unreported_archive_may_hold_are_never_sent_id_less() {
    let (dead, _held) = dead_endpoint();
    let b = up("b", frozen_mock(B_RANGE)).await;
    let c = up("c", live_mock(C_BEGIN, C_HEAD)).await;
    let router = RouterProcess::start(config(vec![
        frozen("a", &dead, None),
        frozen_server(&b),
        live_server(&c),
    ]))
    .await;
    wait_ready(&router, &[&b, &c]).await;
    let (mut w, mut r, _) = client(&router).await;

    send(&mut w, blocks(&request(500, 1010, 1000))).await;
    let (close, waited) = expect_close(&mut r, WAIT).await;
    assert_eq!(close.code, CloseCode::Error);
    assert_eq!(
        close.reason.as_str(),
        "block 500 unavailable on all upstreams"
    );
    assert!(
        waited >= Duration::from_millis(800) && waited < Duration::from_secs(3),
        "closed after {waited:?}"
    );
    assert!(legs(&b.mock).is_empty(), "{:?}", legs(&b.mock));
    assert_eq!(
        metric(&router, "fleet_router_holds_total{why=\"offline\"}").await,
        1.0
    );
}

/// The held request goes on, with ids, once the archive reports its range.
#[tokio::test]
async fn s12a_a_request_held_for_an_unreported_archive_goes_on_when_it_reports() {
    let a = bound("a", frozen_mock(A_RANGE)).await;
    let a_server = frozen(&a.1, &a.0, None);
    let b = up("b", frozen_mock(B_RANGE)).await;
    let c = up("c", live_mock(C_BEGIN, C_HEAD)).await;
    let mut cfg = config(vec![a_server, frozen_server(&b), live_server(&c)]);
    cfg["handshake_timeout_ms"] = json!(500);
    cfg["stitching"]["gap_wait_ms"] = json!(6000);
    let router = RouterProcess::start(cfg).await;
    wait_ready(&router, &[&b, &c]).await;
    let (mut w, mut r, _) = client(&router).await;

    send(&mut w, blocks(&request(500, 510, 100))).await;
    wait_metric(
        router.metrics(),
        "fleet_router_sessions{state=\"holding\"} 1",
        WAIT,
    )
    .await;
    let a = start(a);
    let (got, close) = read_blocks(&mut r, 10, Duration::from_secs(6)).await;
    assert!(close.is_none(), "{close:?}");
    assert_eq!(
        nums(&got),
        (500..510).collect::<Vec<_>>(),
        "every result has its id"
    );
    assert_eq!(legs(&a.mock).first().map(|l| l.start), Some(500));
    assert!(legs(&b.mock).is_empty(), "{:?}", legs(&b.mock));
}

// ----------------------------------------------------------------------
// S13, S14: holes and the fleet's first block
// ----------------------------------------------------------------------

/// S13: a request inside a configured hole is closed at once; status starts
/// at the top component; the hole is exported.
#[tokio::test]
async fn s13_a_coverage_hole_closes_at_once() {
    let a = up("a", frozen_mock(A_RANGE)).await;
    let b = up("b", frozen_mock((1101, 2001))).await;
    let c = up("c", live_mock(C_BEGIN, C_HEAD)).await;
    let router = RouterProcess::start(fleet_config(&a, &b, &c)).await;
    wait_ready(&router, &[&a, &b, &c]).await;
    let (mut w, mut r, _) = client(&router).await;
    assert_eq!(status(&mut w, &mut r, false).await.trace.0, 1101);
    assert_eq!(
        metric(&router, "fleet_router_fleet_coverage_gaps{log=\"trace\"}").await,
        1.0
    );

    send(&mut w, blocks(&request(1050, 1060, 10))).await;
    let (close, waited) = expect_close(&mut r, WAIT).await;
    assert_eq!(close.code, CloseCode::Error);
    assert_eq!(
        close.reason.as_str(),
        "block 1050 unavailable on all upstreams"
    );
    assert!(
        waited < Duration::from_millis(500),
        "closed after {waited:?}"
    );
    assert!(legs(&a.mock).is_empty() && legs(&b.mock).is_empty());
}

/// Stream `900..` through A into the hole that follows it: A is asked only
/// up to its own end, and at the seam the hole closes the client at once,
/// long before a silent A would count as stalled, striking nobody.
async fn stream_into_a_hole(a: &Up, rest: &[&Up], servers: Vec<Value>) {
    let mut cfg = config(servers);
    cfg["upstream_stall_timeout_ms"] = json!(3000);
    let router = RouterProcess::start(cfg).await;
    let mut all = vec![a];
    all.extend_from_slice(rest);
    wait_ready(&router, &all).await;
    let (mut w, mut r, _) = client(&router).await;

    let asked = Instant::now();
    send(&mut w, blocks(&request(900, u32::MAX, 1000))).await;
    let (got, close) = read_blocks(&mut r, 200, WAIT).await;
    assert_eq!(nums(&got), (900..1001).collect::<Vec<_>>());
    let close = close.expect("the hole closes the client");
    assert_eq!(close.code, CloseCode::Error);
    assert_eq!(
        close.reason.as_str(),
        "block 1001 unavailable on all upstreams"
    );
    assert!(
        asked.elapsed() < Duration::from_secs(2),
        "closed after {:?}",
        asked.elapsed()
    );
    let la = legs(&a.mock);
    assert_eq!(
        la.iter().map(|l| (l.start, l.end)).collect::<Vec<_>>(),
        [(900, 1001)]
    );
    for u in rest {
        assert!(legs(&u.mock).is_empty(), "{} was asked", u.name);
    }
    for series in ["upstream_block_failures_total", "upstream_stalls_total"] {
        let series = a.series(series, "");
        assert_eq!(metric(&router, &series).await, 0.0, "{series}");
    }
}

/// S13, streamed into: the hole after A ends the run at A's end.
#[tokio::test]
async fn s13_a_stream_into_a_hole_ends_at_the_archive_end() {
    let a = up("a", frozen_mock(A_RANGE)).await;
    let b = up("b", frozen_mock((1101, 2001))).await;
    let c = up("c", live_mock(C_BEGIN, C_HEAD)).await;
    let servers = vec![frozen_server(&a), frozen_server(&b), live_server(&c)];
    stream_into_a_hole(&a, &[&b, &c], servers).await;
}

/// The same hole between the top archive and a pruned live tier.
#[tokio::test]
async fn s13_a_stream_into_a_hole_before_the_live_tier_ends_at_the_archive_end() {
    let a = up("a", frozen_mock(A_RANGE)).await;
    let c = up("c", live_mock(1101, C_HEAD)).await;
    let servers = vec![frozen_server(&a), live_server(&c)];
    stream_into_a_hole(&a, &[&c], servers).await;
}

/// S14: a request below the fleet's first block goes to the archive that
/// owns it, which answers id-less below its range; that is no violation.
#[tokio::test]
async fn s14_blocks_below_the_fleet_begin_come_id_less_from_its_owner() {
    let (a, b, c) = default_fleet().await;
    let router = RouterProcess::start(fleet_config(&a, &b, &c)).await;
    wait_ready(&router, &[&a, &b, &c]).await;
    let (mut w, mut r, _) = client(&router).await;

    send(&mut w, blocks(&request(1, 11, 10))).await;
    let (got, close) = read_blocks(&mut r, 10, WAIT).await;
    assert!(close.is_none(), "{close:?}");
    assert_eq!(got.len(), 10);
    assert!(got[0].this.is_none(), "block 1 is id-less");
    assert_eq!(nums(&got[1..]), (2..11).collect::<Vec<_>>());
    assert_eq!(legs(&a.mock)[0].start, 1);
    for kind in [
        "idless",
        "gap",
        "rewind",
        "missing_field",
        "credit",
        "variant",
        "discontinuity",
    ] {
        let series = a.series(
            "upstream_contract_violations_total",
            &format!("kind=\"{kind}\""),
        );
        assert_eq!(metric(&router, &series).await, 0.0, "{series}");
    }
    assert_eq!(
        metric(&router, &a.series("upstream_block_failures_total", "")).await,
        0.0
    );
}

// ----------------------------------------------------------------------
// S15: archive contract violations
// ----------------------------------------------------------------------

/// Serve `490..520` from A (with `fault`) while its replica comes up: the
/// fault at 500 is a `kind` violation that strikes A, and the replica
/// continues the sequence exactly.
async fn s15_run(fault: MockShipConfig, kind: &str, fetch_traces: bool) {
    let a = up("a", fault).await;
    let a2 = bound("a2", frozen_mock(A_RANGE)).await;
    let (b, c) = (
        up("b", frozen_mock(B_RANGE)).await,
        up("c", live_mock(C_BEGIN, C_HEAD)).await,
    );
    let a2_server = frozen(&a2.1, &a2.0, None);
    let router = RouterProcess::start(config(vec![
        frozen_server(&a),
        a2_server,
        frozen_server(&b),
        live_server(&c),
    ]))
    .await;
    wait_ready(&router, &[&a, &b, &c]).await;
    let (mut w, mut r, _) = client(&router).await;

    send(
        &mut w,
        blocks(&BlocksReq {
            fetch_traces,
            ..request(490, 520, 5)
        }),
    )
    .await;
    let (first, _) = read_blocks(&mut r, 5, WAIT).await;
    let a2 = start(a2);
    wait_ready(&router, &[&a2]).await;
    send(&mut w, ack(100)).await;
    let (rest, close) = read_blocks(&mut r, 25, WAIT).await;
    assert!(close.is_none(), "{close:?}");
    let mut all = first;
    all.extend(rest);
    assert_eq!(nums(&all), (490..520).collect::<Vec<_>>(), "sequence");
    assert!(all.iter().all(|b| b.this.is_some()), "no id-less result");
    if fetch_traces {
        assert!(
            all.iter().all(|b| b.traces.is_some()),
            "traces in every result"
        );
    }
    assert_eq!(legs(&a2.mock)[0].start, 500);
    assert_eq!(
        metric(
            &router,
            &a.series(
                "upstream_contract_violations_total",
                &format!("kind=\"{kind}\"")
            )
        )
        .await,
        1.0
    );
    assert_eq!(
        metric(&router, &a.series("upstream_block_failures_total", "")).await,
        1.0,
        "a violation strikes"
    );
    assert_eq!(
        metric(
            &router,
            "fleet_router_failovers_total{reason=\"violation\"}"
        )
        .await,
        1.0
    );
}

/// S15: a skipped block is a gap.
#[tokio::test]
async fn s15_a_skipped_archive_block_is_a_gap_violation() {
    let fault = MockShipConfig {
        skip_at_block: Some(500),
        ..frozen_mock(A_RANGE)
    };
    s15_run(fault, "gap", false).await;
}

/// S15: an id-less result inside the archive's range is a violation.
#[tokio::test]
async fn s15_an_id_less_archive_result_is_a_violation() {
    let fault = MockShipConfig {
        headonly_at_block: Some(500),
        ..frozen_mock(A_RANGE)
    };
    s15_run(fault, "idless", false).await;
}

/// S15: requested traces missing where the archive has them.
#[tokio::test]
async fn s15_missing_traces_are_a_violation() {
    let fault = MockShipConfig {
        omit_traces_at_block: Some(500),
        ..frozen_mock(A_RANGE)
    };
    s15_run(fault, "missing_field", true).await;
}

// ----------------------------------------------------------------------
// Archives without a block log
// ----------------------------------------------------------------------

/// A frozen range server without a block log.
fn no_block_log(range: (u32, u32)) -> MockShipConfig {
    MockShipConfig {
        no_block_log: true,
        ..frozen_mock(range)
    }
}

/// `fetch_block` + traces + deltas from block 1 over two replicas of
/// `[1,1001)` without a block log and an archive of `[1001,2001)` that has
/// one: the replicas send no block, which is no violation. The first one
/// tried moves the session to the other (it might hold blocks); that one
/// serves 1..1000 without blocks, as one node with a pruned block log would,
/// and B serves the rest with blocks.
#[tokio::test]
async fn archives_without_a_block_log_serve_fetch_block_without_blocks() {
    let a = up("a", no_block_log((1, 1001))).await;
    let a2 = up("a2", no_block_log((1, 1001))).await;
    let b = up("b", frozen_mock(B_RANGE)).await;
    let router = RouterProcess::start(config(vec![
        frozen_server(&a),
        frozen_server(&a2),
        frozen_server(&b),
    ]))
    .await;
    wait_ready(&router, &[&a, &a2, &b]).await;
    let (mut w, mut r, _) = client(&router).await;

    const WINDOW: u32 = 200;
    send(
        &mut w,
        blocks(&BlocksReq {
            fetch_block: true,
            fetch_traces: true,
            fetch_deltas: true,
            ..request(1, 2001, WINDOW)
        }),
    )
    .await;
    let mut s = Stream::default();
    stream_acking(&mut w, &mut r, &mut s, 2000, WINDOW, 100).await;
    assert_eq!(nums(&s.blocks), (1..2001).collect::<Vec<_>>());
    for b in &s.blocks {
        let n = b.num().unwrap();
        assert_eq!(b.block.is_some(), n >= 1001, "block at {n}");
        assert!(b.traces.is_some() && b.deltas.is_some(), "data at {n}");
    }

    assert_eq!(
        metric(
            &router,
            "fleet_router_failovers_total{reason=\"no_block_log\"}"
        )
        .await,
        1.0
    );
    assert_eq!(
        metric(
            &router,
            "fleet_router_failovers_total{reason=\"violation\"}"
        )
        .await,
        0.0
    );
    let without = metric(
        &router,
        &a.series("upstream_results_without_block_total", ""),
    )
    .await
        + metric(
            &router,
            &a2.series("upstream_results_without_block_total", ""),
        )
        .await;
    assert_eq!(without, 1000.0);
    for u in [&a, &a2] {
        assert_eq!(
            metric(&router, &u.series("upstream_no_block_log", "")).await,
            1.0,
            "{} is flagged",
            u.name
        );
        assert_eq!(
            metric(
                &router,
                &u.series(
                    "upstream_contract_violations_total",
                    "kind=\"missing_field\""
                )
            )
            .await,
            0.0
        );
    }
    assert_eq!(
        metric(&router, &b.series("upstream_no_block_log", "")).await,
        0.0
    );
    for u in [&a, &a2, &b] {
        assert_eq!(
            metric(&router, &u.series("upstream_block_failures_total", "")).await,
            0.0,
            "{} was struck",
            u.name
        );
    }
}

/// Send `req` on a fresh connection to `url` and return the first `n`
/// binary frames exactly as they arrive.
async fn raw_frames(url: &str, req: &BlocksReq, n: usize) -> Vec<Vec<u8>> {
    let (mut w, mut r) = ws_connect(url).await;
    match read_msg(&mut r, WAIT).await {
        Message::Text(_) => {}
        other => panic!("expected the ABI, got {other:?}"),
    }
    send(&mut w, blocks(req)).await;
    let mut frames = Vec::with_capacity(n);
    while frames.len() < n {
        match read_msg(&mut r, WAIT).await {
            Message::Binary(frame) => frames.push(frame.to_vec()),
            Message::Ping(_) | Message::Pong(_) => {}
            other => panic!("after {} frames: {other:?}", frames.len()),
        }
    }
    frames
}

/// A stream over A = `[2,1001)` and B = `[1001,2001)`, where B has no
/// block log and so sends its first block without `prev_block`, is byte
/// for byte the stream of one archive of `[2,2001)`, in v0 and in v1: the
/// router fills B's `prev_block` in from A's last block, and gives A's
/// frames the fleet tip.
#[tokio::test]
async fn a_stitched_stream_equals_one_unsplit_archive_byte_for_byte() {
    let a = up("a", frozen_mock(A_RANGE)).await;
    let b = up("b", no_block_log(B_RANGE)).await;
    let whole = up("whole", frozen_mock((2, 2001))).await;
    let router = RouterProcess::start(config(vec![frozen_server(&a), frozen_server(&b)])).await;
    wait_ready(&router, &[&a, &b]).await;

    for v1 in [false, true] {
        let req = BlocksReq {
            v1,
            fetch_traces: true,
            fetch_deltas: true,
            ..request(2, 2001, 2000)
        };
        let stitched = raw_frames(&router.ws_url(), &req, 1999).await;
        let direct = raw_frames(&format!("ws://{}", whole.ep), &req, 1999).await;
        for (i, (s, d)) in stitched.iter().zip(&direct).enumerate() {
            assert_eq!(s, d, "v1 = {v1}: the result for block {}", 2 + i);
        }
        let seam = BlockView::parse(&stitched[999]).unwrap();
        assert_eq!(seam.num(), Some(1001));
        assert_eq!(seam.prev, Some((1000, fake_block_id(1000))));
    }
    assert_eq!(
        metric(&router, &b.series("upstream_prev_blocks_filled_total", "")).await,
        2.0
    );
    assert_eq!(
        metric(&router, &a.series("upstream_prev_blocks_filled_total", "")).await,
        0.0
    );
    assert_eq!(
        metric(
            &router,
            "fleet_router_failovers_total{reason=\"violation\"}"
        )
        .await,
        0.0
    );
}

// ----------------------------------------------------------------------
// S16, S17: pre-dial and zero credit at a seam
// ----------------------------------------------------------------------

/// S16: the next leg's upstream is connected before the seam and used by
/// the handoff.
#[tokio::test]
async fn s16_the_next_leg_is_pre_dialed() {
    let (a, b, c) = default_fleet().await;
    let mut cfg = fleet_config(&a, &b, &c);
    cfg["stitching"]["predial_blocks"] = json!(200);
    let router = RouterProcess::start(cfg).await;
    wait_ready(&router, &[&a, &b, &c]).await;
    let (mut w, mut r, _) = client(&router).await;

    // The client stops at 949, inside the pre-dial window, 51 blocks short
    // of block 1000.
    send(&mut w, blocks(&request(700, 1100, 250))).await;
    let (got, _) = read_blocks(&mut r, 250, WAIT).await;
    assert_eq!(nums(&got), (700..950).collect::<Vec<_>>());
    wait_for("the pre-dialed link to B", WAIT, || {
        link_sessions(&b.mock) == 1
    })
    .await;
    assert!(legs(&b.mock).is_empty(), "no request before the seam");

    send(&mut w, ack(1000)).await;
    let (rest, _) = read_blocks(&mut r, 150, WAIT).await;
    assert_eq!(nums(&rest), (950..1100).collect::<Vec<_>>());
    assert_eq!(
        link_sessions(&b.mock),
        1,
        "the handoff used the standby link"
    );
    assert_eq!(legs(&b.mock)[0].start, 1001);
    assert_eq!(
        metric(&router, "fleet_router_predials_total{result=\"used\"}").await,
        1.0
    );
    assert_eq!(
        metric(
            &router,
            &b.series("upstream_links_total", "purpose=\"predial\"")
        )
        .await,
        1.0
    );
    assert!(metric(&router, "fleet_router_handoff_latency_seconds_count").await >= 1.0);
}

/// S16 with B at `max_sessions: 1`: the standby link is the session's own
/// slot on B, so the handoff uses it instead of holding for capacity and
/// dialing B again.
#[tokio::test]
async fn s16_a_standby_to_a_single_slot_archive_is_used() {
    let (a, b, c) = default_fleet().await;
    let mut cfg = config(vec![frozen_server(&a), single_slot(&b), live_server(&c)]);
    cfg["stitching"]["predial_blocks"] = json!(200);
    let router = RouterProcess::start(cfg).await;
    wait_ready(&router, &[&a, &b, &c]).await;
    let (mut w, mut r, _) = client(&router).await;

    send(&mut w, blocks(&request(700, 1100, 250))).await;
    let (got, _) = read_blocks(&mut r, 250, WAIT).await;
    assert_eq!(nums(&got), (700..950).collect::<Vec<_>>());
    wait_for("the pre-dialed link to B", WAIT, || {
        link_sessions(&b.mock) == 1
    })
    .await;

    send(&mut w, ack(1000)).await;
    let (rest, close) = read_blocks(&mut r, 150, WAIT).await;
    assert!(close.is_none(), "{close:?}");
    assert_eq!(nums(&rest), (950..1100).collect::<Vec<_>>());
    assert_eq!(
        link_sessions(&b.mock),
        1,
        "the handoff used the standby link"
    );
    for (result, want) in [("used", 1.0), ("discarded", 0.0)] {
        let series = format!("fleet_router_predials_total{{result=\"{result}\"}}");
        assert_eq!(metric(&router, &series).await, want, "{series}");
    }
    assert_eq!(
        metric(&router, "fleet_router_holds_total{why=\"at_capacity\"}").await,
        0.0
    );
}

/// S17: a client out of credit exactly at the seam: the next leg is asked
/// with `max_messages_in_flight` 0 and sends only what later acks allow.
#[tokio::test]
async fn s17_zero_credit_at_the_seam_is_carried_over() {
    let (a, b, c) = default_fleet().await;
    let router = RouterProcess::start(fleet_config(&a, &b, &c)).await;
    wait_ready(&router, &[&a, &b, &c]).await;
    let (mut w, mut r, _) = client(&router).await;

    send(&mut w, blocks(&request(900, 1100, 101))).await;
    let (got, _) = read_blocks(&mut r, 101, WAIT).await;
    assert_eq!(nums(&got), (900..1001).collect::<Vec<_>>());
    wait_for("the request to B", WAIT, || !legs(&b.mock).is_empty()).await;
    assert_eq!(legs(&b.mock)[0].max_in_flight, 0);
    let (none, _) = read_blocks(&mut r, 1, Duration::from_millis(500)).await;
    assert!(none.is_empty(), "sent without credit: {:?}", nums(&none));

    send(&mut w, ack(5)).await;
    let (five, _) = read_blocks(&mut r, 6, Duration::from_millis(800)).await;
    assert_eq!(nums(&five), (1001..1006).collect::<Vec<_>>());
    assert_eq!(
        metric(&router, "fleet_router_credit_overruns_total").await,
        0.0
    );
}

// ----------------------------------------------------------------------
// S18: an upstream with another ABI
// ----------------------------------------------------------------------

/// The SHiP ABI with one extra struct: another fingerprint.
fn other_abi() -> Arc<str> {
    let mut value: Value = serde_json::from_str(mock_ship::SHIP_ABI_JSON).unwrap();
    value["structs"]
        .as_array_mut()
        .unwrap()
        .push(json!({ "name": "extra_test_struct", "base": "", "fields": [] }));
    Arc::from(serde_json::to_string(&value).unwrap())
}

/// S18a: under the default policy an upstream with another ABI is flagged
/// and still serves.
#[tokio::test]
async fn s18a_an_abi_mismatch_is_flagged() {
    let (a, b, c) = default_fleet().await;
    let b2 = up(
        "b2",
        MockShipConfig {
            abi_text: Some(other_abi()),
            ..frozen_mock(B_RANGE)
        },
    )
    .await;
    let router = RouterProcess::start(config(vec![
        frozen_server(&a),
        frozen_server(&b),
        frozen_server(&b2),
        live_server(&c),
    ]))
    .await;
    wait_ready(&router, &[&a, &b, &b2, &c]).await;
    wait_metric(
        router.metrics(),
        &format!("{} 0", b2.series("upstream_abi_match", "")),
        WAIT,
    )
    .await;
    for u in [&a, &b, &c] {
        assert_eq!(
            metric(&router, &u.series("upstream_abi_match", "")).await,
            1.0
        );
    }
}

/// S18b: under `abi_mismatch: "exclude"` that upstream is never dialed.
#[tokio::test]
async fn s18b_an_excluded_abi_mismatch_is_never_dialed() {
    let (a, b, c) = default_fleet().await;
    let b2 = up(
        "b2",
        MockShipConfig {
            abi_text: Some(other_abi()),
            ..frozen_mock(B_RANGE)
        },
    )
    .await;
    let mut cfg = config(vec![
        frozen_server(&a),
        frozen_server(&b),
        frozen_server(&b2),
        live_server(&c),
    ]);
    cfg["stitching"]["abi_mismatch"] = json!("exclude");
    let router = RouterProcess::start(cfg).await;
    wait_ready(&router, &[&a, &b, &b2, &c]).await;
    for _ in 0..4 {
        let (mut w, mut r, _) = client(&router).await;
        send(&mut w, blocks(&request(1500, 1510, 10))).await;
        let (got, _) = read_blocks(&mut r, 10, WAIT).await;
        assert_eq!(nums(&got), (1500..1510).collect::<Vec<_>>());
    }
    assert_eq!(link_sessions(&b2.mock), 0, "B2 was dialed");
    assert_eq!(legs(&b.mock).len(), 4);
}

// ----------------------------------------------------------------------
// S20: capacity
// ----------------------------------------------------------------------

/// The default fleet with two replicas of B, each with `max_sessions: 1`:
/// the router, then A, B, B2 and C.
async fn capacity_fleet(gap_wait_ms: u64) -> (RouterProcess, [Up; 4]) {
    let a = up("a", frozen_mock(A_RANGE)).await;
    let b = up("b", frozen_mock(B_RANGE)).await;
    let b2 = up("b2", frozen_mock(B_RANGE)).await;
    let c = up("c", live_mock(C_BEGIN, C_HEAD)).await;
    let mut limited = vec![frozen_server(&b), frozen_server(&b2)];
    for s in &mut limited {
        s["max_sessions"] = json!(1);
    }
    let mut servers = vec![frozen_server(&a)];
    servers.extend(limited);
    servers.push(live_server(&c));
    let mut cfg = config(servers);
    cfg["stitching"]["gap_wait_ms"] = json!(gap_wait_ms);
    let router = RouterProcess::start(cfg).await;
    wait_ready(&router, &[&a, &b, &b2, &c]).await;
    (router, [a, b, b2, c])
}

/// A client that read `1001..1100` and keeps its link (and slot).
async fn occupy(router: &RouterProcess) -> (WsWriter, WsReader) {
    let (mut w, mut r, _) = client(router).await;
    send(&mut w, blocks(&request(1001, 1100, 100))).await;
    let (got, _) = read_blocks(&mut r, 99, WAIT).await;
    assert_eq!(nums(&got), (1001..1100).collect::<Vec<_>>());
    (w, r)
}

/// S20: a third client holds while both replicas are full and goes on once
/// a slot frees up.
#[tokio::test]
async fn s20_a_full_archive_range_holds_until_a_slot_frees() {
    let (router, [_a, b, b2, _c]) = capacity_fleet(5000).await;
    let (mut w1, _r1) = occupy(&router).await;
    let _second = occupy(&router).await;
    assert_eq!(link_sessions(&b.mock) + link_sessions(&b2.mock), 2);

    let (mut w, mut r, _) = client(&router).await;
    send(&mut w, blocks(&request(1001, 1100, 100))).await;
    wait_metric(
        router.metrics(),
        "fleet_router_sessions{state=\"holding\"} 1",
        WAIT,
    )
    .await;
    w1.close().await.unwrap();
    let (got, close) = read_blocks(&mut r, 99, WAIT).await;
    assert!(close.is_none(), "{close:?}");
    assert_eq!(nums(&got), (1001..1100).collect::<Vec<_>>());
    assert_eq!(
        metric(&router, "fleet_router_holds_total{why=\"at_capacity\"}").await,
        1.0
    );
}

/// S20: with nothing freed within `gap_wait_ms`, the held client is closed.
#[tokio::test]
async fn s20_a_full_archive_range_closes_after_the_gap_wait() {
    let (router, _fleet) = capacity_fleet(500).await;
    let _first = occupy(&router).await;
    let _second = occupy(&router).await;

    let (mut w, mut r, _) = client(&router).await;
    send(&mut w, blocks(&request(1001, 1100, 100))).await;
    let (close, waited) = expect_close(&mut r, WAIT).await;
    assert_eq!(close.code, CloseCode::Error);
    assert_eq!(
        close.reason.as_str(),
        "upstream capacity exhausted for block 1001"
    );
    assert!(
        waited >= Duration::from_millis(400) && waited < Duration::from_secs(2),
        "closed after {waited:?}"
    );
}

/// S20 with A and B at `max_sessions: 1`: each client finishes a range on
/// one archive, whose only slot its lingering link keeps, then asks for the
/// other archive. A session held for capacity closes its idle link, so the
/// two never wait for each other's slot.
#[tokio::test]
async fn s20_held_sessions_free_the_slot_the_other_waits_for() {
    let (a, b, c) = default_fleet().await;
    let router = RouterProcess::start(config(vec![
        single_slot(&a),
        single_slot(&b),
        live_server(&c),
    ]))
    .await;
    wait_ready(&router, &[&a, &b, &c]).await;
    let (mut w1, mut r1, _) = client(&router).await;
    let (mut w2, mut r2, _) = client(&router).await;
    send(&mut w1, blocks(&request(1001, 1100, 1000))).await;
    let (got, _) = read_blocks(&mut r1, 99, WAIT).await;
    assert_eq!(nums(&got), (1001..1100).collect::<Vec<_>>());
    send(&mut w2, blocks(&request(100, 200, 1000))).await;
    let (got, _) = read_blocks(&mut r2, 100, WAIT).await;
    assert_eq!(nums(&got), (100..200).collect::<Vec<_>>());
    for u in [&a, &b] {
        assert_eq!(
            metric(&router, &u.series("active_connections", "")).await,
            1.0,
            "{} is full",
            u.name
        );
    }

    send(&mut w1, blocks(&request(100, 200, 1000))).await;
    send(&mut w2, blocks(&request(1001, 1100, 1000))).await;
    let (got1, close1) = read_blocks(&mut r1, 100, WAIT).await;
    let (got2, close2) = read_blocks(&mut r2, 99, WAIT).await;
    assert!(
        close1.is_none() && close2.is_none(),
        "{close1:?} {close2:?}"
    );
    assert_eq!(nums(&got1), (100..200).collect::<Vec<_>>());
    assert_eq!(nums(&got2), (1001..1100).collect::<Vec<_>>());
    assert_eq!(
        metric(
            &router,
            "fleet_router_hold_closes_total{why=\"at_capacity\"}"
        )
        .await,
        0.0
    );
}

// ----------------------------------------------------------------------
// S21: the live tier goes away at the tip
// ----------------------------------------------------------------------

/// S21: a client at the tip is never closed while the live tier is down,
/// and the stream resumes when the live node is back.
#[tokio::test]
async fn s21_the_tip_waits_out_a_live_outage() {
    let (a, b, c) = default_fleet().await;
    let mut cfg = fleet_config(&a, &b, &c);
    cfg["handshake_timeout_ms"] = json!(1000);
    let router = RouterProcess::start(cfg).await;
    wait_ready(&router, &[&a, &b, &c]).await;
    let (mut w, mut r, _) = client(&router).await;

    let ticker = tick(&c);
    send(&mut w, blocks(&request(2290, u32::MAX, 1000))).await;
    let (got, _) = read_blocks(&mut r, 20, WAIT).await;
    let mut seq = nums(&got);
    drop(ticker);
    sleep(Duration::from_millis(200)).await;
    let head = c.mock.clock().head();
    let (ep, name) = (c.ep.clone(), c.name.clone());
    c.mock.stall_and_drop_sessions();

    // Down for three seconds: whatever was in flight, then nothing, and no
    // close.
    let (late, close) = read_blocks(&mut r, 1000, Duration::from_secs(3)).await;
    assert!(close.is_none(), "closed during the outage: {close:?}");
    seq.extend(nums(&late));
    stop(c).await;
    let c = restart(&ep, &name, live_mock(C_BEGIN, head + 30)).await;
    let last = *seq.last().unwrap();
    let want = (head + 30 - last) as usize;
    let (resumed, close) = read_blocks(&mut r, want, Duration::from_secs(8)).await;
    assert!(close.is_none(), "{close:?}");
    seq.extend(nums(&resumed));
    assert_eq!(
        seq,
        (2290..=head + 30).collect::<Vec<_>>(),
        "gap or duplicate"
    );
    assert!(!legs(&c.mock).is_empty());
}

// ----------------------------------------------------------------------
// S22: readiness and /coverage
// ----------------------------------------------------------------------

/// S22: with an archive down the tip is still ready, history readiness
/// names the archive's segment, and `/coverage` lists the segments.
#[tokio::test]
async fn s22_history_readiness_names_the_missing_segment() {
    let (a, b, c) = default_fleet().await;
    let router = RouterProcess::start(fleet_config(&a, &b, &c)).await;
    wait_ready(&router, &[&a, &b, &c]).await;
    let (status, _) = http_get(router.metrics(), "/ready?scope=history")
        .await
        .unwrap();
    assert_eq!(status, 200);
    let a_up = a.series("upstream_up", "");
    stop(a).await;
    wait_metric(router.metrics(), &format!("{a_up} 0"), WAIT).await;

    let (status, _) = http_get(router.metrics(), "/ready").await.unwrap();
    assert_eq!(status, 200, "the tip is servable");
    let (status, body) = http_get(router.metrics(), "/ready?scope=history")
        .await
        .unwrap();
    assert_eq!(status, 503);
    assert!(
        body.contains("segment [2,1001) has no online upstream"),
        "{body}"
    );

    let (_, body) = http_get(router.metrics(), "/coverage").await.unwrap();
    let coverage: Value = serde_json::from_str(&body).unwrap();
    let trace = &coverage["listeners"][0]["logs"]["trace"];
    let segments = trace["segments"].as_array().unwrap();
    let first = &segments[0];
    assert_eq!(
        (first["begin"].as_u64(), first["end"].as_u64()),
        (Some(2), Some(1001))
    );
    assert_eq!(first["upstreams"][0]["server"], "a");
    assert_eq!(first["upstreams"][0]["online"], false);
    assert!(
        segments.iter().any(|s| s["upstreams"]
            .as_array()
            .unwrap()
            .iter()
            .any(|u| u["server"] == "c" && u["role"] == "live")),
        "{segments:?}"
    );
    assert_eq!(trace["offline_only"][0]["begin"], 2);
    drop((b, c));
}

/// An archive on a fixed port that can go down (dropping its sessions and
/// every new connection) and come back, counting the connections it gets.
struct Flaky {
    ep: String,
    up: Arc<std::sync::atomic::AtomicBool>,
    connections: Arc<std::sync::atomic::AtomicUsize>,
    sessions: Arc<std::sync::Mutex<Vec<JoinHandle<()>>>>,
    accept: JoinHandle<()>,
}

impl Flaky {
    async fn start(config: MockShipConfig) -> Flaky {
        use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let ep = listener.local_addr().unwrap().to_string();
        let up = Arc::new(AtomicBool::new(true));
        let connections = Arc::new(AtomicUsize::new(0));
        let sessions = Arc::new(std::sync::Mutex::new(Vec::new()));
        let accept = {
            let (up, connections, sessions) = (up.clone(), connections.clone(), sessions.clone());
            tokio::spawn(async move {
                while let Ok((tcp, _)) = listener.accept().await {
                    connections.fetch_add(1, Ordering::SeqCst);
                    if !up.load(Ordering::SeqCst) {
                        continue;
                    }
                    let config = config.clone();
                    let session = tokio::spawn(async move {
                        if let Ok(ws) = tokio_tungstenite::accept_async(tcp).await {
                            mock_ship::serve_websocket(config, ws).await;
                        }
                    });
                    sessions.lock().unwrap().push(session);
                }
            })
        };
        Flaky {
            ep,
            up,
            connections,
            sessions,
            accept,
        }
    }

    fn connections(&self) -> usize {
        self.connections.load(std::sync::atomic::Ordering::SeqCst)
    }

    fn take_down(&self) {
        self.up.store(false, std::sync::atomic::Ordering::SeqCst);
        for session in self.sessions.lock().unwrap().drain(..) {
            session.abort();
        }
    }

    fn bring_up(&self) {
        self.up.store(true, std::sync::atomic::Ordering::SeqCst);
    }
}

impl Drop for Flaky {
    fn drop(&mut self) {
        self.take_down();
        self.accept.abort();
    }
}

/// An archive that comes back is ready for history again (`/ready?scope=
/// history`) about one reconnect interval after a client session reaches
/// it, not when the monitor's grown reconnect backoff runs out.
#[tokio::test]
async fn a_returning_archive_is_history_ready_soon_after_a_session_reaches_it() {
    let a = Flaky::start(frozen_mock(A_RANGE)).await;
    let (b, c) = (
        up("b", frozen_mock(B_RANGE)).await,
        up("c", live_mock(C_BEGIN, C_HEAD)).await,
    );
    let mut cfg = config(vec![
        frozen("a", &a.ep, None),
        frozen_server(&b),
        live_server(&c),
    ]);
    cfg["upstream_reconnect_ms"] = json!(100);
    cfg["stitching"]["gap_wait_ms"] = json!(8000);
    let router = RouterProcess::start(cfg).await;
    let a_up = format!("fleet_router_upstream_up{{{}}}", labels(&a.ep, "a"));
    wait_metric(router.metrics(), &format!("{a_up} 1"), WAIT).await;
    wait_ready(&router, &[&b, &c]).await;

    a.take_down();
    wait_metric(router.metrics(), &format!("{a_up} 0"), WAIT).await;
    // Reconnects after 0.1, 0.1, 0.2, 0.4, 0.8 and 1.6 s fail: the monitor
    // now waits 3.2 s before its next try.
    let before = a.connections();
    wait_for("six failed reconnects", Duration::from_secs(8), || {
        a.connections() >= before + 6
    })
    .await;
    a.bring_up();

    let (mut w, mut r, _) = client(&router).await;
    send(&mut w, blocks(&request(500, 600, 100))).await;
    let (first, _) = read_blocks(&mut r, 1, WAIT).await;
    assert_eq!(
        nums(&first),
        vec![500],
        "the held request reached the archive"
    );
    let reached = Instant::now();
    loop {
        let (status, body) = http_get(router.metrics(), "/ready?scope=history")
            .await
            .unwrap();
        if status == 200 {
            break;
        }
        assert!(
            reached.elapsed() < Duration::from_millis(1500),
            "still not history-ready: {body}"
        );
        sleep(Duration::from_millis(20)).await;
    }
    let (rest, close) = read_blocks(&mut r, 99, WAIT).await;
    assert!(close.is_none(), "{close:?}");
    assert_eq!(nums(&rest), (501..600).collect::<Vec<_>>());
}

// ----------------------------------------------------------------------
// S23, S24: socket reuse
// ----------------------------------------------------------------------

/// S23: an indexer's back-to-back ranges inside one archive use one socket.
#[tokio::test]
async fn s23_back_to_back_ranges_reuse_one_archive_socket() {
    let (a, b, c) = default_fleet().await;
    let router = RouterProcess::start(fleet_config(&a, &b, &c)).await;
    wait_ready(&router, &[&a, &b, &c]).await;
    let (mut w, mut r, _) = client(&router).await;
    // Like a history indexer: every inbound message is acked, the ABI and
    // status too.
    send(&mut w, ack(1)).await;
    status(&mut w, &mut r, false).await;
    send(&mut w, ack(1)).await;

    for (start, end) in [(100, 200), (200, 300)] {
        send(&mut w, blocks(&request(start, end, 50))).await;
        let mut s = Stream::default();
        stream_acking(&mut w, &mut r, &mut s, 100, 50, 1).await;
        assert_eq!(nums(&s.blocks), (start..end).collect::<Vec<_>>());
    }
    assert_eq!(link_sessions(&a.mock), 1, "one socket for both ranges");
    let la = legs(&a.mock);
    assert_eq!(la.len(), 2);
    assert_eq!(la[0].session, la[1].session);
}

/// S23 with B at `max_sessions: 1`: the session's own lingering link is
/// the only one on B, and the next range reuses it instead of holding for
/// capacity (and closing after `gap_wait_ms`).
#[tokio::test]
async fn s23_the_next_range_reuses_the_only_slot_of_an_archive() {
    let (a, b, c) = default_fleet().await;
    let cfg = config(vec![frozen_server(&a), single_slot(&b), live_server(&c)]);
    let router = RouterProcess::start(cfg).await;
    wait_ready(&router, &[&a, &b, &c]).await;
    let (mut w, mut r, _) = client(&router).await;

    for (start, end) in [(1001, 1100), (1100, 1200)] {
        send(&mut w, blocks(&request(start, end, 1000))).await;
        let n = (end - start) as usize;
        let (got, close) = read_blocks(&mut r, n, WAIT).await;
        assert!(close.is_none(), "{close:?}");
        assert_eq!(nums(&got), (start..end).collect::<Vec<_>>());
    }
    assert_eq!(link_sessions(&b.mock), 1, "one socket for both ranges");
    let lb = legs(&b.mock);
    assert_eq!(lb.len(), 2, "{lb:?}");
    assert_eq!(lb[0].session, lb[1].session);
    assert_eq!(
        metric(&router, "fleet_router_holds_total{why=\"at_capacity\"}").await,
        0.0
    );
}

/// S24: a new request while the old one is still streaming gets a fresh
/// link, and nothing of the old request follows the new one's first frame.
#[tokio::test]
async fn s24_a_busy_socket_is_not_reused() {
    // A paces its results, so the first request is still streaming (credit
    // left, cursor below its end) when the second one arrives.
    let a = up(
        "a",
        MockShipConfig {
            block_delay: Some(Duration::from_millis(3)),
            ..frozen_mock(A_RANGE)
        },
    )
    .await;
    let b = up("b", frozen_mock(B_RANGE)).await;
    let c = up("c", live_mock(C_BEGIN, C_HEAD)).await;
    let router = RouterProcess::start(fleet_config(&a, &b, &c)).await;
    wait_ready(&router, &[&a, &b, &c]).await;
    let (mut w, mut r, _) = client(&router).await;

    // The new range lies below the old one, so every frame tells which
    // request it belongs to.
    send(&mut w, blocks(&request(500, 1001, 100))).await;
    let (got, _) = read_blocks(&mut r, 50, WAIT).await;
    assert_eq!(nums(&got), (500..550).collect::<Vec<_>>());
    send(&mut w, blocks(&request(100, 200, 100))).await;
    let mut old = Vec::new();
    let mut new = Vec::new();
    while new.len() < 100 {
        let (got, close) = read_blocks(&mut r, 1, WAIT).await;
        assert!(close.is_none() && got.len() == 1, "{close:?}");
        let n = got[0].num().unwrap();
        if new.is_empty() && n >= 500 {
            old.push(n);
        } else {
            new.push(n);
        }
    }
    assert_eq!(
        new,
        (100..200).collect::<Vec<_>>(),
        "old frames after the new request"
    );
    assert_eq!(old, (550..550 + old.len() as u32).collect::<Vec<_>>());
    assert_eq!(link_sessions(&a.mock), 2, "a fresh link");
}

/// A new request that ends before it starts, with a position below its
/// start (so not empty), arrives after `read` of A's 20 credited results:
/// it is satisfied at once, and A's old request stops with it. Nothing of
/// the old request follows, the client's ack never reaches it, and A is not
/// struck for frames that do not match the new cursor. The session goes on
/// with a later request.
async fn ended_request_mid_stream(read: usize) {
    // A paces its results, so with `read` < 20 the old request is still
    // streaming when the new one arrives.
    let a = up(
        "a",
        MockShipConfig {
            block_delay: Some(Duration::from_millis(3)),
            ..frozen_mock(A_RANGE)
        },
    )
    .await;
    let b = up("b", frozen_mock(B_RANGE)).await;
    let c = up("c", live_mock(C_BEGIN, C_HEAD)).await;
    let router = RouterProcess::start(fleet_config(&a, &b, &c)).await;
    wait_ready(&router, &[&a, &b, &c]).await;
    let (mut w, mut r, _) = client(&router).await;

    send(&mut w, blocks(&request(2, 1001, 20))).await;
    let (got, _) = read_blocks(&mut r, read, WAIT).await;
    assert_eq!(nums(&got), (2..2 + read as u32).collect::<Vec<_>>());
    send(
        &mut w,
        blocks(&BlocksReq {
            have_positions: vec![(10, fake_block_id(10))],
            ..request(600, 500, 50)
        }),
    )
    .await;
    send(&mut w, ack(50)).await;
    // Only results A sent under its first 20 credits can still arrive.
    let (late, close) = read_blocks(&mut r, 50, Duration::from_secs(1)).await;
    assert!(close.is_none(), "{close:?}");
    assert!(
        nums(&late).iter().all(|&n| n < 22),
        "the old request went on: {:?}",
        nums(&late)
    );
    let la = legs(&a.mock);
    assert_eq!(la.len(), 1, "{la:?}");
    assert_eq!(la[0].acks, 0, "the ack reached the old request");
    for kind in ["gap", "rewind", "credit", "idless", "variant"] {
        let series = a.series(
            "upstream_contract_violations_total",
            &format!("kind=\"{kind}\""),
        );
        assert_eq!(metric(&router, &series).await, 0.0, "{series}");
    }
    assert_eq!(
        metric(&router, &a.series("upstream_block_failures_total", "")).await,
        0.0
    );

    send(&mut w, blocks(&request(100, 110, 10))).await;
    let (got, close) = read_blocks(&mut r, 10, WAIT).await;
    assert!(close.is_none(), "{close:?}");
    assert_eq!(nums(&got), (100..110).collect::<Vec<_>>());
}

#[tokio::test]
async fn a_request_ending_before_its_start_stops_a_streaming_link() {
    ended_request_mid_stream(10).await;
}

#[tokio::test]
async fn a_request_ending_before_its_start_stops_a_link_out_of_credit() {
    ended_request_mid_stream(20).await;
}

// ----------------------------------------------------------------------
// S25: a clean close mid-leg
// ----------------------------------------------------------------------

/// S25: an archive's Close frame mid-leg fails over to its replica with no
/// strike, and the sequence is exact.
#[tokio::test]
async fn s25_a_close_frame_mid_leg_fails_over_without_a_strike() {
    let a = up(
        "a",
        MockShipConfig {
            close_at_block: Some((500, 1001)),
            ..frozen_mock(A_RANGE)
        },
    )
    .await;
    let a2 = bound("a2", frozen_mock(A_RANGE)).await;
    let (b, c) = (
        up("b", frozen_mock(B_RANGE)).await,
        up("c", live_mock(C_BEGIN, C_HEAD)).await,
    );
    let a2_server = frozen(&a2.1, &a2.0, None);
    let router = RouterProcess::start(config(vec![
        frozen_server(&a),
        a2_server,
        frozen_server(&b),
        live_server(&c),
    ]))
    .await;
    wait_ready(&router, &[&a, &b, &c]).await;
    let (mut w, mut r, _) = client(&router).await;

    send(&mut w, blocks(&request(490, 520, 5))).await;
    let (first, _) = read_blocks(&mut r, 5, WAIT).await;
    let a2 = start(a2);
    wait_ready(&router, &[&a2]).await;
    send(&mut w, ack(100)).await;
    let (rest, close) = read_blocks(&mut r, 25, WAIT).await;
    assert!(
        close.is_none(),
        "the archive's Close reached the client: {close:?}"
    );
    let mut seq = nums(&first);
    seq.extend(nums(&rest));
    assert_eq!(seq, (490..520).collect::<Vec<_>>());
    assert_eq!(legs(&a2.mock)[0].start, 500);
    assert_eq!(
        metric(&router, "fleet_router_failovers_total{reason=\"clean\"}").await,
        1.0
    );
    assert_eq!(
        metric(&router, &a.series("upstream_block_failures_total", "")).await,
        0.0
    );
}

// ----------------------------------------------------------------------
// config test --strict
// ----------------------------------------------------------------------

/// Run `fleet-router config test [--strict]` on `cfg`: exit success and the
/// output.
async fn config_test(cfg: &Value, strict: bool) -> (bool, String) {
    let mut cfg = cfg.clone();
    let probe = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    cfg["listen_port"] = json!(probe.local_addr().unwrap().port());
    drop(probe);
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

/// `config test` prints a stitched listener's coverage; `--strict` fails on
/// a hole or on a frozen upstream that did not report its ranges.
#[tokio::test]
async fn config_test_strict_checks_stitched_coverage() {
    let (a, b, c) = default_fleet().await;
    let mut cfg = fleet_config(&a, &b, &c);
    cfg["handshake_timeout_ms"] = json!(1000);
    let (ok, out) = config_test(&cfg, true).await;
    assert!(ok, "a complete fleet passes:\n{out}");
    assert!(out.contains("Coverage"), "{out}");
    assert!(out.contains("[2,"), "{out}");

    let hole = up("hole", frozen_mock((1101, 2001))).await;
    let mut cfg = config(vec![
        frozen_server(&a),
        frozen_server(&hole),
        live_server(&c),
    ]);
    cfg["handshake_timeout_ms"] = json!(1000);
    let (ok, out) = config_test(&cfg, false).await;
    assert!(ok, "without --strict a hole is only a warning:\n{out}");
    assert!(out.contains("hole [1001,1101) in trace coverage"), "{out}");
    let (ok, out) = config_test(&cfg, true).await;
    assert!(!ok, "--strict fails on a hole:\n{out}");

    let (dead, _held) = dead_endpoint();
    let mut cfg = config(vec![
        frozen("gone", &dead, Some(A_RANGE)),
        frozen_server(&b),
        live_server(&c),
    ]);
    cfg["handshake_timeout_ms"] = json!(1000);
    let (ok, out) = config_test(&cfg, true).await;
    assert!(!ok, "--strict fails on an unreported archive:\n{out}");
    assert!(
        out.contains("frozen upstream 'gone' did not report its ranges"),
        "{out}"
    );
}

// ----------------------------------------------------------------------
// Seams, unknown requests, startup
// ----------------------------------------------------------------------

/// An archive whose first block at a seam does not continue the last
/// delivered block (another branch) breaks the contract: it is struck and
/// the leg moves to a replica before anything reaches the client.
#[tokio::test]
async fn a_seam_that_does_not_continue_is_a_discontinuity_violation() {
    let a = up("a", frozen_mock(A_RANGE)).await;
    // B's blocks from 900 are on another branch, so its 1001 does not
    // follow A's 1000.
    let b = up(
        "b",
        MockShipConfig {
            fork: Some((900, 9)),
            ..frozen_mock(B_RANGE)
        },
    )
    .await;
    let b2 = bound("b2", frozen_mock(B_RANGE)).await;
    let c = up("c", live_mock(C_BEGIN, C_HEAD)).await;
    let b2_server = frozen(&b2.1, &b2.0, None);
    let router = RouterProcess::start(config(vec![
        frozen_server(&a),
        frozen_server(&b),
        b2_server,
        live_server(&c),
    ]))
    .await;
    wait_ready(&router, &[&a, &b, &c]).await;
    let (mut w, mut r, _) = client(&router).await;

    // Out of credit exactly at the seam: B (the only replica up) gets the
    // leg and sends nothing yet.
    send(&mut w, blocks(&request(990, 1020, 11))).await;
    let (first, _) = read_blocks(&mut r, 11, WAIT).await;
    assert_eq!(nums(&first), (990..1001).collect::<Vec<_>>());
    wait_for("the leg on B", WAIT, || !legs(&b.mock).is_empty()).await;
    let b2 = start(b2);
    wait_ready(&router, &[&b2]).await;
    send(&mut w, ack(100)).await;
    let (rest, close) = read_blocks(&mut r, 19, WAIT).await;
    assert!(close.is_none(), "{close:?}");
    let mut all = first;
    all.extend(rest);
    assert_eq!(nums(&all), (990..1020).collect::<Vec<_>>());
    for pair in all.windows(2) {
        assert_eq!(pair[1].prev, pair[0].this, "the chain continues");
    }
    assert_eq!(legs(&b2.mock)[0].start, 1001);
    assert_eq!(
        metric(
            &router,
            &b.series(
                "upstream_contract_violations_total",
                "kind=\"discontinuity\""
            )
        )
        .await,
        1.0
    );
    assert_eq!(
        metric(&router, &b.series("upstream_block_failures_total", "")).await,
        1.0
    );
}

/// A stitched listener never forwards a request it does not know: the
/// client is closed with 1003 and no upstream is touched.
#[tokio::test]
async fn an_unknown_request_variant_closes_the_client_with_1003() {
    let (a, b, c) = default_fleet().await;
    let router = RouterProcess::start(fleet_config(&a, &b, &c)).await;
    wait_ready(&router, &[&a, &b, &c]).await;
    let (mut w, mut r, _) = client(&router).await;
    send(&mut w, vec![9, 1, 2, 3]).await;
    let (close, _) = expect_close(&mut r, WAIT).await;
    assert_eq!(close.code, CloseCode::Unsupported);
    assert_eq!(close.reason.as_str(), "unsupported request variant 9");
    assert_eq!(
        metric(
            &router,
            "fleet_router_client_protocol_errors_total{kind=\"unknown_variant\"}"
        )
        .await,
        1.0
    );
    for u in [&a, &b, &c] {
        assert_eq!(link_sessions(&u.mock), 0, "{} got a link", u.name);
    }
}

/// With no upstream online there is no ABI to send: the client is closed
/// with 1011 after the handshake timeout.
#[tokio::test]
async fn a_client_is_closed_when_no_upstream_is_online() {
    let (dead, _held) = dead_endpoint();
    let mut cfg = config(vec![frozen("a", &dead, Some(A_RANGE))]);
    cfg["handshake_timeout_ms"] = json!(1000);
    let router = RouterProcess::start(cfg).await;
    let (_w, mut r) = ws_connect(&router.ws_url()).await;
    let (close, waited) = expect_close(&mut r, WAIT).await;
    assert_eq!(close.code, CloseCode::Error);
    assert_eq!(close.reason.as_str(), "No upstream server available");
    assert!(
        waited >= Duration::from_millis(800),
        "closed after {waited:?}"
    );
}

/// An archive at its own session limit answers 503: the session holds as
/// busy (the archive is never marked offline) and, with nothing freed within
/// `gap_wait_ms`, closes with the capacity reason.
#[tokio::test]
async fn a_busy_archive_holds_and_closes_as_capacity_exhausted() {
    let a = up("a", frozen_mock(A_RANGE)).await;
    // The router's monitor holds B's only slot.
    let b = up(
        "b",
        MockShipConfig {
            max_sessions: Some(1),
            ..frozen_mock(B_RANGE)
        },
    )
    .await;
    let c = up("c", live_mock(C_BEGIN, C_HEAD)).await;
    let router = RouterProcess::start(fleet_config(&a, &b, &c)).await;
    wait_ready(&router, &[&a, &b, &c]).await;
    let (mut w, mut r, _) = client(&router).await;

    send(&mut w, blocks(&request(1500, 1510, 10))).await;
    let (close, waited) = expect_close(&mut r, WAIT).await;
    assert_eq!(close.code, CloseCode::Error);
    assert_eq!(
        close.reason.as_str(),
        "upstream capacity exhausted for block 1500"
    );
    assert!(
        waited >= Duration::from_millis(800) && waited < Duration::from_secs(3),
        "closed after {waited:?}"
    );
    assert!(
        metric(
            &router,
            &b.series("upstream_dial_failures_total", "reason=\"busy\"")
        )
        .await
            >= 1.0
    );
    assert_eq!(
        metric(&router, "fleet_router_hold_closes_total{why=\"busy\"}").await,
        1.0
    );
    assert_eq!(metric(&router, &b.series("upstream_up", "")).await, 1.0);
}
