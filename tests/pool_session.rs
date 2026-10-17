//! End-to-end tests of a pool listener's Spring-exact session: failover
//! resumes with the client's outstanding credit, de-duplication with refunds,
//! the keep-alive rule, rerouting before forwarding, owed-gated stall and
//! idle detection, busy upstreams, status answered by the router, and client
//! protocol errors.
//!
//! Mocks run with Spring fidelity unless a test says otherwise. Each test
//! finishes in a few seconds.
//!
//! Run: cargo test --test pool_session

mod common;

use std::sync::Arc;
use std::time::{Duration, Instant};

use common::{
    base_config, metric_value, read_blocks, read_msg, server, wait_metric, ws_connect,
    RouterProcess, WsReader, WsWriter,
};
use futures::SinkExt;
use mock_ship::client::{ack, blocks, status_v0, status_v1, BlockView, BlocksReq, StatusView};
use mock_ship::{Fidelity, MockShipConfig, MockShipHandle, MockShipServer, Observed};
use serde_json::{json, Value};
use tokio::time::{sleep, timeout};
use tokio_tungstenite::tungstenite::protocol::frame::coding::CloseCode;
use tokio_tungstenite::tungstenite::Message;

const WAIT: Duration = Duration::from_secs(5);

/// A router config for `servers` named `a`, `b`, ... with status polled
/// every 500 ms (more forgiving than the harness default under load).
fn config(endpoints: &[&str]) -> Value {
    let servers: Vec<Value> = endpoints
        .iter()
        .enumerate()
        .map(|(i, ep)| server(&name(i), ep))
        .collect();
    let mut cfg = base_config(&servers);
    cfg["upstream_status_ms"] = json!(500);
    cfg["upstream_monitoring_ms"] = json!(500);
    cfg
}

fn name(i: usize) -> String {
    ((b'a' + i as u8) as char).to_string()
}

fn labels(endpoint: &str, server: &str) -> String {
    format!("endpoint=\"{endpoint}\",server=\"{server}\"")
}

async fn wait_up(router: &RouterProcess, endpoint: &str, server: &str) {
    wait_metric(
        router.metrics(),
        &format!("fleet_router_upstream_up{{{}}} 1", labels(endpoint, server)),
        WAIT,
    )
    .await;
}

async fn metric(router: &RouterProcess, series: &str) -> f64 {
    metric_value(router.metrics(), series).await.unwrap_or(0.0)
}

async fn mock(config: MockShipConfig) -> (String, MockShipServer) {
    let server = MockShipServer::new(config).await;
    (server.endpoint(), server)
}

/// Connect a client and read the ABI.
async fn client(router: &RouterProcess) -> (WsWriter, WsReader) {
    let (writer, mut reader) = ws_connect(&router.ws_url()).await;
    match read_msg(&mut reader, WAIT).await {
        Message::Text(_) => {}
        other => panic!("expected the ABI, got {other:?}"),
    }
    (writer, reader)
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

/// The sessions of `mock` that received a get_blocks request (the client's,
/// not the monitor's), with what each received in order.
fn client_sessions(mock: &MockShipHandle) -> Vec<(u32, Vec<Observed>)> {
    let log = mock.requests();
    let mut sessions: Vec<(u32, Vec<Observed>)> = Vec::new();
    for o in &log {
        let session = match o {
            Observed::Accepted { session }
            | Observed::Status { session, .. }
            | Observed::Blocks { session, .. }
            | Observed::Ack { session, .. }
            | Observed::Unknown { session, .. } => *session,
        };
        if matches!(o, Observed::Accepted { .. }) {
            continue;
        }
        match sessions.iter_mut().find(|(s, _)| *s == session) {
            Some((_, entries)) => entries.push(o.clone()),
            None => sessions.push((session, vec![o.clone()])),
        }
    }
    sessions.retain(|(_, entries)| entries.iter().any(|o| matches!(o, Observed::Blocks { .. })));
    sessions
}

/// Sessions `mock` accepted that never asked for status: client links, since
/// the router answers client status itself and only its monitor asks.
fn link_sessions(mock: &MockShipHandle) -> usize {
    let log = mock.requests();
    log.iter()
        .filter_map(|o| match o {
            Observed::Accepted { session } => Some(*session),
            _ => None,
        })
        .filter(|s| {
            !log.iter()
                .any(|o| matches!(o, Observed::Status { session, .. } if session == s))
        })
        .count()
}

/// `(start, end, max_in_flight, positions)` of every get_blocks `mock` got.
fn block_requests(mock: &MockShipHandle) -> Vec<(u32, u32, u32, Vec<u32>)> {
    mock.requests()
        .into_iter()
        .filter_map(|o| match o {
            Observed::Blocks {
                start,
                end,
                max_in_flight,
                have_positions,
                ..
            } => Some((
                start,
                end,
                max_in_flight,
                have_positions.iter().map(|(n, _)| *n).collect(),
            )),
            _ => None,
        })
        .collect()
}

/// Poll until `pred` holds for `mock`'s request log.
async fn wait_requests(mock: &MockShipHandle, what: &str, pred: impl Fn(&[Observed]) -> bool) {
    let deadline = Instant::now() + WAIT;
    while !pred(&mock.requests()) {
        assert!(
            Instant::now() < deadline,
            "timed out waiting for {what}: {:?}",
            mock.requests()
        );
        sleep(Duration::from_millis(20)).await;
    }
}

fn spring_live(begin: u32, head: u32) -> MockShipConfig {
    MockShipConfig::live(begin, head)
}

/// P1: an upstream Close frame is a transparent, strike-free failover.
#[tokio::test]
async fn p1_an_upstream_close_fails_over_without_reaching_the_client() {
    let (ep_a, a) = mock(MockShipConfig {
        close_at_block: Some((150, 1000)),
        ..spring_live(1, 1000)
    })
    .await;
    let (ep_b, b) = mock(spring_live(1, 1000)).await;
    let a = a.start();
    let router = RouterProcess::start(config(&[&ep_a, &ep_b])).await;
    wait_up(&router, &ep_a, "a").await;
    let (mut w, mut r) = client(&router).await;
    let _b = b.start();
    wait_up(&router, &ep_b, "b").await;

    send(&mut w, blocks(&request(100, 200, 1000))).await;
    let (got, close) = read_blocks(&mut r, 100, WAIT).await;
    assert!(
        close.is_none(),
        "the upstream's Close reached the client: {close:?}"
    );
    assert_eq!(nums(&got), (100..200).collect::<Vec<_>>());
    assert_eq!(client_sessions(&a).len(), 1);
    assert_eq!(
        metric(&router, "fleet_router_failovers_total{reason=\"clean\"}").await,
        1.0
    );
    for (ep, n) in [(&ep_a, "a"), (&ep_b, "b")] {
        let series = format!(
            "fleet_router_upstream_block_failures_total{{{}}}",
            labels(ep, n)
        );
        assert_eq!(metric(&router, &series).await, 0.0, "{n} was struck");
    }
}

/// P2: a failover resumes with the client's outstanding credit; acks sent
/// while no link was up are folded into it, never replayed.
#[tokio::test]
async fn p2_failover_carries_the_outstanding_credit_and_folds_window_acks() {
    let (ep_a, a) = mock(MockShipConfig {
        corrupt_block: Some(150),
        ..spring_live(1, 1000)
    })
    .await;
    let (ep_b, b) = mock(spring_live(1, 1000)).await;
    let _a = a.start();
    let router = RouterProcess::start(config(&[&ep_a, &ep_b])).await;
    wait_up(&router, &ep_a, "a").await;
    let (mut w, mut r) = client(&router).await;
    let b = b.start();
    wait_up(&router, &ep_b, "b").await;

    const WINDOW: u32 = 10;
    send(&mut w, blocks(&request(100, 200, WINDOW))).await;
    let (mut received, mut acked) = (Vec::new(), 0u32);
    while received.len() < 100 {
        let (got, close) = read_blocks(&mut r, 1, WAIT).await;
        assert!(close.is_none(), "closed after {received:?}");
        assert_eq!(got.len(), 1, "stalled after {:?}", received.last());
        received.extend(nums(&got));
        let unacked = received.len() as u32 - acked;
        assert!(unacked <= WINDOW, "{unacked} unacked results");
        sleep(Duration::from_millis(20)).await;
        send(&mut w, ack(1)).await;
        acked += 1;
    }
    assert_eq!(received, (100..200).collect::<Vec<_>>());
    sleep(Duration::from_millis(300)).await;

    let sessions = client_sessions(&b);
    assert_eq!(sessions.len(), 1, "{sessions:?}");
    let entries = &sessions[0].1;
    let Observed::Blocks {
        start,
        max_in_flight,
        ..
    } = entries[0].clone()
    else {
        panic!("B's first message must be the request, got {entries:?}");
    };
    assert_eq!(start, 150);
    let acks_to_b: u32 = entries
        .iter()
        .filter_map(|o| match o {
            Observed::Ack { n, .. } => Some(*n),
            _ => None,
        })
        .sum();
    // B's credit left equals the client's: nothing replayed, nothing lost.
    let from_b = 50;
    assert_eq!(
        i64::from(max_in_flight) + i64::from(acks_to_b) - from_b,
        i64::from(WINDOW) + i64::from(acked) - 100,
        "B got max_in_flight {max_in_flight} and {acks_to_b} acks"
    );
}

/// P3: status requests sent during a failover are answered at once, in
/// order, from the fleet view (the session has no link at that moment).
#[tokio::test]
async fn p3_status_during_a_failover_is_answered_in_order() {
    let (ep_a, a) = mock(spring_live(1, 1000)).await;
    let (ep_b, b) = mock(MockShipConfig {
        // The ABI alone takes about 1.5 s to arrive.
        throttle_bytes_per_sec: Some(40_000),
        ..spring_live(1, 1005)
    })
    .await;
    let a = a.start();
    let router = RouterProcess::start(config(&[&ep_a, &ep_b])).await;
    wait_up(&router, &ep_a, "a").await;
    let (mut w, mut r) = client(&router).await;
    let _b = b.start();
    wait_up(&router, &ep_b, "b").await;

    send(&mut w, blocks(&request(990, u32::MAX, 100))).await;
    let (got, _) = read_blocks(&mut r, 11, WAIT).await;
    assert_eq!(nums(&got), (990..=1000).collect::<Vec<_>>());

    a.drop_sessions();
    sleep(Duration::from_millis(50)).await;
    let asked = Instant::now();
    send(&mut w, status_v0()).await;
    send(&mut w, status_v1()).await;
    for variant in [0, 3] {
        let Message::Binary(reply) = read_msg(&mut r, WAIT).await else {
            panic!("expected a status result");
        };
        let status = StatusView::parse(&reply).expect("a status result");
        assert_eq!(status.variant, variant, "replies keep request order");
        assert_eq!(status.head.0, 1005, "the fleet head");
    }
    assert!(
        asked.elapsed() < Duration::from_millis(1000),
        "status waited for the reconnect ({:?})",
        asked.elapsed()
    );
    for version in ["v0", "v1"] {
        let series =
            format!("fleet_router_status_replies_total{{version=\"{version}\",source=\"router\"}}");
        assert_eq!(metric(&router, &series).await, 1.0);
    }
    // The stream continues from B once it is connected.
    let (got, close) = read_blocks(&mut r, 5, WAIT).await;
    assert!(close.is_none());
    assert_eq!(nums(&got), (1001..=1005).collect::<Vec<_>>());
}

/// P4: a request is routed by range before it is forwarded: an upstream that
/// lags far behind the fleet for the start block is left for one that has it.
#[tokio::test]
async fn p4_a_request_is_rerouted_before_it_is_forwarded() {
    let (ep_a, a) = mock(MockShipConfig {
        fidelity: Fidelity::Spring,
        head_block: 999,
        lib_block: 989,
        blocks_to_stream: 0,
        trace_begin_block: 1,
        trace_end_block: 1000,
        ..MockShipConfig::default()
    })
    .await;
    let (ep_b, b) = mock(MockShipConfig {
        fidelity: Fidelity::Spring,
        head_block: 2999,
        lib_block: 2989,
        blocks_to_stream: 0,
        trace_begin_block: 1000,
        trace_end_block: 3000,
        ..MockShipConfig::default()
    })
    .await;
    let a = a.start();
    let router = RouterProcess::start(config(&[&ep_a, &ep_b])).await;
    wait_up(&router, &ep_a, "a").await;
    let (mut w, mut r) = client(&router).await;
    let b = b.start();
    wait_up(&router, &ep_b, "b").await;

    send(&mut w, blocks(&request(1500, 1600, 200))).await;
    let (got, close) = read_blocks(&mut r, 100, WAIT).await;
    assert!(close.is_none());
    assert_eq!(nums(&got), (1500..1600).collect::<Vec<_>>());
    assert!(
        block_requests(&a).is_empty(),
        "A got {:?}",
        block_requests(&a)
    );
    assert_eq!(block_requests(&b), vec![(1500, 1600, 200, vec![])]);
    assert_eq!(metric(&router, "fleet_router_reroutes_total").await, 1.0);
}

/// P5: a resume carries the reversible tail; the replica rewinds to its first
/// unknown block, and the replays are dropped and refunded.
#[tokio::test]
async fn p5_replayed_blocks_are_dropped_and_refunded() {
    let (ep_a, a) = mock(MockShipConfig {
        lib_block: 190,
        ..spring_live(1, 199)
    })
    .await;
    let (ep_b, b) = mock(spring_live(1, 195)).await;
    let a = a.start();
    let router = RouterProcess::start(config(&[&ep_a, &ep_b])).await;
    wait_up(&router, &ep_a, "a").await;
    let (mut w, mut r) = client(&router).await;
    let b = b.start();
    wait_up(&router, &ep_b, "b").await;

    send(&mut w, blocks(&request(150, 1000, 100))).await;
    let (got, _) = read_blocks(&mut r, 50, WAIT).await;
    assert_eq!(nums(&got), (150..200).collect::<Vec<_>>());

    a.drop_sessions();
    wait_requests(&b, "B's resume request", |log| {
        log.iter().any(|o| matches!(o, Observed::Blocks { .. }))
    })
    .await;
    let resumed = block_requests(&b);
    assert_eq!(resumed.len(), 1);
    let (start, _, max_in_flight, positions) = &resumed[0];
    assert_eq!(*start, 200);
    assert_eq!(*max_in_flight, 50, "the client's outstanding credit");
    assert_eq!(
        positions,
        &(191..=199).collect::<Vec<_>>(),
        "the reversible tail"
    );

    // B rewound to 196, its first unknown block, and waits for it.
    b.set_head(205, 195);
    let (got, close) = read_blocks(&mut r, 6, WAIT).await;
    assert!(close.is_none());
    assert_eq!(nums(&got), (200..=205).collect::<Vec<_>>());
    let (more, _) = read_blocks(&mut r, 1, Duration::from_millis(300)).await;
    assert!(more.is_empty(), "duplicate delivered: {:?}", nums(&more));
    let refunds = client_sessions(&b)[0]
        .1
        .iter()
        .filter(|o| matches!(o, Observed::Ack { n: 1, .. }))
        .count();
    assert_eq!(refunds, 4, "one refund per replayed block (196..=199)");
    assert_eq!(
        metric(&router, "fleet_router_duplicates_dropped_total").await,
        4.0
    );
}

/// P6: EOSIO 2.0 head keep-alives reach the client and cost a credit, but
/// never move the resume block.
#[tokio::test]
async fn p6_keepalives_cost_credit_and_do_not_move_the_cursor() {
    let (ep_a, a) = mock(MockShipConfig {
        fidelity: Fidelity::Eosio2,
        ..spring_live(1, 100)
    })
    .await;
    let (ep_b, b) = mock(spring_live(1, 100)).await;
    let a = a.start();
    let router = RouterProcess::start(config(&[&ep_a, &ep_b])).await;
    wait_up(&router, &ep_a, "a").await;
    let (mut w, mut r) = client(&router).await;
    let b = b.start();
    wait_up(&router, &ep_b, "b").await;

    send(&mut w, blocks(&request(105, 200, 50))).await;
    let (got, _) = read_blocks(&mut r, 1, WAIT).await;
    a.set_head(102, 92);
    let (more, _) = read_blocks(&mut r, 1, WAIT).await;
    let keepalives: Vec<&BlockView> = got.iter().chain(&more).collect();
    assert_eq!(keepalives.len(), 2);
    assert!(
        keepalives.iter().all(|k| k.this.is_none()),
        "id-less keep-alives"
    );

    a.drop_sessions();
    wait_requests(&b, "B's resume request", |log| {
        log.iter().any(|o| matches!(o, Observed::Blocks { .. }))
    })
    .await;
    let resumed = block_requests(&b);
    assert_eq!(resumed[0].0, 105, "keep-alives never move the resume block");
    assert_eq!(resumed[0].2, 48, "each keep-alive cost a credit");
    assert!(metric(&router, "fleet_router_keepalives_total").await >= 2.0);
}

/// P7: a Leap 4/5 node starts at its first available block; the jump is
/// adopted without a strike.
#[tokio::test]
async fn p7_a_first_available_jump_is_adopted() {
    let (ep_a, a) = mock(MockShipConfig {
        first_available_jump: true,
        ..spring_live(500, 1000)
    })
    .await;
    let _a = a.start();
    let router = RouterProcess::start(config(&[&ep_a])).await;
    wait_up(&router, &ep_a, "a").await;
    let (mut w, mut r) = client(&router).await;

    send(&mut w, blocks(&request(100, 1000, 10))).await;
    let (got, _) = read_blocks(&mut r, 10, WAIT).await;
    assert_eq!(nums(&got), (500..510).collect::<Vec<_>>());
    let l = labels(&ep_a, "a");
    assert_eq!(
        metric(
            &router,
            &format!("fleet_router_upstream_sequence_jumps_total{{{l}}}")
        )
        .await,
        1.0
    );
    assert_eq!(
        metric(
            &router,
            &format!("fleet_router_upstream_block_failures_total{{{l}}}")
        )
        .await,
        0.0
    );
}

/// P8a: an upstream that goes silent while it owes blocks is failed over
/// after `upstream_stall_timeout_ms`, without a strike.
#[tokio::test]
async fn p8a_a_hung_upstream_is_failed_over_after_the_stall_timeout() {
    let (ep_a, a) = mock(MockShipConfig {
        hold_at_block: Some(115),
        ..spring_live(1, 1000)
    })
    .await;
    let (ep_b, b) = mock(spring_live(1, 1000)).await;
    let _a = a.start();
    let mut cfg = config(&[&ep_a, &ep_b]);
    cfg["upstream_stall_timeout_ms"] = json!(1000);
    let router = RouterProcess::start(cfg).await;
    wait_up(&router, &ep_a, "a").await;
    let (mut w, mut r) = client(&router).await;
    let _b = b.start();
    wait_up(&router, &ep_b, "b").await;

    send(&mut w, blocks(&request(100, 200, 1000))).await;
    let (got, _) = read_blocks(&mut r, 15, WAIT).await;
    assert_eq!(nums(&got), (100..115).collect::<Vec<_>>());
    let held = Instant::now();
    let (got, close) = read_blocks(&mut r, 85, WAIT).await;
    assert!(close.is_none());
    assert!(
        held.elapsed() >= Duration::from_millis(900),
        "failed over too early"
    );
    assert_eq!(nums(&got), (115..200).collect::<Vec<_>>());
    assert_eq!(
        metric(&router, "fleet_router_failovers_total{reason=\"stall\"}").await,
        1.0
    );
    let l = labels(&ep_a, "a");
    assert_eq!(
        metric(
            &router,
            &format!("fleet_router_upstream_stalls_total{{{l}}}")
        )
        .await,
        1.0
    );
    assert_eq!(
        metric(
            &router,
            &format!("fleet_router_upstream_block_failures_total{{{l}}}")
        )
        .await,
        0.0,
        "a live stall never strikes"
    );
}

/// P8b: a slow transport is not a stall: bytes keep arriving, so a message
/// that takes longer than the stall timeout is still delivered intact.
#[tokio::test]
async fn p8b_a_slow_large_message_is_not_a_stall() {
    const SIZE: usize = 1536 * 1024;
    let (ep_a, a) = mock(MockShipConfig {
        block_data_size: SIZE,
        throttle_bytes_per_sec: Some(1024 * 1024),
        ..spring_live(1, 1000)
    })
    .await;
    let _a = a.start();
    let mut cfg = config(&[&ep_a]);
    cfg["upstream_stall_timeout_ms"] = json!(1000);
    let router = RouterProcess::start(cfg).await;
    wait_up(&router, &ep_a, "a").await;
    let (mut w, mut r) = client(&router).await;

    let req = BlocksReq {
        fetch_block: true,
        ..request(100, 102, 10)
    };
    send(&mut w, blocks(&req)).await;
    let (got, close) = read_blocks(&mut r, 2, Duration::from_secs(8)).await;
    assert!(close.is_none());
    assert_eq!(nums(&got), vec![100, 101]);
    assert!(got
        .iter()
        .all(|b| b.block.as_ref().is_some_and(|d| d.len() == SIZE)));
    for reason in ["stall", "idle", "unclean", "offline"] {
        let series = format!("fleet_router_failovers_total{{reason=\"{reason}\"}}");
        assert_eq!(metric(&router, &series).await, 0.0, "{reason}");
    }
}

/// P9: a client at the tip resumes on the replica within a second of a drop,
/// with a contiguous sequence.
#[tokio::test]
async fn p9_tip_failover_is_fast_and_contiguous() {
    let (ep_a, a) = mock(spring_live(1, 1000)).await;
    let (ep_b, b) = mock(spring_live(1, 1000)).await;
    let (a, b) = (Arc::new(a.start()), Arc::new(b.start()));
    let ticker = {
        let (a, b) = (a.clone(), b.clone());
        tokio::spawn(async move {
            loop {
                sleep(Duration::from_millis(100)).await;
                a.advance(1);
                b.advance(1);
            }
        })
    };
    let router = RouterProcess::start(config(&[&ep_a, &ep_b])).await;
    wait_up(&router, &ep_a, "a").await;
    wait_up(&router, &ep_b, "b").await;
    let (mut w, mut r) = client(&router).await;

    let start = a.clock().head() + 1;
    send(&mut w, blocks(&request(start, u32::MAX, 100))).await;
    let (mut got, _) = read_blocks(&mut r, 5, WAIT).await;
    let serving = if client_sessions(&a).is_empty() {
        &b
    } else {
        &a
    };
    serving.drop_sessions();
    let dropped = Instant::now();
    let (next, close) = read_blocks(&mut r, 1, WAIT).await;
    assert!(close.is_none());
    assert_eq!(next.len(), 1, "no block after the drop");
    assert!(
        dropped.elapsed() < Duration::from_secs(1),
        "resumed after {:?}",
        dropped.elapsed()
    );
    got.extend(next);
    let (more, _) = read_blocks(&mut r, 5, WAIT).await;
    got.extend(more);
    let seq = nums(&got);
    let expected: Vec<u32> = (start..start + seq.len() as u32).collect();
    assert_eq!(seq, expected, "gap or duplicate across the failover");
    ticker.abort();
}

/// P10: a session waiting at the real tip (nothing owed) never idles out its
/// upstream.
#[tokio::test]
async fn p10_waiting_at_the_tip_is_not_an_idle_failover() {
    let (ep_a, a) = mock(spring_live(1, 1000)).await;
    let _a = a.start();
    let mut cfg = config(&[&ep_a]);
    cfg["idle_timeout_ms"] = json!(200);
    let router = RouterProcess::start(cfg).await;
    wait_up(&router, &ep_a, "a").await;
    let (mut w, mut r) = client(&router).await;

    send(&mut w, blocks(&request(1001, u32::MAX, 10))).await;
    let until = Instant::now() + Duration::from_secs(2);
    while Instant::now() < until {
        // Client traffic keeps the client side of the idle timer alive.
        sleep(Duration::from_millis(50)).await;
        send(&mut w, ack(0)).await;
    }
    let (got, close) = read_blocks(&mut r, 1, Duration::from_millis(100)).await;
    assert!(got.is_empty() && close.is_none(), "{got:?} {close:?}");
    assert_eq!(
        metric(&router, "fleet_router_failovers_total{reason=\"idle\"}").await,
        0.0
    );
}

/// P11: an upstream that drops right after a forwarded unknown request
/// variant is not blamed; the client is closed with 1003.
#[tokio::test]
async fn p11_a_drop_caused_by_an_unknown_variant_closes_the_client() {
    let (ep_a, a) = mock(spring_live(1, 1000)).await;
    let _a = a.start();
    let router = RouterProcess::start(config(&[&ep_a])).await;
    wait_up(&router, &ep_a, "a").await;
    let (mut w, mut r) = client(&router).await;

    send(&mut w, vec![9, 1, 2, 3]).await;
    let (_, close) = read_blocks(&mut r, 1, WAIT).await;
    let close = close.expect("a close frame");
    assert_eq!(close.code, CloseCode::Unsupported);
    assert_eq!(close.reason.as_str(), "unsupported request");
    let l = labels(&ep_a, "a");
    assert_eq!(
        metric(
            &router,
            &format!("fleet_router_upstream_block_failures_total{{{l}}}")
        )
        .await,
        0.0
    );
    assert_eq!(
        metric(
            &router,
            "fleet_router_client_protocol_errors_total{kind=\"unknown_variant\"}"
        )
        .await,
        1.0
    );
}

/// P12: a malformed known request is never forwarded; the client is closed
/// with 1007.
#[tokio::test]
async fn p12_a_malformed_request_closes_the_client_with_1007() {
    let (ep_a, a) = mock(spring_live(1, 1000)).await;
    let a = a.start();
    let router = RouterProcess::start(config(&[&ep_a])).await;
    wait_up(&router, &ep_a, "a").await;
    let (mut w, mut r) = client(&router).await;

    let mut bad = blocks(&request(100, 200, 10));
    // [variant][start][end][max][positions=0][irreversible_only]...
    bad[14] = 2;
    send(&mut w, bad).await;
    let (_, close) = read_blocks(&mut r, 1, WAIT).await;
    let close = close.expect("a close frame");
    assert_eq!(close.code, CloseCode::Invalid);
    assert_eq!(close.reason.as_str(), "invalid request");
    assert!(
        !a.requests()
            .iter()
            .any(|o| matches!(o, Observed::Blocks { .. } | Observed::Unknown { .. })),
        "the malformed request reached the upstream: {:?}",
        a.requests()
    );
}

/// P13: an upstream at its session limit answers 503: busy, not offline.
#[tokio::test]
async fn p13_a_full_upstream_is_busy_not_offline() {
    // The router's monitor holds A's only slot.
    let (ep_a, a) = mock(MockShipConfig {
        max_sessions: Some(1),
        ..spring_live(1, 1000)
    })
    .await;
    let (ep_b, b) = mock(spring_live(1, 1000)).await;
    let (a, b) = (a.start(), b.start());
    let router = RouterProcess::start(config(&[&ep_a, &ep_b])).await;
    wait_up(&router, &ep_a, "a").await;
    wait_up(&router, &ep_b, "b").await;

    let mut clients = Vec::new();
    for _ in 0..4 {
        let (mut w, mut r) = client(&router).await;
        send(&mut w, status_v0()).await;
        assert!(matches!(read_msg(&mut r, WAIT).await, Message::Binary(_)));
        clients.push((w, r));
    }
    // Every client link is on B; A only ever accepted its monitor (the
    // monitor's sessions are the ones that ask for status).
    assert_eq!(link_sessions(&a), 0);
    assert_eq!(link_sessions(&b), 4);
    let la = labels(&ep_a, "a");
    assert!(
        metric(
            &router,
            &format!("fleet_router_upstream_dial_failures_total{{{la},reason=\"busy\"}}")
        )
        .await
            >= 1.0
    );
    assert_eq!(
        metric(&router, &format!("fleet_router_upstream_up{{{la}}}")).await,
        1.0,
        "busy is not offline"
    );
}

/// P14: a range reader's pattern (status, then consecutive ranges, acking every
/// message) stays on one upstream socket; a new request while blocks are
/// still owed opens a fresh one, and nothing of the old request follows.
#[tokio::test]
async fn p14_consecutive_ranges_reuse_the_socket_and_busy_ones_do_not() {
    let (ep_a, a) = mock(spring_live(1, 1000)).await;
    let a = a.start();
    let router = RouterProcess::start(config(&[&ep_a])).await;
    wait_up(&router, &ep_a, "a").await;
    let (mut w, mut r) = client(&router).await;
    send(&mut w, ack(1)).await; // range readers ack the ABI too

    send(&mut w, status_v0()).await;
    assert!(StatusView::parse(&match read_msg(&mut r, WAIT).await {
        Message::Binary(b) => b,
        other => panic!("{other:?}"),
    })
    .is_some());
    send(&mut w, ack(1)).await;

    for (start, end) in [(100, 200), (200, 300)] {
        send(&mut w, blocks(&request(start, end, 10))).await;
        let mut got = Vec::new();
        while got.len() < 100 {
            let (b, close) = read_blocks(&mut r, 1, WAIT).await;
            assert!(close.is_none() && b.len() == 1, "stalled after {got:?}");
            got.extend(nums(&b));
            send(&mut w, ack(1)).await;
        }
        assert_eq!(got, (start..end).collect::<Vec<_>>());
    }
    let sessions = client_sessions(&a);
    assert_eq!(sessions.len(), 1, "both ranges on one socket: {sessions:?}");

    // Mid-stream with credit outstanding and an end above LIB + 1: blocks
    // can still arrive, so the socket is not reused.
    send(&mut w, blocks(&request(300, 1000, 1000))).await;
    let (first, _) = read_blocks(&mut r, 2, WAIT).await;
    assert_eq!(nums(&first), vec![300, 301]);
    // The new range lies below the old one, so every frame tells which
    // request it belongs to, however many old frames were in flight.
    send(&mut w, blocks(&request(200, 250, 5))).await;
    let mut seq = Vec::new();
    while seq.last() != Some(&204) {
        let (b, close) = read_blocks(&mut r, 1, WAIT).await;
        assert!(close.is_none() && b.len() == 1, "stalled after {seq:?}");
        seq.extend(nums(&b));
    }
    let new_at = seq
        .iter()
        .position(|&n| n == 200)
        .expect("the new request's first block");
    assert!(seq[..new_at].iter().all(|&n| n >= 300), "{seq:?}");
    assert_eq!(seq[new_at..], [200, 201, 202, 203, 204], "{seq:?}");
    assert_eq!(
        client_sessions(&a).len(),
        2,
        "a fresh link for the busy re-request"
    );
}

/// P15: after a failover to a lagging replica, head and LIB never go back.
#[tokio::test]
async fn p15_head_and_lib_never_regress_after_a_switch_to_a_lagging_node() {
    let (ep_a, a) = mock(spring_live(1, 1000)).await;
    let (ep_b, b) = mock(spring_live(1, 990)).await;
    let a = a.start();
    let router = RouterProcess::start(config(&[&ep_a, &ep_b])).await;
    wait_up(&router, &ep_a, "a").await;
    let (mut w, mut r) = client(&router).await;
    let _b = b.start();
    wait_up(&router, &ep_b, "b").await;

    send(&mut w, blocks(&request(900, 2000, 50))).await;
    let (got, _) = read_blocks(&mut r, 50, WAIT).await;
    assert_eq!(nums(&got), (900..950).collect::<Vec<_>>());
    let (head0, lib0) = (got[49].head.0, got[49].lib.0);
    assert_eq!((head0, lib0), (1000, 990));

    a.drop_sessions();
    sleep(Duration::from_millis(100)).await;
    send(&mut w, ack(50)).await;
    let (from_b, close) = read_blocks(&mut r, 41, WAIT).await;
    assert!(close.is_none());
    assert_eq!(nums(&from_b), (950..=990).collect::<Vec<_>>());
    let mut lib = lib0;
    for frame in &from_b {
        assert!(frame.head.0 >= head0, "head went back to {}", frame.head.0);
        assert!(frame.lib.0 >= lib, "LIB went back to {}", frame.lib.0);
        assert!(frame.lib.0 <= frame.head.0);
        lib = frame.lib.0;
    }
    assert!(metric(&router, "fleet_router_patched_results_total").await > 0.0);
}

/// P16: a message above `max_message_bytes` closes the client with a clear
/// reason, and the upstream is not struck.
#[tokio::test]
async fn p16_an_oversized_message_closes_the_client_without_a_strike() {
    let (ep_a, a) = mock(MockShipConfig {
        block_data_size: 131_072,
        ..spring_live(1, 1000)
    })
    .await;
    let _a = a.start();
    let mut cfg = config(&[&ep_a]);
    cfg["max_message_bytes"] = json!(65_536);
    let router = RouterProcess::start(cfg).await;
    wait_up(&router, &ep_a, "a").await;
    let (mut w, mut r) = client(&router).await;

    let req = BlocksReq {
        fetch_block: true,
        ..request(100, 200, 10)
    };
    send(&mut w, blocks(&req)).await;
    let (got, close) = timeout(WAIT, read_blocks(&mut r, 1, WAIT)).await.unwrap();
    assert!(got.is_empty());
    let close = close.expect("a close frame");
    assert_eq!(close.code, CloseCode::Error);
    assert_eq!(close.reason.as_str(), "block 100 exceeds max_message_bytes");
    let l = labels(&ep_a, "a");
    assert_eq!(
        metric(
            &router,
            &format!("fleet_router_upstream_block_failures_total{{{l}}}")
        )
        .await,
        0.0
    );
    assert_eq!(
        metric(&router, "fleet_router_too_large_closes_total").await,
        1.0
    );
}
