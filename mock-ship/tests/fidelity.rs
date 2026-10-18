//! Session behaviour of the mock under each fidelity, through its public API.

use std::time::{Duration, Instant};

use futures::{SinkExt, StreamExt};
use mock_ship::client::{ack, blocks, status_v0, status_v1, BlockView, BlocksReq, StatusView};
use mock_ship::{
    fake_block_id, fake_block_id_seeded, Fidelity, MockShipConfig, MockShipHandle, MockShipServer,
    Observed,
};
use tokio::net::TcpStream;
use tokio::time::{sleep, timeout};
use tokio_tungstenite::tungstenite::{Error as WsError, Message};
use tokio_tungstenite::{connect_async, MaybeTlsStream, WebSocketStream};

const WAIT: Duration = Duration::from_secs(3);
const QUIET: Duration = Duration::from_millis(250);

async fn start(config: MockShipConfig) -> (MockShipHandle, String) {
    let server = MockShipServer::new(config).await;
    let url = server.ws_url();
    (server.start(), url)
}

fn req(start: u32, end: u32, max_in_flight: u32) -> BlocksReq {
    BlocksReq {
        start,
        end,
        max_in_flight,
        ..BlocksReq::default()
    }
}

struct Client {
    ws: WebSocketStream<MaybeTlsStream<TcpStream>>,
}

impl Client {
    /// Connect and consume the ABI text frame.
    async fn connect(url: &str) -> Self {
        let (ws, _) = timeout(WAIT, connect_async(url))
            .await
            .expect("connect timed out")
            .expect("connect failed");
        let mut client = Self { ws };
        match client.next(WAIT).await {
            Some(Ok(Message::Text(_))) => {}
            other => panic!("expected the ABI text frame, got {other:?}"),
        }
        client
    }

    async fn send(&mut self, frame: Vec<u8>) {
        self.ws
            .send(Message::Binary(frame.into()))
            .await
            .expect("send failed");
    }

    /// The next item, or `None` when nothing arrives within `within`.
    async fn next(&mut self, within: Duration) -> Option<Result<Message, WsError>> {
        match timeout(within, self.ws.next()).await {
            Ok(Some(item)) => Some(item),
            Ok(None) => Some(Err(WsError::ConnectionClosed)),
            Err(_) => None,
        }
    }

    async fn binary(&mut self) -> Vec<u8> {
        loop {
            match self.next(WAIT).await {
                Some(Ok(Message::Binary(data))) => return data.to_vec(),
                Some(Ok(Message::Ping(_) | Message::Pong(_))) => continue,
                Some(Ok(other)) => panic!("expected a binary frame, got {other:?}"),
                Some(Err(e)) => panic!("expected a binary frame, the socket ended: {e}"),
                None => panic!("no binary frame within {WAIT:?}"),
            }
        }
    }

    async fn block(&mut self) -> BlockView {
        let data = self.binary().await;
        BlockView::parse(&data)
            .unwrap_or_else(|| panic!("not a block result: {:02x?}", &data[..data.len().min(16)]))
    }

    /// `this_block.block_num` of the next `n` results (`None` = id-less).
    async fn nums(&mut self, n: usize) -> Vec<Option<u32>> {
        let mut out = Vec::with_capacity(n);
        for _ in 0..n {
            out.push(self.block().await.num());
        }
        out
    }

    async fn status(&mut self, v1: bool) -> StatusView {
        self.send(if v1 { status_v1() } else { status_v0() }).await;
        let data = self.binary().await;
        StatusView::parse(&data).expect("not a status result")
    }

    async fn quiet(&mut self) {
        if let Some(item) = self.next(QUIET).await {
            panic!("expected silence, got {item:?}");
        }
    }

    /// The socket ends without a Close frame.
    async fn assert_dropped(&mut self) {
        loop {
            match self.next(WAIT).await {
                Some(Ok(Message::Close(frame))) => {
                    panic!("expected a TCP drop, got Close {frame:?}")
                }
                Some(Ok(Message::Binary(_) | Message::Ping(_) | Message::Pong(_))) => continue,
                Some(Ok(other)) => panic!("unexpected {other:?}"),
                Some(Err(_)) => return,
                None => panic!("the socket stayed open"),
            }
        }
    }
}

fn some(range: std::ops::RangeInclusive<u32>) -> Vec<Option<u32>> {
    range.map(Some).collect()
}

#[tokio::test]
async fn spring_send_rule_gates_on_head_lib_and_end() {
    let (mock, url) = start(MockShipConfig::live(1, 105)).await;
    let mut c = Client::connect(&url).await;

    c.send(blocks(&req(100, 200, 50))).await;
    let first = c.block().await;
    assert_eq!(first.variant, 1);
    assert_eq!(first.head, (105, fake_block_id(105)));
    assert_eq!(first.lib, (95, fake_block_id(95)));
    assert_eq!(first.num(), Some(100));
    assert_eq!(c.nums(5).await, some(101..=105));
    c.quiet().await;

    mock.set_head(108, 98);
    assert_eq!(c.nums(3).await, some(106..=108));
    c.quiet().await;

    // irreversible_only gates on LIB. A new request replaces cursor and credit.
    c.send(blocks(&BlocksReq {
        irreversible_only: true,
        ..req(95, 200, 50)
    }))
    .await;
    assert_eq!(c.nums(4).await, some(95..=98));
    c.quiet().await;
    mock.advance(2);
    assert_eq!(c.nums(2).await, some(99..=100));
    c.quiet().await;

    // end is exclusive.
    c.send(blocks(&req(50, 53, 50))).await;
    assert_eq!(c.nums(3).await, some(50..=52));
    c.quiet().await;
}

#[tokio::test]
async fn spring_results_and_status_follow_the_request_version() {
    let (_mock, url) = start(MockShipConfig {
        chain_state_begin_block: Some(55),
        finality_data_begin_block: 52,
        chain_id: [9; 32],
        ..MockShipConfig::live(50, 60)
    })
    .await;
    let mut c = Client::connect(&url).await;

    let v0 = c.status(false).await;
    assert_eq!((v0.variant, v0.len), (0, 121));
    assert_eq!((v0.trace, v0.chain_state), ((50, 61), (55, 61)));
    assert_eq!(v0.chain_id, Some([9; 32]));
    let v1 = c.status(true).await;
    assert_eq!((v1.variant, v1.len), (3, 129));
    assert_eq!(v1.finality, Some((52, 61)));

    let fetch_all = BlocksReq {
        v1: true,
        fetch_traces: true,
        fetch_deltas: true,
        fetch_finality_data: true,
        ..req(48, 70, 4)
    };
    c.send(blocks(&fetch_all)).await;
    // Below the first known id: id-less results that still cost a credit.
    for expected in [48, 49] {
        let b = c.block().await;
        assert_eq!(
            (b.variant, b.this, b.prev, b.traces.as_ref()),
            (2, None, None, None)
        );
        assert_eq!(b.head.0, 60, "block {expected}");
    }
    let b50 = c.block().await;
    assert_eq!(b50.num(), Some(50));
    assert_eq!(b50.prev, Some((49, fake_block_id(49))));
    assert!(b50.traces.is_some());
    assert_eq!((b50.deltas.as_ref(), b50.finality.as_ref()), (None, None));
    assert_eq!(c.nums(1).await, vec![Some(51)]);
    c.quiet().await;

    c.send(ack(3)).await;
    let b52 = c.block().await;
    assert!(b52.finality.is_some());
    assert_eq!(c.nums(2).await, some(53..=54));
    c.quiet().await;

    // A v0 request gets index 1, and no finality field exists.
    c.send(blocks(&BlocksReq {
        fetch_deltas: true,
        ..req(55, 57, 10)
    }))
    .await;
    let b55 = c.block().await;
    assert_eq!((b55.variant, b55.num()), (1, Some(55)));
    assert!(b55.deltas.is_some());
}

#[tokio::test]
async fn acks_wrap_like_spring() {
    let (mock, url) = start(MockShipConfig::live(1, 11)).await;
    let mut c = Client::connect(&url).await;
    c.send(blocks(&req(10, 100, 2))).await;
    assert_eq!(c.nums(2).await, some(10..=11));

    // (u32::MAX - 1) + 3 wraps to a credit of 1.
    c.send(ack(u32::MAX - 1)).await;
    c.send(ack(3)).await;
    c.quiet().await;
    mock.set_head(20, 10);
    assert_eq!(c.nums(1).await, vec![Some(12)]);
    c.quiet().await;

    let acks: Vec<u32> = mock
        .requests()
        .iter()
        .filter_map(|o| match o {
            Observed::Ack { n, .. } => Some(*n),
            _ => None,
        })
        .collect();
    assert_eq!(acks, vec![u32::MAX - 1, 3]);
}

#[tokio::test]
async fn legacy_ack_overflow_does_not_kill_the_session() {
    let (_mock, url) = start(MockShipConfig::default()).await;
    let mut c = Client::connect(&url).await;
    c.send(blocks(&req(100, 103, u32::MAX))).await;
    assert_eq!(c.nums(3).await, some(100..=102));
    c.send(ack(u32::MAX)).await;
    assert_eq!(c.status(false).await.head.0, 1000);
}

#[tokio::test]
async fn legacy_parses_positions_and_records_requests() {
    let (mock, url) = start(MockShipConfig {
        block_data_size: 8,
        ..MockShipConfig::default()
    })
    .await;
    let mut c = Client::connect(&url).await;
    let positions = vec![(90, fake_block_id(90)), (91, fake_block_id(91))];
    c.send(blocks(&BlocksReq {
        have_positions: positions.clone(),
        fetch_traces: true,
        ..req(100, 102, 10)
    }))
    .await;
    for n in 100..102 {
        let b = c.block().await;
        assert_eq!(b.num(), Some(n));
        assert_eq!((b.block.as_ref(), b.deltas.as_ref()), (None, None));
        assert_eq!(b.traces.as_ref().map(Vec::len), Some(8));
    }
    c.send(ack(1)).await;
    c.status(false).await;

    let log = mock.requests();
    assert_eq!(log[0], Observed::Accepted { session: 1 });
    assert_eq!(
        log[1],
        Observed::Blocks {
            session: 1,
            v1: false,
            start: 100,
            end: 102,
            max_in_flight: 10,
            have_positions: positions,
            irreversible_only: false,
            fetch_block: false,
            fetch_traces: true,
            fetch_deltas: false,
            fetch_finality_data: false,
        }
    );
    assert_eq!(log[2], Observed::Ack { session: 1, n: 1 });
    assert_eq!(
        log[3],
        Observed::Status {
            session: 1,
            v1: false
        }
    );
    assert_eq!(mock.sessions_accepted(), 1);
}

#[tokio::test]
async fn eosio2_sends_head_only_keepalives() {
    let (mock, url) = start(MockShipConfig {
        fidelity: Fidelity::Eosio2,
        ..MockShipConfig::live(1, 100)
    })
    .await;
    let mut c = Client::connect(&url).await;

    // Above head: one keep-alive per request and per new block, each costing
    // a credit without moving the cursor.
    c.send(blocks(&req(105, 200, 4))).await;
    let keepalive = c.block().await;
    assert_eq!((keepalive.variant, keepalive.this), (1, None));
    assert_eq!(keepalive.head, (100, fake_block_id(100)));
    c.quiet().await;
    mock.set_head(101, 91);
    assert_eq!(c.block().await.head.0, 101);
    c.quiet().await;
    mock.set_head(105, 95);
    assert_eq!(c.nums(1).await, vec![Some(105)]);
    c.quiet().await;
    mock.set_head(105, 95);
    assert_eq!(c.nums(1).await, vec![None]);
    // Credit is spent: nothing until an ack.
    mock.set_head(106, 96);
    c.quiet().await;
    c.send(ack(1)).await;
    assert_eq!(c.nums(1).await, vec![Some(106)]);

    // EOSIO 2.0 knows no v1 requests.
    c.send(status_v1()).await;
    c.assert_dropped().await;
    let mut v1 = Client::connect(&url).await;
    v1.send(blocks(&BlocksReq {
        v1: true,
        ..req(1, 10, 10)
    }))
    .await;
    v1.assert_dropped().await;
}

#[tokio::test]
async fn max_sessions_answers_503_with_retry_after() {
    let (mock, url) = start(MockShipConfig {
        max_sessions: Some(1),
        ..MockShipConfig::live(1, 100)
    })
    .await;
    let first = Client::connect(&url).await;

    match connect_async(&url).await {
        Err(WsError::Http(response)) => {
            assert_eq!(response.status().as_u16(), 503);
            assert_eq!(
                response
                    .headers()
                    .get("retry-after")
                    .and_then(|v| v.to_str().ok()),
                Some("1")
            );
        }
        Ok(_) => panic!("the second session was accepted"),
        Err(e) => panic!("expected HTTP 503, got {e}"),
    }

    // The slot frees once the first session ends.
    drop(first);
    let deadline = Instant::now() + WAIT;
    loop {
        if connect_async(&url).await.is_ok() {
            break;
        }
        assert!(Instant::now() < deadline, "the slot was never released");
        sleep(Duration::from_millis(50)).await;
    }
    assert_eq!(mock.sessions_accepted(), 2);
}

#[tokio::test]
async fn fork_rewinds_sessions_past_the_fork_point() {
    let (mock, url) = start(MockShipConfig::live(1, 110)).await;
    let mut c = Client::connect(&url).await;
    c.send(blocks(&req(100, 200, 100))).await;
    for n in 100..=110 {
        assert_eq!(c.block().await.this, Some((n, fake_block_id(n))));
    }
    c.quiet().await;

    mock.fork(105, 7);
    assert_eq!(mock.clock().id(105), fake_block_id_seeded(105, 7));
    let first = c.block().await;
    assert_eq!(first.this, Some((105, fake_block_id_seeded(105, 7))));
    assert_eq!(first.prev, Some((104, fake_block_id(104))));
    for n in 106..=110 {
        let b = c.block().await;
        assert_eq!(b.this, Some((n, fake_block_id_seeded(n, 7))));
        assert_eq!(b.prev, Some((n - 1, fake_block_id_seeded(n - 1, 7))));
    }
    assert_eq!(first.head, (110, fake_block_id_seeded(110, 7)));
    c.quiet().await;
}

#[tokio::test]
async fn have_positions_rewind_uses_the_mock_ids() {
    let (mock, url) = start(MockShipConfig::live(1, 120)).await;
    let mut c = Client::connect(&url).await;
    let positions = vec![
        (100, fake_block_id(100)),
        (105, fake_block_id_seeded(105, 3)), // differs: rewind to 105
        (108, fake_block_id(108)),
        (115, [0; 32]), // at or above start: ignored
    ];
    c.send(blocks(&BlocksReq {
        have_positions: positions.clone(),
        ..req(110, 200, 3)
    }))
    .await;
    assert_eq!(c.nums(3).await, some(105..=107));
    assert!(mock.requests().iter().any(|o| matches!(
        o,
        Observed::Blocks { start: 110, have_positions, .. } if *have_positions == positions
    )));
}

#[tokio::test]
async fn first_available_jump_starts_at_the_first_known_block() {
    let (_mock, url) = start(MockShipConfig {
        first_available_jump: true,
        ..MockShipConfig::live(500, 510)
    })
    .await;
    let mut c = Client::connect(&url).await;
    c.send(blocks(&req(100, 1000, 2))).await;
    assert_eq!(c.nums(2).await, some(500..=501));
}

#[tokio::test]
async fn frozen_holds_open_at_its_end_and_takes_a_new_request() {
    let (mock, url) = start(MockShipConfig::frozen(2, 11)).await;
    let mut c = Client::connect(&url).await;
    let status = c.status(false).await;
    assert_eq!((status.head.0, status.lib.0), (10, 10));
    assert_eq!((status.trace, status.chain_state), ((2, 11), (2, 11)));

    c.send(blocks(&req(1, u32::MAX, 50))).await;
    assert_eq!(c.nums(1).await, vec![None]);
    let b2 = c.block().await;
    assert_eq!(b2.this, Some((2, fake_block_id(2))));
    assert_eq!(b2.prev, Some((1, fake_block_id(1))));
    assert_eq!(c.nums(8).await, some(3..=10));
    c.quiet().await;

    assert_eq!(c.status(true).await.len, 129);
    c.send(blocks(&req(5, 7, 50))).await;
    assert_eq!(c.nums(2).await, some(5..=6));
    c.quiet().await;
    assert_eq!(mock.sessions_accepted(), 1);
}

#[tokio::test]
async fn an_archive_without_a_block_log_sends_no_block_and_no_first_prev() {
    let (_mock, url) = start(MockShipConfig {
        no_block_log: true,
        ..MockShipConfig::frozen(100, 111)
    })
    .await;
    let mut c = Client::connect(&url).await;
    c.send(blocks(&BlocksReq {
        fetch_block: true,
        fetch_traces: true,
        fetch_deltas: true,
        ..req(100, 111, 50)
    }))
    .await;
    let first = c.block().await;
    assert_eq!(first.this, Some((100, fake_block_id(100))));
    assert_eq!(
        first.prev, None,
        "the id before the range needs a block log"
    );
    let second = c.block().await;
    assert_eq!(second.prev, Some((100, fake_block_id(100))));
    for b in [&first, &second] {
        assert!(b.block.is_none(), "no block log, no block");
        assert!(b.traces.is_some() && b.deltas.is_some());
    }
    assert_eq!(c.nums(9).await, some(102..=110));
    c.quiet().await;
    // A request that starts inside the range still gets prev_block.
    c.send(blocks(&BlocksReq {
        fetch_block: true,
        ..req(105, 106, 1)
    }))
    .await;
    let mid = c.block().await;
    assert_eq!(mid.prev, Some((104, fake_block_id(104))));
    assert!(mid.block.is_none());
}

#[tokio::test]
async fn sequence_faults_fire_under_spring() {
    let (_skip, skip_url) = start(MockShipConfig {
        skip_at_block: Some(5),
        ..MockShipConfig::frozen(1, 20)
    })
    .await;
    let mut c = Client::connect(&skip_url).await;
    c.send(blocks(&req(3, 10, 50))).await;
    let expected: Vec<Option<u32>> = [3, 4, 6, 7, 8, 9].into_iter().map(Some).collect();
    assert_eq!(c.nums(6).await, expected);
    c.quiet().await;

    let (_headonly, headonly_url) = start(MockShipConfig {
        headonly_at_block: Some(5),
        ..MockShipConfig::frozen(1, 20)
    })
    .await;
    let mut c = Client::connect(&headonly_url).await;
    c.send(blocks(&req(3, 8, 50))).await;
    assert_eq!(
        c.nums(5).await,
        vec![Some(3), Some(4), None, Some(6), Some(7)]
    );

    let (_omit, omit_url) = start(MockShipConfig {
        omit_traces_at_block: Some(5),
        ..MockShipConfig::frozen(1, 20)
    })
    .await;
    let mut c = Client::connect(&omit_url).await;
    c.send(blocks(&BlocksReq {
        fetch_traces: true,
        ..req(4, 7, 50)
    }))
    .await;
    let traces: Vec<bool> = [c.block().await, c.block().await, c.block().await]
        .iter()
        .map(|b| b.traces.is_some())
        .collect();
    assert_eq!(traces, vec![true, false, true]);
}

#[tokio::test]
async fn close_at_block_sends_a_close_frame() {
    let (_mock, url) = start(MockShipConfig {
        close_at_block: Some((5, 1001)),
        ..MockShipConfig::frozen(1, 20)
    })
    .await;
    let mut c = Client::connect(&url).await;
    c.send(blocks(&req(3, 10, 50))).await;
    assert_eq!(c.nums(2).await, some(3..=4));
    match c.next(WAIT).await {
        Some(Ok(Message::Close(Some(frame)))) => assert_eq!(u16::from(frame.code), 1001),
        other => panic!("expected Close 1001, got {other:?}"),
    }
}

#[tokio::test]
async fn spring_drops_malformed_requests_without_close() {
    let (mock, url) = start(MockShipConfig::live(1, 100)).await;
    let mut c = Client::connect(&url).await;
    c.send(vec![9]).await;
    c.assert_dropped().await;

    let mut c = Client::connect(&url).await;
    let mut bad_bool = blocks(&req(1, 10, 10));
    let last = bad_bool.len() - 1;
    bad_bool[last] = 2;
    c.send(bad_bool).await;
    c.assert_dropped().await;

    let unknown: Vec<(u32, u32)> = mock
        .requests()
        .iter()
        .filter_map(|o| match o {
            Observed::Unknown { session, variant } => Some((*session, *variant)),
            _ => None,
        })
        .collect();
    assert_eq!(unknown, vec![(1, 9), (2, 1)]);
}

#[tokio::test]
async fn drop_sessions_kills_running_sessions_and_keeps_accepting() {
    let (mock, url) = start(MockShipConfig::live(1, 1000)).await;
    let mut a = Client::connect(&url).await;
    a.send(blocks(&req(10, 20, 2))).await;
    assert_eq!(a.nums(2).await, some(10..=11));

    mock.drop_sessions();
    a.assert_dropped().await;

    let mut b = Client::connect(&url).await;
    b.send(blocks(&req(10, 20, 2))).await;
    assert_eq!(b.nums(2).await, some(10..=11));
    assert_eq!(mock.sessions_accepted(), 2);
}

#[tokio::test]
async fn throttle_paces_socket_writes() {
    let (_mock, url) = start(MockShipConfig {
        throttle_bytes_per_sec: Some(400_000),
        block_data_size: 100_000,
        ..MockShipConfig::live(1, 100)
    })
    .await;
    let mut c = Client::connect(&url).await;
    let began = Instant::now();
    c.send(blocks(&BlocksReq {
        fetch_block: true,
        ..req(10, 13, 3)
    }))
    .await;
    for n in 10..13 {
        let b = c.block().await;
        assert_eq!(b.num(), Some(n));
        assert_eq!(b.block.as_ref().map(Vec::len), Some(100_000));
    }
    // 300 kB at 400 kB/s, less the last 50 ms pause that is never awaited.
    let elapsed = began.elapsed();
    assert!(
        elapsed >= Duration::from_millis(550),
        "too fast: {elapsed:?}"
    );
    assert!(elapsed < Duration::from_secs(3), "too slow: {elapsed:?}");
}
