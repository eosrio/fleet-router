//! The upstream side of a session: the connection slot counted against an
//! upstream, the spawned dial task, and the live link.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use futures::stream::{SplitSink, SplitStream};
use futures::StreamExt;
use tokio::net::TcpStream;
use tokio::task::JoinHandle;
use tokio::time::{timeout, Instant};
use tokio_tungstenite::tungstenite::protocol::WebSocketConfig;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::{connect_async_with_config, MaybeTlsStream, WebSocketStream};
use tracing::Instrument;

use crate::functions::{select_backend_server, select_failover_server, RouteRequest};
use crate::models::{ProxyLimits, Server, ServerConfigDb, ServerStateDb};

pub(super) type UpstreamWs = WebSocketStream<MaybeTlsStream<TcpStream>>;

/// Upstream picks per connect or failover before the client is closed. A pick
/// that finds no candidate uses one up.
pub(super) const MAX_DIAL_ATTEMPTS: u32 = 3;

/// Holds one unit of an upstream's active-connection counter. Decrements
/// synchronously (and underflow-safely) when dropped — no spawn, no lock, no
/// dependency on a live runtime handle.
pub struct ConnectionGuard {
    counter: Arc<AtomicUsize>,
}

impl ConnectionGuard {
    /// Take one unit of `counter`; it is given back when the guard drops.
    fn acquire(counter: Arc<AtomicUsize>) -> Self {
        counter.fetch_add(1, Ordering::Relaxed);
        Self { counter }
    }
}

impl Drop for ConnectionGuard {
    fn drop(&mut self) {
        // Saturating decrement: never underflows below zero.
        let _ = self
            .counter
            .try_update(Ordering::Relaxed, Ordering::Relaxed, |c| c.checked_sub(1));
    }
}

/// What a dial is for.
#[derive(Clone, Debug)]
pub(super) struct DialPlan {
    /// The in-flight request's start block and finality flag, if any.
    pub route: RouteRequest,
    /// Lets an existing session retry an enabled upstream the monitor has
    /// already marked offline. New client sessions pass `false` and still
    /// require `online`.
    pub failover: bool,
    /// The upstream that just failed at this block, when another upstream at
    /// the same or better rank should be preferred.
    pub exclude: Option<String>,
}

impl DialPlan {
    /// The first dial of a session. The client has not sent a blocks request
    /// yet, so this pick is least-connections. A later
    /// `get_blocks_request_v1` with `fetch_finality_data` may reroute.
    pub fn initial() -> Self {
        Self {
            route: RouteRequest::default(),
            failover: false,
            exclude: None,
        }
    }
}

/// Select an upstream for `plan` and take a connection slot on it.
///
/// The counter is incremented optimistically under the state lock, so
/// concurrent sessions see each other's picks. The guard decrements on drop,
/// including when the dial is aborted (for example because the client session
/// is closing).
pub(super) async fn pick_upstream(
    backend_servers: &ServerStateDb,
    server_config_db: &ServerConfigDb,
    plan: &DialPlan,
) -> Option<(Server, ConnectionGuard)> {
    let mut state = backend_servers.lock().await;
    let selected = if plan.failover {
        select_failover_server(&mut state, plan.route, plan.exclude.as_deref())
    } else {
        select_backend_server(&mut state, plan.route)
    }
    .ok()?;
    let counter = state.get(&selected)?.connections.clone();
    let server = server_config_db.lock().await.get(&selected).cloned()?;
    Some((server, ConnectionGuard::acquire(counter)))
}

async fn mark_offline(backend_servers: &ServerStateDb, endpoint: &str) {
    if let Some(s) = backend_servers.lock().await.get_mut(endpoint) {
        s.online = false;
    }
}

/// Log a failed dial and mark that upstream offline, so the next pick in this
/// session (and new sessions) moves on to another upstream.
pub(super) async fn report_dial_failure(
    backend_servers: &ServerStateDb,
    endpoint: &str,
    error: DialError,
) {
    match error {
        DialError::Connect(error) => {
            tracing::warn!(upstream = %endpoint, error = %error, "error connecting to upstream");
        }
        DialError::NoAbi => {
            tracing::warn!(upstream = %endpoint, "invalid or absent ABI handshake; trying another upstream");
        }
    }
    mark_offline(backend_servers, endpoint).await;
}

/// Read the upstream's first meaningful frame, which must be the Text ABI.
/// Leading Ping/Pong control frames are skipped. Returns `None` (so the caller
/// can try another upstream) on timeout, Close, Binary-first, or error.
async fn read_first_abi(ws: &mut UpstreamWs, handshake_timeout_ms: u64) -> Option<String> {
    // Single overall deadline for the whole handshake: a peer that drips
    // Ping/Pong frames cannot keep it alive past the timeout.
    let read = async {
        loop {
            match ws.next().await {
                Some(Ok(Message::Text(text))) => return Some(text.to_string()),
                Some(Ok(Message::Ping(_))) | Some(Ok(Message::Pong(_))) => continue,
                _ => return None,
            }
        }
    };
    if handshake_timeout_ms > 0 {
        // On timeout (Err), unwrap_or_default yields None — treat as a failed handshake.
        timeout(Duration::from_millis(handshake_timeout_ms), read)
            .await
            .unwrap_or_default()
    } else {
        read.await
    }
}

/// A connected upstream that has sent its ABI.
pub(super) struct Established {
    pub ws: UpstreamWs,
    pub abi: String,
    pub guard: ConnectionGuard,
    pub endpoint: String,
    pub name: String,
}

#[derive(Debug)]
pub(super) enum DialError {
    /// The TCP, TLS or WebSocket handshake failed.
    Connect(String),
    /// The upstream did not open with its Text ABI (timeout, Close,
    /// Binary-first, or a read error).
    NoAbi,
}

/// A dial running in its own task, so the session loop keeps serving the
/// client (and shutdown) while the handshake is in progress.
///
/// Dropping a `DialTask` aborts it; the connection slot it holds is released
/// when the aborted task is dropped.
pub(super) struct DialTask {
    endpoint: String,
    handle: JoinHandle<Result<Established, DialError>>,
}

impl DialTask {
    /// Start dialing `server`. The task runs in the caller's tracing span.
    pub fn spawn(server: Server, guard: ConnectionGuard, limits: ProxyLimits) -> Self {
        let endpoint = server.endpoint.clone();
        let handle = tokio::spawn(dial(server, guard, limits).in_current_span());
        Self { endpoint, handle }
    }

    /// The upstream being dialed.
    pub fn endpoint(&self) -> &str {
        &self.endpoint
    }

    /// Wait for the dial to finish. Cancel-safe: a wait that is dropped
    /// leaves the task running, and a later call picks up its result.
    pub async fn outcome(&mut self) -> Result<Established, DialError> {
        match (&mut self.handle).await {
            Ok(outcome) => outcome,
            Err(error) => Err(DialError::Connect(format!("dial task failed: {error}"))),
        }
    }
}

impl Drop for DialTask {
    fn drop(&mut self) {
        self.handle.abort();
    }
}

/// Connect to `server` and read its ABI. There is no connect timeout; the
/// session aborts the task when the client leaves or the router shuts down.
async fn dial(
    server: Server,
    guard: ConnectionGuard,
    limits: ProxyLimits,
) -> Result<Established, DialError> {
    let mut config = WebSocketConfig::default();
    config.max_message_size = Some(limits.max_message_bytes);
    config.max_frame_size = Some(limits.max_message_bytes);

    let mut ws = match connect_async_with_config(server.ws_url(), Some(config), true).await {
        Ok((ws, _)) => ws,
        Err(error) => {
            drop(guard);
            return Err(DialError::Connect(error.to_string()));
        }
    };

    match read_first_abi(&mut ws, limits.handshake_timeout_ms).await {
        Some(abi) => Ok(Established {
            ws,
            abi,
            guard,
            endpoint: server.endpoint,
            name: server.name,
        }),
        None => {
            drop(guard);
            let _ = ws.close(None).await;
            Err(DialError::NoAbi)
        }
    }
}

/// The session's live upstream socket.
pub(super) struct Link {
    pub tx: SplitSink<UpstreamWs, Message>,
    pub rx: SplitStream<UpstreamWs>,
    guard: ConnectionGuard,
    /// A Close frame was read from this link, so its end is clean.
    pub close_seen: bool,
    /// When the wait for this link's next frame began (`idle_timeout_ms`).
    pub idle_from: Instant,
}

impl Link {
    pub fn new(ws: UpstreamWs, guard: ConnectionGuard) -> Self {
        let (tx, rx) = ws.split();
        Self {
            tx,
            rx,
            guard,
            close_seen: false,
            idle_from: Instant::now(),
        }
    }

    /// Drop the socket and keep only the connection slot.
    pub fn retire(self) -> ConnectionGuard {
        self.guard
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::{build_config_db, build_state_db};
    use futures::SinkExt;
    use tokio::net::TcpListener;

    const ABI: &str = r#"{"version":"eosio::abi/1.1"}"#;

    fn limits() -> ProxyLimits {
        ProxyLimits {
            handshake_timeout_ms: 2_000,
            idle_timeout_ms: 0,
            max_message_bytes: 1 << 20,
            max_client_message_bytes: 64 * 1024,
        }
    }

    fn server(endpoint: &str) -> Server {
        Server {
            name: "test-upstream".to_string(),
            endpoint: endpoint.to_string(),
            enabled: true,
        }
    }

    fn slots(counter: &AtomicUsize) -> usize {
        counter.load(Ordering::Relaxed)
    }

    /// One-connection upstream that completes the WebSocket handshake, sends
    /// `first`, then reads until the dialer goes away.
    async fn upstream_sending(first: Message) -> String {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = listener.local_addr().unwrap().to_string();
        tokio::spawn(async move {
            let (tcp, _) = listener.accept().await.unwrap();
            let mut ws = tokio_tungstenite::accept_async(tcp).await.unwrap();
            ws.send(first).await.unwrap();
            while let Some(Ok(_)) = ws.next().await {}
        });
        endpoint
    }

    #[tokio::test]
    async fn dial_returns_the_text_abi_and_holds_the_slot() {
        let endpoint = upstream_sending(Message::text(ABI)).await;
        let counter = Arc::new(AtomicUsize::new(0));
        let guard = ConnectionGuard::acquire(counter.clone());
        let mut task = DialTask::spawn(server(&endpoint), guard, limits());
        assert_eq!(task.endpoint(), endpoint);

        let established = task.outcome().await.expect("dial should succeed");
        assert_eq!(established.abi, ABI);
        assert_eq!(established.endpoint, endpoint);
        assert_eq!(established.name, "test-upstream");
        assert_eq!(slots(&counter), 1, "an established link keeps its slot");

        let link = Link::new(established.ws, established.guard);
        let guard = link.retire();
        assert_eq!(slots(&counter), 1, "a retired link keeps its slot");
        drop(guard);
        assert_eq!(slots(&counter), 0);
    }

    #[tokio::test]
    async fn dial_rejects_a_binary_first_frame_and_releases_the_slot() {
        let endpoint = upstream_sending(Message::binary(vec![0u8; 4])).await;
        let counter = Arc::new(AtomicUsize::new(0));
        let guard = ConnectionGuard::acquire(counter.clone());
        let mut task = DialTask::spawn(server(&endpoint), guard, limits());

        assert!(matches!(task.outcome().await, Err(DialError::NoAbi)));
        assert_eq!(slots(&counter), 0);
    }

    #[tokio::test]
    async fn dial_reports_a_refused_connect_and_releases_the_slot() {
        let endpoint = {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            listener.local_addr().unwrap().to_string()
        };
        let counter = Arc::new(AtomicUsize::new(0));
        let guard = ConnectionGuard::acquire(counter.clone());
        let mut task = DialTask::spawn(server(&endpoint), guard, limits());

        assert!(matches!(task.outcome().await, Err(DialError::Connect(_))));
        assert_eq!(slots(&counter), 0);
    }

    #[tokio::test]
    async fn dropping_a_dial_task_aborts_it_and_releases_the_slot() {
        // Accept TCP and never answer the WebSocket upgrade, so the dial hangs.
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = listener.local_addr().unwrap().to_string();
        let (accepted_tx, accepted_rx) = tokio::sync::oneshot::channel();
        let holder = tokio::spawn(async move {
            let (tcp, _) = listener.accept().await.unwrap();
            let _ = accepted_tx.send(());
            std::future::pending::<()>().await;
            drop(tcp);
        });

        let counter = Arc::new(AtomicUsize::new(0));
        let guard = ConnectionGuard::acquire(counter.clone());
        let task = DialTask::spawn(server(&endpoint), guard, limits());
        timeout(Duration::from_secs(5), accepted_rx)
            .await
            .expect("dial should reach the upstream")
            .unwrap();
        assert_eq!(slots(&counter), 1);

        drop(task);
        timeout(Duration::from_secs(5), async {
            while slots(&counter) != 0 {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("an aborted dial must release its slot");
        holder.abort();
    }

    #[tokio::test]
    async fn pick_takes_a_slot_on_the_selected_upstream() {
        let servers = vec![server("127.0.0.1:1")];
        let state = build_state_db(servers.clone());
        let config = build_config_db(servers);

        // A new session requires an online upstream; a failover does not.
        assert!(pick_upstream(&state, &config, &DialPlan::initial())
            .await
            .is_none());
        let failover = DialPlan {
            route: RouteRequest::default(),
            failover: true,
            exclude: None,
        };
        let (picked, guard) = pick_upstream(&state, &config, &failover)
            .await
            .expect("failover may retry an offline upstream");
        assert_eq!(picked.endpoint, "127.0.0.1:1");
        let counter = state
            .lock()
            .await
            .get("127.0.0.1:1")
            .unwrap()
            .connections
            .clone();
        assert_eq!(slots(&counter), 1);
        drop(guard);
        assert_eq!(slots(&counter), 0);

        state.lock().await.get_mut("127.0.0.1:1").unwrap().online = true;
        let (_, guard) = pick_upstream(&state, &config, &DialPlan::initial())
            .await
            .expect("an online upstream is picked for a new session");
        assert_eq!(slots(&counter), 1);
        drop(guard);
    }

    #[tokio::test]
    async fn a_failed_dial_marks_the_upstream_offline() {
        let servers = vec![server("127.0.0.1:1")];
        let state = build_state_db(servers);
        state.lock().await.get_mut("127.0.0.1:1").unwrap().online = true;

        report_dial_failure(&state, "127.0.0.1:1", DialError::NoAbi).await;
        assert!(!state.lock().await.get("127.0.0.1:1").unwrap().online);
    }
}
