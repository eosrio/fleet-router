use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, AtomicUsize};
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{anyhow, bail, Result};
use serde::{Deserialize, Serialize};
use tokio::sync::Mutex;
use tokio::sync::Notify;

use crate::abi::{accept_upstream_abi, AbiEntry};
use crate::ship::{BlockPos, Range};

pub type ServerStateDb = Arc<Mutex<HashMap<String, ServerState>>>;
pub type ServerConfigDb = Arc<Mutex<HashMap<String, Server>>>;

// ---------------------------------------------------------------------------
// serde defaults for optional, backwards-compatible config fields
// ---------------------------------------------------------------------------

fn default_max_connections() -> usize {
    10_000
}
fn default_handshake_timeout_ms() -> u64 {
    10_000
}
fn default_idle_timeout_ms() -> u64 {
    0 // 0 = disabled
}
fn default_upstream_liveness_timeout_ms() -> u64 {
    4_000
}
fn default_max_message_bytes() -> usize {
    256 * 1024 * 1024 // 256 MiB
}
fn default_max_client_message_bytes() -> usize {
    64 * 1024 // 64 KiB; client SHiP requests are tiny
}
fn default_shutdown_grace_ms() -> u64 {
    5_000
}
fn default_quarantine_strikes() -> u32 {
    2
}
fn default_quarantine_window_secs() -> u64 {
    300
}
fn default_quarantine_ttl_secs() -> u64 {
    600
}
fn default_quarantine_range() -> u32 {
    1
}
fn default_predial_blocks() -> u32 {
    2_000
}
fn default_gap_wait_ms() -> u64 {
    30_000
}
fn default_patch_head_lib() -> bool {
    true
}
const DEFAULT_STALL_TIMEOUT_MS: u64 = 60_000;
fn default_stall_timeout_ms() -> u64 {
    DEFAULT_STALL_TIMEOUT_MS
}
fn is_default_stall_timeout_ms(value: &u64) -> bool {
    *value == DEFAULT_STALL_TIMEOUT_MS
}
const DEFAULT_LIVE_LAG_BLOCKS: u32 = 1_200;
fn default_live_lag_blocks() -> u32 {
    DEFAULT_LIVE_LAG_BLOCKS
}
fn is_default_live_lag_blocks(value: &u32) -> bool {
    *value == DEFAULT_LIVE_LAG_BLOCKS
}

/// Upper bound for [`StitchingPolicy::gap_wait_ms`] (one hour).
const MAX_GAP_WAIT_MS: u64 = 3_600_000;

/// What kind of upstream a server is.
///
/// `live` is a node at the chain head (today's only kind). `frozen` is an
/// archive upstream that serves a fixed block range from archived logs and
/// never advances. The role is declared, never inferred: a stuck live node
/// looks exactly like a frozen one.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Role {
    #[default]
    Live,
    Frozen,
}

impl Role {
    pub fn is_live(&self) -> bool {
        matches!(self, Role::Live)
    }

    /// Label value used on `/metrics`.
    pub fn as_str(&self) -> &'static str {
        match self {
            Role::Live => "live",
            Role::Frozen => "frozen",
        }
    }
}

/// How a listener presents its pool to clients.
///
/// `pool`: every upstream is a full node and the client talks to one of them
/// (today's behaviour). `stitched`: the router presents the pool as a single
/// SHiP node whose history is the union of its upstreams. `auto` picks
/// `stitched` when any enabled server has `role: "frozen"`.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Topology {
    #[default]
    Auto,
    Pool,
    Stitched,
}

impl Topology {
    pub fn is_auto(&self) -> bool {
        matches!(self, Topology::Auto)
    }

    /// Resolve `auto` against the listener's servers.
    pub fn resolve(self, servers: &[Server]) -> ResolvedTopology {
        match self {
            Topology::Pool => ResolvedTopology::Pool,
            Topology::Stitched => ResolvedTopology::Stitched,
            Topology::Auto if has_enabled_frozen(servers) => ResolvedTopology::Stitched,
            Topology::Auto => ResolvedTopology::Pool,
        }
    }
}

/// [`Topology`] after `auto` has been resolved.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ResolvedTopology {
    Pool,
    Stitched,
}

/// Which role serves blocks that both a frozen and a live upstream cover.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum HistoryPreference {
    /// Prefer frozen upstreams, keeping history reads off the live nodes.
    #[default]
    Frozen,
    /// No role preference; among upstreams of equal `priority`, the longest
    /// run wins, and load per `weight` only splits equal runs.
    Any,
}

/// What to do with an upstream whose ABI differs from the one clients get.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum AbiMismatch {
    /// Log a warning and export the mismatch; keep using the upstream.
    #[default]
    Warn,
    /// Stop routing sessions to the upstream.
    Exclude,
}

/// Session policy for a stitched listener. Ignored by pool listeners.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StitchingPolicy {
    /// Default `frozen`.
    #[serde(default)]
    pub history_preference: HistoryPreference,
    /// Open the next range's upstream this many blocks before a boundary.
    /// Default 2000. 0 disables pre-dialing.
    #[serde(default = "default_predial_blocks")]
    pub predial_blocks: u32,
    /// How long (ms) a session waits for an offline, busy or full archive
    /// range before closing with 1011. Default 30000, `1..=3600000`.
    #[serde(default = "default_gap_wait_ms")]
    pub gap_wait_ms: u64,
    /// Rewrite head and LIB in archive results with the fleet's live values.
    /// Default `true`.
    #[serde(default = "default_patch_head_lib")]
    pub patch_head_lib: bool,
    /// Serve this exact ABI text to clients instead of one taken from an
    /// upstream.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub abi_file: Option<PathBuf>,
    /// Default `warn`.
    #[serde(default)]
    pub abi_mismatch: AbiMismatch,
}

impl Default for StitchingPolicy {
    fn default() -> Self {
        StitchingPolicy {
            history_preference: HistoryPreference::default(),
            predial_blocks: default_predial_blocks(),
            gap_wait_ms: default_gap_wait_ms(),
            patch_head_lib: default_patch_head_lib(),
            abi_file: None,
            abi_mismatch: AbiMismatch::default(),
        }
    }
}

fn has_enabled_frozen(servers: &[Server]) -> bool {
    servers.iter().any(|s| s.enabled && s.role == Role::Frozen)
}
fn default_server_weight() -> u32 {
    1
}
fn is_default_server_weight(value: &u32) -> bool {
    *value == default_server_weight()
}
fn is_default_priority(value: &u32) -> bool {
    *value == 0
}

/// Opt-in shared exclusion of one upstream for a short block range.
///
/// After `strikes` unclean mid-`get_blocks` drops at the same block K inside
/// `window_secs`, from any session on this listener, that upstream is skipped
/// for requests starting in `K..K+range` for `ttl_secs`. It stays online for
/// every other range. Omit the object to disable this; per-session retry
/// bounding still applies.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct BlockFailureQuarantine {
    /// Drops at one block required before exclusion. Default 2.
    #[serde(default = "default_quarantine_strikes")]
    pub strikes: u32,
    /// Seconds a strike stays in the window. Default 300.
    #[serde(default = "default_quarantine_window_secs")]
    pub window_secs: u64,
    /// Seconds the exclusion lasts once armed. Default 600.
    #[serde(default = "default_quarantine_ttl_secs")]
    pub ttl_secs: u64,
    /// Blocks excluded starting at K (`K..K+range`). Default 1 (only K).
    #[serde(default = "default_quarantine_range")]
    pub range: u32,
}

/// One upstream skipped for `[block, block + range)` until `until`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BlockQuarantine {
    pub block: u32,
    pub range: u32,
    pub until: Instant,
}

impl BlockQuarantine {
    /// Whether a request starting at `block` falls in this exclusion.
    pub fn covers(&self, block: u32) -> bool {
        match self.block.checked_add(self.range) {
            Some(end) => self.block <= block && block < end,
            None => block >= self.block,
        }
    }

    pub fn active(&self, now: Instant) -> bool {
        now < self.until
    }
}

#[derive(Debug, Clone, Copy)]
struct BlockStrike {
    block: u32,
    at: Instant,
}

/// Connect / first-read bound used by the upstream monitor and `config test`
/// when `handshake_timeout_ms` is 0. A zero handshake timeout only disables the
/// *client* handshake limit; a silent upstream must not stall monitoring.
const DEFAULT_UPSTREAM_CONNECT_TIMEOUT_MS: u64 = 10_000;

/// A single upstream SHiP node, as declared in `config.json`.
#[derive(Debug, Deserialize, Serialize, Hash, Eq, PartialEq, Clone)]
pub struct Server {
    /// Human-readable name used in logs and as the `server` label on `/metrics`.
    pub name: String,
    /// Upstream WebSocket address.
    ///
    /// `host:port` is dialed as plaintext `ws://`. An explicit `ws://` or
    /// `wss://` scheme is preserved (`wss://` is TLS, verified against the OS
    /// trust store).
    pub endpoint: String,
    /// Whether the router is allowed to use this upstream.
    pub enabled: bool,
    /// `live` (default) or `frozen`. See [`Role`].
    #[serde(default, skip_serializing_if = "Role::is_live")]
    pub role: Role,
    /// Declared coverage of a frozen server, `[begin, end)`. Used until the
    /// server reports its ranges, and while it is down. Frozen servers only.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub range: Option<Range>,
    /// Maximum links the router's sessions open to this upstream. The monitor
    /// connection is not counted.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_sessions: Option<u32>,
    /// Preference tier. Lower is preferred; `0` (the default) is the highest.
    ///
    /// Among upstreams that are equally able to serve a request, the router
    /// picks from the lowest tier first. Coverage of the requested block and
    /// freshness still rank above priority, so a preferred upstream that lacks
    /// the block never wins over one that has it.
    #[serde(default, skip_serializing_if = "is_default_priority")]
    pub priority: u32,
    /// Relative share of client sessions within the same priority tier.
    /// Must be at least `1` (the default).
    ///
    /// Selection compares active connections divided by weight, so an
    /// upstream with weight `2` takes about twice the sessions of one with `1`.
    #[serde(
        default = "default_server_weight",
        skip_serializing_if = "is_default_server_weight"
    )]
    pub weight: u32,
    /// The server's nodeos-compatible HTTP chain API: `http://host[:port]`
    /// or `host:port`. For a live server, its nodeos `http-server-address`;
    /// for a frozen range server, its chain API address. Used only by a
    /// listener with [`ChainApiConfig`].
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub api_endpoint: Option<String>,
}

impl Server {
    /// URL passed to the WebSocket client.
    ///
    /// A bare `host:port` becomes `ws://host:port`. An explicit scheme is
    /// lowercased; `validate` has already rejected anything other than `ws`
    /// or `wss`.
    pub fn ws_url(&self) -> String {
        dial_url(&self.endpoint)
    }
}

/// Test fixtures build servers with `..Default::default()`. Not derived, so
/// the default `weight` is the configured default (1), not an invalid 0.
#[cfg(test)]
impl Default for Server {
    fn default() -> Self {
        Server {
            name: String::new(),
            endpoint: String::new(),
            enabled: false,
            role: Role::default(),
            range: None,
            max_sessions: None,
            priority: 0,
            weight: default_server_weight(),
            api_endpoint: None,
        }
    }
}

/// True when `scheme` matches RFC 3986's scheme production.
fn is_uri_scheme(scheme: &str) -> bool {
    let mut chars = scheme.chars();
    match chars.next() {
        Some(c) if c.is_ascii_alphabetic() => {}
        _ => return false,
    }
    chars.all(|c| c.is_ascii_alphanumeric() || matches!(c, '+' | '-' | '.'))
}

/// Dial URL for `endpoint`. Bare addresses get a `ws://` prefix.
fn dial_url(endpoint: &str) -> String {
    let endpoint = endpoint.trim();
    if let Some((scheme, rest)) = endpoint.split_once("://") {
        if is_uri_scheme(scheme) {
            return format!("{}://{rest}", scheme.to_ascii_lowercase());
        }
    }
    format!("ws://{endpoint}")
}

/// Canonical dial target used to reject two spellings of the same upstream.
///
/// `127.0.0.1:8080` and `ws://127.0.0.1:8080` share an identity. `ws://` and
/// `wss://` do not. The default port is included (`80` for `ws`, `443` for
/// `wss`) so `ws://host` and `ws://host:80` collide.
fn upstream_identity(endpoint: &str) -> Result<String> {
    let trimmed = endpoint.trim();
    if let Some((scheme, _)) = trimmed.split_once("://") {
        if !is_uri_scheme(scheme) {
            bail!(
                "endpoint '{endpoint}' has a malformed scheme; expected host:port, ws://, or wss://"
            );
        }
        let scheme = scheme.to_ascii_lowercase();
        if scheme != "ws" && scheme != "wss" {
            bail!(
                "endpoint '{endpoint}' uses unsupported scheme '{scheme}'; expected host:port, ws://, or wss://"
            );
        }
    }

    let url = dial_url(trimmed);
    let uri: http::Uri = url.parse().map_err(|err| {
        anyhow!(
            "endpoint '{endpoint}' is not a valid WebSocket URL ({err}); expected host:port, ws://, or wss://"
        )
    })?;
    let scheme = uri.scheme_str().unwrap_or("");
    if scheme != "ws" && scheme != "wss" {
        bail!(
            "endpoint '{endpoint}' uses unsupported scheme '{scheme}'; expected host:port, ws://, or wss://"
        );
    }
    let Some(authority) = uri.authority() else {
        bail!(
            "endpoint '{endpoint}' is missing a host; expected host:port, ws://host:port, or wss://host:port"
        );
    };
    // `Authority::host` keeps the brackets on an IPv6 literal.
    let host = authority.host();
    if host.is_empty() {
        bail!(
            "endpoint '{endpoint}' is missing a host; expected host:port, ws://host:port, or wss://host:port"
        );
    }
    let port = authority
        .port_u16()
        .unwrap_or(if scheme == "wss" { 443 } else { 80 });
    let host = host.to_ascii_lowercase();
    let userinfo = match authority.as_str().rsplit_once('@') {
        Some((user, _)) => format!("{user}@"),
        None => String::new(),
    };
    let path = uri.path();
    let query = uri
        .query()
        .map(|query| format!("?{query}"))
        .unwrap_or_default();
    Ok(format!("{scheme}://{userinfo}{host}:{port}{path}{query}"))
}

/// An ingress listener mapping a local port to a dedicated pool of upstream SHiP nodes.
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub struct ListenerConfig {
    /// Unique name for this listener/network (used in logs, `/metrics`, and `/ready/<name>`).
    pub name: String,
    /// Optional bind address override; falls back to top-level `listen_address` or `"0.0.0.0"`.
    #[serde(default)]
    pub listen_address: Option<String>,
    /// Port the proxy listens on for client connections for this network.
    pub listen_port: u16,
    /// The upstream SHiP nodes to load-balance across for this listener.
    pub servers: Vec<Server>,
    /// Optional per-listener connection cap; falls back to top-level `max_connections`.
    #[serde(default)]
    pub max_connections: Option<usize>,
    /// Optional expected Antelope `chain_id` (64-character hex string) for upstreams in this pool.
    #[serde(default)]
    pub chain_id: Option<String>,
    /// When `true`, a status that omits `chain_id` marks the upstream offline.
    /// Requires [`Self::chain_id`]. Default `false` keeps older nodeos builds
    /// (which never send that optional extension) online.
    #[serde(default)]
    pub require_chain_id: bool,
    /// Opt-in per-range exclusion after repeated unclean drops at one block.
    /// See [`BlockFailureQuarantine`].
    #[serde(default)]
    pub block_failure_quarantine: Option<BlockFailureQuarantine>,
    /// See [`Topology`]. Default `auto`.
    #[serde(default, skip_serializing_if = "Topology::is_auto")]
    pub topology: Topology,
    /// Stitched-listener policy. Ignored (with a warning) by pool listeners.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stitching: Option<StitchingPolicy>,
    /// Opt-in nodeos-compatible HTTP chain API in front of this listener's
    /// servers. See [`ChainApiConfig`].
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub chain_api: Option<ChainApiConfig>,
}

/// A validated, normalized listener ready to be bound and monitored at runtime.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResolvedListener {
    /// `None` in legacy single-port mode, `Some(name)` in multi-listener mode.
    pub name: Option<String>,
    pub listen_address: String,
    pub listen_port: u16,
    pub max_connections: usize,
    pub expected_chain_id: Option<[u8; 32]>,
    /// See [`ListenerConfig::require_chain_id`].
    pub require_chain_id: bool,
    /// See [`BlockFailureQuarantine`]. `None` disables shared quarantine.
    pub block_failure_quarantine: Option<BlockFailureQuarantine>,
    pub servers: Vec<Server>,
    /// `auto` resolved against [`Self::servers`].
    pub topology: ResolvedTopology,
    /// The configured policy, or the defaults when the listener has none.
    pub stitching: StitchingPolicy,
    /// Text of [`StitchingPolicy::abi_file`] for a stitched listener.
    pub abi_pin: Option<String>,
}

/// The on-disk `config.json` schema.
///
/// Supports two mutually exclusive topology modes:
/// 1. **Single-listener mode** (legacy / default): top-level `listen_address`,
///    `listen_port`, and `servers`.
/// 2. **Multi-listener mode**: top-level `listeners` array, where each entry
///    binds its own `listen_port` and load-balances across an isolated `servers`
///    pool (e.g. one ingress port per blockchain network).
#[derive(Debug, Serialize, Deserialize)]
pub struct ServerConfig {
    /// Address the proxy listens on for client connections (required in single-listener mode;
    /// acts as the default bind address for `listeners` and `metrics_address` in multi-listener mode).
    #[serde(default)]
    pub listen_address: String,
    /// Port the proxy listens on for client connections in single-listener mode.
    #[serde(default)]
    pub listen_port: u16,
    /// Interval (ms) between reconnection attempts to a downed upstream.
    pub upstream_reconnect_ms: u64,
    /// Interval (ms) at which upstream status is logged.
    pub upstream_monitoring_ms: u64,
    /// Interval (ms) at which status requests are sent to upstreams.
    ///
    /// The monitor keeps a single `get_status_request` in flight and treats this
    /// same interval as the reply deadline. A miss marks the upstream offline.
    pub upstream_status_ms: u64,
    /// The upstream SHiP nodes to load-balance across in single-listener mode.
    #[serde(default)]
    pub servers: Vec<Server>,
    /// Optional expected Antelope `chain_id` (64-character hex string) in single-listener mode.
    #[serde(default)]
    pub chain_id: Option<String>,
    /// When `true` in single-listener mode, a status that omits `chain_id` marks
    /// the upstream offline. Requires [`Self::chain_id`]. Default `false` keeps
    /// older nodeos builds (which never send that optional extension) online.
    /// Cannot be combined with [`Self::listeners`]; set it on each listener.
    #[serde(default)]
    pub require_chain_id: bool,
    /// Opt-in shared quarantine for unclean mid-`get_blocks` drops at one block.
    /// Single-listener mode only. With `listeners`, set it on each listener.
    /// See [`BlockFailureQuarantine`].
    #[serde(default)]
    pub block_failure_quarantine: Option<BlockFailureQuarantine>,
    /// See [`Topology`]. Single-listener mode only. With `listeners`, set it
    /// on each listener.
    #[serde(default, skip_serializing_if = "Topology::is_auto")]
    pub topology: Topology,
    /// See [`StitchingPolicy`]. Single-listener mode only. With `listeners`,
    /// set it on each listener.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stitching: Option<StitchingPolicy>,
    /// See [`ChainApiConfig`]. Single-listener mode only. With `listeners`,
    /// set it on each listener.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub chain_api: Option<ChainApiConfig>,
    /// Named ingress listeners for multi-port / multi-network deployments.
    #[serde(default)]
    pub listeners: Vec<ListenerConfig>,

    /// Maximum number of concurrent client connections accepted (backpressure).
    /// In multi-listener mode, acts as the default per-listener cap when a
    /// listener omits `max_connections`.
    #[serde(default = "default_max_connections")]
    pub max_connections: usize,
    /// Timeout (ms) for completing the client WebSocket handshake. 0 = disabled.
    #[serde(default = "default_handshake_timeout_ms")]
    pub handshake_timeout_ms: u64,
    /// Idle timeout (ms): close a connection that exchanges no data for this long. 0 = disabled.
    #[serde(default = "default_idle_timeout_ms")]
    pub idle_timeout_ms: u64,
    /// Fail a session over when its upstream socket goes silent (ms).
    ///
    /// After half this time with no frame from the upstream, the session
    /// sends a WebSocket ping. If no frame at all arrives within the full
    /// time, the upstream is treated as dead: it is marked offline and the
    /// session fails over. Catches a node that stops answering without
    /// closing the connection. 0 = disabled.
    #[serde(default = "default_upstream_liveness_timeout_ms")]
    pub upstream_liveness_timeout_ms: u64,
    /// Maximum WebSocket message size (bytes) accepted on upstream links.
    #[serde(default = "default_max_message_bytes")]
    pub max_message_bytes: usize,
    /// Maximum WebSocket message and frame size (bytes) accepted from a client.
    ///
    /// Independent of [`Self::max_message_bytes`]. Clients only send small SHiP
    /// requests, so this defaults much lower than the upstream cap and stops an
    /// unauthenticated peer from forcing multi-megabyte allocations. Configs
    /// that omit the field stay valid and use the default.
    #[serde(default = "default_max_client_message_bytes")]
    pub max_client_message_bytes: usize,
    /// How long (ms) to wait for in-flight connections to drain on shutdown.
    #[serde(default = "default_shutdown_grace_ms")]
    pub shutdown_grace_ms: u64,
    /// Fail over an upstream that sends no bytes for this long (ms) while it
    /// owes blocks. Default 60000. 0 disables it; otherwise at least 1000.
    #[serde(
        default = "default_stall_timeout_ms",
        skip_serializing_if = "is_default_stall_timeout_ms"
    )]
    pub upstream_stall_timeout_ms: u64,
    /// A live upstream this many blocks behind the pool's head is not chosen
    /// for reads above its own head while a closer node is available.
    /// Default 1200. 0 disables it.
    #[serde(
        default = "default_live_lag_blocks",
        skip_serializing_if = "is_default_live_lag_blocks"
    )]
    pub live_lag_blocks: u32,
    /// Optional address for the HTTP health/metrics endpoint. Defaults to `listen_address` (or `"0.0.0.0"`).
    #[serde(default)]
    pub metrics_address: Option<String>,
    /// Optional port for the HTTP health/metrics endpoint. When unset, the endpoint is disabled.
    #[serde(default)]
    pub metrics_port: Option<u16>,
    /// Opt-in dashboard, status page, JSON API and control API on the
    /// health/metrics endpoint. Off when omitted.
    #[serde(default)]
    pub dashboard: DashboardConfig,
}

/// Minimum length of a control API token.
pub const MIN_CONTROL_TOKEN_LEN: usize = 16;

/// Environment variable that supplies the control API token.
pub const CONTROL_TOKEN_ENV: &str = "FLEET_ROUTER_CONTROL_TOKEN";

/// `dashboard` settings. Everything here is served on `metrics_port`.
#[derive(Debug, Serialize, Deserialize, Clone, Default, PartialEq, Eq)]
pub struct DashboardConfig {
    /// Serve `/ui/`, `/status` and `/api/v1`. Requires `metrics_port`.
    #[serde(default)]
    pub enabled: bool,
    /// Bearer token for the control API, inline. Prefer
    /// [`Self::control_token_file`] or [`CONTROL_TOKEN_ENV`].
    #[serde(default)]
    pub control_token: Option<String>,
    /// File holding the control API token. Surrounding whitespace is ignored.
    #[serde(default)]
    pub control_token_file: Option<String>,
    /// Host names the dashboard answers to besides IP literals and
    /// `localhost`, for access through a reverse proxy that keeps the name.
    #[serde(default)]
    pub allowed_hosts: Vec<String>,
}

impl DashboardConfig {
    /// Resolve the control API token: the environment variable first, then
    /// `control_token_file`, then `control_token`. `Ok(None)` disables control.
    pub fn resolve_control_token(&self) -> Result<Option<String>> {
        let env = std::env::var(CONTROL_TOKEN_ENV).ok();
        let token = if let Some(token) = env.filter(|token| !token.trim().is_empty()) {
            Some(token)
        } else if let Some(path) = &self.control_token_file {
            Some(
                std::fs::read_to_string(path)
                    .map_err(|e| anyhow!("dashboard.control_token_file '{path}': {e}"))?,
            )
        } else {
            self.control_token.clone()
        };
        let Some(token) = token.map(|token| token.trim().to_string()) else {
            return Ok(None);
        };
        if token.len() < MIN_CONTROL_TOKEN_LEN {
            bail!("the control API token must be at least {MIN_CONTROL_TOKEN_LEN} characters");
        }
        Ok(Some(token))
    }
}

/// Format a `host` and `port` for `TcpListener::bind`, bracketing bare IPv6
/// literals (e.g. `::` -> `[::]:17000`, `::1` -> `[::1]:17000`).
pub fn format_bind_addr(host: &str, port: u16) -> String {
    let host = host.trim();
    if host.contains(':') && !host.starts_with('[') {
        format!("[{host}]:{port}")
    } else {
        format!("{host}:{port}")
    }
}

fn normalize_bind_host(addr: &str) -> &str {
    let trimmed = addr.trim();
    trimmed
        .strip_prefix('[')
        .and_then(|s| s.strip_suffix(']'))
        .unwrap_or(trimmed)
}

fn is_unspecified_addr(addr: &str) -> bool {
    matches!(
        normalize_bind_host(addr),
        "0.0.0.0" | "::" | "0:0:0:0:0:0:0:0"
    )
}

fn addresses_conflict(a: &str, b: &str) -> bool {
    let a = normalize_bind_host(a);
    let b = normalize_bind_host(b);
    a.eq_ignore_ascii_case(b) || is_unspecified_addr(a) || is_unspecified_addr(b)
}

/// Parse a 64-character hex string into a 32-byte `chain_id`.
pub fn parse_chain_id(raw: &str, listener_ctx: Option<&str>) -> Result<[u8; 32]> {
    let prefix = match listener_ctx {
        Some(name) => format!("listener '{name}': "),
        None => String::new(),
    };
    let hex = raw.trim();
    if hex.len() != 64 || !hex.bytes().all(|b| b.is_ascii_hexdigit()) {
        bail!("{prefix}chain_id '{raw}' must be a 64-character hex string (32 bytes)");
    }
    let mut out = [0u8; 32];
    for (i, chunk) in hex.as_bytes().as_chunks::<2>().0.iter().enumerate() {
        let s = std::str::from_utf8(chunk).unwrap();
        out[i] = u8::from_str_radix(s, 16).unwrap();
    }
    Ok(out)
}

fn validate_servers(servers: &[Server], listener_ctx: Option<&str>) -> Result<()> {
    let prefix = match listener_ctx {
        Some(name) => format!("listener '{name}': "),
        None => String::new(),
    };
    if servers.is_empty() {
        bail!("{prefix}config must define at least one server");
    }
    let enabled: Vec<&Server> = servers.iter().filter(|s| s.enabled).collect();
    if enabled.is_empty() {
        bail!("{prefix}config must have at least one enabled server");
    }
    // Uniqueness is the dial target, not the raw string, so `host:port`
    // and `ws://host:port` cannot both be enabled. Disabled entries are
    // still parsed so a bad scheme fails `config test` before it is turned on.
    let mut seen: HashMap<String, String> = HashMap::new();
    for s in servers {
        if s.endpoint.trim().is_empty() {
            if s.enabled {
                bail!("{prefix}server '{}' has an empty endpoint", s.name);
            }
            continue;
        }
        let identity = upstream_identity(&s.endpoint)
            .map_err(|err| anyhow!("{prefix}server '{}': {err}", s.name))?;
        if s.weight == 0 {
            bail!(
                "{prefix}server '{}': weight must be at least 1 (set enabled: false to stop using it)",
                s.name
            );
        }
        if !s.enabled {
            continue;
        }
        if let Some(previous) = seen.insert(identity, s.endpoint.clone()) {
            if previous == s.endpoint {
                bail!(
                    "{prefix}duplicate upstream endpoint '{}' — endpoints must be unique",
                    s.endpoint
                );
            }
            bail!(
                "{prefix}duplicate upstream endpoint '{}' — same target as '{}'; endpoints must be unique",
                s.endpoint,
                previous
            );
        }
    }
    Ok(())
}

fn validate_block_failure_quarantine(
    policy: &BlockFailureQuarantine,
    listener_ctx: Option<&str>,
) -> Result<()> {
    let prefix = match listener_ctx {
        Some(name) => format!("listener '{name}': "),
        None => String::new(),
    };
    if policy.strikes == 0 {
        bail!("{prefix}block_failure_quarantine.strikes must be greater than 0");
    }
    if policy.window_secs == 0 {
        bail!("{prefix}block_failure_quarantine.window_secs must be greater than 0");
    }
    if policy.ttl_secs == 0 {
        bail!("{prefix}block_failure_quarantine.ttl_secs must be greater than 0");
    }
    if policy.range == 0 {
        bail!("{prefix}block_failure_quarantine.range must be greater than 0");
    }
    Ok(())
}

fn listener_prefix(listener_ctx: Option<&str>) -> String {
    match listener_ctx {
        Some(name) => format!("listener '{name}': "),
        None => String::new(),
    }
}

/// `role`, `range` and `max_sessions` of every server, enabled or not.
fn validate_server_roles(servers: &[Server], listener_ctx: Option<&str>) -> Result<()> {
    let prefix = listener_prefix(listener_ctx);
    for s in servers {
        if let Some(range) = s.range {
            if s.role != Role::Frozen {
                bail!(
                    "{prefix}server '{}': range requires role \"frozen\"",
                    s.name
                );
            }
            if range.begin == 0 || range.is_empty() {
                bail!(
                    "{prefix}server '{}': range.begin must be >= 1 and below range.end",
                    s.name
                );
            }
        }
        if s.max_sessions == Some(0) {
            bail!("{prefix}server '{}': max_sessions must be > 0", s.name);
        }
    }
    Ok(())
}

/// Topology and stitching rules for one listener (or the single-listener
/// top level).
fn validate_topology(
    topology: Topology,
    stitching: Option<&StitchingPolicy>,
    chain_id: Option<&str>,
    servers: &[Server],
    listener_ctx: Option<&str>,
) -> Result<()> {
    let prefix = listener_prefix(listener_ctx);
    if topology == Topology::Pool && has_enabled_frozen(servers) {
        bail!("{prefix}servers with role \"frozen\" require topology \"stitched\" or \"auto\"");
    }
    if topology.resolve(servers) == ResolvedTopology::Stitched && chain_id.is_none() {
        bail!("{prefix}topology \"stitched\" requires chain_id");
    }
    if let Some(policy) = stitching {
        if policy.gap_wait_ms == 0 || policy.gap_wait_ms > MAX_GAP_WAIT_MS {
            bail!("{prefix}stitching.gap_wait_ms must be between 1 and {MAX_GAP_WAIT_MS}");
        }
        if let Some(path) = &policy.abi_file {
            load_abi_file(path).map_err(|err| anyhow!("{prefix}stitching.abi_file: {err}"))?;
        }
    }
    Ok(())
}

/// Read an `abi_file` and check that it loads like an upstream handshake ABI.
fn load_abi_file(path: &std::path::Path) -> Result<String> {
    let text = std::fs::read_to_string(path)
        .map_err(|err| anyhow!("cannot read '{}': {err}", path.display()))?;
    accept_upstream_abi(&text)
        .map_err(|err| anyhow!("'{}' is not a valid SHiP ABI: {err}", path.display()))?;
    Ok(text)
}

/// Non-fatal configuration hints for one listener. See
/// [`ServerConfig::warnings`].
fn listener_warnings(
    topology: Topology,
    stitching: Option<&StitchingPolicy>,
    servers: &[Server],
    listener_ctx: Option<&str>,
) -> Vec<String> {
    let prefix = listener_prefix(listener_ctx);
    let mut out = Vec::new();
    if topology.resolve(servers) == ResolvedTopology::Pool {
        if stitching.is_some() {
            out.push(format!("{prefix}stitching is ignored for pool topology"));
        }
        return out;
    }
    if !servers.iter().any(|s| s.enabled && s.role.is_live()) {
        out.push(format!(
            "{prefix}no live server: status head is the newest archived block and sessions idle at the end of history"
        ));
    }
    let frozen: Vec<&Server> = servers
        .iter()
        .filter(|s| s.enabled && s.role == Role::Frozen)
        .collect();
    for s in &frozen {
        if s.range.is_none() {
            out.push(format!(
                "{prefix}server '{}': declare range so status is not held while this server is down at startup",
                s.name
            ));
        }
    }
    let mut declared: Vec<Range> = frozen
        .iter()
        .filter_map(|s| s.range)
        .filter(|r| !r.is_empty())
        .collect();
    declared.sort_by_key(|r| (r.begin, r.end));
    let mut merged: Vec<Range> = Vec::new();
    for r in declared {
        match merged.last_mut() {
            Some(last) if r.begin <= last.end => last.end = last.end.max(r.end),
            _ => merged.push(r),
        }
    }
    for pair in merged.windows(2) {
        out.push(format!(
            "{prefix}coverage hole [{}, {}) between declared archive ranges",
            pair[0].end, pair[1].begin
        ));
    }
    out
}

// ---------------------------------------------------------------------------
// Chain API: an opt-in nodeos-compatible HTTP port per listener
// ---------------------------------------------------------------------------

/// Upper bound of every connection and in-flight cap. Each cap sizes a
/// semaphore, which cannot hold more than `usize::MAX >> 3` permits; no
/// deployment needs more than a million.
pub const MAX_CAPACITY: usize = 1_000_000;

/// nodeos's default `max-body-size`.
const DEFAULT_API_MAX_BODY_BYTES: usize = 2 * 1024 * 1024;
const API_MAX_BODY_BYTES: std::ops::RangeInclusive<usize> = 1024..=64 * 1024 * 1024;
const API_MAX_ATTEMPTS: std::ops::RangeInclusive<u32> = 1..=8;
const API_PROBE_MS: std::ops::RangeInclusive<u64> = 100..=60_000;
const API_MIN_UPSTREAM_TIMEOUT_MS: u64 = 100;

fn default_api_allow() -> Vec<ApiClass> {
    vec![ApiClass::Block, ApiClass::Info, ApiClass::State]
}
fn default_api_max_body_bytes() -> usize {
    DEFAULT_API_MAX_BODY_BYTES
}
fn default_api_max_connections() -> usize {
    1_024
}
fn default_api_max_in_flight() -> usize {
    256
}
fn default_api_max_in_flight_per_upstream() -> usize {
    64
}
fn default_api_upstream_queue_ms() -> u64 {
    1_000
}
fn default_api_upstream_timeout_ms() -> u64 {
    10_000
}
fn default_api_request_deadline_ms() -> u64 {
    30_000
}
fn default_api_max_attempts() -> u32 {
    3
}
fn default_api_probe_ms() -> u64 {
    1_000
}

/// A group of chain API endpoints a listener can serve. See
/// [`ChainApiConfig::allow`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ApiClass {
    /// Block endpoints, routed by block number: `get_block`,
    /// `get_raw_block`, `get_block_header`, `get_block_header_state` and
    /// `get_block_info`.
    Block,
    /// `get_info`, answered by one live node.
    Info,
    /// Every other read-only `/v1/chain/*` call, answered by live nodes.
    State,
    /// `compute_transaction` and `send_read_only_transaction`.
    Exec,
    /// `push_transaction`, `push_transactions`, `send_transaction` and
    /// `send_transaction2`: the port relays transactions.
    Push,
}

/// Who answers first for an irreversible block that both a frozen range
/// server and a live node hold. `get_block` always goes to live nodes.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BlockSource {
    /// The frozen range server, keeping history reads off the live nodes.
    #[default]
    FrozenFirst,
    /// The live node; the frozen range server serves what no live node holds.
    LiveFirst,
}

/// CORS headers the router sets on every chain API response. Without it,
/// the router answers like a nodeos with no CORS configured.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ChainApiCors {
    /// `Access-Control-Allow-Origin`.
    pub allow_origin: String,
    /// `Access-Control-Allow-Headers`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub allow_headers: Option<String>,
    /// `Access-Control-Max-Age`, in seconds.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_age: Option<u32>,
    /// Also send `Access-Control-Allow-Credentials: true`.
    #[serde(default)]
    pub allow_credentials: bool,
}

/// An opt-in nodeos-compatible HTTP chain API port for one listener.
///
/// Block endpoints are routed by block number: an irreversible block goes to
/// the frozen range server that holds it (falling back to live nodes), a
/// reversible block to live nodes only. `get_info` and read-only state calls
/// go to live nodes unchanged. Each server takes part through its
/// [`Server::api_endpoint`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ChainApiConfig {
    /// Port of the chain API. Required, non-zero.
    pub listen_port: u16,
    /// Bind address. Defaults to the listener's `listen_address`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub listen_address: Option<String>,
    /// Endpoint classes served; anything else answers nodeos's 404.
    /// Default `["block", "info", "state"]`.
    #[serde(default = "default_api_allow")]
    pub allow: Vec<ApiClass>,
    /// Default `frozen_first`.
    #[serde(default)]
    pub block_source: BlockSource,
    /// Live servers (by name) that receive pushed transactions, in order.
    /// Empty: the live node that answers `get_info`. Requires `push` in
    /// [`Self::allow`].
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub push_servers: Vec<String>,
    /// Largest request body accepted. Default 2097152 (nodeos's
    /// `max-body-size`), `1024..=67108864`.
    #[serde(default = "default_api_max_body_bytes")]
    pub max_body_bytes: usize,
    /// Client connections open at once; excess sockets are closed at accept.
    /// Default 1024.
    #[serde(default = "default_api_max_connections")]
    pub max_connections: usize,
    /// Requests processed at once on this port. Default 256.
    #[serde(default = "default_api_max_in_flight")]
    pub max_in_flight: usize,
    /// Requests in flight to one upstream API. Default 64. For a frozen
    /// range server, the lower of this and the server's own limit.
    #[serde(default = "default_api_max_in_flight_per_upstream")]
    pub max_in_flight_per_upstream: usize,
    /// How long (ms) a request waits for a free upstream slot. Default 1000.
    #[serde(default = "default_api_upstream_queue_ms")]
    pub upstream_queue_ms: u64,
    /// Per attempt, until the upstream's response head (ms). Default 10000.
    #[serde(default = "default_api_upstream_timeout_ms")]
    pub upstream_timeout_ms: u64,
    /// Whole request, every attempt included (ms). Default 30000.
    #[serde(default = "default_api_request_deadline_ms")]
    pub request_deadline_ms: u64,
    /// Upstreams tried for one request. Default 3, `1..=8`.
    #[serde(default = "default_api_max_attempts")]
    pub max_attempts: u32,
    /// Interval (ms) between probes of each upstream API. Default 1000,
    /// `100..=60000`.
    #[serde(default = "default_api_probe_ms")]
    pub probe_ms: u64,
    /// See [`ChainApiCors`].
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cors: Option<ChainApiCors>,
}

/// A server's chain API address, parsed from [`Server::api_endpoint`].
///
/// Two endpoints are equal when their hosts are equal ignoring ASCII case
/// and their ports are equal; `host` keeps the configured case, because it
/// is what the upstream receives as `Host`.
#[derive(Debug, Clone)]
pub struct ApiEndpoint {
    /// As configured. An IPv6 literal keeps its brackets.
    pub host: String,
    pub port: u16,
}

impl PartialEq for ApiEndpoint {
    fn eq(&self, other: &Self) -> bool {
        self.port == other.port && self.host.eq_ignore_ascii_case(&other.host)
    }
}

impl Eq for ApiEndpoint {}

impl std::hash::Hash for ApiEndpoint {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        for byte in self.host.bytes() {
            state.write_u8(byte.to_ascii_lowercase());
        }
        state.write_u8(0xff);
        self.port.hash(state);
    }
}

/// Why an `api_endpoint` was rejected.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ApiEndpointError {
    /// `https://`: TLS to an upstream API is not supported yet.
    Https,
    /// Not `http://host[:port]` or `host:port`.
    Invalid,
}

impl ApiEndpoint {
    /// Parse `http://host[:port]` (port 80 by default) or `host:port`. A
    /// path (other than a lone `/`), query, fragment or userinfo is
    /// rejected.
    pub fn parse(raw: &str) -> Result<ApiEndpoint, ApiEndpointError> {
        let raw = raw.trim();
        let (explicit_http, rest) = match raw.split_once("://") {
            Some((scheme, rest)) => match scheme.to_ascii_lowercase().as_str() {
                "http" => (true, rest),
                "https" => return Err(ApiEndpointError::Https),
                _ => return Err(ApiEndpointError::Invalid),
            },
            None => (false, raw),
        };
        let authority = if explicit_http {
            rest.strip_suffix('/').unwrap_or(rest)
        } else {
            rest
        };
        if authority.is_empty()
            || authority.ends_with(':')
            || authority.contains(['/', '?', '#', '@'])
        {
            return Err(ApiEndpointError::Invalid);
        }
        let parsed: http::uri::Authority =
            authority.parse().map_err(|_| ApiEndpointError::Invalid)?;
        let host = parsed.host();
        if host.is_empty() {
            return Err(ApiEndpointError::Invalid);
        }
        // `host()` is a prefix of the authority; whatever follows it is the
        // port. A port that is present must parse: `http://h:88888` is an
        // error, not port 80.
        let port = match parsed.as_str()[host.len()..].strip_prefix(':') {
            Some(text) => text.parse::<u16>().map_err(|_| ApiEndpointError::Invalid)?,
            None if parsed.as_str().len() == host.len() && explicit_http => 80,
            None => return Err(ApiEndpointError::Invalid),
        };
        if port == 0 {
            return Err(ApiEndpointError::Invalid);
        }
        Ok(ApiEndpoint {
            host: host.to_string(),
            port,
        })
    }

    /// `host:port`, with the port always written and the host as
    /// configured. Sent as the upstream `Host` header, which nodeos
    /// validates by default.
    pub fn authority(&self) -> String {
        format!("{}:{}", self.host, self.port)
    }

    /// `host:port` with a lowercase host: the same for every spelling of one
    /// endpoint.
    pub fn canonical(&self) -> String {
        format!("{}:{}", self.host.to_ascii_lowercase(), self.port)
    }

    /// `http://host:port`, the base of every upstream request.
    pub fn base_url(&self) -> String {
        format!("http://{}:{}", self.host, self.port)
    }
}

/// A listener's chain API after validation, ready to be bound.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResolvedChainApi {
    /// `None` in single-listener mode.
    pub listener: Option<String>,
    /// [`ChainApiConfig::listen_address`], or the listener's address.
    pub listen_address: String,
    /// `host:port` for `TcpListener::bind`.
    pub bind: String,
    pub cfg: ChainApiConfig,
    /// [`ApiUpstream::index`] of each `push_servers` entry, in order.
    pub push: Vec<usize>,
    /// Every enabled server with an `api_endpoint`, in config order.
    pub upstreams: Vec<ApiUpstream>,
}

/// One server's chain API as the router routes to it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ApiUpstream {
    /// `ServerState::index` of the server.
    pub index: usize,
    pub name: String,
    /// The server's SHiP endpoint, the key of its `ServerState`.
    pub endpoint: String,
    pub role: Role,
    pub api: ApiEndpoint,
    /// Preference inside a candidate tier. Lower is preferred.
    pub priority: u32,
    /// Share of requests inside a priority tier. At least 1.
    pub weight: u32,
    /// A frozen server's declared [`Server::range`].
    pub range: Option<Range>,
}

fn resolve_chain_api(
    api: Option<&ChainApiConfig>,
    listener: Option<&str>,
    listener_address: &str,
    servers: &[Server],
) -> Option<ResolvedChainApi> {
    let cfg = api?.clone();
    let listen_address = cfg
        .listen_address
        .as_deref()
        .map(str::trim)
        .unwrap_or(listener_address)
        .to_string();
    let upstreams: Vec<ApiUpstream> = servers
        .iter()
        .filter(|s| s.enabled)
        .enumerate()
        .filter_map(|(index, s)| {
            let api = ApiEndpoint::parse(s.api_endpoint.as_deref()?).ok()?;
            Some(ApiUpstream {
                index,
                name: s.name.clone(),
                endpoint: s.endpoint.clone(),
                role: s.role,
                api,
                priority: s.priority,
                weight: s.weight.max(1),
                range: s.range,
            })
        })
        .collect();
    let push = cfg
        .push_servers
        .iter()
        .filter_map(|name| {
            upstreams
                .iter()
                .find(|u| u.name == *name && u.role.is_live())
                .map(|u| u.index)
        })
        .collect();
    Some(ResolvedChainApi {
        listener: listener.map(str::to_string),
        bind: format_bind_addr(&listen_address, cfg.listen_port),
        listen_address,
        cfg,
        push,
        upstreams,
    })
}

/// Syntax of every server's `api_endpoint`, enabled or not, and uniqueness
/// among the enabled ones.
fn validate_api_endpoints(servers: &[Server], listener_ctx: Option<&str>) -> Result<()> {
    let prefix = listener_prefix(listener_ctx);
    let mut seen: HashMap<ApiEndpoint, &str> = HashMap::new();
    for s in servers {
        let Some(raw) = &s.api_endpoint else {
            continue;
        };
        let api = ApiEndpoint::parse(raw).map_err(|err| match err {
            ApiEndpointError::Https => anyhow!(
                "{prefix}server '{}': https api_endpoint is not supported yet",
                s.name
            ),
            ApiEndpointError::Invalid => anyhow!(
                "{prefix}server '{}': api_endpoint must be http://host[:port] or host:port",
                s.name
            ),
        })?;
        if !s.enabled {
            continue;
        }
        if let Some(previous) = seen.insert(api, &s.name) {
            bail!(
                "{prefix}server '{}': api_endpoint duplicates server '{previous}'",
                s.name
            );
        }
    }
    Ok(())
}

/// A CORS header value: non-empty and a valid HTTP header value (no CR, LF
/// or other control bytes).
fn valid_header_value(value: &str) -> bool {
    !value.trim().is_empty() && http::HeaderValue::from_str(value).is_ok()
}

/// One listener's `chain_api`. Port conflicts are checked across the whole
/// config by `ServerConfig::validate_chain_api_ports`.
fn validate_chain_api(
    api: &ChainApiConfig,
    servers: &[Server],
    listener_ctx: Option<&str>,
) -> Result<()> {
    let prefix = listener_prefix(listener_ctx);
    if api.listen_port == 0 {
        bail!("{prefix}chain_api.listen_port must be non-zero");
    }
    if api
        .listen_address
        .as_deref()
        .is_some_and(|addr| addr.trim().is_empty())
    {
        bail!("{prefix}chain_api.listen_address must not be empty");
    }
    if !servers
        .iter()
        .any(|s| s.enabled && s.api_endpoint.is_some())
    {
        bail!("{prefix}chain_api requires at least one enabled server with api_endpoint");
    }
    if api.allow.is_empty() {
        bail!("{prefix}chain_api.allow must not be empty");
    }
    if api
        .allow
        .iter()
        .enumerate()
        .any(|(i, class)| api.allow[..i].contains(class))
    {
        bail!("{prefix}chain_api.allow must list each class at most once");
    }
    if !API_MAX_BODY_BYTES.contains(&api.max_body_bytes) {
        bail!(
            "{prefix}chain_api.max_body_bytes must be between {} and {}",
            API_MAX_BODY_BYTES.start(),
            API_MAX_BODY_BYTES.end()
        );
    }
    if !API_MAX_ATTEMPTS.contains(&api.max_attempts) {
        bail!(
            "{prefix}chain_api.max_attempts must be between {} and {}",
            API_MAX_ATTEMPTS.start(),
            API_MAX_ATTEMPTS.end()
        );
    }
    if !API_PROBE_MS.contains(&api.probe_ms) {
        bail!(
            "{prefix}chain_api.probe_ms must be between {} and {}",
            API_PROBE_MS.start(),
            API_PROBE_MS.end()
        );
    }
    if api.upstream_timeout_ms < API_MIN_UPSTREAM_TIMEOUT_MS {
        bail!(
            "{prefix}chain_api.upstream_timeout_ms must be at least {API_MIN_UPSTREAM_TIMEOUT_MS}"
        );
    }
    if api.request_deadline_ms < api.upstream_timeout_ms {
        bail!("{prefix}chain_api.request_deadline_ms must be at least upstream_timeout_ms");
    }
    if api.upstream_queue_ms > api.request_deadline_ms {
        bail!("{prefix}chain_api.upstream_queue_ms must be at most request_deadline_ms");
    }
    for (field, value) in [
        ("max_connections", api.max_connections),
        ("max_in_flight", api.max_in_flight),
        ("max_in_flight_per_upstream", api.max_in_flight_per_upstream),
    ] {
        if value == 0 {
            bail!("{prefix}chain_api.{field} must be at least 1");
        }
        if value > MAX_CAPACITY {
            bail!("{prefix}chain_api.{field} must be at most {MAX_CAPACITY}");
        }
    }
    if !api.push_servers.is_empty() && !api.allow.contains(&ApiClass::Push) {
        bail!("{prefix}chain_api.push_servers requires \"push\" in chain_api.allow");
    }
    for (i, name) in api.push_servers.iter().enumerate() {
        if api.push_servers[..i].contains(name) {
            bail!("{prefix}chain_api.push_servers lists '{name}' more than once");
        }
        if !servers
            .iter()
            .any(|s| s.name == *name && s.enabled && s.role.is_live() && s.api_endpoint.is_some())
        {
            bail!(
                "{prefix}chain_api.push_servers: '{name}' is not an enabled live server with api_endpoint"
            );
        }
    }
    if let Some(cors) = &api.cors {
        if !valid_header_value(&cors.allow_origin) {
            bail!("{prefix}chain_api.cors.allow_origin is invalid");
        }
        if cors
            .allow_headers
            .as_deref()
            .is_some_and(|value| !valid_header_value(value))
        {
            bail!("{prefix}chain_api.cors.allow_headers is invalid");
        }
    }
    Ok(())
}

/// Non-fatal chain API hints for one listener.
fn chain_api_warnings(
    api: Option<&ResolvedChainApi>,
    servers: &[Server],
    listener_ctx: Option<&str>,
) -> Vec<String> {
    let prefix = listener_prefix(listener_ctx);
    let mut out = Vec::new();
    let Some(api) = api else {
        if servers.iter().any(|s| s.api_endpoint.is_some()) {
            out.push(format!("{prefix}api_endpoint is ignored without chain_api"));
        }
        return out;
    };
    for s in servers.iter().filter(|s| s.enabled) {
        if s.role == Role::Frozen && s.api_endpoint.is_none() {
            out.push(format!(
                "{prefix}server '{}': no api_endpoint; the chain API does not use it",
                s.name
            ));
        }
    }
    if !servers
        .iter()
        .any(|s| s.enabled && s.role.is_live() && s.api_endpoint.is_some())
    {
        out.push(format!(
            "{prefix}chain_api: no enabled live server has api_endpoint; get_info, state, push and reversible blocks answer 503"
        ));
    }
    if api.cfg.allow.contains(&ApiClass::Push) {
        out.push(format!(
            "{prefix}chain_api allows push: the port relays transactions for anyone who can reach it"
        ));
    }
    if is_unspecified_addr(&api.listen_address) {
        out.push(format!(
            "{prefix}chain_api listens on {} (every interface): the chain API has no authentication",
            api.bind
        ));
    }
    out
}

impl ServerConfig {
    /// Validate the configuration, returning a clear error for any invalid field.
    pub fn validate(&self) -> Result<()> {
        if self.upstream_reconnect_ms == 0 {
            bail!("upstream_reconnect_ms must be greater than 0");
        }
        if self.upstream_monitoring_ms == 0 {
            bail!("upstream_monitoring_ms must be greater than 0");
        }
        if self.upstream_status_ms == 0 {
            bail!("upstream_status_ms must be greater than 0");
        }
        if self.max_connections == 0 {
            bail!("max_connections must be greater than 0");
        }
        if self.max_connections > MAX_CAPACITY {
            bail!("max_connections must be at most {MAX_CAPACITY}");
        }
        if self.max_message_bytes == 0 {
            bail!("max_message_bytes must be greater than 0");
        }
        if self.max_client_message_bytes == 0 {
            bail!("max_client_message_bytes must be greater than 0");
        }
        if let Some(0) = self.metrics_port {
            bail!("metrics_port must be non-zero when set");
        }
        if self.upstream_stall_timeout_ms != 0 && self.upstream_stall_timeout_ms < 1_000 {
            bail!("upstream_stall_timeout_ms must be 0 or at least 1000");
        }
        if self.dashboard.enabled && self.metrics_port.is_none() {
            bail!("dashboard.enabled requires metrics_port");
        }
        if self.dashboard.control_token.is_some() && self.dashboard.control_token_file.is_some() {
            bail!("set only one of dashboard.control_token and dashboard.control_token_file");
        }
        if let Some(token) = &self.dashboard.control_token {
            if token.trim().len() < MIN_CONTROL_TOKEN_LEN {
                bail!(
                    "dashboard.control_token must be at least {MIN_CONTROL_TOKEN_LEN} characters"
                );
            }
        }

        if !self.servers.is_empty() && !self.listeners.is_empty() {
            bail!(
                "config cannot specify both top-level 'servers' and 'listeners'; move all pools into 'listeners'"
            );
        }

        if self.listeners.is_empty() {
            if self.listen_address.trim().is_empty() {
                bail!("listen_address must not be empty");
            }
            if self.listen_port == 0 {
                bail!("listen_port must be non-zero");
            }
            if let Some(cid) = &self.chain_id {
                parse_chain_id(cid, None)?;
            }
            if self.require_chain_id && self.chain_id.is_none() {
                bail!("require_chain_id requires chain_id to be set");
            }
            if let Some(policy) = &self.block_failure_quarantine {
                validate_block_failure_quarantine(policy, None)?;
            }
            validate_servers(&self.servers, None)?;
            validate_server_roles(&self.servers, None)?;
            validate_topology(
                self.topology,
                self.stitching.as_ref(),
                self.chain_id.as_deref(),
                &self.servers,
                None,
            )?;
            validate_api_endpoints(&self.servers, None)?;
            if let Some(api) = &self.chain_api {
                validate_chain_api(api, &self.servers, None)?;
            }
        } else {
            if self.listen_port != 0 {
                bail!(
                    "top-level 'listen_port' cannot be combined with 'listeners'; set 'listen_port' on each listener"
                );
            }
            if self.chain_id.is_some() {
                bail!(
                    "top-level 'chain_id' cannot be combined with 'listeners'; set 'chain_id' on each listener"
                );
            }
            if self.require_chain_id {
                bail!(
                    "top-level 'require_chain_id' cannot be combined with 'listeners'; set 'require_chain_id' on each listener"
                );
            }
            if self.block_failure_quarantine.is_some() {
                bail!(
                    "top-level 'block_failure_quarantine' cannot be combined with 'listeners'; set 'block_failure_quarantine' on each listener"
                );
            }
            if !self.topology.is_auto() {
                bail!(
                    "top-level 'topology' cannot be combined with 'listeners'; set 'topology' on each listener"
                );
            }
            if self.stitching.is_some() {
                bail!(
                    "top-level 'stitching' cannot be combined with 'listeners'; set 'stitching' on each listener"
                );
            }
            if self.chain_api.is_some() {
                bail!(
                    "top-level 'chain_api' cannot be combined with 'listeners'; set 'chain_api' on each listener"
                );
            }
            let mut seen_names = HashMap::<String, usize>::new();
            let mut bound_listeners = Vec::<(String, String, u16)>::new();

            for (idx, listener) in self.listeners.iter().enumerate() {
                let name = listener.name.trim();
                if name.is_empty() {
                    bail!("listener at index {idx} has an empty name");
                }
                if name.contains('/') || name.chars().any(char::is_whitespace) {
                    bail!("listener '{name}': name must not contain '/' or whitespace");
                }
                if seen_names.insert(name.to_string(), idx).is_some() {
                    bail!("duplicate listener name '{name}' — listener names must be unique");
                }

                let addr = match &listener.listen_address {
                    Some(a) => {
                        if a.trim().is_empty() {
                            bail!("listener '{name}': listen_address must not be empty");
                        }
                        a.trim().to_string()
                    }
                    None => {
                        let top = self.listen_address.trim();
                        if top.is_empty() {
                            "0.0.0.0".to_string()
                        } else {
                            top.to_string()
                        }
                    }
                };

                if listener.listen_port == 0 {
                    bail!("listener '{name}': listen_port must be non-zero");
                }
                match listener.max_connections {
                    Some(0) => bail!("listener '{name}': max_connections must be greater than 0"),
                    Some(n) if n > MAX_CAPACITY => {
                        bail!("listener '{name}': max_connections must be at most {MAX_CAPACITY}")
                    }
                    _ => {}
                }
                if let Some(cid) = &listener.chain_id {
                    parse_chain_id(cid, Some(name))?;
                }
                if listener.require_chain_id && listener.chain_id.is_none() {
                    bail!("listener '{name}': require_chain_id requires chain_id to be set");
                }
                if let Some(policy) = &listener.block_failure_quarantine {
                    validate_block_failure_quarantine(policy, Some(name))?;
                }

                for (prev_name, prev_addr, prev_port) in &bound_listeners {
                    if *prev_port == listener.listen_port && addresses_conflict(prev_addr, &addr) {
                        bail!(
                            "listener '{name}' ({}) conflicts with listener '{prev_name}' ({})",
                            format_bind_addr(&addr, listener.listen_port),
                            format_bind_addr(prev_addr, *prev_port)
                        );
                    }
                }
                bound_listeners.push((name.to_string(), addr, listener.listen_port));

                validate_servers(&listener.servers, Some(name))?;
                validate_server_roles(&listener.servers, Some(name))?;
                validate_topology(
                    listener.topology,
                    listener.stitching.as_ref(),
                    listener.chain_id.as_deref(),
                    &listener.servers,
                    Some(name),
                )?;
                validate_api_endpoints(&listener.servers, Some(name))?;
                if let Some(api) = &listener.chain_api {
                    validate_chain_api(api, &listener.servers, Some(name))?;
                }
            }
        }

        if let Some((metrics_host, metrics_port)) = self.resolved_metrics_host_port() {
            for listener in self.resolved_listeners() {
                if listener.listen_port == metrics_port
                    && addresses_conflict(&metrics_host, &listener.listen_address)
                {
                    bail!(
                        "metrics_port {metrics_port} on {metrics_host} conflicts with listener port {}",
                        listener.listen_port
                    );
                }
            }
        }
        self.validate_chain_api_ports()?;

        Ok(())
    }

    /// Every `chain_api` port must differ from the SHiP listeners, the
    /// metrics port and the other chain API ports on overlapping addresses.
    fn validate_chain_api_ports(&self) -> Result<()> {
        let mut taken: Vec<(String, u16, String)> = self
            .listener_binds()
            .into_iter()
            .map(|(name, address, port)| {
                let what = match name {
                    Some(name) => {
                        format!("listener '{name}' ({})", format_bind_addr(&address, port))
                    }
                    None => format!("listen_port {port}"),
                };
                (address, port, what)
            })
            .collect();
        if let Some((host, port)) = self.resolved_metrics_host_port() {
            taken.push((host, port, format!("metrics_port {port}")));
        }
        for api in self.resolved_chain_apis().into_iter().flatten() {
            let prefix = listener_prefix(api.listener.as_deref());
            let (host, port) = (api.listen_address, api.cfg.listen_port);
            for (other_host, other_port, what) in &taken {
                if *other_port == port && addresses_conflict(other_host, &host) {
                    bail!("{prefix}chain_api port {port} on {host} conflicts with {what}");
                }
            }
            let what = match &api.listener {
                Some(name) => format!("the chain_api of listener '{name}'"),
                None => "chain_api".to_string(),
            };
            taken.push((host, port, what));
        }
        Ok(())
    }

    /// The chain API of each listener, in the order of
    /// [`Self::resolved_listeners`]; `None` for a listener without
    /// `chain_api`.
    pub fn resolved_chain_apis(&self) -> Vec<Option<ResolvedChainApi>> {
        if self.listeners.is_empty() {
            vec![resolve_chain_api(
                self.chain_api.as_ref(),
                None,
                self.listen_address.trim(),
                &self.servers,
            )]
        } else {
            self.listeners
                .iter()
                .zip(self.listener_binds())
                .map(|(l, (name, address, _))| {
                    resolve_chain_api(l.chain_api.as_ref(), name.as_deref(), &address, &l.servers)
                })
                .collect()
        }
    }

    /// `(name, listen_address, listen_port)` of each SHiP listener, as
    /// [`Self::resolved_listeners`] resolves them, without loading any
    /// `abi_file`.
    fn listener_binds(&self) -> Vec<(Option<String>, String, u16)> {
        if self.listeners.is_empty() {
            return vec![(
                None,
                self.listen_address.trim().to_string(),
                self.listen_port,
            )];
        }
        let top = self.listen_address.trim();
        let default_addr = if top.is_empty() { "0.0.0.0" } else { top };
        self.listeners
            .iter()
            .map(|l| {
                let address = l
                    .listen_address
                    .as_deref()
                    .map(str::trim)
                    .unwrap_or(default_addr);
                (
                    Some(l.name.trim().to_string()),
                    address.to_string(),
                    l.listen_port,
                )
            })
            .collect()
    }

    /// Normalized list of ingress listeners to run.
    ///
    /// Returns a single unnamed [`ResolvedListener`] in legacy single-port mode,
    /// or one named [`ResolvedListener`] per entry in `self.listeners`.
    pub fn resolved_listeners(&self) -> Vec<ResolvedListener> {
        if self.listeners.is_empty() {
            let (topology, stitching, abi_pin) =
                resolve_stitching(self.topology, self.stitching.as_ref(), &self.servers);
            vec![ResolvedListener {
                name: None,
                listen_address: self.listen_address.trim().to_string(),
                listen_port: self.listen_port,
                max_connections: self.max_connections,
                expected_chain_id: self
                    .chain_id
                    .as_deref()
                    .and_then(|c| parse_chain_id(c, None).ok()),
                require_chain_id: self.require_chain_id,
                block_failure_quarantine: self.block_failure_quarantine,
                servers: self.servers.clone(),
                topology,
                stitching,
                abi_pin,
            }]
        } else {
            let default_addr = {
                let top = self.listen_address.trim();
                if top.is_empty() {
                    "0.0.0.0".to_string()
                } else {
                    top.to_string()
                }
            };
            self.listeners
                .iter()
                .map(|l| {
                    let (topology, stitching, abi_pin) =
                        resolve_stitching(l.topology, l.stitching.as_ref(), &l.servers);
                    ResolvedListener {
                        name: Some(l.name.trim().to_string()),
                        listen_address: l
                            .listen_address
                            .as_deref()
                            .map(str::trim)
                            .unwrap_or(&default_addr)
                            .to_string(),
                        listen_port: l.listen_port,
                        max_connections: l.max_connections.unwrap_or(self.max_connections),
                        expected_chain_id: l
                            .chain_id
                            .as_deref()
                            .and_then(|c| parse_chain_id(c, Some(&l.name)).ok()),
                        require_chain_id: l.require_chain_id,
                        block_failure_quarantine: l.block_failure_quarantine,
                        servers: l.servers.clone(),
                        topology,
                        stitching,
                        abi_pin,
                    }
                })
                .collect()
        }
    }

    /// Non-fatal configuration hints, one line each, prefixed with
    /// `listener '<name>': ` in multi-listener mode. Logged at startup and
    /// printed by `config test`.
    pub fn warnings(&self) -> Vec<String> {
        let apis = self.resolved_chain_apis();
        if self.listeners.is_empty() {
            let mut out =
                listener_warnings(self.topology, self.stitching.as_ref(), &self.servers, None);
            out.extend(chain_api_warnings(
                apis.first().and_then(Option::as_ref),
                &self.servers,
                None,
            ));
            out
        } else {
            self.listeners
                .iter()
                .zip(&apis)
                .flat_map(|(l, api)| {
                    let name = Some(l.name.trim());
                    let mut out =
                        listener_warnings(l.topology, l.stitching.as_ref(), &l.servers, name);
                    out.extend(chain_api_warnings(api.as_ref(), &l.servers, name));
                    out
                })
                .collect()
        }
    }

    /// Resolved `(host, port)` for the optional health/metrics HTTP server.
    pub fn resolved_metrics_host_port(&self) -> Option<(String, u16)> {
        let port = self.metrics_port?;
        let host = self
            .metrics_address
            .as_deref()
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(ToOwned::to_owned)
            .unwrap_or_else(|| {
                let top = self.listen_address.trim();
                if !top.is_empty() {
                    top.to_string()
                } else if let Some(first) = self.listeners.first() {
                    first
                        .listen_address
                        .as_deref()
                        .map(str::trim)
                        .filter(|s| !s.is_empty())
                        .unwrap_or("0.0.0.0")
                        .to_string()
                } else {
                    "0.0.0.0".to_string()
                }
            });
        Some((host, port))
    }

    /// Bound for monitor connects, the wait for an upstream ABI, and `config test`.
    pub fn upstream_connect_timeout_ms(&self) -> u64 {
        if self.handshake_timeout_ms == 0 {
            DEFAULT_UPSTREAM_CONNECT_TIMEOUT_MS
        } else {
            self.handshake_timeout_ms
        }
    }

    /// The per-connection proxy limits derived from this config.
    pub fn proxy_limits(&self) -> ProxyLimits {
        ProxyLimits {
            handshake_timeout_ms: self.handshake_timeout_ms,
            idle_timeout_ms: self.idle_timeout_ms,
            upstream_liveness_timeout_ms: self.upstream_liveness_timeout_ms,
            max_message_bytes: self.max_message_bytes,
            max_client_message_bytes: self.max_client_message_bytes,
            stall_timeout_ms: self.upstream_stall_timeout_ms,
            connect_timeout_ms: self.upstream_connect_timeout_ms(),
        }
    }
}

/// Resolve a listener's topology and stitching policy. The ABI pin is read
/// only for a stitched listener; `validate` has already checked the file.
fn resolve_stitching(
    topology: Topology,
    stitching: Option<&StitchingPolicy>,
    servers: &[Server],
) -> (ResolvedTopology, StitchingPolicy, Option<String>) {
    let resolved = topology.resolve(servers);
    let policy = stitching.cloned().unwrap_or_default();
    let abi_pin = match (&policy.abi_file, resolved) {
        (Some(path), ResolvedTopology::Stitched) => match load_abi_file(path) {
            Ok(text) => Some(text),
            Err(err) => {
                tracing::warn!(error = %err, "stitching.abi_file not loaded; serving an upstream ABI");
                None
            }
        },
        _ => None,
    };
    (resolved, policy, abi_pin)
}

/// Cumulative per-upstream counters, updated by sessions without taking the
/// state lock.
#[derive(Debug, Default)]
pub struct UpstreamCounters {
    /// Sessions assigned to this upstream, including failovers to it.
    pub sessions_total: AtomicU64,
    /// Sessions that left this upstream through failover or an operator move.
    pub failovers_total: AtomicU64,
    /// Block frames forwarded from this upstream to clients.
    pub blocks_total: AtomicU64,
    /// Bytes forwarded from this upstream to clients.
    pub bytes_total: AtomicU64,
    /// Woken when the upstream goes from online to offline, so sessions on
    /// it can check their socket right away instead of waiting for their own
    /// liveness timeout.
    pub went_offline: Notify,
    /// Woken when a client session's dial to the upstream succeeds, so a
    /// monitor waiting out a long reconnect backoff reconnects at once.
    pub dial_succeeded: Notify,
}

/// Live state tracked per upstream by the monitoring loop and the proxy.
#[derive(Debug)]
pub struct ServerState {
    /// Configured [`Server::name`], copied when the state map is built so
    /// `/metrics` can emit a `server` label.
    pub name: String,
    /// Connections the router's sessions hold or are dialing to this upstream
    /// (links, and in a stitched listener pre-dialed standbys).
    /// `Arc<AtomicUsize>` so [`crate::session::ConnectionGuard`] can
    /// decrement synchronously on drop without locking the whole map.
    pub connections: Arc<AtomicUsize>,
    pub counters: Arc<UpstreamCounters>,
    pub enabled: bool,
    /// Set through the control API. Unlike [`Self::enabled`], the monitor never
    /// clears it. New sessions and failovers skip the upstream while it is set.
    pub operator_disabled: bool,
    pub online: bool,
    /// Set by the monitoring loop when an upstream stops advancing its chain
    /// state. Stale upstreams are deprioritized (but not excluded) when routing.
    pub stale: bool,
    /// Configured [`Server::priority`]. Lower is preferred.
    pub priority: u32,
    /// Configured [`Server::weight`]. Always at least 1.
    pub weight: u32,
    /// Optional configured `chain_id` that every upstream in this pool must match.
    pub expected_chain_id: Option<[u8; 32]>,
    /// When `true` and [`Self::expected_chain_id`] is set, a status that omits
    /// `chain_id` marks this upstream offline. Otherwise the omission is warned
    /// once and the upstream stays online.
    pub require_chain_id: bool,
    /// Set after the one-time warning that this upstream omitted `chain_id`.
    /// Cleared when a later status reports a `chain_id`.
    pub chain_id_unverified_warned: bool,
    /// Last valid `chain_id` reported by this upstream in `get_status_result`.
    pub chain_id: Option<[u8; 32]>,
    pub trace_begin_block: u32,
    pub trace_end_block: u32,
    pub chain_state_begin_block: u32,
    pub chain_state_end_block: u32,
    /// First block for which this upstream has finality data (`get_status_result_v1`).
    /// Zero until a v1 status result is observed.
    pub finality_data_begin_block: u32,
    /// One past the last block with finality data. Zero means the range is unknown
    /// (the upstream has only reported `get_status_result_v0`, or reported an empty range).
    pub finality_data_end_block: u32,
    /// Unclean mid-`get_blocks` drops observed for this upstream. Rendered as
    /// `fleet_router_upstream_block_failures_total`.
    pub block_failures: u64,
    /// Strikes still inside the configured window. Not consulted when quarantine
    /// is disabled; the failure counter above is still incremented.
    block_strikes: Vec<BlockStrike>,
    /// Block-range exclusions. Expired entries are ignored by selection and metrics.
    pub quarantines: Vec<BlockQuarantine>,
    /// Position among the listener's enabled servers, in config order. Dense
    /// (`0..n`), so it can index per-upstream arrays.
    pub index: usize,
    /// Copied from [`Server::role`].
    pub role: Role,
    /// Copied from [`Server::range`].
    pub declared: Option<Range>,
    /// Copied from [`Server::max_sessions`].
    pub max_sessions: Option<u32>,
    /// Head block from the last accepted status result.
    pub head: Option<BlockPos>,
    /// Last irreversible block from the last accepted status result.
    pub lib: Option<BlockPos>,
    /// Set by the first accepted status with `head_block_num > 0`, and kept
    /// while the upstream is offline (the ranges above are kept too).
    pub ranges_known: bool,
    /// When the last accepted status result arrived.
    pub status_at: Option<Instant>,
    /// `Some(true)` after a `get_status_result_v1`; `Some(false)` after a
    /// `get_status_result_v0`, or once the monitor has fallen back to
    /// `get_status_request_v0` (the upstream speaks only status v0).
    pub status_v1: Option<bool>,
    /// The upstream's handshake ABI, as seen by the monitor.
    pub abi: Option<Arc<AbiEntry>>,
    /// When the monitor marked this upstream offline. `None` while online and
    /// before the first verdict.
    pub offline_since: Option<Instant>,
    /// A frozen upstream sent a client session a result without the `block`
    /// it asked for, for a block it covers: it has no block log there.
    /// Stitched sessions then prefer other upstreams for `fetch_block`
    /// requests. Set by a session; cleared when the upstream reconnects or
    /// reports a different status.
    pub no_block_log: bool,
    /// One-time monitor warnings already logged for this upstream.
    pub warned: MonitorWarnings,
}

/// Monitor warnings that are logged once, not on every status result.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct MonitorWarnings {
    /// A live upstream that stopped advancing with head == LIB == its last block.
    pub looks_frozen: bool,
    /// A frozen upstream whose reported ranges grew.
    pub frozen_advanced: bool,
    /// Reported ranges that differ from the declared `range`. Cleared when
    /// they match again, so a later mismatch is logged again.
    pub range_mismatch: bool,
    /// Fingerprint last reported as differing from the rest of the pool.
    pub abi_fp: Option<u64>,
}

impl ServerState {
    pub fn new() -> ServerState {
        Self::with_expected_chain_id(None)
    }

    pub fn with_expected_chain_id(expected_chain_id: Option<[u8; 32]>) -> ServerState {
        Self::with_chain_policy(expected_chain_id, false)
    }

    pub fn with_chain_policy(
        expected_chain_id: Option<[u8; 32]>,
        require_chain_id: bool,
    ) -> ServerState {
        ServerState {
            name: String::new(),
            connections: Arc::new(AtomicUsize::new(0)),
            counters: Arc::new(UpstreamCounters::default()),
            enabled: true,
            operator_disabled: false,
            online: false,
            stale: false,
            priority: 0,
            weight: 1,
            expected_chain_id,
            require_chain_id,
            chain_id_unverified_warned: false,
            chain_id: None,
            trace_begin_block: 0,
            trace_end_block: 0,
            chain_state_begin_block: 0,
            chain_state_end_block: 0,
            finality_data_begin_block: 0,
            finality_data_end_block: 0,
            block_failures: 0,
            block_strikes: Vec::new(),
            quarantines: Vec::new(),
            index: 0,
            role: Role::Live,
            declared: None,
            max_sessions: None,
            head: None,
            lib: None,
            ranges_known: false,
            status_at: None,
            status_v1: None,
            abi: None,
            offline_since: None,
            no_block_log: false,
            warned: MonitorWarnings::default(),
        }
    }

    /// Record that the monitor (or a session) lost this upstream.
    ///
    /// Clears `online` and the reported `chain_id`. Ranges, head, LIB and the
    /// ABI are kept, so coverage stays sticky. `offline_since` keeps the first
    /// instant of an outage. Going from online to offline wakes the sessions
    /// on this upstream (see [`UpstreamCounters::went_offline`]).
    pub fn mark_offline(&mut self, now: Instant) {
        if self.online {
            self.online = false;
            self.counters.went_offline.notify_waiters();
        }
        self.chain_id = None;
        if self.offline_since.is_none() {
            self.offline_since = Some(now);
        }
    }

    /// Whether this upstream's ranges can be used: reported at least once,
    /// or declared by a frozen server.
    pub fn ranges_usable(&self) -> bool {
        self.ranges_known || (self.role == Role::Frozen && self.declared.is_some())
    }

    /// Whether reported trace or chain-state ranges differ from the declared
    /// `range` beyond the shapes real logs have. False until the first
    /// report, and for undeclared servers.
    ///
    /// One declared range cannot describe two logs exactly, so each log may
    /// start at block 2 when the range starts at 1 (the chain's first block
    /// has no entry in it), and may end one block before the range when the
    /// other log reaches its end (one log was written one block further at
    /// the tail of the data). A log that reaches outside the range, is
    /// absent, or leaves any other hole in it is a mismatch.
    pub fn range_mismatch(&self) -> bool {
        match self.declared {
            Some(declared) if self.ranges_known => {
                let trace = Range {
                    begin: self.trace_begin_block,
                    end: self.trace_end_block,
                };
                let chain_state = Range {
                    begin: self.chain_state_begin_block,
                    end: self.chain_state_end_block,
                };
                let fits = |r: Range| {
                    let begin = r.begin == declared.begin || (declared.begin == 1 && r.begin == 2);
                    let end = r.end == declared.end || r.end.checked_add(1) == Some(declared.end);
                    begin && end && !r.is_empty()
                };
                let tail_served = trace.end.max(chain_state.end) == declared.end;
                !(fits(trace) && fits(chain_state) && tail_served)
            }
            _ => false,
        }
    }

    /// One past the highest block of any reported log.
    pub fn reported_end(&self) -> u32 {
        self.trace_end_block
            .max(self.chain_state_end_block)
            .max(self.finality_data_end_block)
    }

    /// Count an unclean mid-`get_blocks` drop whose next block is `block`.
    ///
    /// Always increments [`Self::block_failures`]. When `policy` is set and
    /// this upstream reaches `strikes` at `block` inside the window, it is
    /// excluded for `K..K+range` for the TTL. [`Self::online`] is left unchanged.
    /// Further drops at that block while the exclusion is active do not extend it.
    ///
    /// Returns `true` when this call arms a new exclusion.
    pub fn record_block_failure(
        &mut self,
        block: u32,
        now: Instant,
        policy: Option<BlockFailureQuarantine>,
    ) -> bool {
        self.block_failures = self.block_failures.saturating_add(1);
        self.prune_expired_quarantines(now);
        let Some(policy) = policy else {
            return false;
        };
        if self.quarantined_for(block, now) {
            return false;
        }
        let window = Duration::from_secs(policy.window_secs);
        self.block_strikes.push(BlockStrike { block, at: now });
        self.block_strikes
            .retain(|strike| now.saturating_duration_since(strike.at) <= window);
        let count = self
            .block_strikes
            .iter()
            .filter(|strike| strike.block == block)
            .count();
        if count < usize::try_from(policy.strikes).unwrap_or(usize::MAX) {
            return false;
        }
        self.block_strikes.retain(|strike| strike.block != block);
        self.quarantines
            .retain(|quarantine| quarantine.block != block);
        self.quarantines.push(BlockQuarantine {
            block,
            range: policy.range,
            until: quarantine_deadline(now, policy.ttl_secs),
        });
        true
    }

    /// Whether selection may use this upstream (online or not).
    pub fn routable(&self) -> bool {
        self.enabled && !self.operator_disabled
    }

    /// Remove every block-range exclusion and pending strike.
    pub fn clear_quarantines(&mut self) {
        self.quarantines.clear();
        self.block_strikes.clear();
    }

    /// Drop exclusions whose TTL has passed so the list cannot grow without bound.
    pub fn prune_expired_quarantines(&mut self, now: Instant) {
        self.quarantines.retain(|quarantine| quarantine.active(now));
    }

    /// Whether a request starting at `block` must skip this upstream.
    ///
    /// Expired exclusions are removed as part of the check.
    pub fn quarantined_for(&mut self, block: u32, now: Instant) -> bool {
        self.prune_expired_quarantines(now);
        self.quarantines
            .iter()
            .any(|quarantine| quarantine.covers(block))
    }

    pub fn any_quarantine_active(&mut self, now: Instant) -> bool {
        self.prune_expired_quarantines(now);
        !self.quarantines.is_empty()
    }

    /// Lowest start block among active exclusions, if any.
    ///
    /// Expired exclusions are removed as part of the check.
    pub fn active_quarantine_block(&mut self, now: Instant) -> Option<u32> {
        self.prune_expired_quarantines(now);
        self.quarantines
            .iter()
            .map(|quarantine| quarantine.block)
            .min()
    }
}

fn quarantine_deadline(now: Instant, ttl_secs: u64) -> Instant {
    now.checked_add(Duration::from_secs(ttl_secs))
        .unwrap_or_else(|| {
            now.checked_add(Duration::from_secs(86_400 * 365))
                .unwrap_or(now)
        })
}

impl Default for ServerState {
    fn default() -> Self {
        Self::new()
    }
}

/// Interval settings shared with the background monitoring tasks.
#[derive(Debug, Clone)]
pub struct StaticConfig {
    pub upstream_reconnect_ms: u64,
    pub upstream_monitoring_ms: u64,
    pub upstream_status_ms: u64,
    /// Always non-zero. Bounds the monitor's TCP+WebSocket connect and the
    /// wait for the upstream ABI frame.
    pub connect_timeout_ms: u64,
    /// `max_message_size` / `max_frame_size` for the monitor WebSocket.
    pub max_message_bytes: usize,
}

/// Per-connection limits applied by the proxy data path.
#[derive(Debug, Clone, Copy)]
pub struct ProxyLimits {
    pub handshake_timeout_ms: u64,
    pub idle_timeout_ms: u64,
    /// See [`ServerConfig::upstream_liveness_timeout_ms`]. 0 = disabled.
    pub upstream_liveness_timeout_ms: u64,
    /// Upstream WebSocket message/frame cap.
    pub max_message_bytes: usize,
    /// Client WebSocket message/frame cap.
    pub max_client_message_bytes: usize,
    /// [`ServerConfig::upstream_stall_timeout_ms`]. 0 disables stall detection.
    pub stall_timeout_ms: u64,
    /// Bound for a session's upstream connect. Always non-zero; see
    /// [`ServerConfig::upstream_connect_timeout_ms`].
    pub connect_timeout_ms: u64,
}

#[cfg(test)]
pub fn build_state_db(servers: Vec<Server>) -> ServerStateDb {
    build_state_db_with_chain_id(servers, None, false)
}

pub fn build_state_db_with_chain_id(
    servers: Vec<Server>,
    expected_chain_id: Option<[u8; 32]>,
    require_chain_id: bool,
) -> ServerStateDb {
    Arc::new(Mutex::new(
        servers
            .iter()
            .filter(|s| s.enabled)
            .enumerate()
            .map(|(index, s)| {
                let mut state = ServerState::with_chain_policy(expected_chain_id, require_chain_id);
                state.name = s.name.clone();
                state.index = index;
                state.role = s.role;
                state.declared = s.range;
                state.max_sessions = s.max_sessions;
                state.priority = s.priority;
                state.weight = s.weight.max(1);
                (s.endpoint.clone(), state)
            })
            .collect::<HashMap<String, ServerState>>(),
    ))
}

pub fn build_config_db(servers: Vec<Server>) -> ServerConfigDb {
    Arc::new(Mutex::new(
        servers
            .into_iter()
            .filter(|s| s.enabled)
            .map(|s| (s.endpoint.clone(), s))
            .collect::<HashMap<String, Server>>(),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn minimal_config() -> serde_json::Value {
        serde_json::json!({
            "listen_address": "127.0.0.1",
            "listen_port": 17000,
            "upstream_reconnect_ms": 1000,
            "upstream_monitoring_ms": 1000,
            "upstream_status_ms": 1000,
            "servers": [
                { "name": "a", "endpoint": "127.0.0.1:18080", "enabled": true }
            ]
        })
    }

    #[test]
    fn legacy_config_defaults_the_client_message_cap() {
        let cfg: ServerConfig = serde_json::from_value(minimal_config()).unwrap();
        assert_eq!(cfg.max_message_bytes, 256 * 1024 * 1024);
        assert_eq!(cfg.max_client_message_bytes, 64 * 1024);
        cfg.validate().unwrap();
        let limits = cfg.proxy_limits();
        assert_eq!(limits.max_message_bytes, cfg.max_message_bytes);
        assert_eq!(limits.max_client_message_bytes, 64 * 1024);
    }

    #[test]
    fn example_config_without_client_cap_stays_valid() {
        let cfg: ServerConfig =
            serde_json::from_str(include_str!("../example.config.json")).unwrap();
        assert_eq!(cfg.max_client_message_bytes, 64 * 1024);
        cfg.validate().unwrap();
    }

    #[test]
    fn custom_client_message_cap_is_honored() {
        let mut value = minimal_config();
        value["max_client_message_bytes"] = serde_json::json!(4096);
        let cfg: ServerConfig = serde_json::from_value(value).unwrap();
        assert_eq!(cfg.max_client_message_bytes, 4096);
        cfg.validate().unwrap();
        assert_eq!(cfg.proxy_limits().max_client_message_bytes, 4096);
    }

    #[test]
    fn zero_client_message_cap_is_rejected() {
        let mut value = minimal_config();
        value["max_client_message_bytes"] = serde_json::json!(0);
        let cfg: ServerConfig = serde_json::from_value(value).unwrap();
        let err = cfg.validate().unwrap_err().to_string();
        assert!(
            err.contains("max_client_message_bytes"),
            "unexpected error: {err}"
        );
    }

    fn config_with_servers(servers: serde_json::Value) -> ServerConfig {
        let mut value = minimal_config();
        value["servers"] = servers;
        serde_json::from_value(value).unwrap()
    }

    #[test]
    fn bare_endpoint_dials_plaintext_ws() {
        let cfg = config_with_servers(serde_json::json!([
            { "name": "a", "endpoint": " 127.0.0.1:18080 ", "enabled": true }
        ]));
        cfg.validate().unwrap();
        assert_eq!(cfg.servers[0].ws_url(), "ws://127.0.0.1:18080");
    }

    #[test]
    fn explicit_ws_and_wss_schemes_are_preserved() {
        let cfg = config_with_servers(serde_json::json!([
            { "name": "plain", "endpoint": "WS://ship.example:18080/v1", "enabled": true },
            { "name": "tls", "endpoint": "wss://ship.example:443", "enabled": true }
        ]));
        cfg.validate().unwrap();
        assert_eq!(cfg.servers[0].ws_url(), "ws://ship.example:18080/v1");
        assert_eq!(cfg.servers[1].ws_url(), "wss://ship.example:443");
    }

    #[test]
    fn ipv6_wss_endpoint_keeps_brackets() {
        let cfg = config_with_servers(serde_json::json!([
            { "name": "a", "endpoint": "wss://[::1]:8443", "enabled": true }
        ]));
        cfg.validate().unwrap();
        assert_eq!(cfg.servers[0].ws_url(), "wss://[::1]:8443");
    }

    #[test]
    fn priority_and_weight_default_and_reach_the_state_map() {
        let cfg = config_with_servers(serde_json::json!([
            { "name": "plain", "endpoint": "127.0.0.1:18080", "enabled": true },
            { "name": "backup", "endpoint": "127.0.0.1:18081", "enabled": true,
              "priority": 10, "weight": 3 }
        ]));
        cfg.validate().unwrap();
        assert_eq!((cfg.servers[0].priority, cfg.servers[0].weight), (0, 1));
        assert_eq!((cfg.servers[1].priority, cfg.servers[1].weight), (10, 3));

        let db = build_state_db(cfg.servers.clone());
        let states = db.try_lock().unwrap();
        let backup = &states["127.0.0.1:18081"];
        assert_eq!((backup.priority, backup.weight), (10, 3));
        let plain = &states["127.0.0.1:18080"];
        assert_eq!((plain.priority, plain.weight), (0, 1));
    }

    #[test]
    fn priority_and_weight_serialize_only_when_set() {
        let cfg = config_with_servers(serde_json::json!([
            { "name": "plain", "endpoint": "127.0.0.1:18080", "enabled": true },
            { "name": "backup", "endpoint": "127.0.0.1:18081", "enabled": true,
              "priority": 10, "weight": 3 }
        ]));
        let servers = serde_json::to_value(&cfg).unwrap()["servers"].clone();
        assert!(servers[0].get("priority").is_none());
        assert!(servers[0].get("weight").is_none());
        assert_eq!(servers[1]["priority"], 10);
        assert_eq!(servers[1]["weight"], 3);
    }

    #[test]
    fn dashboard_is_off_by_default_and_needs_metrics_port() {
        let cfg: ServerConfig = serde_json::from_value(minimal_config()).unwrap();
        assert_eq!(cfg.dashboard, DashboardConfig::default());
        assert!(!cfg.dashboard.enabled);

        let mut value = minimal_config();
        value["dashboard"] = serde_json::json!({ "enabled": true });
        let cfg: ServerConfig = serde_json::from_value(value.clone()).unwrap();
        let err = cfg.validate().unwrap_err().to_string();
        assert!(
            err.contains("requires metrics_port"),
            "unexpected error: {err}"
        );

        value["metrics_port"] = serde_json::json!(9090);
        let cfg: ServerConfig = serde_json::from_value(value).unwrap();
        cfg.validate().unwrap();
    }

    #[test]
    fn control_token_must_be_long_and_from_one_config_source() {
        let mut value = minimal_config();
        value["metrics_port"] = serde_json::json!(9090);
        value["dashboard"] = serde_json::json!({ "enabled": true, "control_token": "short" });
        let cfg: ServerConfig = serde_json::from_value(value.clone()).unwrap();
        let err = cfg.validate().unwrap_err().to_string();
        assert!(err.contains("at least 16"), "unexpected error: {err}");

        value["dashboard"] = serde_json::json!({
            "enabled": true,
            "control_token": "0123456789abcdef",
            "control_token_file": "/tmp/token"
        });
        let cfg: ServerConfig = serde_json::from_value(value).unwrap();
        let err = cfg.validate().unwrap_err().to_string();
        assert!(err.contains("only one of"), "unexpected error: {err}");
    }

    #[test]
    fn control_token_file_is_trimmed() {
        let mut file = tempfile::NamedTempFile::new().unwrap();
        std::io::Write::write_all(&mut file, b"  0123456789abcdef\n").unwrap();
        let dashboard = DashboardConfig {
            enabled: true,
            control_token: None,
            control_token_file: Some(file.path().display().to_string()),
            allowed_hosts: Vec::new(),
        };
        // The environment variable takes precedence; only check the file
        // when it is not set in this test environment.
        if std::env::var(CONTROL_TOKEN_ENV).is_err() {
            assert_eq!(
                dashboard.resolve_control_token().unwrap().as_deref(),
                Some("0123456789abcdef")
            );
        }
    }

    #[test]
    fn zero_weight_is_rejected() {
        let cfg = config_with_servers(serde_json::json!([
            { "name": "a", "endpoint": "127.0.0.1:18080", "enabled": true },
            { "name": "b", "endpoint": "127.0.0.1:18081", "enabled": false, "weight": 0 }
        ]));
        let err = cfg.validate().unwrap_err().to_string();
        assert!(
            err.contains("server 'b': weight must be at least 1"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn unknown_and_malformed_schemes_are_rejected() {
        let https = config_with_servers(serde_json::json!([
            { "name": "a", "endpoint": "https://127.0.0.1:443", "enabled": true }
        ]));
        let err = https.validate().unwrap_err().to_string();
        assert!(
            err.contains("unsupported scheme 'https'"),
            "unexpected error: {err}"
        );

        let disabled = config_with_servers(serde_json::json!([
            { "name": "ok", "endpoint": "127.0.0.1:1", "enabled": true },
            { "name": "bad", "endpoint": "http://127.0.0.1:2", "enabled": false }
        ]));
        let err = disabled.validate().unwrap_err().to_string();
        assert!(
            err.contains("unsupported scheme 'http'"),
            "unexpected error: {err}"
        );

        let malformed = config_with_servers(serde_json::json!([
            { "name": "a", "endpoint": "://127.0.0.1:1", "enabled": true }
        ]));
        let err = malformed.validate().unwrap_err().to_string();
        assert!(err.contains("malformed scheme"), "unexpected error: {err}");
    }

    #[test]
    fn endpoint_uniqueness_ignores_ws_spelling_and_default_port() {
        let spelled = config_with_servers(serde_json::json!([
            { "name": "a", "endpoint": "127.0.0.1:18080", "enabled": true },
            { "name": "b", "endpoint": "ws://127.0.0.1:18080", "enabled": true }
        ]));
        let err = spelled.validate().unwrap_err().to_string();
        assert!(
            err.contains("duplicate upstream endpoint"),
            "unexpected error: {err}"
        );
        assert!(err.contains("127.0.0.1:18080"), "unexpected error: {err}");

        let ports = config_with_servers(serde_json::json!([
            { "name": "a", "endpoint": "ws://ship.example", "enabled": true },
            { "name": "b", "endpoint": "ws://SHIP.example:80", "enabled": true }
        ]));
        assert!(ports.validate().is_err());

        let tls_is_distinct = config_with_servers(serde_json::json!([
            { "name": "plain", "endpoint": "ws://127.0.0.1:18080", "enabled": true },
            { "name": "tls", "endpoint": "wss://127.0.0.1:18080", "enabled": true }
        ]));
        tls_is_distinct.validate().unwrap();

        let wss_default_port = config_with_servers(serde_json::json!([
            { "name": "a", "endpoint": "wss://ship.example", "enabled": true },
            { "name": "b", "endpoint": "wss://ship.example:443", "enabled": true }
        ]));
        assert!(wss_default_port.validate().is_err());
    }

    #[test]
    fn multi_listener_config_resolves_defaults_and_overrides() {
        let cfg: ServerConfig = serde_json::from_value(serde_json::json!({
            "listen_address": "127.0.0.1",
            "upstream_reconnect_ms": 1000,
            "upstream_monitoring_ms": 1000,
            "upstream_status_ms": 1000,
            "max_connections": 2000,
            "metrics_port": 19090,
            "listeners": [
                {
                    "name": "eos",
                    "listen_port": 17000,
                    "servers": [
                        { "name": "eos-1", "endpoint": "127.0.0.1:18080", "enabled": true },
                        { "name": "eos-2", "endpoint": "127.0.0.1:18081", "enabled": true }
                    ]
                },
                {
                    "name": "wax",
                    "listen_address": "0.0.0.0",
                    "listen_port": 17001,
                    "max_connections": 500,
                    "servers": [
                        { "name": "wax-1", "endpoint": "127.0.0.1:28080", "enabled": true }
                    ]
                }
            ]
        }))
        .unwrap();

        cfg.validate().unwrap();
        let resolved = cfg.resolved_listeners();
        assert_eq!(resolved.len(), 2);
        assert_eq!(resolved[0].name.as_deref(), Some("eos"));
        assert_eq!(resolved[0].listen_address, "127.0.0.1");
        assert_eq!(resolved[0].listen_port, 17000);
        assert_eq!(resolved[0].max_connections, 2000);
        assert_eq!(resolved[0].servers.len(), 2);

        assert_eq!(resolved[1].name.as_deref(), Some("wax"));
        assert_eq!(resolved[1].listen_address, "0.0.0.0");
        assert_eq!(resolved[1].listen_port, 17001);
        assert_eq!(resolved[1].max_connections, 500);
        assert_eq!(resolved[1].servers.len(), 1);

        assert_eq!(
            cfg.resolved_metrics_host_port(),
            Some(("127.0.0.1".to_string(), 19090))
        );
    }

    #[test]
    fn multi_listener_rejects_mixed_servers_and_listeners() {
        let mut value = minimal_config();
        value["listeners"] = serde_json::json!([
            {
                "name": "eos",
                "listen_port": 17001,
                "servers": [
                    { "name": "a", "endpoint": "127.0.0.1:18081", "enabled": true }
                ]
            }
        ]);
        let cfg: ServerConfig = serde_json::from_value(value).unwrap();
        let err = cfg.validate().unwrap_err().to_string();
        assert!(
            err.contains("cannot specify both top-level 'servers' and 'listeners'"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn multi_listener_rejects_duplicate_names_and_conflicting_ports() {
        let dup_names: ServerConfig = serde_json::from_value(serde_json::json!({
            "upstream_reconnect_ms": 1000,
            "upstream_monitoring_ms": 1000,
            "upstream_status_ms": 1000,
            "listeners": [
                {
                    "name": "eos",
                    "listen_port": 17000,
                    "servers": [{ "name": "a", "endpoint": "127.0.0.1:18080", "enabled": true }]
                },
                {
                    "name": "eos",
                    "listen_port": 17001,
                    "servers": [{ "name": "b", "endpoint": "127.0.0.1:18081", "enabled": true }]
                }
            ]
        }))
        .unwrap();
        let err = dup_names.validate().unwrap_err().to_string();
        assert!(
            err.contains("duplicate listener name 'eos'"),
            "unexpected error: {err}"
        );

        let dup_ports: ServerConfig = serde_json::from_value(serde_json::json!({
            "upstream_reconnect_ms": 1000,
            "upstream_monitoring_ms": 1000,
            "upstream_status_ms": 1000,
            "listeners": [
                {
                    "name": "eos",
                    "listen_address": "127.0.0.1",
                    "listen_port": 17000,
                    "servers": [{ "name": "a", "endpoint": "127.0.0.1:18080", "enabled": true }]
                },
                {
                    "name": "wax",
                    "listen_address": "0.0.0.0",
                    "listen_port": 17000,
                    "servers": [{ "name": "b", "endpoint": "127.0.0.1:18081", "enabled": true }]
                }
            ]
        }))
        .unwrap();
        let err = dup_ports.validate().unwrap_err().to_string();
        assert!(
            err.contains("conflicts with listener 'eos'"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn multi_listener_rejects_invalid_names_and_metrics_port_collision() {
        let slash_name: ServerConfig = serde_json::from_value(serde_json::json!({
            "upstream_reconnect_ms": 1000,
            "upstream_monitoring_ms": 1000,
            "upstream_status_ms": 1000,
            "listeners": [
                {
                    "name": "eos/main",
                    "listen_port": 17000,
                    "servers": [{ "name": "a", "endpoint": "127.0.0.1:18080", "enabled": true }]
                }
            ]
        }))
        .unwrap();
        let err = slash_name.validate().unwrap_err().to_string();
        assert!(
            err.contains("name must not contain '/' or whitespace"),
            "unexpected error: {err}"
        );

        let metrics_collision: ServerConfig = serde_json::from_value(serde_json::json!({
            "upstream_reconnect_ms": 1000,
            "upstream_monitoring_ms": 1000,
            "upstream_status_ms": 1000,
            "metrics_port": 17000,
            "listeners": [
                {
                    "name": "eos",
                    "listen_port": 17000,
                    "servers": [{ "name": "a", "endpoint": "127.0.0.1:18080", "enabled": true }]
                }
            ]
        }))
        .unwrap();
        let err = metrics_collision.validate().unwrap_err().to_string();
        assert!(
            err.contains("metrics_port 17000"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn ipv6_bind_formatting_and_conflict_detection() {
        assert_eq!(format_bind_addr("::", 17000), "[::]:17000");
        assert_eq!(format_bind_addr("::1", 17000), "[::1]:17000");
        assert_eq!(format_bind_addr("[::1]", 17000), "[::1]:17000");
        assert_eq!(format_bind_addr("127.0.0.1", 17000), "127.0.0.1:17000");

        let ipv6_conflict: ServerConfig = serde_json::from_value(serde_json::json!({
            "upstream_reconnect_ms": 1000,
            "upstream_monitoring_ms": 1000,
            "upstream_status_ms": 1000,
            "listeners": [
                {
                    "name": "eos",
                    "listen_address": "::1",
                    "listen_port": 17000,
                    "servers": [{ "name": "a", "endpoint": "127.0.0.1:18080", "enabled": true }]
                },
                {
                    "name": "wax",
                    "listen_address": "[::1]",
                    "listen_port": 17000,
                    "servers": [{ "name": "b", "endpoint": "127.0.0.1:18081", "enabled": true }]
                }
            ]
        }))
        .unwrap();
        let err = ipv6_conflict.validate().unwrap_err().to_string();
        assert!(
            err.contains("conflicts with listener 'eos'"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn chain_id_validation_accepts_64_hex_and_rejects_invalid() {
        let valid_cid = "aca376f206b8fc25a6ed44dbdc66547c36c6c33e3a119ffbeaef943642f0e906";
        let cfg: ServerConfig = serde_json::from_value(serde_json::json!({
            "upstream_reconnect_ms": 1000,
            "upstream_monitoring_ms": 1000,
            "upstream_status_ms": 1000,
            "listeners": [
                {
                    "name": "eos",
                    "listen_port": 17000,
                    "chain_id": valid_cid,
                    "servers": [{ "name": "a", "endpoint": "127.0.0.1:18080", "enabled": true }]
                }
            ]
        }))
        .unwrap();
        cfg.validate().expect("valid 64-hex chain_id should pass");
        assert!(cfg.resolved_listeners()[0].expected_chain_id.is_some());

        let bad_cid: ServerConfig = serde_json::from_value(serde_json::json!({
            "listen_address": "127.0.0.1",
            "upstream_reconnect_ms": 1000,
            "upstream_monitoring_ms": 1000,
            "upstream_status_ms": 1000,
            "chain_id": "1234",
            "listen_port": 17000,
            "servers": [{ "name": "a", "endpoint": "127.0.0.1:18080", "enabled": true }]
        }))
        .unwrap();
        let err = bad_cid.validate().unwrap_err().to_string();
        assert!(
            err.contains("must be a 64-character hex string"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn require_chain_id_defaults_false_and_needs_chain_id() {
        let cfg: ServerConfig = serde_json::from_value(minimal_config()).unwrap();
        assert!(!cfg.require_chain_id);
        cfg.validate().unwrap();
        let resolved = cfg.resolved_listeners();
        assert!(!resolved[0].require_chain_id);
        assert!(resolved[0].expected_chain_id.is_none());

        let missing: ServerConfig = serde_json::from_value({
            let mut value = minimal_config();
            value["require_chain_id"] = serde_json::json!(true);
            value
        })
        .unwrap();
        let err = missing.validate().unwrap_err().to_string();
        assert!(
            err.contains("require_chain_id requires chain_id to be set"),
            "unexpected error: {err}"
        );

        let valid_cid = "aca376f206b8fc25a6ed44dbdc66547c36c6c33e3a119ffbeaef943642f0e906";
        let mut pinned = minimal_config();
        pinned["chain_id"] = serde_json::json!(valid_cid);
        pinned["require_chain_id"] = serde_json::json!(true);
        let cfg: ServerConfig = serde_json::from_value(pinned).unwrap();
        cfg.validate().unwrap();
        let resolved = cfg.resolved_listeners();
        assert!(resolved[0].require_chain_id);
        assert!(resolved[0].expected_chain_id.is_some());

        let listener_missing: ServerConfig = serde_json::from_value(serde_json::json!({
            "upstream_reconnect_ms": 1000,
            "upstream_monitoring_ms": 1000,
            "upstream_status_ms": 1000,
            "listeners": [
                {
                    "name": "fio",
                    "listen_port": 17000,
                    "require_chain_id": true,
                    "servers": [{ "name": "a", "endpoint": "127.0.0.1:18080", "enabled": true }]
                }
            ]
        }))
        .unwrap();
        let err = listener_missing.validate().unwrap_err().to_string();
        assert!(
            err.contains("listener 'fio': require_chain_id requires chain_id to be set"),
            "unexpected error: {err}"
        );

        let listener_ok: ServerConfig = serde_json::from_value(serde_json::json!({
            "upstream_reconnect_ms": 1000,
            "upstream_monitoring_ms": 1000,
            "upstream_status_ms": 1000,
            "listeners": [
                {
                    "name": "fio",
                    "listen_port": 17000,
                    "chain_id": valid_cid,
                    "require_chain_id": true,
                    "servers": [{ "name": "a", "endpoint": "127.0.0.1:18080", "enabled": true }]
                }
            ]
        }))
        .unwrap();
        listener_ok.validate().unwrap();
        assert!(listener_ok.resolved_listeners()[0].require_chain_id);

        let top_level_with_listeners: ServerConfig = serde_json::from_value(serde_json::json!({
            "upstream_reconnect_ms": 1000,
            "upstream_monitoring_ms": 1000,
            "upstream_status_ms": 1000,
            "require_chain_id": true,
            "listeners": [
                {
                    "name": "fio",
                    "listen_port": 17000,
                    "chain_id": valid_cid,
                    "servers": [{ "name": "a", "endpoint": "127.0.0.1:18080", "enabled": true }]
                }
            ]
        }))
        .unwrap();
        let err = top_level_with_listeners.validate().unwrap_err().to_string();
        assert!(
            err.contains("top-level 'require_chain_id' cannot be combined with 'listeners'"),
            "unexpected error: {err}"
        );
    }

    fn quarantine_policy() -> BlockFailureQuarantine {
        BlockFailureQuarantine {
            strikes: 2,
            window_secs: 10,
            ttl_secs: 30,
            range: 1,
        }
    }

    #[test]
    fn block_failure_quarantine_defaults_and_rejects_zero() {
        let cfg: ServerConfig = serde_json::from_value(minimal_config()).unwrap();
        assert!(cfg.block_failure_quarantine.is_none());
        cfg.validate().unwrap();
        assert!(cfg.resolved_listeners()[0]
            .block_failure_quarantine
            .is_none());

        let mut value = minimal_config();
        value["block_failure_quarantine"] = serde_json::json!({});
        let cfg: ServerConfig = serde_json::from_value(value).unwrap();
        cfg.validate().unwrap();
        assert_eq!(
            cfg.resolved_listeners()[0].block_failure_quarantine,
            Some(BlockFailureQuarantine {
                strikes: 2,
                window_secs: 300,
                ttl_secs: 600,
                range: 1,
            })
        );

        let mut partial = minimal_config();
        partial["block_failure_quarantine"] = serde_json::json!({"strikes": 1, "range": 3});
        let cfg: ServerConfig = serde_json::from_value(partial).unwrap();
        cfg.validate().unwrap();
        let policy = cfg.resolved_listeners()[0]
            .block_failure_quarantine
            .unwrap();
        assert_eq!(policy.strikes, 1);
        assert_eq!(policy.window_secs, 300);
        assert_eq!(policy.ttl_secs, 600);
        assert_eq!(policy.range, 3);

        for (field, value) in [
            ("strikes", 0),
            ("window_secs", 0),
            ("ttl_secs", 0),
            ("range", 0),
        ] {
            let mut bad = minimal_config();
            bad["block_failure_quarantine"] = serde_json::json!({ field: value });
            let cfg: ServerConfig = serde_json::from_value(bad).unwrap();
            let err = cfg.validate().unwrap_err().to_string();
            assert!(
                err.contains(&format!(
                    "block_failure_quarantine.{field} must be greater than 0"
                )),
                "unexpected error: {err}"
            );
        }
    }

    #[test]
    fn block_failure_quarantine_is_per_listener() {
        let listener_ok: ServerConfig = serde_json::from_value(serde_json::json!({
            "upstream_reconnect_ms": 1000,
            "upstream_monitoring_ms": 1000,
            "upstream_status_ms": 1000,
            "listeners": [
                {
                    "name": "eos",
                    "listen_port": 17000,
                    "block_failure_quarantine": { "strikes": 4 },
                    "servers": [{ "name": "a", "endpoint": "127.0.0.1:18080", "enabled": true }]
                }
            ]
        }))
        .unwrap();
        listener_ok.validate().unwrap();
        let policy = listener_ok.resolved_listeners()[0]
            .block_failure_quarantine
            .unwrap();
        assert_eq!(policy.strikes, 4);
        assert_eq!(policy.ttl_secs, 600);

        let bad: ServerConfig = serde_json::from_value(serde_json::json!({
            "upstream_reconnect_ms": 1000,
            "upstream_monitoring_ms": 1000,
            "upstream_status_ms": 1000,
            "listeners": [
                {
                    "name": "eos",
                    "listen_port": 17000,
                    "block_failure_quarantine": { "window_secs": 0 },
                    "servers": [{ "name": "a", "endpoint": "127.0.0.1:18080", "enabled": true }]
                }
            ]
        }))
        .unwrap();
        let err = bad.validate().unwrap_err().to_string();
        assert!(
            err.contains(
                "listener 'eos': block_failure_quarantine.window_secs must be greater than 0"
            ),
            "unexpected error: {err}"
        );

        let top_level: ServerConfig = serde_json::from_value(serde_json::json!({
            "upstream_reconnect_ms": 1000,
            "upstream_monitoring_ms": 1000,
            "upstream_status_ms": 1000,
            "block_failure_quarantine": {},
            "listeners": [
                {
                    "name": "eos",
                    "listen_port": 17000,
                    "servers": [{ "name": "a", "endpoint": "127.0.0.1:18080", "enabled": true }]
                }
            ]
        }))
        .unwrap();
        let err = top_level.validate().unwrap_err().to_string();
        assert!(
            err.contains(
                "top-level 'block_failure_quarantine' cannot be combined with 'listeners'"
            ),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn block_failure_strikes_quarantine_one_block_then_expire() {
        let mut state = ServerState::new();
        state.online = true;
        let t0 = Instant::now();
        let policy = quarantine_policy();

        assert!(!state.record_block_failure(115, t0, None));
        assert_eq!(state.block_failures, 1);
        assert!(state.quarantines.is_empty());
        assert!(state.online);

        assert!(!state.record_block_failure(115, t0, Some(policy)));
        assert!(!state.record_block_failure(116, t0 + Duration::from_secs(1), Some(policy)));
        assert!(
            !state.any_quarantine_active(t0 + Duration::from_secs(1)),
            "strikes at different blocks must not combine"
        );

        assert!(state.record_block_failure(115, t0 + Duration::from_secs(2), Some(policy)));
        assert!(
            state.online,
            "quarantine must not mark the upstream offline"
        );
        assert!(state.quarantined_for(115, t0 + Duration::from_secs(2)));
        assert!(!state.quarantined_for(116, t0 + Duration::from_secs(2)));
        assert_eq!(
            state.active_quarantine_block(t0 + Duration::from_secs(2)),
            Some(115)
        );

        let armed_at = t0 + Duration::from_secs(2);
        assert!(
            !state.record_block_failure(115, armed_at + Duration::from_secs(1), Some(policy)),
            "drops during the TTL must not extend it"
        );
        let expired_at = armed_at + Duration::from_secs(policy.ttl_secs);
        assert!(!state.quarantined_for(115, expired_at));
        assert!(!state.any_quarantine_active(expired_at));
        assert_eq!(state.active_quarantine_block(expired_at), None);
    }

    #[test]
    fn block_failure_strikes_outside_the_window_do_not_quarantine() {
        let mut state = ServerState::new();
        let t0 = Instant::now();
        let policy = quarantine_policy();
        assert!(!state.record_block_failure(50, t0, Some(policy)));
        assert!(!state.record_block_failure(
            50,
            t0 + Duration::from_secs(policy.window_secs + 1),
            Some(policy)
        ));
        assert!(!state.any_quarantine_active(t0 + Duration::from_secs(policy.window_secs + 1)));
        assert!(state.record_block_failure(
            50,
            t0 + Duration::from_secs(policy.window_secs + 2),
            Some(policy)
        ));
    }

    #[test]
    fn block_failure_quarantine_range_covers_only_the_configured_span() {
        let mut state = ServerState::new();
        let t0 = Instant::now();
        let policy = BlockFailureQuarantine {
            range: 3,
            ..quarantine_policy()
        };
        assert!(state.record_block_failure(
            115,
            t0,
            Some(BlockFailureQuarantine {
                strikes: 1,
                ..policy
            })
        ));
        assert!(state.quarantined_for(115, t0));
        assert!(state.quarantined_for(117, t0));
        assert!(!state.quarantined_for(118, t0));
        assert!(!state.quarantined_for(114, t0));
    }

    #[test]
    fn expired_quarantines_are_pruned_when_read() {
        let now = Instant::now();
        let expired_at = now.checked_sub(Duration::from_secs(1)).unwrap_or(now);
        let mut state = ServerState::new();
        for block in 1..=49 {
            state.quarantines.push(BlockQuarantine {
                block,
                range: 1,
                until: expired_at,
            });
        }
        state.quarantines.push(BlockQuarantine {
            block: 115,
            range: 1,
            until: now + Duration::from_secs(60),
        });
        assert_eq!(state.quarantines.len(), 50);

        assert!(state.quarantined_for(115, now));
        assert!(!state.quarantined_for(1, now));
        assert_eq!(
            state.quarantines.len(),
            1,
            "a block check must drop expired exclusions without arming a new one"
        );
        assert_eq!(state.quarantines[0].block, 115);

        let after_ttl = now + Duration::from_secs(60);
        assert!(!state.any_quarantine_active(after_ttl));
        assert!(
            state.quarantines.is_empty(),
            "an activity check must drop an exclusion once its TTL has passed"
        );
        assert_eq!(state.active_quarantine_block(after_ttl), None);

        let mut only_expired = ServerState::new();
        only_expired.quarantines.push(BlockQuarantine {
            block: 10,
            range: 1,
            until: expired_at,
        });
        assert_eq!(only_expired.active_quarantine_block(now), None);
        assert!(
            only_expired.quarantines.is_empty(),
            "the metrics snapshot must drop expired exclusions"
        );
    }

    const CHAIN_ID: &str = "aca376f206b8fc25a6ed44dbdc66547c36c6c33e3a119ffbeaef943642f0e906";
    const SHIP_ABI_JSON: &str = include_str!("../mock-ship/src/ship_abi.json");

    /// Config files shipped with the repository.
    const FIXTURES: [(&str, &str); 3] = [
        (
            "example.config.json",
            include_str!("../example.config.json"),
        ),
        (
            "docker/fleet-router.json",
            include_str!("../docker/fleet-router.json"),
        ),
        ("config sample", crate::config_sample::CONFIG_SAMPLE),
    ];

    /// The stitched example from the design: two archive replicas of one
    /// range, one archive of the next range, two live nodes.
    fn stitched_example() -> serde_json::Value {
        serde_json::json!({
            "listen_address": "0.0.0.0",
            "upstream_reconnect_ms": 3000,
            "upstream_monitoring_ms": 5000,
            "upstream_status_ms": 1000,
            "metrics_port": 9090,
            "listeners": [{
                "name": "main",
                "listen_port": 17000,
                "chain_id": CHAIN_ID,
                "stitching": { "history_preference": "frozen", "predial_blocks": 2000, "gap_wait_ms": 30000 },
                "servers": [
                    { "name": "archive-0a", "endpoint": "10.0.3.10:8080", "enabled": true, "role": "frozen", "range": { "begin": 2, "end": 50000001 }, "max_sessions": 64 },
                    { "name": "archive-0b", "endpoint": "10.0.3.11:8080", "enabled": true, "role": "frozen", "range": { "begin": 2, "end": 50000001 }, "max_sessions": 64 },
                    { "name": "archive-1a", "endpoint": "10.0.3.12:8080", "enabled": true, "role": "frozen", "range": { "begin": 50000001, "end": 100000001 } },
                    { "name": "live-1", "endpoint": "10.0.1.10:8080", "enabled": true },
                    { "name": "live-2", "endpoint": "10.0.1.11:8080", "enabled": true }
                ]
            }]
        })
    }

    /// Single-listener config with `servers` and extra top-level fields.
    fn single(servers: serde_json::Value, extra: serde_json::Value) -> ServerConfig {
        let mut value = minimal_config();
        value["servers"] = servers;
        for (key, item) in extra.as_object().unwrap() {
            value[key] = item.clone();
        }
        serde_json::from_value(value).unwrap()
    }

    /// Multi-listener config with one listener named `eos`.
    fn multi(listener: serde_json::Value, extra: serde_json::Value) -> ServerConfig {
        let mut listener_value = serde_json::json!({ "name": "eos", "listen_port": 17000 });
        for (key, item) in listener.as_object().unwrap() {
            listener_value[key] = item.clone();
        }
        let mut value = serde_json::json!({
            "upstream_reconnect_ms": 1000,
            "upstream_monitoring_ms": 1000,
            "upstream_status_ms": 1000,
            "listeners": [listener_value]
        });
        for (key, item) in extra.as_object().unwrap() {
            value[key] = item.clone();
        }
        serde_json::from_value(value).unwrap()
    }

    fn validation_error(cfg: &ServerConfig) -> String {
        cfg.validate().unwrap_err().to_string()
    }

    fn frozen(name: &str, port: u16, range: Option<(u32, u32)>) -> serde_json::Value {
        let mut server = serde_json::json!({
            "name": name,
            "endpoint": format!("127.0.0.1:{port}"),
            "enabled": true,
            "role": "frozen"
        });
        if let Some((begin, end)) = range {
            server["range"] = serde_json::json!({ "begin": begin, "end": end });
        }
        server
    }

    fn live(name: &str, port: u16) -> serde_json::Value {
        serde_json::json!({ "name": name, "endpoint": format!("127.0.0.1:{port}"), "enabled": true })
    }

    // M1
    #[test]
    fn existing_config_fixtures_parse_and_serialize_without_new_fields() {
        for (name, text) in FIXTURES {
            let cfg: ServerConfig = serde_json::from_str(text)
                .unwrap_or_else(|err| panic!("{name} does not parse: {err}"));
            cfg.validate()
                .unwrap_or_else(|err| panic!("{name} does not validate: {err}"));
            assert!(cfg.warnings().is_empty(), "{name}: {:?}", cfg.warnings());
            for listener in cfg.resolved_listeners() {
                assert_eq!(listener.topology, ResolvedTopology::Pool, "{name}");
                assert_eq!(listener.stitching, StitchingPolicy::default(), "{name}");
                assert_eq!(listener.abi_pin, None, "{name}");
            }

            // Every field the file sets serializes back unchanged, and the new
            // fields are skipped at their defaults (servers included).
            let original: serde_json::Value = serde_json::from_str(text).unwrap();
            let reserialized = serde_json::to_value(&cfg).unwrap();
            for (key, value) in original.as_object().unwrap() {
                assert_eq!(&reserialized[key], value, "{name}: field {key}");
            }
            for key in [
                "topology",
                "stitching",
                "upstream_stall_timeout_ms",
                "live_lag_blocks",
            ] {
                assert!(
                    reserialized.get(key).is_none(),
                    "{name}: {key} must be skipped at its default"
                );
            }
        }
    }

    #[test]
    fn stitched_example_parses_resolves_and_round_trips() {
        let cfg: ServerConfig = serde_json::from_value(stitched_example()).unwrap();
        cfg.validate().unwrap();
        assert!(cfg.warnings().is_empty(), "{:?}", cfg.warnings());

        let resolved = cfg.resolved_listeners();
        assert_eq!(resolved.len(), 1);
        assert_eq!(resolved[0].topology, ResolvedTopology::Stitched);
        assert_eq!(resolved[0].stitching, StitchingPolicy::default());
        assert_eq!(resolved[0].abi_pin, None);
        let servers = &resolved[0].servers;
        assert_eq!(servers[0].role, Role::Frozen);
        assert_eq!(
            servers[0].range,
            Some(Range {
                begin: 2,
                end: 50_000_001
            })
        );
        assert_eq!(servers[0].max_sessions, Some(64));
        assert_eq!(servers[2].max_sessions, None);
        assert_eq!(servers[3].role, Role::Live);
        assert_eq!(servers[3].range, None);

        let original = stitched_example();
        let reserialized = serde_json::to_value(&cfg).unwrap();
        assert_eq!(
            reserialized["listeners"][0]["servers"],
            original["listeners"][0]["servers"]
        );
        let policy = &reserialized["listeners"][0]["stitching"];
        assert_eq!(policy["predial_blocks"], 2000);
        assert!(policy.get("abi_file").is_none());
    }

    // M2: V1
    #[test]
    fn range_requires_role_frozen() {
        let mut server = live("a", 18080);
        server["range"] = serde_json::json!({ "begin": 2, "end": 10 });
        let cfg = single(serde_json::json!([server.clone()]), serde_json::json!({}));
        assert_eq!(
            validation_error(&cfg),
            "server 'a': range requires role \"frozen\""
        );

        let cfg = multi(
            serde_json::json!({ "servers": [server] }),
            serde_json::json!({}),
        );
        assert_eq!(
            validation_error(&cfg),
            "listener 'eos': server 'a': range requires role \"frozen\""
        );
    }

    // M2: V2
    #[test]
    fn range_must_start_at_one_and_be_non_empty() {
        for (begin, end) in [(0, 10), (10, 10), (11, 10)] {
            let cfg = single(
                serde_json::json!([frozen("a", 18080, Some((begin, end)))]),
                serde_json::json!({ "chain_id": CHAIN_ID }),
            );
            assert_eq!(
                validation_error(&cfg),
                "server 'a': range.begin must be >= 1 and below range.end",
                "range [{begin}, {end})"
            );
        }
        let ok = single(
            serde_json::json!([frozen("a", 18080, Some((1, 2)))]),
            serde_json::json!({ "chain_id": CHAIN_ID }),
        );
        ok.validate().unwrap();
    }

    // M2: V3
    #[test]
    fn max_sessions_must_be_positive() {
        let mut server = live("a", 18080);
        server["max_sessions"] = serde_json::json!(0);
        let cfg = single(serde_json::json!([server.clone()]), serde_json::json!({}));
        assert_eq!(
            validation_error(&cfg),
            "server 'a': max_sessions must be > 0"
        );

        let cfg = multi(
            serde_json::json!({ "servers": [server] }),
            serde_json::json!({}),
        );
        assert_eq!(
            validation_error(&cfg),
            "listener 'eos': server 'a': max_sessions must be > 0"
        );

        let mut one = live("a", 18080);
        one["max_sessions"] = serde_json::json!(1);
        let cfg = single(serde_json::json!([one]), serde_json::json!({}));
        cfg.validate().unwrap();
        assert_eq!(cfg.servers[0].max_sessions, Some(1));
    }

    // M2: V4
    #[test]
    fn pool_topology_rejects_enabled_frozen_servers() {
        let servers =
            serde_json::json!([live("live", 18080), frozen("arc", 18081, Some((2, 1001)))]);
        let cfg = single(
            servers.clone(),
            serde_json::json!({ "topology": "pool", "chain_id": CHAIN_ID }),
        );
        assert_eq!(
            validation_error(&cfg),
            "servers with role \"frozen\" require topology \"stitched\" or \"auto\""
        );

        let cfg = multi(
            serde_json::json!({ "topology": "pool", "chain_id": CHAIN_ID, "servers": servers }),
            serde_json::json!({}),
        );
        assert_eq!(
            validation_error(&cfg),
            "listener 'eos': servers with role \"frozen\" require topology \"stitched\" or \"auto\""
        );

        // A disabled frozen server does not make the pool stitched.
        let mut disabled = frozen("arc", 18081, Some((2, 1001)));
        disabled["enabled"] = serde_json::json!(false);
        let cfg = single(
            serde_json::json!([live("live", 18080), disabled]),
            serde_json::json!({ "topology": "pool" }),
        );
        cfg.validate().unwrap();
        assert_eq!(cfg.resolved_listeners()[0].topology, ResolvedTopology::Pool);
    }

    // M2: V5
    #[test]
    fn stitched_listener_requires_chain_id() {
        let auto = single(
            serde_json::json!([live("live", 18080), frozen("arc", 18081, Some((2, 1001)))]),
            serde_json::json!({}),
        );
        assert_eq!(
            validation_error(&auto),
            "topology \"stitched\" requires chain_id"
        );

        let explicit = multi(
            serde_json::json!({ "topology": "stitched", "servers": [live("live", 18080)] }),
            serde_json::json!({}),
        );
        assert_eq!(
            validation_error(&explicit),
            "listener 'eos': topology \"stitched\" requires chain_id"
        );

        let pinned = multi(
            serde_json::json!({ "topology": "stitched", "chain_id": CHAIN_ID, "servers": [live("live", 18080)] }),
            serde_json::json!({}),
        );
        pinned.validate().unwrap();
    }

    // M2: V6 and M5
    #[test]
    fn topology_and_stitching_are_per_listener_with_listeners() {
        let listener =
            serde_json::json!({ "chain_id": CHAIN_ID, "servers": [live("live", 18080)] });

        let top_topology = multi(
            listener.clone(),
            serde_json::json!({ "topology": "stitched" }),
        );
        assert_eq!(
            validation_error(&top_topology),
            "top-level 'topology' cannot be combined with 'listeners'; set 'topology' on each listener"
        );

        let top_stitching = multi(listener.clone(), serde_json::json!({ "stitching": {} }));
        assert_eq!(
            validation_error(&top_stitching),
            "top-level 'stitching' cannot be combined with 'listeners'; set 'stitching' on each listener"
        );

        // The same fields on the listener, or at the top level without
        // `listeners`, are fine.
        let mut per_listener = listener.clone();
        per_listener["topology"] = serde_json::json!("stitched");
        per_listener["stitching"] = serde_json::json!({ "gap_wait_ms": 500 });
        let cfg = multi(per_listener, serde_json::json!({}));
        cfg.validate().unwrap();
        let resolved = cfg.resolved_listeners();
        assert_eq!(resolved[0].topology, ResolvedTopology::Stitched);
        assert_eq!(resolved[0].stitching.gap_wait_ms, 500);

        let top_single = single(
            serde_json::json!([live("live", 18080)]),
            serde_json::json!({ "topology": "stitched", "chain_id": CHAIN_ID, "stitching": { "gap_wait_ms": 500 } }),
        );
        top_single.validate().unwrap();
        let resolved = top_single.resolved_listeners();
        assert_eq!(resolved[0].topology, ResolvedTopology::Stitched);
        assert_eq!(resolved[0].stitching.gap_wait_ms, 500);
    }

    // M2: V7
    #[test]
    fn gap_wait_ms_must_be_within_one_hour() {
        for bad in [0u64, 3_600_001] {
            let cfg = single(
                serde_json::json!([live("live", 18080)]),
                serde_json::json!({ "stitching": { "gap_wait_ms": bad } }),
            );
            assert_eq!(
                validation_error(&cfg),
                "stitching.gap_wait_ms must be between 1 and 3600000",
                "gap_wait_ms {bad}"
            );
            let cfg = multi(
                serde_json::json!({ "stitching": { "gap_wait_ms": bad }, "servers": [live("live", 18080)] }),
                serde_json::json!({}),
            );
            assert_eq!(
                validation_error(&cfg),
                "listener 'eos': stitching.gap_wait_ms must be between 1 and 3600000"
            );
        }
        for ok in [1u64, 3_600_000] {
            let cfg = single(
                serde_json::json!([live("live", 18080)]),
                serde_json::json!({ "stitching": { "gap_wait_ms": ok } }),
            );
            cfg.validate().unwrap();
        }
    }

    // M2: V8
    #[test]
    fn stall_timeout_is_zero_or_at_least_one_second() {
        for bad in [1u64, 999] {
            let cfg = single(
                serde_json::json!([live("live", 18080)]),
                serde_json::json!({ "upstream_stall_timeout_ms": bad }),
            );
            assert_eq!(
                validation_error(&cfg),
                "upstream_stall_timeout_ms must be 0 or at least 1000"
            );
        }
        for ok in [0u64, 1_000, 120_000] {
            let cfg = single(
                serde_json::json!([live("live", 18080)]),
                serde_json::json!({ "upstream_stall_timeout_ms": ok }),
            );
            cfg.validate().unwrap();
            assert_eq!(cfg.proxy_limits().stall_timeout_ms, ok);
            // A non-default value is written back.
            assert_eq!(
                serde_json::to_value(&cfg).unwrap()["upstream_stall_timeout_ms"],
                ok
            );
        }
    }

    // M2: V9
    #[test]
    fn abi_file_must_be_readable_and_a_valid_abi() {
        let dir = tempfile::tempdir().unwrap();
        let good = dir.path().join("ship_abi.json");
        std::fs::write(&good, SHIP_ABI_JSON).unwrap();
        let garbage = dir.path().join("garbage.json");
        std::fs::write(&garbage, "{").unwrap();
        let missing = dir.path().join("missing.json");

        let with_file = |path: &std::path::Path, topology: &str| {
            multi(
                serde_json::json!({
                    "topology": topology,
                    "chain_id": CHAIN_ID,
                    "stitching": { "abi_file": path },
                    "servers": [live("live", 18080)]
                }),
                serde_json::json!({}),
            )
        };

        let err = validation_error(&with_file(&missing, "stitched"));
        assert!(
            err.starts_with("listener 'eos': stitching.abi_file: cannot read '"),
            "unexpected error: {err}"
        );
        let err = validation_error(&with_file(&garbage, "stitched"));
        assert!(
            err.starts_with("listener 'eos': stitching.abi_file: '")
                && err.contains("is not a valid SHiP ABI"),
            "unexpected error: {err}"
        );
        // Checked even where it is ignored: the config is wrong either way.
        let err = validation_error(&with_file(&garbage, "pool"));
        assert!(
            err.contains("stitching.abi_file: "),
            "unexpected error: {err}"
        );

        let single_missing = single(
            serde_json::json!([live("live", 18080)]),
            serde_json::json!({ "stitching": { "abi_file": missing } }),
        );
        assert!(validation_error(&single_missing).starts_with("stitching.abi_file: cannot read '"));

        let stitched = with_file(&good, "stitched");
        stitched.validate().unwrap();
        assert_eq!(
            stitched.resolved_listeners()[0].abi_pin.as_deref(),
            Some(SHIP_ABI_JSON)
        );
        // A pool listener ignores the pin.
        let pool = with_file(&good, "pool");
        pool.validate().unwrap();
        assert_eq!(pool.resolved_listeners()[0].abi_pin, None);
    }

    // M3
    #[test]
    fn auto_topology_resolves_from_enabled_frozen_servers() {
        let only_live = single(
            serde_json::json!([live("a", 18080), live("b", 18081)]),
            serde_json::json!({}),
        );
        only_live.validate().unwrap();
        assert_eq!(
            only_live.resolved_listeners()[0].topology,
            ResolvedTopology::Pool
        );

        let with_frozen = single(
            serde_json::json!([live("a", 18080), frozen("arc", 18081, Some((2, 1001)))]),
            serde_json::json!({ "chain_id": CHAIN_ID }),
        );
        with_frozen.validate().unwrap();
        assert_eq!(
            with_frozen.resolved_listeners()[0].topology,
            ResolvedTopology::Stitched
        );

        let stitched = single(
            serde_json::json!([live("a", 18080)]),
            serde_json::json!({ "topology": "stitched", "chain_id": CHAIN_ID }),
        );
        stitched.validate().unwrap();
        assert_eq!(
            stitched.resolved_listeners()[0].topology,
            ResolvedTopology::Stitched
        );

        let servers: Vec<Server> = serde_json::from_value(serde_json::json!([
            live("a", 18080),
            { "name": "arc", "endpoint": "127.0.0.1:18081", "enabled": false, "role": "frozen" }
        ]))
        .unwrap();
        assert_eq!(Topology::Auto.resolve(&servers), ResolvedTopology::Pool);
        assert_eq!(Topology::Pool.resolve(&servers), ResolvedTopology::Pool);
        assert_eq!(
            Topology::Stitched.resolve(&servers),
            ResolvedTopology::Stitched
        );
    }

    // M4
    #[test]
    fn new_fields_default_to_todays_behaviour() {
        let policy = StitchingPolicy::default();
        assert_eq!(policy.history_preference, HistoryPreference::Frozen);
        assert_eq!(policy.predial_blocks, 2000);
        assert_eq!(policy.gap_wait_ms, 30_000);
        assert!(policy.patch_head_lib);
        assert_eq!(policy.abi_file, None);
        assert_eq!(policy.abi_mismatch, AbiMismatch::Warn);
        let parsed: StitchingPolicy = serde_json::from_str("{}").unwrap();
        assert_eq!(parsed, policy);

        let cfg: ServerConfig = serde_json::from_value(minimal_config()).unwrap();
        assert_eq!(cfg.upstream_stall_timeout_ms, 60_000);
        assert_eq!(cfg.live_lag_blocks, 1200);
        assert_eq!(cfg.topology, Topology::Auto);
        assert_eq!(cfg.stitching, None);
        assert_eq!(cfg.servers[0].role, Role::Live);
        assert_eq!(cfg.servers[0].range, None);
        assert_eq!(cfg.servers[0].max_sessions, None);

        let limits = cfg.proxy_limits();
        assert_eq!(limits.stall_timeout_ms, 60_000);
        assert_eq!(limits.connect_timeout_ms, cfg.upstream_connect_timeout_ms());
        assert_eq!(limits.connect_timeout_ms, 10_000);

        let mut no_handshake = minimal_config();
        no_handshake["handshake_timeout_ms"] = serde_json::json!(0);
        let cfg: ServerConfig = serde_json::from_value(no_handshake).unwrap();
        assert_eq!(cfg.proxy_limits().connect_timeout_ms, 10_000);

        let parsed: StitchingPolicy = serde_json::from_value(serde_json::json!({
            "history_preference": "any",
            "predial_blocks": 0,
            "patch_head_lib": false,
            "abi_mismatch": "exclude"
        }))
        .unwrap();
        assert_eq!(parsed.history_preference, HistoryPreference::Any);
        assert_eq!(parsed.predial_blocks, 0);
        assert!(!parsed.patch_head_lib);
        assert_eq!(parsed.abi_mismatch, AbiMismatch::Exclude);
        assert_eq!(parsed.gap_wait_ms, 30_000);
    }

    #[test]
    fn unknown_role_and_topology_values_are_rejected() {
        let mut value = minimal_config();
        value["servers"][0]["role"] = serde_json::json!("archive");
        assert!(serde_json::from_value::<ServerConfig>(value).is_err());
        let mut value = minimal_config();
        value["topology"] = serde_json::json!("mesh");
        assert!(serde_json::from_value::<ServerConfig>(value).is_err());
    }

    #[test]
    fn warnings_cover_ignored_stitching_and_archive_hints() {
        let pool = single(
            serde_json::json!([live("a", 18080)]),
            serde_json::json!({ "topology": "pool", "stitching": {} }),
        );
        pool.validate().unwrap();
        assert_eq!(
            pool.warnings(),
            vec!["stitching is ignored for pool topology".to_string()]
        );

        let archive_only = multi(
            serde_json::json!({
                "chain_id": CHAIN_ID,
                "servers": [
                    frozen("arc-0", 18080, Some((2, 1001))),
                    frozen("arc-1", 18081, None),
                    frozen("arc-2", 18082, Some((1101, 2001))),
                    frozen("arc-3", 18083, Some((3001, 4001)))
                ]
            }),
            serde_json::json!({}),
        );
        archive_only.validate().unwrap();
        assert_eq!(
            archive_only.warnings(),
            vec![
                "listener 'eos': no live server: status head is the newest archived block and sessions idle at the end of history".to_string(),
                "listener 'eos': server 'arc-1': declare range so status is not held while this server is down at startup".to_string(),
                "listener 'eos': coverage hole [1001, 1101) between declared archive ranges".to_string(),
                "listener 'eos': coverage hole [2001, 3001) between declared archive ranges".to_string(),
            ]
        );

        // Touching and overlapping declared ranges leave no hole, and a live
        // server silences the archive-only hint.
        let contiguous = single(
            serde_json::json!([
                live("live", 18080),
                frozen("arc-0", 18081, Some((2, 1001))),
                frozen("arc-1", 18082, Some((1001, 2001))),
                frozen("arc-2", 18083, Some((1500, 2500)))
            ]),
            serde_json::json!({ "chain_id": CHAIN_ID }),
        );
        contiguous.validate().unwrap();
        assert!(
            contiguous.warnings().is_empty(),
            "{:?}",
            contiguous.warnings()
        );
    }

    #[test]
    fn state_db_copies_role_range_and_a_dense_index() {
        let servers: Vec<Server> = serde_json::from_value(serde_json::json!([
            frozen("arc", 18080, Some((2, 1001))),
            { "name": "off", "endpoint": "127.0.0.1:18081", "enabled": false },
            { "name": "live", "endpoint": "127.0.0.1:18082", "enabled": true, "max_sessions": 8 }
        ]))
        .unwrap();
        let db = build_state_db_with_chain_id(servers, None, false);
        let db = db.try_lock().unwrap();
        assert_eq!(db.len(), 2);
        let arc = &db["127.0.0.1:18080"];
        assert_eq!(arc.index, 0);
        assert_eq!(arc.role, Role::Frozen);
        assert_eq!(
            arc.declared,
            Some(Range {
                begin: 2,
                end: 1001
            })
        );
        assert_eq!(arc.max_sessions, None);
        let live = &db["127.0.0.1:18082"];
        assert_eq!(live.index, 1, "disabled servers do not take an index");
        assert_eq!(live.role, Role::Live);
        assert_eq!(live.declared, None);
        assert_eq!(live.max_sessions, Some(8));
    }

    #[test]
    fn mark_offline_keeps_ranges_head_lib_and_abi() {
        let mut state = ServerState::new();
        state.online = true;
        state.chain_id = Some([0xAB; 32]);
        state.trace_begin_block = 2;
        state.trace_end_block = 1001;
        state.head = Some(BlockPos {
            num: 1000,
            id: [1; 32],
        });
        state.lib = Some(BlockPos {
            num: 990,
            id: [2; 32],
        });
        state.ranges_known = true;
        state.abi = Some(Arc::new(AbiEntry::new(SHIP_ABI_JSON)));

        let t0 = Instant::now();
        state.mark_offline(t0);
        assert!(!state.online);
        assert_eq!(state.chain_id, None);
        assert_eq!(state.offline_since, Some(t0));
        assert_eq!(state.trace_end_block, 1001);
        assert_eq!(state.head.map(|h| h.num), Some(1000));
        assert_eq!(state.lib.map(|l| l.num), Some(990));
        assert!(state.ranges_known);
        assert!(state.abi.is_some());

        state.mark_offline(t0 + Duration::from_secs(5));
        assert_eq!(
            state.offline_since,
            Some(t0),
            "a repeated verdict keeps the start of the outage"
        );
    }

    #[test]
    fn declared_ranges_count_as_known_and_mismatches_are_detected() {
        let mut state = ServerState::new();
        assert!(!state.ranges_usable());
        state.ranges_known = true;
        assert!(state.ranges_usable());
        assert!(!state.range_mismatch(), "nothing declared");

        let mut frozen = ServerState::new();
        frozen.role = Role::Frozen;
        frozen.declared = Some(Range {
            begin: 2,
            end: 1001,
        });
        assert!(
            frozen.ranges_usable(),
            "a declared range is usable before any report"
        );
        assert!(!frozen.range_mismatch(), "no report yet");

        frozen.ranges_known = true;
        frozen.trace_begin_block = 2;
        frozen.trace_end_block = 1001;
        frozen.chain_state_begin_block = 2;
        frozen.chain_state_end_block = 1001;
        assert!(!frozen.range_mismatch());
        frozen.chain_state_end_block = 900;
        assert!(frozen.range_mismatch());
        assert_eq!(frozen.reported_end(), 1001);
        frozen.finality_data_end_block = 1100;
        assert_eq!(frozen.reported_end(), 1100);
    }

    /// The shapes real logs have against one declared range are no
    /// mismatch: a log starting at block 2 when the range starts at 1, and
    /// one log ending a block before the other at the tail. A log that
    /// reaches outside the range, is missing, or leaves another hole is.
    #[test]
    fn the_normal_log_shapes_are_no_range_mismatch() {
        let report = |declared: (u32, u32), trace: (u32, u32), chain_state: (u32, u32)| {
            let mut s = ServerState::new();
            s.role = Role::Frozen;
            s.declared = Some(Range {
                begin: declared.0,
                end: declared.1,
            });
            s.ranges_known = true;
            (s.trace_begin_block, s.trace_end_block) = trace;
            (s.chain_state_begin_block, s.chain_state_end_block) = chain_state;
            s.range_mismatch()
        };
        // Matches and normal shapes.
        for (declared, trace, chain_state) in [
            ((1, 30000), (1, 30000), (1, 30000)),
            // The chain's first block has no trace entry.
            ((1, 30000), (2, 30000), (1, 30000)),
            ((1, 30000), (2, 30000), (2, 30000)),
            // Chain state ends a block before trace at the data tail.
            ((60000, 91919), (60000, 91919), (60000, 91918)),
            ((60000, 91919), (60000, 91918), (60000, 91919)),
            ((1, 91919), (2, 91919), (1, 91918)),
        ] {
            assert!(
                !report(declared, trace, chain_state),
                "{declared:?} {trace:?} {chain_state:?}"
            );
        }
        // Real mismatches.
        for (declared, trace, chain_state) in [
            // Outside the declared range.
            ((30000, 60000), (29999, 60000), (30000, 60000)),
            ((30000, 60000), (30000, 60001), (30000, 60000)),
            // Both logs short of the end: the last block is served by none.
            ((60000, 91919), (60000, 91918), (60000, 91918)),
            // Holes: a later start, an earlier end, a missing log.
            ((30000, 60000), (30001, 60000), (30000, 60000)),
            ((2, 1001), (3, 1001), (2, 1001)),
            ((30000, 60000), (30000, 59000), (30000, 60000)),
            ((30000, 60000), (30000, 60000), (0, 0)),
        ] {
            assert!(
                report(declared, trace, chain_state),
                "{declared:?} {trace:?} {chain_state:?}"
            );
        }
    }

    // ---- chain API (CU5) ----

    fn with_api(mut server: serde_json::Value, api: &str) -> serde_json::Value {
        server["api_endpoint"] = serde_json::json!(api);
        server
    }

    /// One frozen and one live server, both with an API.
    fn api_servers() -> serde_json::Value {
        serde_json::json!([
            with_api(
                frozen("archive-0a", 18080, Some((2, 1001))),
                "http://10.0.3.10:8890"
            ),
            with_api(live("live-1", 18081), "10.0.1.10:8888")
        ])
    }

    fn single_api(api: serde_json::Value) -> ServerConfig {
        single(
            api_servers(),
            serde_json::json!({ "chain_id": CHAIN_ID, "chain_api": api }),
        )
    }

    fn multi_api(api: serde_json::Value) -> ServerConfig {
        multi(
            serde_json::json!({ "chain_id": CHAIN_ID, "servers": api_servers(), "chain_api": api }),
            serde_json::json!({}),
        )
    }

    #[test]
    fn chain_api_defaults_and_resolution() {
        let cfg = multi_api(serde_json::json!({ "listen_port": 18888 }));
        cfg.validate().unwrap();
        let api = cfg.listeners[0].chain_api.as_ref().unwrap();
        assert_eq!(api.listen_address, None);
        assert_eq!(
            api.allow,
            vec![ApiClass::Block, ApiClass::Info, ApiClass::State]
        );
        assert_eq!(api.block_source, BlockSource::FrozenFirst);
        assert!(api.push_servers.is_empty());
        assert_eq!(api.max_body_bytes, 2_097_152);
        assert_eq!(api.max_connections, 1024);
        assert_eq!(api.max_in_flight, 256);
        assert_eq!(api.max_in_flight_per_upstream, 64);
        assert_eq!(api.upstream_queue_ms, 1000);
        assert_eq!(api.upstream_timeout_ms, 10_000);
        assert_eq!(api.request_deadline_ms, 30_000);
        assert_eq!(api.max_attempts, 3);
        assert_eq!(api.probe_ms, 1000);
        assert_eq!(api.cors, None);

        let resolved = cfg.resolved_chain_apis();
        assert_eq!(resolved.len(), 1);
        let resolved = resolved[0].as_ref().unwrap();
        assert_eq!(resolved.listener.as_deref(), Some("eos"));
        // The listener's address (here the top-level default).
        assert_eq!(resolved.listen_address, "0.0.0.0");
        assert_eq!(resolved.bind, "0.0.0.0:18888");
        assert!(resolved.push.is_empty());
        assert_eq!(
            resolved.upstreams,
            vec![
                ApiUpstream {
                    index: 0,
                    name: "archive-0a".to_string(),
                    endpoint: "127.0.0.1:18080".to_string(),
                    role: Role::Frozen,
                    api: ApiEndpoint {
                        host: "10.0.3.10".to_string(),
                        port: 8890
                    },
                    priority: 0,
                    weight: 1,
                    range: Some(Range {
                        begin: 2,
                        end: 1001
                    }),
                },
                ApiUpstream {
                    index: 1,
                    name: "live-1".to_string(),
                    endpoint: "127.0.0.1:18081".to_string(),
                    role: Role::Live,
                    api: ApiEndpoint {
                        host: "10.0.1.10".to_string(),
                        port: 8888
                    },
                    priority: 0,
                    weight: 1,
                    range: None,
                },
            ]
        );
        assert_eq!(resolved.upstreams[0].api.authority(), "10.0.3.10:8890");
        assert_eq!(
            resolved.upstreams[1].api.base_url(),
            "http://10.0.1.10:8888"
        );
    }

    #[test]
    fn chain_api_resolution_uses_state_indices_push_order_and_bind_address() {
        let mut weighted = with_api(live("live-2", 18082), "http://[::1]:8888/");
        weighted["priority"] = serde_json::json!(5);
        weighted["weight"] = serde_json::json!(3);
        let cfg = single(
            serde_json::json!([
                with_api(live("off", 18079), "10.0.1.9:8888")
                    .as_object()
                    .cloned()
                    .map(|mut s| {
                        s.insert("enabled".into(), serde_json::json!(false));
                        serde_json::Value::Object(s)
                    })
                    .unwrap(),
                live("no-api", 18080),
                with_api(live("live-1", 18081), "10.0.1.10:8888"),
                weighted
            ]),
            serde_json::json!({
                "chain_api": {
                    "listen_port": 18888,
                    "listen_address": "10.0.0.5",
                    "allow": ["block", "push"],
                    "push_servers": ["live-2", "live-1"]
                }
            }),
        );
        cfg.validate().unwrap();
        let resolved = cfg.resolved_chain_apis()[0].clone().unwrap();
        assert_eq!(resolved.listener, None);
        assert_eq!(resolved.bind, "10.0.0.5:18888");
        // ServerState indices count enabled servers only.
        let indices: Vec<(usize, &str)> = resolved
            .upstreams
            .iter()
            .map(|u| (u.index, u.name.as_str()))
            .collect();
        assert_eq!(indices, vec![(1, "live-1"), (2, "live-2")]);
        let db = build_state_db(cfg.servers.clone());
        let db = db.try_lock().unwrap();
        for u in &resolved.upstreams {
            assert_eq!(db[&u.endpoint].index, u.index);
            assert_eq!(db[&u.endpoint].name, u.name);
            assert_eq!(db[&u.endpoint].priority, u.priority);
            assert_eq!(db[&u.endpoint].weight, u.weight);
        }
        // Each server's priority and weight carry over to its API.
        let tiers: Vec<(u32, u32)> = resolved
            .upstreams
            .iter()
            .map(|u| (u.priority, u.weight))
            .collect();
        assert_eq!(tiers, vec![(0, 1), (5, 3)]);
        assert_eq!(resolved.push, vec![2, 1]);
        assert_eq!(resolved.upstreams[1].api.authority(), "[::1]:8888");

        // No chain_api: `None`, aligned with the listeners.
        let none = multi(
            serde_json::json!({ "servers": [live("a", 18080)] }),
            serde_json::json!({}),
        );
        assert_eq!(none.resolved_chain_apis(), vec![None]);
    }

    #[test]
    fn listener_binds_match_the_resolved_listeners() {
        let configs = [
            serde_json::json!({
                "listen_address": " 127.0.0.1 ",
                "listen_port": 17000,
                "upstream_reconnect_ms": 1000,
                "upstream_monitoring_ms": 1000,
                "upstream_status_ms": 1000,
                "servers": [live("a", 18080)]
            }),
            serde_json::json!({
                "listen_address": "10.0.0.5",
                "upstream_reconnect_ms": 1000,
                "upstream_monitoring_ms": 1000,
                "upstream_status_ms": 1000,
                "listeners": [
                    { "name": " eos ", "listen_port": 17000, "servers": [live("a", 18080)] },
                    { "name": "wax", "listen_address": " ::1 ", "listen_port": 17001, "servers": [live("b", 18081)] }
                ]
            }),
            serde_json::json!({
                "upstream_reconnect_ms": 1000,
                "upstream_monitoring_ms": 1000,
                "upstream_status_ms": 1000,
                "listeners": [{ "name": "eos", "listen_port": 17000, "servers": [live("a", 18080)] }]
            }),
        ];
        for value in configs {
            let cfg: ServerConfig = serde_json::from_value(value).unwrap();
            let resolved: Vec<(Option<String>, String, u16)> = cfg
                .resolved_listeners()
                .into_iter()
                .map(|l| (l.name, l.listen_address, l.listen_port))
                .collect();
            assert_eq!(cfg.listener_binds(), resolved);
        }
    }

    #[test]
    fn chain_api_round_trips_and_absent_fields_stay_absent() {
        let full = serde_json::json!({
            "listen_port": 18888,
            "listen_address": "10.0.0.5",
            "allow": ["block", "info", "state", "exec", "push"],
            "block_source": "live_first",
            "push_servers": ["live-1"],
            "max_body_bytes": 1024,
            "max_connections": 10,
            "max_in_flight": 20,
            "max_in_flight_per_upstream": 5,
            "upstream_queue_ms": 100,
            "upstream_timeout_ms": 200,
            "request_deadline_ms": 300,
            "max_attempts": 8,
            "probe_ms": 100,
            "cors": {
                "allow_origin": "*",
                "allow_headers": "Content-Type",
                "max_age": 600,
                "allow_credentials": true
            }
        });
        let cfg = single_api(full.clone());
        cfg.validate().unwrap();
        let api = cfg.chain_api.as_ref().unwrap();
        assert_eq!(api.block_source, BlockSource::LiveFirst);
        assert_eq!(
            api.cors,
            Some(ChainApiCors {
                allow_origin: "*".to_string(),
                allow_headers: Some("Content-Type".to_string()),
                max_age: Some(600),
                allow_credentials: true,
            })
        );
        let reserialized = serde_json::to_value(&cfg).unwrap();
        assert_eq!(reserialized["chain_api"], full);
        assert_eq!(reserialized["servers"], api_servers());

        // Optional parts are omitted when unset.
        let minimal = single_api(serde_json::json!({ "listen_port": 18888 }));
        let value = serde_json::to_value(minimal.chain_api.as_ref().unwrap()).unwrap();
        for key in ["listen_address", "push_servers", "cors"] {
            assert!(value.get(key).is_none(), "{key} must be skipped");
        }
        assert_eq!(value["block_source"], "frozen_first");
        let cors: ChainApiCors =
            serde_json::from_value(serde_json::json!({ "allow_origin": "*" })).unwrap();
        assert_eq!(
            serde_json::to_value(&cors).unwrap(),
            serde_json::json!({ "allow_origin": "*", "allow_credentials": false })
        );

        // Configs without the chain API serialize exactly as before: every
        // server with the same keys, and no new key anywhere.
        for (name, text) in FIXTURES {
            let original: serde_json::Value = serde_json::from_str(text).unwrap();
            let cfg: ServerConfig = serde_json::from_str(text).unwrap();
            let reserialized = serde_json::to_value(&cfg).unwrap();
            assert!(reserialized.get("chain_api").is_none(), "{name}");
            assert_eq!(reserialized["servers"], original["servers"], "{name}");
        }
        let example: ServerConfig =
            serde_json::from_str(include_str!("../example.config.json")).unwrap();
        assert_eq!(
            serde_json::to_string(&example.servers[0]).unwrap(),
            r#"{"name":"SHiP Node 1","endpoint":"127.0.0.1:18080","enabled":true}"#
        );
        let listeners = multi(
            serde_json::json!({ "servers": [live("a", 18080)] }),
            serde_json::json!({}),
        );
        let reserialized = serde_json::to_value(&listeners).unwrap();
        assert!(reserialized["listeners"][0].get("chain_api").is_none());
        assert_eq!(reserialized["listeners"][0]["servers"][0], live("a", 18080));

        // Unknown classes and block sources are rejected by the parser.
        for bad in [
            serde_json::json!({ "listen_port": 1, "allow": ["blocks"] }),
            serde_json::json!({ "listen_port": 1, "block_source": "frozen" }),
            serde_json::json!({}),
        ] {
            assert!(
                serde_json::from_value::<ChainApiConfig>(bad.clone()).is_err(),
                "{bad}"
            );
        }
    }

    #[test]
    fn api_endpoint_parsing() {
        let ok = [
            ("http://10.0.1.10:8888", "10.0.1.10", 8888),
            ("HTTP://Node.Example:8888/", "node.example", 8888),
            ("http://10.0.1.10", "10.0.1.10", 80),
            (" 10.0.1.10:8888 ", "10.0.1.10", 8888),
            ("node.example:1", "node.example", 1),
            ("[::1]:8888", "[::1]", 8888),
            ("http://[::1]", "[::1]", 80),
        ];
        for (raw, host, port) in ok {
            assert_eq!(
                ApiEndpoint::parse(raw),
                Ok(ApiEndpoint {
                    host: host.to_string(),
                    port
                }),
                "{raw}"
            );
        }
        // Equality ignores the host's case; the configured case is what the
        // upstream gets as `Host`.
        let named = ApiEndpoint::parse("HTTP://Node.Example:8888/").unwrap();
        assert_eq!(named.host, "Node.Example");
        assert_eq!(named.authority(), "Node.Example:8888");
        assert_eq!(named.base_url(), "http://Node.Example:8888");
        assert_eq!(named.canonical(), "node.example:8888");
        let hash = |e: &ApiEndpoint| {
            use std::hash::{BuildHasher, BuildHasherDefault};
            BuildHasherDefault::<std::collections::hash_map::DefaultHasher>::default().hash_one(e)
        };
        let lower = ApiEndpoint::parse("node.example:8888").unwrap();
        assert_eq!(named, lower);
        assert_eq!(hash(&named), hash(&lower));
        assert_ne!(named, ApiEndpoint::parse("node.example:8889").unwrap());
        assert_eq!(
            ApiEndpoint::parse("https://10.0.1.10:443"),
            Err(ApiEndpointError::Https)
        );
        assert_eq!(
            ApiEndpoint::parse("HTTPS://10.0.1.10"),
            Err(ApiEndpointError::Https)
        );
        for raw in [
            "",
            "10.0.1.10",
            "10.0.1.10:",
            "10.0.1.10:0",
            "10.0.1.10:99999",
            "10.0.1.10:port",
            // A port that is present must parse, with or without a scheme.
            "http://10.0.1.10:88888",
            "http://10.0.1.10:port",
            "http://10.0.1.10:-1",
            "http://10.0.1.10:0",
            "http://[::1]:65536",
            "ws://10.0.1.10:8888",
            "http://",
            "http://:8888",
            "http://10.0.1.10:8888/v1",
            "http://10.0.1.10:8888//",
            "http://10.0.1.10:8888?x=1",
            "http://10.0.1.10:8888#x",
            "http://user@10.0.1.10:8888",
            "10.0.1.10:8888/",
            "://10.0.1.10:8888",
            "::1:8888",
            "http://10.0.1.10:8888 extra",
        ] {
            assert_eq!(
                ApiEndpoint::parse(raw),
                Err(ApiEndpointError::Invalid),
                "{raw:?}"
            );
        }
    }

    // C1, C2
    #[test]
    fn api_endpoint_syntax_is_checked_on_every_server() {
        let bad = single(
            serde_json::json!([with_api(live("a", 18080), "http://10.0.1.10:8888/v1")]),
            serde_json::json!({}),
        );
        assert_eq!(
            validation_error(&bad),
            "server 'a': api_endpoint must be http://host[:port] or host:port"
        );
        let mut disabled = with_api(live("off", 18081), "10.0.1.10");
        disabled["enabled"] = serde_json::json!(false);
        let bad = multi(
            serde_json::json!({ "servers": [live("a", 18080), disabled] }),
            serde_json::json!({}),
        );
        assert_eq!(
            validation_error(&bad),
            "listener 'eos': server 'off': api_endpoint must be http://host[:port] or host:port"
        );
        let https = single(
            serde_json::json!([with_api(live("a", 18080), "https://10.0.1.10")]),
            serde_json::json!({}),
        );
        assert_eq!(
            validation_error(&https),
            "server 'a': https api_endpoint is not supported yet"
        );
    }

    // C3
    #[test]
    fn api_endpoint_is_unique_within_a_listener() {
        for (a, b) in [
            ("10.0.1.10:8888", "http://10.0.1.10:8888"),
            ("http://Node.Example", "node.example:80"),
            ("[::1]:8888", "http://[::1]:8888/"),
        ] {
            let cfg = single(
                serde_json::json!([with_api(live("a", 18080), a), with_api(live("b", 18081), b)]),
                serde_json::json!({}),
            );
            assert_eq!(
                validation_error(&cfg),
                "server 'b': api_endpoint duplicates server 'a'",
                "{a} {b}"
            );
        }
        // A disabled duplicate is fine, and so are other ports.
        let mut off = with_api(live("off", 18082), "10.0.1.10:8888");
        off["enabled"] = serde_json::json!(false);
        let cfg = single(
            serde_json::json!([
                with_api(live("a", 18080), "10.0.1.10:8888"),
                with_api(live("b", 18081), "10.0.1.10:8889"),
                off
            ]),
            serde_json::json!({}),
        );
        cfg.validate().unwrap();
        // Unique per listener, not across listeners.
        let cfg: ServerConfig = serde_json::from_value(serde_json::json!({
            "upstream_reconnect_ms": 1000,
            "upstream_monitoring_ms": 1000,
            "upstream_status_ms": 1000,
            "listeners": [
                { "name": "eos", "listen_port": 17000,
                  "servers": [with_api(live("a", 18080), "10.0.1.10:8888")] },
                { "name": "wax", "listen_port": 17001,
                  "servers": [with_api(live("b", 18081), "10.0.1.10:8888")] }
            ]
        }))
        .unwrap();
        cfg.validate().unwrap();
    }

    // C4
    #[test]
    fn chain_api_port_must_not_conflict() {
        let zero = single_api(serde_json::json!({ "listen_port": 0 }));
        assert_eq!(
            validation_error(&zero),
            "chain_api.listen_port must be non-zero"
        );
        let empty = multi_api(serde_json::json!({ "listen_port": 18888, "listen_address": " " }));
        assert_eq!(
            validation_error(&empty),
            "listener 'eos': chain_api.listen_address must not be empty"
        );

        // The SHiP port of the same listener.
        let own = single_api(serde_json::json!({ "listen_port": 17000 }));
        assert_eq!(
            validation_error(&own),
            "chain_api port 17000 on 127.0.0.1 conflicts with listen_port 17000"
        );
        // Another address does not conflict.
        let elsewhere =
            single_api(serde_json::json!({ "listen_port": 17000, "listen_address": "127.0.0.2" }));
        elsewhere.validate().unwrap();
        // ...unless one side binds every interface.
        let any =
            single_api(serde_json::json!({ "listen_port": 17000, "listen_address": "0.0.0.0" }));
        assert_eq!(
            validation_error(&any),
            "chain_api port 17000 on 0.0.0.0 conflicts with listen_port 17000"
        );

        // The metrics port.
        let mut metrics = single_api(serde_json::json!({ "listen_port": 19090 }));
        metrics.metrics_port = Some(19090);
        assert_eq!(
            validation_error(&metrics),
            "chain_api port 19090 on 127.0.0.1 conflicts with metrics_port 19090"
        );

        // Another listener's SHiP port, and another chain_api.
        let two = |eos_api: u16, wax_api: u16| -> ServerConfig {
            serde_json::from_value(serde_json::json!({
                "upstream_reconnect_ms": 1000,
                "upstream_monitoring_ms": 1000,
                "upstream_status_ms": 1000,
                "listeners": [
                    { "name": "eos", "listen_port": 17000,
                      "servers": [with_api(live("a", 18080), "10.0.1.10:8888")],
                      "chain_api": { "listen_port": eos_api } },
                    { "name": "wax", "listen_port": 17001,
                      "servers": [with_api(live("b", 18081), "10.0.1.11:8888")],
                      "chain_api": { "listen_port": wax_api } }
                ]
            }))
            .unwrap()
        };
        two(18888, 18889).validate().unwrap();
        assert_eq!(
            validation_error(&two(17001, 18889)),
            "listener 'eos': chain_api port 17001 on 0.0.0.0 conflicts with listener 'wax' (0.0.0.0:17001)"
        );
        assert_eq!(
            validation_error(&two(18888, 18888)),
            "listener 'wax': chain_api port 18888 on 0.0.0.0 conflicts with the chain_api of listener 'eos'"
        );
    }

    // C5
    #[test]
    fn chain_api_needs_an_enabled_server_with_api_endpoint() {
        let mut off = with_api(live("off", 18081), "10.0.1.10:8888");
        off["enabled"] = serde_json::json!(false);
        for servers in [
            serde_json::json!([live("a", 18080)]),
            serde_json::json!([live("a", 18080), off]),
        ] {
            let cfg = multi(
                serde_json::json!({ "servers": servers, "chain_api": { "listen_port": 18888 } }),
                serde_json::json!({}),
            );
            assert_eq!(
                validation_error(&cfg),
                "listener 'eos': chain_api requires at least one enabled server with api_endpoint"
            );
        }
        // An archive alone is enough.
        let archive_only = single(
            serde_json::json!([with_api(
                frozen("arc", 18080, Some((2, 1001))),
                "10.0.3.10:8890"
            )]),
            serde_json::json!({ "chain_id": CHAIN_ID, "chain_api": { "listen_port": 18888 } }),
        );
        archive_only.validate().unwrap();
    }

    // C6
    #[test]
    fn chain_api_allow_is_a_non_empty_set() {
        let empty = single_api(serde_json::json!({ "listen_port": 18888, "allow": [] }));
        assert_eq!(
            validation_error(&empty),
            "chain_api.allow must not be empty"
        );
        let twice = single_api(
            serde_json::json!({ "listen_port": 18888, "allow": ["block", "info", "block"] }),
        );
        assert_eq!(
            validation_error(&twice),
            "chain_api.allow must list each class at most once"
        );
        let blocks = single_api(serde_json::json!({ "listen_port": 18888, "allow": ["block"] }));
        blocks.validate().unwrap();
    }

    // C7
    #[test]
    fn chain_api_limits_are_bounded() {
        let api = |extra: serde_json::Value| {
            let mut value = serde_json::json!({ "listen_port": 18888 });
            for (key, item) in extra.as_object().unwrap() {
                value[key] = item.clone();
            }
            single_api(value)
        };
        let cases = [
            (
                serde_json::json!({ "max_body_bytes": 1023 }),
                "chain_api.max_body_bytes must be between 1024 and 67108864",
            ),
            (
                serde_json::json!({ "max_body_bytes": 67_108_865 }),
                "chain_api.max_body_bytes must be between 1024 and 67108864",
            ),
            (
                serde_json::json!({ "max_attempts": 0 }),
                "chain_api.max_attempts must be between 1 and 8",
            ),
            (
                serde_json::json!({ "max_attempts": 9 }),
                "chain_api.max_attempts must be between 1 and 8",
            ),
            (
                serde_json::json!({ "probe_ms": 99 }),
                "chain_api.probe_ms must be between 100 and 60000",
            ),
            (
                serde_json::json!({ "probe_ms": 60_001 }),
                "chain_api.probe_ms must be between 100 and 60000",
            ),
            (
                serde_json::json!({ "upstream_timeout_ms": 99 }),
                "chain_api.upstream_timeout_ms must be at least 100",
            ),
            (
                serde_json::json!({ "upstream_timeout_ms": 5000, "request_deadline_ms": 4999 }),
                "chain_api.request_deadline_ms must be at least upstream_timeout_ms",
            ),
            (
                serde_json::json!({ "upstream_queue_ms": 30_001 }),
                "chain_api.upstream_queue_ms must be at most request_deadline_ms",
            ),
            (
                serde_json::json!({ "max_connections": 0 }),
                "chain_api.max_connections must be at least 1",
            ),
            (
                serde_json::json!({ "max_in_flight": 0 }),
                "chain_api.max_in_flight must be at least 1",
            ),
            (
                serde_json::json!({ "max_in_flight_per_upstream": 0 }),
                "chain_api.max_in_flight_per_upstream must be at least 1",
            ),
            // Each cap sizes a semaphore, which panics past its limit.
            (
                serde_json::json!({ "max_connections": 1_000_001 }),
                "chain_api.max_connections must be at most 1000000",
            ),
            (
                serde_json::json!({ "max_in_flight": u64::MAX }),
                "chain_api.max_in_flight must be at most 1000000",
            ),
            (
                serde_json::json!({ "max_in_flight_per_upstream": 1_000_001 }),
                "chain_api.max_in_flight_per_upstream must be at most 1000000",
            ),
        ];
        for (extra, want) in cases {
            assert_eq!(validation_error(&api(extra.clone())), want, "{extra}");
        }
        let edges = serde_json::json!({
            "max_body_bytes": 1024, "max_attempts": 1, "probe_ms": 60_000,
            "upstream_timeout_ms": 100, "request_deadline_ms": 100, "upstream_queue_ms": 100,
            "max_connections": 1, "max_in_flight": 1, "max_in_flight_per_upstream": 1
        });
        api(edges).validate().unwrap();
        api(serde_json::json!({ "max_body_bytes": 67_108_864, "max_attempts": 8, "probe_ms": 100, "upstream_queue_ms": 0 }))
            .validate()
            .unwrap();
        api(serde_json::json!({
            "max_connections": 1_000_000, "max_in_flight": 1_000_000,
            "max_in_flight_per_upstream": 1_000_000
        }))
        .validate()
        .unwrap();
    }

    #[test]
    fn listener_connection_caps_are_bounded() {
        let mut cfg = minimal_config();
        cfg["max_connections"] = serde_json::json!(1_000_001);
        let cfg: ServerConfig = serde_json::from_value(cfg).unwrap();
        assert_eq!(
            validation_error(&cfg),
            "max_connections must be at most 1000000"
        );
        let mut cfg = minimal_config();
        cfg["max_connections"] = serde_json::json!(1_000_000);
        serde_json::from_value::<ServerConfig>(cfg)
            .unwrap()
            .validate()
            .unwrap();
        let listener = |cap: u64| {
            multi(
                serde_json::json!({ "servers": [live("a", 18080)], "max_connections": cap }),
                serde_json::json!({}),
            )
        };
        assert_eq!(
            validation_error(&listener(u64::MAX)),
            "listener 'eos': max_connections must be at most 1000000"
        );
        assert_eq!(
            validation_error(&listener(0)),
            "listener 'eos': max_connections must be greater than 0"
        );
        listener(1_000_000).validate().unwrap();
    }

    // C8
    #[test]
    fn push_servers_name_enabled_live_servers_with_an_api() {
        let push = |names: serde_json::Value, allow: serde_json::Value| {
            let mut off = with_api(live("off", 18082), "10.0.1.12:8888");
            off["enabled"] = serde_json::json!(false);
            multi(
                serde_json::json!({
                    "chain_id": CHAIN_ID,
                    "servers": [
                        with_api(frozen("archive-0a", 18080, Some((2, 1001))), "10.0.3.10:8890"),
                        with_api(live("live-1", 18081), "10.0.1.10:8888"),
                        off,
                        live("no-api", 18083)
                    ],
                    "chain_api": { "listen_port": 18888, "allow": allow, "push_servers": names }
                }),
                serde_json::json!({}),
            )
        };
        let with_push = serde_json::json!(["block", "push"]);
        push(serde_json::json!(["live-1"]), with_push.clone())
            .validate()
            .unwrap();
        for name in ["archive-0a", "off", "no-api", "missing"] {
            assert_eq!(
                validation_error(&push(serde_json::json!([name]), with_push.clone())),
                format!("listener 'eos': chain_api.push_servers: '{name}' is not an enabled live server with api_endpoint")
            );
        }
        assert_eq!(
            validation_error(&push(serde_json::json!(["live-1", "live-1"]), with_push)),
            "listener 'eos': chain_api.push_servers lists 'live-1' more than once"
        );
        assert_eq!(
            validation_error(&push(
                serde_json::json!(["live-1"]),
                serde_json::json!(["block"])
            )),
            "listener 'eos': chain_api.push_servers requires \"push\" in chain_api.allow"
        );
    }

    // C9
    #[test]
    fn top_level_chain_api_cannot_be_combined_with_listeners() {
        let cfg = multi(
            serde_json::json!({ "servers": [with_api(live("a", 18080), "10.0.1.10:8888")] }),
            serde_json::json!({ "chain_api": { "listen_port": 18888 } }),
        );
        assert_eq!(
            validation_error(&cfg),
            "top-level 'chain_api' cannot be combined with 'listeners'; set 'chain_api' on each listener"
        );
    }

    // C10
    #[test]
    fn cors_values_must_be_valid_header_values() {
        let cors = |cors: serde_json::Value| {
            single_api(serde_json::json!({ "listen_port": 18888, "cors": cors }))
        };
        cors(serde_json::json!({ "allow_origin": "https://explorer.example" }))
            .validate()
            .unwrap();
        for bad in ["", "  ", "a\r\nX-Injected: 1", "a\nb", "a\u{0}b"] {
            assert_eq!(
                validation_error(&cors(serde_json::json!({ "allow_origin": bad }))),
                "chain_api.cors.allow_origin is invalid",
                "{bad:?}"
            );
            assert_eq!(
                validation_error(&cors(
                    serde_json::json!({ "allow_origin": "*", "allow_headers": bad })
                )),
                "chain_api.cors.allow_headers is invalid",
                "{bad:?}"
            );
        }
    }

    #[test]
    fn chain_api_warnings() {
        // api_endpoint without chain_api.
        let ignored = single(
            serde_json::json!([with_api(live("a", 18080), "10.0.1.10:8888")]),
            serde_json::json!({}),
        );
        ignored.validate().unwrap();
        assert_eq!(
            ignored.warnings(),
            vec!["api_endpoint is ignored without chain_api".to_string()]
        );

        // A frozen server without an API, no live API, push allowed, and a
        // wildcard bind.
        let cfg = multi(
            serde_json::json!({
                "chain_id": CHAIN_ID,
                "listen_address": "0.0.0.0",
                "servers": [
                    with_api(frozen("archive-0a", 18080, Some((2, 1001))), "10.0.3.10:8890"),
                    frozen("archive-0b", 18081, Some((2, 1001))),
                    live("live-1", 18082)
                ],
                "chain_api": { "listen_port": 18888, "allow": ["block", "push"] }
            }),
            serde_json::json!({}),
        );
        cfg.validate().unwrap();
        assert_eq!(
            cfg.warnings(),
            vec![
                "listener 'eos': server 'archive-0b': no api_endpoint; the chain API does not use it".to_string(),
                "listener 'eos': chain_api: no enabled live server has api_endpoint; get_info, state, push and reversible blocks answer 503".to_string(),
                "listener 'eos': chain_api allows push: the port relays transactions for anyone who can reach it".to_string(),
                "listener 'eos': chain_api listens on 0.0.0.0:18888 (every interface): the chain API has no authentication".to_string(),
            ]
        );
        let v6 = multi_api(serde_json::json!({ "listen_port": 18888, "listen_address": "::" }));
        v6.validate().unwrap();
        assert_eq!(
            v6.warnings(),
            vec!["listener 'eos': chain_api listens on [::]:18888 (every interface): the chain API has no authentication".to_string()]
        );

        // A private bind with live and frozen APIs: no warning.
        let quiet = single_api(serde_json::json!({ "listen_port": 18888 }));
        quiet.validate().unwrap();
        assert!(quiet.warnings().is_empty(), "{:?}", quiet.warnings());
        let quiet =
            multi_api(serde_json::json!({ "listen_port": 18888, "listen_address": "10.0.0.5" }));
        quiet.validate().unwrap();
        assert!(quiet.warnings().is_empty(), "{:?}", quiet.warnings());
    }
}
