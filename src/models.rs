use std::collections::HashMap;
use std::sync::atomic::AtomicUsize;
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{anyhow, bail, Result};
use serde::{Deserialize, Serialize};
use tokio::sync::Mutex;

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
    /// Optional address for the HTTP health/metrics endpoint. Defaults to `listen_address` (or `"0.0.0.0"`).
    #[serde(default)]
    pub metrics_address: Option<String>,
    /// Optional port for the HTTP health/metrics endpoint. When unset, the endpoint is disabled.
    #[serde(default)]
    pub metrics_port: Option<u16>,
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
        if self.max_message_bytes == 0 {
            bail!("max_message_bytes must be greater than 0");
        }
        if self.max_client_message_bytes == 0 {
            bail!("max_client_message_bytes must be greater than 0");
        }
        if let Some(0) = self.metrics_port {
            bail!("metrics_port must be non-zero when set");
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
                if let Some(0) = listener.max_connections {
                    bail!("listener '{name}': max_connections must be greater than 0");
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

        Ok(())
    }

    /// Normalized list of ingress listeners to run.
    ///
    /// Returns a single unnamed [`ResolvedListener`] in legacy single-port mode,
    /// or one named [`ResolvedListener`] per entry in `self.listeners`.
    pub fn resolved_listeners(&self) -> Vec<ResolvedListener> {
        if self.listeners.is_empty() {
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
                .map(|l| ResolvedListener {
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
            max_message_bytes: self.max_message_bytes,
            max_client_message_bytes: self.max_client_message_bytes,
        }
    }
}

/// Live state tracked per upstream by the monitoring loop and the proxy.
#[derive(Debug)]
pub struct ServerState {
    /// Configured [`Server::name`], copied when the state map is built so
    /// `/metrics` can emit a `server` label.
    pub name: String,
    /// Active client connections currently routed to this upstream.
    /// `Arc<AtomicUsize>` so [`crate::connection_handler::ConnectionGuard`] can
    /// decrement synchronously on drop without locking the whole map.
    pub connections: Arc<AtomicUsize>,
    pub enabled: bool,
    pub online: bool,
    /// Set by the monitoring loop when an upstream stops advancing its chain
    /// state. Stale upstreams are deprioritized (but not excluded) when routing.
    pub stale: bool,
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
            enabled: true,
            online: false,
            stale: false,
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
        }
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
    /// Upstream WebSocket message/frame cap.
    pub max_message_bytes: usize,
    /// Client WebSocket message/frame cap.
    pub max_client_message_bytes: usize,
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
            .map(|s| {
                let mut state = ServerState::with_chain_policy(expected_chain_id, require_chain_id);
                state.name = s.name.clone();
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
}
