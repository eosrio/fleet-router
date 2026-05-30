use std::collections::{HashMap, HashSet};
use std::sync::atomic::AtomicUsize;
use std::sync::Arc;

use anyhow::{bail, Result};
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
fn default_shutdown_grace_ms() -> u64 {
    5_000
}

/// A single upstream SHiP node, as declared in `config.json`.
#[derive(Debug, Deserialize, Serialize, Hash, Eq, PartialEq, Clone)]
pub struct Server {
    /// Human-readable name used in logs.
    pub name: String,
    /// Upstream WebSocket endpoint as `host:port` (no scheme; `ws://` is prepended).
    pub endpoint: String,
    /// Whether the router is allowed to use this upstream.
    pub enabled: bool,
}

impl Server {
    pub fn ws_url(&self) -> String {
        format!("ws://{}", self.endpoint)
    }
}

/// The on-disk `config.json` schema.
#[derive(Debug, Serialize, Deserialize)]
pub struct ServerConfig {
    /// Address the proxy listens on for client connections.
    pub listen_address: String,
    /// Port the proxy listens on for client connections.
    pub listen_port: u16,
    /// Interval (ms) between reconnection attempts to a downed upstream.
    pub upstream_reconnect_ms: u64,
    /// Interval (ms) at which upstream status is logged.
    pub upstream_monitoring_ms: u64,
    /// Interval (ms) at which status requests are sent to upstreams.
    pub upstream_status_ms: u64,
    /// The upstream SHiP nodes to load-balance across.
    pub servers: Vec<Server>,

    /// Maximum number of concurrent client connections accepted (backpressure).
    #[serde(default = "default_max_connections")]
    pub max_connections: usize,
    /// Timeout (ms) for completing the client WebSocket handshake. 0 = disabled.
    #[serde(default = "default_handshake_timeout_ms")]
    pub handshake_timeout_ms: u64,
    /// Idle timeout (ms): close a connection that exchanges no data for this long. 0 = disabled.
    #[serde(default = "default_idle_timeout_ms")]
    pub idle_timeout_ms: u64,
    /// Maximum WebSocket message size (bytes) accepted on both client and upstream links.
    #[serde(default = "default_max_message_bytes")]
    pub max_message_bytes: usize,
    /// How long (ms) to wait for in-flight connections to drain on shutdown.
    #[serde(default = "default_shutdown_grace_ms")]
    pub shutdown_grace_ms: u64,
    /// Optional address for the HTTP health/metrics endpoint. Defaults to `listen_address`.
    #[serde(default)]
    pub metrics_address: Option<String>,
    /// Optional port for the HTTP health/metrics endpoint. When unset, the endpoint is disabled.
    #[serde(default)]
    pub metrics_port: Option<u16>,
}

impl ServerConfig {
    /// Validate the configuration, returning a clear error for any invalid field.
    pub fn validate(&self) -> Result<()> {
        if self.listen_address.trim().is_empty() {
            bail!("listen_address must not be empty");
        }
        if self.listen_port == 0 {
            bail!("listen_port must be non-zero");
        }
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
        if self.servers.is_empty() {
            bail!("config must define at least one server");
        }
        let enabled: Vec<&Server> = self.servers.iter().filter(|s| s.enabled).collect();
        if enabled.is_empty() {
            bail!("config must have at least one enabled server");
        }
        let mut seen = HashSet::new();
        for s in &enabled {
            if s.endpoint.trim().is_empty() {
                bail!("server '{}' has an empty endpoint", s.name);
            }
            if !seen.insert(s.endpoint.as_str()) {
                bail!(
                    "duplicate upstream endpoint '{}' — endpoints must be unique",
                    s.endpoint
                );
            }
        }
        if let Some(0) = self.metrics_port {
            bail!("metrics_port must be non-zero when set");
        }
        Ok(())
    }

    /// The per-connection proxy limits derived from this config.
    pub fn proxy_limits(&self) -> ProxyLimits {
        ProxyLimits {
            handshake_timeout_ms: self.handshake_timeout_ms,
            idle_timeout_ms: self.idle_timeout_ms,
            max_message_bytes: self.max_message_bytes,
        }
    }
}

/// Live state tracked per upstream by the monitoring loop and the proxy.
#[derive(Debug)]
pub struct ServerState {
    /// Active client connections currently routed to this upstream.
    /// `Arc<AtomicUsize>` so [`crate::connection_handler::ConnectionGuard`] can
    /// decrement synchronously on drop without locking the whole map.
    pub connections: Arc<AtomicUsize>,
    pub enabled: bool,
    pub online: bool,
    /// Set by the monitoring loop when an upstream stops advancing its chain
    /// state. Stale upstreams are deprioritized (but not excluded) when routing.
    pub stale: bool,
    pub trace_begin_block: u32,
    pub trace_end_block: u32,
    pub chain_state_begin_block: u32,
    pub chain_state_end_block: u32,
}

impl ServerState {
    pub fn new() -> ServerState {
        ServerState {
            connections: Arc::new(AtomicUsize::new(0)),
            enabled: true,
            online: false,
            stale: false,
            trace_begin_block: 0,
            trace_end_block: 0,
            chain_state_begin_block: 0,
            chain_state_end_block: 0,
        }
    }
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
}

/// Per-connection limits applied by the proxy data path.
#[derive(Debug, Clone, Copy)]
pub struct ProxyLimits {
    pub handshake_timeout_ms: u64,
    pub idle_timeout_ms: u64,
    pub max_message_bytes: usize,
}

pub fn build_state_db(servers: Vec<Server>) -> ServerStateDb {
    Arc::new(Mutex::new(
        servers
            .iter()
            .filter(|s| s.enabled)
            .map(|s| (s.endpoint.clone(), ServerState::new()))
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
