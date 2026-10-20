//! Transactional configuration replacement. Acceptors read one published routing
//! table. Established connections retain their listener, policy and monitors until
//! they finish; a reload never sends the process shutdown signal to them.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::atomic::Ordering;
use std::sync::{Arc, RwLock};
use std::time::Duration;

use anyhow::{bail, Context, Result};
use serde::Serialize;
use tokio::net::{lookup_host, TcpListener};
use tokio::sync::{broadcast, Mutex, Semaphore};
use tokio::task::JoinHandle;
use tracing::Instrument;

use crate::api::{AppState, ListenerContext, SharedState};
use crate::chain_api::ChainApiCtx;
use crate::health::ListenerHealthState;
use crate::models::{
    build_config_db, build_state_db_with_chain_id, format_bind_addr, ProxyLimits, ResolvedChainApi,
    ResolvedListener, ServerConfig, StaticConfig,
};
use crate::session::{handle_client, ListenerCtx};
use crate::{api, fleet::FleetHub, tasks, telemetry};

mod config;
pub use config::routes;

struct Generation {
    resolved: ResolvedListener,
    chain_config: Option<ResolvedChainApi>,
    static_config: StaticConfig,
    ctx: Arc<ListenerCtx>,
    api: Option<Arc<ChainApiCtx>>,
    info: ListenerContext,
    connections: Arc<Semaphore>,
    stop: broadcast::Sender<()>,
}

impl Generation {
    fn new(
        resolved: ResolvedListener,
        chain_config: Option<ResolvedChainApi>,
        static_config: StaticConfig,
        limits: ProxyLimits,
        live_lag: u32,
        revision: u64,
    ) -> Arc<Self> {
        let config_db = build_config_db(resolved.servers.clone());
        let state = build_state_db_with_chain_id(
            resolved.servers.clone(),
            resolved.expected_chain_id,
            resolved.require_chain_id,
        );
        let hub = FleetHub::new(&resolved, state.clone(), config_db.clone());
        let ctx = ListenerCtx::with_generation(&resolved, limits, live_lag, hub.clone(), revision);
        let api = chain_config.as_ref().map(|cfg| {
            ChainApiCtx::new(
                cfg,
                hub.clone(),
                Duration::from_millis(limits.connect_timeout_ms),
                limits.handshake_timeout_ms,
                live_lag,
            )
        });
        let info = ListenerContext {
            config_generation: revision,
            health: ListenerHealthState {
                name: resolved.name.clone(),
                state_db: state,
                hub: Some(hub),
                chain_api: api.clone(),
            },
            config_db,
            order: resolved
                .servers
                .iter()
                .filter(|s| s.enabled)
                .map(|s| s.endpoint.clone())
                .collect(),
            port: resolved.listen_port,
            chain_id: resolved.expected_chain_id,
            max_connections: resolved.max_connections,
            stats: Arc::default(),
        };
        Arc::new(Self {
            connections: Arc::new(Semaphore::new(resolved.max_connections)),
            resolved,
            chain_config,
            static_config,
            ctx,
            api,
            info,
            stop: broadcast::channel(16).0,
        })
    }

    /// Keep samples and shared capacity/counters for the same upstream identity.
    /// Each generation still owns its policy and monitoring lifecycle.
    async fn inherit_observations(&self, previous: &Self) {
        let old = previous.ctx.hub.state.lock().await;
        let mut states = self.ctx.hub.state.lock().await;
        for (endpoint, state) in states.iter_mut() {
            if let Some(sample) = old.get(endpoint) {
                let configured = state.clone();
                *state = sample.clone();
                state.name = configured.name;
                state.index = configured.index;
                state.role = configured.role;
                state.declared = configured.declared;
                state.max_sessions = configured.max_sessions;
                state.priority = configured.priority;
                state.weight = configured.weight;
            }
        }
        self.ctx.hub.rebuild(&states);
    }

    fn start(&self) {
        tracing::info!(listener = self.info.name(), generation = self.info.config_generation,
            address = %self.resolved.listen_address, port = self.resolved.listen_port,
            "listener configuration active");
        if self.resolved.name.is_some() && self.resolved.expected_chain_id.is_none() {
            tracing::warn!(listener = self.info.name(), "listener has no pinned chain_id; first online upstream will establish the pool chain_id (set chain_id in config to prevent cross-chain misconfiguration)");
        }
        for server in self.resolved.servers.iter().filter(|s| s.enabled) {
            tracing::info!(listener = self.info.name(), name = %server.name,
                upstream = %server.endpoint, "starting upstream monitor");
            tokio::spawn(tasks::monitoring_connection(
                server.clone(),
                self.static_config.clone(),
                self.ctx.hub.clone(),
                self.stop.subscribe(),
            ));
        }
        tokio::spawn(tasks::state_monitoring_loop(
            self.ctx.hub.clone(),
            self.static_config.upstream_monitoring_ms,
            self.stop.subscribe(),
        ));
        if let Some(api) = &self.api {
            tracing::info!(listener = self.info.name(), address = %api.bind, "chain API listening");
            api.start_probes(&self.stop);
            api.stats.set_bound(true);
        }
    }

    fn empty(&self) -> bool {
        self.connections.available_permits() == self.resolved.max_connections
            && self
                .api
                .as_ref()
                .is_none_or(|a| a.connections.available_permits() == a.max_connections())
    }
}

impl Drop for Generation {
    fn drop(&mut self) {
        let _ = self.stop.send(());
    }
}

#[derive(Clone)]
enum Binding {
    Ship(Arc<Generation>),
    Chain(Arc<Generation>),
    Dashboard,
}

struct Live {
    app: SharedState,
    bindings: HashMap<SocketAddr, Binding>,
    draining: Vec<Arc<Generation>>,
}

struct Engine {
    revision: u64,
    config: Option<ServerConfig>,
    source: String,
    active: Vec<Arc<Generation>>,
    draining: Vec<Arc<Generation>>,
    sockets: HashMap<SocketAddr, JoinHandle<()>>,
    stopping: bool,
}

struct Prepared {
    config: ServerConfig,
    token: Option<String>,
    active: Vec<Arc<Generation>>,
    fresh: Vec<Arc<Generation>>,
    bindings: HashMap<SocketAddr, Binding>,
    sockets: Vec<(SocketAddr, TcpListener)>,
}

pub struct Runtime {
    path: PathBuf,
    live: RwLock<Arc<Live>>,
    engine: Mutex<Engine>,
    shutdown: broadcast::Sender<()>,
    http_slots: Arc<Semaphore>,
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct DrainStatus {
    listener: String,
    generation: u64,
    sessions: usize,
    chain_api_connections: usize,
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct ConfigStatus {
    revision: u64,
    draining: Vec<DrainStatus>,
}

impl ConfigStatus {
    pub fn from_app(app: &AppState, counts: &HashMap<(String, u64), usize>) -> Self {
        Self {
            revision: app.config_revision,
            draining: app
                .draining
                .iter()
                .map(|ctx| DrainStatus {
                    listener: ctx.name().to_string(),
                    generation: ctx.config_generation,
                    sessions: *counts
                        .get(&(ctx.name().to_string(), ctx.config_generation))
                        .unwrap_or(&0),
                    chain_api_connections: ctx.health.chain_api.as_ref().map_or(0, |a| {
                        a.max_connections()
                            .saturating_sub(a.connections.available_permits())
                    }),
                })
                .collect(),
        }
    }
}

impl Runtime {
    pub async fn start(path: PathBuf) -> Result<Arc<Self>> {
        let source = config::read_source(&path)?;
        let config: ServerConfig = serde_json::from_str(&source)?;
        let app = Arc::new(AppState {
            runtime: Default::default(),
            config_revision: 0,
            draining: Vec::new(),
            listeners: Vec::new(),
            health: Vec::new(),
            dashboard: false,
            control_token: None,
            allowed_hosts: Vec::new(),
            streams: Arc::new(Semaphore::new(api::MAX_STREAMS)),
            dashboard_feed: Arc::default(),
        });
        let runtime = Arc::new(Self {
            path,
            live: RwLock::new(Arc::new(Live {
                app,
                bindings: HashMap::new(),
                draining: Vec::new(),
            })),
            engine: Mutex::new(Engine {
                revision: 0,
                config: None,
                source: String::new(),
                active: Vec::new(),
                draining: Vec::new(),
                sockets: HashMap::new(),
                stopping: false,
            }),
            shutdown: broadcast::channel(64).0,
            http_slots: Arc::new(Semaphore::new(crate::http::MAX_HTTP_CONNECTIONS)),
        });
        let mut engine = runtime.engine.lock().await;
        let prepared = runtime.prepare(&engine, config).await?;
        runtime.commit(&mut engine, prepared, source).await;
        drop(engine);
        // One feed survives configuration replacements and HTTP port changes.
        tokio::spawn(api::run_dashboard(
            runtime.app(),
            runtime.shutdown.subscribe(),
        ));
        let weak = Arc::downgrade(&runtime);
        let mut shutdown = runtime.shutdown.subscribe();
        tokio::spawn(async move {
            loop {
                tokio::select! { _ = shutdown.recv() => break, _ = tokio::time::sleep(Duration::from_secs(1)) => {} }
                let Some(runtime) = weak.upgrade() else {
                    break;
                };
                runtime.reap().await;
            }
        });
        telemetry::global().emit(telemetry::Event::new(
            telemetry::Level::Info,
            telemetry::EventKind::Started,
            format!("fleet-router {} started", env!("CARGO_PKG_VERSION")),
        ));
        Ok(runtime)
    }

    fn snapshot(&self) -> Arc<Live> {
        self.live.read().unwrap_or_else(|p| p.into_inner()).clone()
    }
    pub fn app(&self) -> SharedState {
        self.snapshot().app.clone()
    }

    async fn prepare(&self, engine: &Engine, config: ServerConfig) -> Result<Prepared> {
        if engine.stopping {
            bail!("router is shutting down");
        }
        config.validate()?;
        let token = if config.dashboard.enabled {
            config.dashboard.resolve_control_token()?
        } else {
            None
        };
        let statics = StaticConfig {
            upstream_reconnect_ms: config.upstream_reconnect_ms,
            upstream_monitoring_ms: config.upstream_monitoring_ms,
            upstream_status_ms: config.upstream_status_ms,
            connect_timeout_ms: config.upstream_connect_timeout_ms(),
            max_message_bytes: config.max_message_bytes,
        };
        let limits = config.proxy_limits();
        let mut prepared = Prepared {
            config,
            token,
            active: Vec::new(),
            fresh: Vec::new(),
            bindings: HashMap::new(),
            sockets: Vec::new(),
        };
        for (resolved, chain) in prepared
            .config
            .resolved_listeners()
            .into_iter()
            .zip(prepared.config.resolved_chain_apis())
        {
            let reused = engine
                .active
                .iter()
                .find(|g| {
                    g.resolved == resolved
                        && g.chain_config == chain
                        && g.static_config == statics
                        && g.ctx.limits == limits
                        && g.ctx.live_lag_blocks == prepared.config.live_lag_blocks
                })
                .cloned();
            let generation = match reused {
                Some(g) => g,
                None => {
                    let generation = Generation::new(
                        resolved.clone(),
                        chain.clone(),
                        statics.clone(),
                        limits,
                        prepared.config.live_lag_blocks,
                        engine.revision + 1,
                    );
                    // An upstream can disappear and return while an older pool
                    // still serves streams. Retain its shared capacity too;
                    // newer observations take precedence over retired samples.
                    for previous in engine.draining.iter().chain(&engine.active).filter(|g| {
                        g.resolved.name == resolved.name
                            && g.resolved.expected_chain_id == resolved.expected_chain_id
                            && g.resolved.require_chain_id == resolved.require_chain_id
                    }) {
                        generation.inherit_observations(previous).await;
                    }
                    prepared.fresh.push(generation.clone());
                    generation
                }
            };
            self.prepare_bind(
                engine,
                &mut prepared,
                &format_bind_addr(&resolved.listen_address, resolved.listen_port),
                Binding::Ship(generation.clone()),
            )
            .await?;
            if let Some(chain) = chain {
                self.prepare_bind(
                    engine,
                    &mut prepared,
                    &chain.bind,
                    Binding::Chain(generation.clone()),
                )
                .await?;
            }
            prepared.active.push(generation);
        }
        if let Some((host, port)) = prepared.config.resolved_metrics_host_port() {
            self.prepare_bind(
                engine,
                &mut prepared,
                &format_bind_addr(&host, port),
                Binding::Dashboard,
            )
            .await?;
        }
        Ok(prepared)
    }

    async fn prepare_bind(
        &self,
        engine: &Engine,
        prepared: &mut Prepared,
        bind: &str,
        target: Binding,
    ) -> Result<()> {
        let addresses: Vec<_> = tokio::time::timeout(Duration::from_secs(5), lookup_host(bind))
            .await
            .context("bind address lookup timed out")??
            .collect();
        let existing = addresses
            .iter()
            .find(|addr| engine.sockets.contains_key(addr))
            .copied();
        let addr = match existing {
            Some(addr) => addr,
            None => {
                let socket = TcpListener::bind(addresses.as_slice())
                    .await
                    .with_context(|| {
                        format!("cannot bind {bind}; running configuration is unchanged")
                    })?;
                let addr = socket.local_addr()?;
                prepared.sockets.push((addr, socket));
                addr
            }
        };
        if prepared.bindings.insert(addr, target).is_some() {
            bail!("multiple listeners resolve to {addr}");
        }
        Ok(())
    }

    async fn commit(self: &Arc<Self>, engine: &mut Engine, prepared: Prepared, source: String) {
        let _change = telemetry::global().begin_change();
        let old = self.snapshot();
        let Prepared {
            config,
            token,
            active,
            fresh,
            bindings,
            sockets,
        } = prepared;
        for generation in engine.active.drain(..) {
            if !active.iter().any(|g| Arc::ptr_eq(g, &generation)) {
                engine.draining.push(generation);
            }
        }
        engine.active = active;
        engine.revision += 1;
        engine.source = source;
        engine.config = Some(config);
        // Publication and accept dispatch share this lock. A socket accepted
        // before publication already owns its permit and belongs to the old pool.
        {
            let mut live = self.live.write().unwrap_or_else(|p| p.into_inner());
            // No old accept can acquire a permit after this point. Empty pools
            // retire immediately, avoiding duplicate idle monitors on reload.
            engine.draining.retain(|g| !g.empty());
            let app = self.make_app(engine, token, &old.app);
            *live = Arc::new(Live {
                app,
                bindings,
                draining: engine.draining.clone(),
            });
        }
        for generation in fresh {
            generation.start();
        }
        for (addr, listener) in sockets {
            let weak = Arc::downgrade(self);
            let handle = tokio::spawn(async move {
                loop {
                    match listener.accept().await {
                        Ok((stream, peer)) => {
                            let Some(runtime) = weak.upgrade() else {
                                break;
                            };
                            runtime.accept(addr, stream, peer);
                        }
                        Err(error) => {
                            tracing::warn!(%addr, %error, "accept failed");
                            tokio::time::sleep(Duration::from_secs(1)).await;
                        }
                    }
                }
            });
            engine.sockets.insert(addr, handle);
        }
        let live = self.snapshot();
        let removed: Vec<_> = engine
            .sockets
            .keys()
            .filter(|addr| !live.bindings.contains_key(addr))
            .copied()
            .collect();
        for addr in removed {
            if let Some(task) = engine.sockets.remove(&addr) {
                task.abort();
                let _ = task.await;
            }
        }
        drop(old);
        tracing::info!(
            revision = engine.revision,
            listeners = engine.active.len(),
            draining = engine.draining.len(),
            "configuration applied"
        );
        if let Some((host, port)) = engine.config.as_ref().unwrap().resolved_metrics_host_port() {
            tracing::info!(%host, port, dashboard = self.app().dashboard, "health/metrics endpoint active");
        }
        let dashboard = &engine.config.as_ref().unwrap().dashboard;
        if !dashboard.enabled
            && (dashboard.control_token.is_some() || dashboard.control_token_file.is_some())
        {
            tracing::warn!("a control token is configured but dashboard.enabled is false; the control API stays off");
        }
        if self.app().control_token.is_some() {
            if let Some((host, port)) = engine.config.as_ref().unwrap().resolved_metrics_host_port()
            {
                if !crate::is_loopback_host(&host) {
                    tracing::warn!(%host, port, "control API enabled on a non-loopback address; restrict access to this port");
                }
            }
        }
        for warning in engine.config.as_ref().unwrap().warnings() {
            tracing::warn!("{warning}");
        }
        telemetry::global().emit(telemetry::Event::new(
            telemetry::Level::Info,
            telemetry::EventKind::Control,
            format!(
                "configuration {} applied; established streams retain their previous configuration",
                engine.revision
            ),
        ));
    }

    fn make_app(
        self: &Arc<Self>,
        engine: &Engine,
        control_token: Option<String>,
        previous: &AppState,
    ) -> SharedState {
        let config = engine.config.as_ref().unwrap();
        Arc::new(AppState {
            runtime: Arc::downgrade(self),
            config_revision: engine.revision,
            listeners: engine.active.iter().map(|g| g.info.clone()).collect(),
            draining: engine.draining.iter().map(|g| g.info.clone()).collect(),
            health: engine
                .active
                .iter()
                .map(|g| g.info.health.clone())
                .collect(),
            dashboard: config.dashboard.enabled,
            control_token,
            allowed_hosts: config
                .dashboard
                .allowed_hosts
                .iter()
                .map(|s| s.trim().to_ascii_lowercase())
                .collect(),
            streams: previous.streams.clone(),
            dashboard_feed: previous.dashboard_feed.clone(),
        })
    }

    fn accept(self: &Arc<Self>, addr: SocketAddr, stream: tokio::net::TcpStream, peer: SocketAddr) {
        let live = self.live.read().unwrap_or_else(|p| p.into_inner());
        match live.bindings.get(&addr) {
            Some(Binding::Ship(generation)) => {
                let existing = generation
                    .resolved
                    .max_connections
                    .saturating_sub(generation.connections.available_permits())
                    + live
                        .draining
                        .iter()
                        .filter(|g| g.info.name() == generation.info.name())
                        .map(|g| {
                            g.resolved
                                .max_connections
                                .saturating_sub(g.connections.available_permits())
                        })
                        .sum::<usize>();
                if existing >= generation.resolved.max_connections {
                    generation
                        .info
                        .stats
                        .rejected_total
                        .fetch_add(1, Ordering::Relaxed);
                    return;
                }
                let Ok(permit) = generation.connections.clone().try_acquire_owned() else {
                    generation
                        .info
                        .stats
                        .rejected_total
                        .fetch_add(1, Ordering::Relaxed);
                    return;
                };
                generation
                    .info
                    .stats
                    .accepted_total
                    .fetch_add(1, Ordering::Relaxed);
                let generation = generation.clone();
                let shutdown = self.shutdown.subscribe();
                let span =
                    tracing::info_span!("client", listener = %generation.ctx.label, addr = %peer);
                tokio::spawn(
                    async move {
                        let _permit = permit;
                        handle_client(stream, peer, generation.ctx.clone(), shutdown).await;
                    }
                    .instrument(span),
                );
            }
            Some(Binding::Chain(generation)) => {
                if let Some(api) = &generation.api {
                    let existing = api.max_connections() - api.connections.available_permits()
                        + live
                            .draining
                            .iter()
                            .filter(|g| g.info.name() == generation.info.name())
                            .filter_map(|g| g.api.as_ref())
                            .map(|old| old.max_connections() - old.connections.available_permits())
                            .sum::<usize>();
                    if existing >= api.max_connections() {
                        api.stats
                            .rejected
                            .inc(crate::chain_api::stats::Rejection::Connections);
                        return;
                    }
                    api.accept(stream, peer, self.shutdown.subscribe());
                }
            }
            Some(Binding::Dashboard) => {
                if let Ok(permit) = self.http_slots.clone().try_acquire_owned() {
                    let app = live.app.clone();
                    let shutdown = self.shutdown.subscribe();
                    tokio::spawn(async move {
                        let _permit = permit;
                        crate::http::connection(stream, app, shutdown).await;
                    });
                }
            }
            None => {}
        }
    }

    async fn reap(self: &Arc<Self>) {
        let mut engine = self.engine.lock().await;
        if engine.stopping {
            return;
        }
        let before = engine.draining.len();
        engine.draining.retain(|g| !g.empty());
        if before != engine.draining.len() {
            let _change = telemetry::global().begin_change();
            let live = self.snapshot();
            let app = self.make_app(&engine, live.app.control_token.clone(), &live.app);
            *self.live.write().unwrap_or_else(|p| p.into_inner()) = Arc::new(Live {
                app,
                bindings: live.bindings.clone(),
                draining: engine.draining.clone(),
            });
        }
    }

    pub async fn shutdown(&self) {
        let mut engine = self.engine.lock().await;
        engine.stopping = true;
        let _ = self.shutdown.send(());
        for (_, task) in engine.sockets.drain() {
            task.abort();
            let _ = task.await;
        }
        let mut limits = Vec::new();
        for g in engine.active.iter().chain(&engine.draining) {
            let _ = g.stop.send(());
            limits.push((g.connections.clone(), g.resolved.max_connections));
            if let Some(a) = &g.api {
                limits.push((a.connections.clone(), a.max_connections()));
            }
        }
        let grace = engine.config.as_ref().map_or(0, |c| c.shutdown_grace_ms);
        drop(engine);
        crate::drain_connections(&limits, grace).await;
    }
}
