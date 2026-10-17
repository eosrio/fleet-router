//! HTTP endpoint on `metrics_port`.
//!
//! Always served (GET):
//!   - `/health`, `/healthz`           -> 200 while the process is running
//!   - `/ready`,  `/readyz`            -> 200 if every listener has ≥1 usable upstream online, else 503
//!   - `/ready/<name>`, `/readyz/<name>` -> 200 / 503 for one listener, 404 if unknown
//!   - `/metrics`                      -> Prometheus text exposition
//!
//! With `dashboard.enabled`: `/` and `/ui` redirect to `/ui/` (the embedded
//! dashboard), `/status` is a plain HTML status page, and `/api/v1` is the
//! JSON API from [`crate::api`].

use std::fmt::Write as _;
use std::sync::Arc;
use std::time::Duration;

use std::net::IpAddr;

use axum::extract::{Path, Request, State};
use axum::http::{header, StatusCode};
use axum::middleware::{self, Next};
use axum::response::{Html, IntoResponse, Redirect, Response};
use axum::routing::get;
use axum::Router;
use hyper::server::conn::http1;
use hyper_util::rt::{TokioIo, TokioTimer};
use hyper_util::service::TowerToHyperService;
use tokio::net::TcpListener;
use tokio::sync::{broadcast, Semaphore};

use crate::api::{self, SharedState, StatusView};
use crate::health::{evaluate_listener_readiness, evaluate_overall_readiness, render_metrics};

include!(concat!(env!("OUT_DIR"), "/ui_assets.rs"));

/// Concurrent HTTP connections, including open dashboard streams.
const MAX_HTTP_CONNECTIONS: usize = 256;
/// A client must send its request headers within this time.
const HEADER_READ_TIMEOUT: Duration = Duration::from_secs(5);

/// Run the HTTP endpoint until a shutdown signal is received.
pub async fn run_http_server(addr: String, app: SharedState, shutdown: broadcast::Receiver<()>) {
    let listener = match TcpListener::bind(&addr).await {
        Ok(listener) => listener,
        Err(e) => {
            tracing::error!(%addr, error = %e, "failed to bind health/metrics endpoint");
            return;
        }
    };
    if app.dashboard {
        tracing::info!(%addr, "health/metrics endpoint and dashboard listening (/ui/)");
    } else {
        tracing::info!(%addr, "health/metrics endpoint listening");
    }
    serve(listener, app, shutdown).await;
}

/// Accept connections on `listener` and serve [`router`] until shutdown.
pub async fn serve(listener: TcpListener, app: SharedState, mut shutdown: broadcast::Receiver<()>) {
    let router = router(app);
    let slots = Arc::new(Semaphore::new(MAX_HTTP_CONNECTIONS));
    loop {
        tokio::select! {
            accepted = listener.accept() => {
                let (stream, _peer) = match accepted {
                    Ok(pair) => pair,
                    Err(e) => {
                        tracing::warn!(error = %e, "health/metrics accept error");
                        continue;
                    }
                };
                let Ok(permit) = slots.clone().try_acquire_owned() else {
                    tracing::debug!("health/metrics connection limit reached; rejecting");
                    continue;
                };
                let service = TowerToHyperService::new(router.clone());
                let mut conn_shutdown = shutdown.resubscribe();
                tokio::spawn(async move {
                    let _permit = permit;
                    // One request per connection, as before the move to hyper:
                    // idle keep-alive connections must not hold slots.
                    let conn = http1::Builder::new()
                        .timer(TokioTimer::new())
                        .header_read_timeout(HEADER_READ_TIMEOUT)
                        .keep_alive(false)
                        .serve_connection(TokioIo::new(stream), service);
                    // Dashboard streams never end on their own, so shutdown
                    // drops the connection instead of waiting for it.
                    tokio::select! {
                        result = conn => {
                            if let Err(e) = result {
                                tracing::debug!(error = %e, "health/metrics connection error");
                            }
                        }
                        _ = conn_shutdown.recv() => {}
                    }
                });
            }
            _ = shutdown.recv() => {
                tracing::info!("health/metrics endpoint shutting down");
                break;
            }
        }
    }
}

pub fn router(app: SharedState) -> Router {
    let mut router = Router::new()
        .route("/health", get(health))
        .route("/healthz", get(health))
        .route("/ready", get(ready))
        .route("/readyz", get(ready))
        .route("/ready/{name}", get(ready_listener))
        .route("/readyz/{name}", get(ready_listener))
        .route("/metrics", get(metrics));
    if app.dashboard {
        let dashboard = Router::new()
            .route("/", get(to_dashboard))
            .route("/ui", get(to_dashboard))
            .route("/ui/", get(ui_index))
            .route("/ui/{*path}", get(ui_asset))
            .route("/status", get(status_page))
            .nest("/api/v1", api::router())
            .route_layer(middleware::from_fn_with_state(app.clone(), check_host));
        router = router.merge(dashboard);
    }
    router
        .fallback(not_found)
        .method_not_allowed_fallback(method_not_allowed)
        .with_state(app)
}

/// Whether the dashboard answers to this `Host` header value. IP literals and
/// `localhost` always pass, other names only when listed in
/// `dashboard.allowed_hosts`. This stops DNS rebinding: a page on an
/// attacker's domain that resolves to this router still sends its own name.
fn host_allowed(host: &str, allowed: &[String]) -> bool {
    let host = host.trim().to_ascii_lowercase();
    let name = match host.strip_prefix('[') {
        Some(bracketed) => bracketed.split(']').next().unwrap_or_default(),
        None => host
            .rsplit_once(':')
            .map_or(host.as_str(), |(name, _)| name),
    };
    name == "localhost" || name.parse::<IpAddr>().is_ok() || allowed.iter().any(|a| a == name)
}

async fn check_host(State(app): State<SharedState>, request: Request, next: Next) -> Response {
    let host = request
        .headers()
        .get(header::HOST)
        .and_then(|value| value.to_str().ok());
    match host {
        Some(host) if !host_allowed(host, &app.allowed_hosts) => text(
            "403 Forbidden",
            "text/plain",
            "host not allowed: add it to dashboard.allowed_hosts\n".to_string(),
        ),
        _ => next.run(request).await,
    }
}

fn text(status: &str, content_type: &'static str, body: String) -> Response {
    let code = status
        .get(..3)
        .and_then(|code| code.parse::<u16>().ok())
        .and_then(|code| StatusCode::from_u16(code).ok())
        .unwrap_or(StatusCode::INTERNAL_SERVER_ERROR);
    (code, [(header::CONTENT_TYPE, content_type)], body).into_response()
}

async fn health() -> Response {
    text("200 OK", "text/plain", "ok\n".to_string())
}

async fn ready(State(app): State<SharedState>) -> Response {
    let (status, content_type, body) = evaluate_overall_readiness(&app.health).await;
    text(status, content_type, body)
}

async fn ready_listener(State(app): State<SharedState>, Path(name): Path<String>) -> Response {
    let (status, content_type, body) = evaluate_listener_readiness(&app.health, &name).await;
    text(status, content_type, body)
}

async fn metrics(State(app): State<SharedState>) -> Response {
    text(
        "200 OK",
        "text/plain; version=0.0.4",
        render_metrics(&app.health).await,
    )
}

async fn not_found() -> Response {
    text("404 Not Found", "text/plain", "not found\n".to_string())
}

async fn method_not_allowed() -> Response {
    text(
        "405 Method Not Allowed",
        "text/plain",
        "method not allowed\n".to_string(),
    )
}

async fn to_dashboard() -> Redirect {
    Redirect::temporary("/ui/")
}

fn content_type(path: &str) -> &'static str {
    match path.rsplit('.').next().unwrap_or_default() {
        "html" => "text/html; charset=utf-8",
        "css" => "text/css; charset=utf-8",
        "js" | "mjs" => "text/javascript; charset=utf-8",
        "json" => "application/json",
        "svg" => "image/svg+xml",
        "png" => "image/png",
        "ico" => "image/x-icon",
        "woff2" => "font/woff2",
        "txt" => "text/plain; charset=utf-8",
        _ => "application/octet-stream",
    }
}

/// The dashboard loads only its own scripts, styles and fonts, and only talks
/// to this origin. It cannot be framed.
const DASHBOARD_CSP: &str = "default-src 'none'; script-src 'self'; style-src 'self'; \
     font-src 'self'; img-src 'self' data:; connect-src 'self'; base-uri 'none'; \
     form-action 'none'; frame-ancestors 'none'";

/// The status page has one inline stylesheet and no scripts.
const STATUS_CSP: &str =
    "default-src 'none'; style-src 'unsafe-inline'; base-uri 'none'; form-action 'none'; \
     frame-ancestors 'none'";

fn asset(path: &str) -> Response {
    match UI_ASSETS.binary_search_by(|(name, _)| (*name).cmp(path)) {
        Ok(index) => {
            let (name, bytes) = UI_ASSETS[index];
            (
                [
                    (header::CONTENT_TYPE, content_type(name)),
                    // Assets change with the binary; revalidate on every load.
                    (header::CACHE_CONTROL, "no-cache"),
                    (header::X_CONTENT_TYPE_OPTIONS, "nosniff"),
                    (header::CONTENT_SECURITY_POLICY, DASHBOARD_CSP),
                    (header::REFERRER_POLICY, "no-referrer"),
                ],
                bytes,
            )
                .into_response()
        }
        Err(_) => (StatusCode::NOT_FOUND, "not found\n").into_response(),
    }
}

async fn ui_index() -> Response {
    asset("index.html")
}

async fn ui_asset(Path(path): Path<String>) -> Response {
    asset(&path)
}

fn escape_html(value: &str) -> String {
    let mut out = String::with_capacity(value.len());
    for c in value.chars() {
        match c {
            '&' => out.push_str("&amp;"),
            '<' => out.push_str("&lt;"),
            '>' => out.push_str("&gt;"),
            '"' => out.push_str("&quot;"),
            '\'' => out.push_str("&#39;"),
            _ => out.push(c),
        }
    }
    out
}

fn format_uptime(secs: u64) -> String {
    let (days, hours, minutes) = (secs / 86_400, secs / 3_600 % 24, secs / 60 % 60);
    if days > 0 {
        format!("{days}d {hours}h {minutes}m")
    } else if hours > 0 {
        format!("{hours}h {minutes}m")
    } else {
        format!("{minutes}m {}s", secs % 60)
    }
}

fn format_range(range: Option<&api::RangeView>) -> String {
    range.map_or_else(
        || "&ndash;".to_string(),
        |r| format!("{}&ndash;{}", r.begin, r.end.saturating_sub(1)),
    )
}

/// Render the plain status page. No JavaScript; it reloads every 5 seconds.
pub fn render_status_page(status: &StatusView) -> String {
    let mut out = String::with_capacity(4096);
    let _ = write!(
        out,
        r#"<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<meta http-equiv="refresh" content="5">
<title>fleet-router status</title>
<style>
:root {{ color-scheme: dark light; --paper:#050505; --ink:#f0f0f0; --ink-3:#8c8c8c; --rule:#1f1f1f; --accent:#14b8a6; --warn:#f59e0b; --bad:#ef4444; }}
@media (prefers-color-scheme: light) {{ :root {{ --paper:#f6f6f3; --ink:#0a0a0a; --ink-3:#5c5c5c; --rule:#dcdcd6; --accent:#0f766e; --warn:#b45309; --bad:#b91c1c; }} }}
body {{ margin:0; padding:24px 16px; background:var(--paper); color:var(--ink); font:14px/1.5 ui-sans-serif, system-ui, -apple-system, "Segoe UI", sans-serif; }}
main {{ max-width:1100px; margin:0 auto; }}
h1 {{ font-size:18px; margin:0 0 4px; }} h2 {{ font-size:15px; margin:28px 0 8px; }}
.meta {{ color:var(--ink-3); }} .meta a {{ color:var(--accent); }}
.wrap {{ overflow-x:auto; }}
table {{ border-collapse:collapse; width:100%; font:13px/1.4 ui-monospace, "Cascadia Mono", Menlo, Consolas, monospace; }}
th, td {{ text-align:left; padding:6px 10px; border-bottom:1px solid var(--rule); white-space:nowrap; }}
th {{ color:var(--ink-3); font-weight:500; }}
.up {{ color:var(--accent); }} .stale, .quarantined, .disabled {{ color:var(--warn); }} .down {{ color:var(--bad); }}
</style>
</head>
<body>
<main>
<h1>fleet-router {version}</h1>
<p class="meta">Up {uptime} &middot; {sessions} session(s) &middot; {up}/{total} upstream(s) up &middot; <a href="/ui/">Open the live dashboard</a></p>
"#,
        version = escape_html(status.version),
        uptime = format_uptime(status.uptime_secs),
        sessions = status.totals.sessions,
        up = status.totals.upstreams_up,
        total = status.totals.upstreams,
    );
    for listener in &status.listeners {
        let _ = write!(
            out,
            r#"<h2>{name} &middot; port {port} &middot; {ready}</h2>
<p class="meta">{sessions} / {max} session(s) &middot; {accepted} accepted &middot; {rejected} rejected</p>
<div class="wrap"><table>
<thead><tr><th>Status</th><th>Upstream</th><th>Endpoint</th><th>Priority</th><th>Weight</th><th>Sessions</th><th>Head</th><th>Trace range</th><th>Failovers</th><th>Block failures</th></tr></thead>
<tbody>
"#,
            name = escape_html(&listener.name),
            port = listener.port,
            ready = if listener.ready { "ready" } else { "not ready" },
            sessions = listener.sessions,
            max = listener.max_connections,
            accepted = listener.accepted_total,
            rejected = listener.rejected_total,
        );
        for upstream in &listener.upstreams {
            let _ = writeln!(
                out,
                r#"<tr><td class="{status}">&#9679; {status}</td><td>{name}</td><td>{endpoint}</td><td>{priority}</td><td>{weight}</td><td>{sessions}</td><td>{head}</td><td>{trace}</td><td>{failovers}</td><td>{failures}</td></tr>"#,
                status = upstream.status,
                name = escape_html(&upstream.name),
                endpoint = escape_html(&upstream.endpoint),
                priority = upstream.priority,
                weight = upstream.weight,
                sessions = upstream.sessions,
                head = upstream
                    .head_block
                    .map_or_else(|| "&ndash;".to_string(), |b| b.to_string()),
                trace = format_range(upstream.trace.as_ref()),
                failovers = upstream.failovers_total,
                failures = upstream.block_failures_total,
            );
        }
        out.push_str("</tbody></table></div>\n");
    }
    out.push_str("</main>\n</body>\n</html>\n");
    out
}

async fn status_page(State(app): State<SharedState>) -> Response {
    (
        [
            (header::CONTENT_SECURITY_POLICY, STATUS_CSP),
            (header::REFERRER_POLICY, "no-referrer"),
        ],
        Html(render_status_page(&api::status_view(&app).await)),
    )
        .into_response()
}

#[cfg(test)]
mod tests {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpStream;

    use super::*;
    use crate::api::{AppState, ListenerContext};
    use crate::health::ListenerHealthState;
    use crate::models::{build_config_db, build_state_db, Server};
    use crate::telemetry::ListenerStats;

    fn server(name: &str, endpoint: &str) -> Server {
        Server {
            name: name.to_string(),
            endpoint: endpoint.to_string(),
            enabled: true,
            priority: 0,
            weight: 1,
        }
    }

    async fn app(dashboard: bool, token: Option<&str>) -> SharedState {
        let servers = vec![server("eos-1", "127.0.0.1:18080")];
        let state_db = build_state_db(servers.clone());
        state_db
            .lock()
            .await
            .get_mut("127.0.0.1:18080")
            .unwrap()
            .online = true;
        let health = ListenerHealthState {
            name: Some("eos".to_string()),
            state_db,
        };
        Arc::new(AppState {
            listeners: vec![ListenerContext {
                health: health.clone(),
                config_db: build_config_db(servers),
                order: vec!["127.0.0.1:18080".to_string()],
                port: 17000,
                chain_id: None,
                max_connections: 100,
                stats: Arc::new(ListenerStats::default()),
            }],
            health: vec![health],
            dashboard,
            control_token: token.map(str::to_string),
            allowed_hosts: vec!["router.internal".to_string()],
            streams: Arc::new(Semaphore::new(api::MAX_STREAMS)),
        })
    }

    /// Send one raw HTTP/1.1 request and return the full response text.
    async fn request(app: SharedState, raw: &str) -> String {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let (shutdown_tx, shutdown_rx) = broadcast::channel(1);
        let server = tokio::spawn(serve(listener, app, shutdown_rx));
        let mut client = TcpStream::connect(addr).await.unwrap();
        client.write_all(raw.as_bytes()).await.unwrap();
        let mut resp = String::new();
        client.read_to_string(&mut resp).await.unwrap();
        let _ = shutdown_tx.send(());
        let _ = server.await;
        resp
    }

    fn get(path: &str) -> String {
        format!("GET {path} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
    }

    #[tokio::test]
    async fn ready_route_strips_query_string() {
        let resp = request(app(false, None).await, &get("/ready/eos?probe=k8s&v=1")).await;
        assert!(resp.starts_with("HTTP/1.1 200 OK\r\n"), "resp: {resp}");
        assert!(resp.contains("ready (eos): 1 upstream(s) online\n"));
    }

    #[tokio::test]
    async fn dashboard_routes_are_absent_unless_enabled() {
        for path in ["/ui/", "/status", "/api/v1/status", "/"] {
            let resp = request(app(false, None).await, &get(path)).await;
            assert!(resp.starts_with("HTTP/1.1 404"), "{path}: {resp}");
        }
        let resp = request(app(false, None).await, &get("/metrics")).await;
        assert!(resp.starts_with("HTTP/1.1 200 OK"), "resp: {resp}");
    }

    #[tokio::test]
    async fn dashboard_serves_status_json_and_page_when_enabled() {
        let resp = request(app(true, None).await, &get("/api/v1/status")).await;
        assert!(resp.starts_with("HTTP/1.1 200 OK"), "resp: {resp}");
        assert!(resp.contains(r#""name":"eos""#), "resp: {resp}");
        assert!(
            resp.contains(r#""control":{"enabled":false}"#),
            "resp: {resp}"
        );

        let resp = request(app(true, None).await, &get("/status")).await;
        assert!(resp.contains("eos-1"), "resp: {resp}");

        let resp = request(app(true, None).await, &get("/")).await;
        assert!(resp.starts_with("HTTP/1.1 307"), "resp: {resp}");
        assert!(resp.contains("location: /ui/"), "resp: {resp}");
    }

    #[tokio::test]
    async fn control_requires_a_configured_and_matching_token() {
        let disable =
            "POST /api/v1/listeners/eos/upstreams/127.0.0.1%3A18080/disable HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\nContent-Length: 0\r\n";

        let resp = request(app(true, None).await, &format!("{disable}\r\n")).await;
        assert!(resp.starts_with("HTTP/1.1 403"), "resp: {resp}");

        let token = "0123456789abcdef";
        let resp = request(app(true, Some(token)).await, &format!("{disable}\r\n")).await;
        assert!(resp.starts_with("HTTP/1.1 401"), "resp: {resp}");

        let app = app(true, Some(token)).await;
        let resp = request(
            app.clone(),
            &format!("{disable}Authorization: Bearer {token}\r\n\r\n"),
        )
        .await;
        assert!(resp.starts_with("HTTP/1.1 200 OK"), "resp: {resp}");
        assert!(resp.contains(r#""status":"disabled""#), "resp: {resp}");
        let db = app.listeners[0].health.state_db.lock().await;
        assert!(db["127.0.0.1:18080"].operator_disabled);
    }

    #[tokio::test]
    async fn patch_validates_weight() {
        let token = "0123456789abcdef";
        let body = r#"{"weight":0}"#;
        let raw = format!(
            "PATCH /api/v1/listeners/eos/upstreams/127.0.0.1%3A18080 HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\nAuthorization: Bearer {token}\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{body}",
            body.len()
        );
        let resp = request(app(true, Some(token)).await, &raw).await;
        assert!(resp.starts_with("HTTP/1.1 400"), "resp: {resp}");

        let body = r#"{"priority":7,"weight":3}"#;
        let raw = format!(
            "PATCH /api/v1/listeners/eos/upstreams/127.0.0.1%3A18080 HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\nAuthorization: Bearer {token}\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{body}",
            body.len()
        );
        let resp = request(app(true, Some(token)).await, &raw).await;
        assert!(resp.starts_with("HTTP/1.1 200 OK"), "resp: {resp}");
        assert!(
            resp.contains(r#""priority":7,"weight":3,"config":{"priority":0,"weight":1}"#),
            "resp: {resp}"
        );
    }

    #[test]
    fn status_page_escapes_names() {
        let status = StatusView {
            version: "1.0.0",
            now_ms: 0,
            started_ms: 0,
            uptime_secs: 3_700,
            control: api::ControlView { enabled: false },
            totals: api::TotalsView {
                sessions: 0,
                upstreams: 0,
                upstreams_up: 0,
            },
            listeners: vec![api::ListenerView {
                name: "<eos>".to_string(),
                port: 1,
                chain_id: None,
                ready: false,
                max_connections: 1,
                sessions: 0,
                accepted_total: 0,
                rejected_total: 0,
                upstreams: vec![],
            }],
        };
        let html = render_status_page(&status);
        assert!(html.contains("&lt;eos&gt;"));
        assert!(!html.contains("<eos>"));
        assert!(html.contains("Up 1h 1m"));
    }

    #[test]
    fn host_check_accepts_ip_literals_localhost_and_listed_names_only() {
        let allowed = vec!["router.internal".to_string()];
        for host in [
            "127.0.0.1:9090",
            "10.0.0.5",
            "[::1]:9090",
            "LOCALHOST:9090",
            "router.internal:9090",
        ] {
            assert!(host_allowed(host, &allowed), "{host} should pass");
        }
        for host in [
            "evil.example",
            "evil.example:9090",
            "router.internal.evil.example",
        ] {
            assert!(!host_allowed(host, &allowed), "{host} should be rejected");
        }
    }

    #[tokio::test]
    async fn dashboard_rejects_a_foreign_host_but_metrics_does_not() {
        let raw = |path: &str| {
            format!("GET {path} HTTP/1.1\r\nHost: evil.example:9090\r\nConnection: close\r\n\r\n")
        };
        let resp = request(app(true, None).await, &raw("/api/v1/sessions")).await;
        assert!(resp.starts_with("HTTP/1.1 403"), "resp: {resp}");
        let resp = request(app(true, None).await, &raw("/metrics")).await;
        assert!(resp.starts_with("HTTP/1.1 200 OK"), "resp: {resp}");
    }

    #[test]
    fn asset_table_is_sorted_for_binary_search() {
        assert!(UI_ASSETS.windows(2).all(|pair| pair[0].0 < pair[1].0));
    }
}
