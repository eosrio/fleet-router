//! Minimal, dependency-free HTTP endpoint exposing liveness, readiness, and
//! Prometheus metrics. Opt-in via the `metrics_port` config field.
//!
//! Routes (GET only):
//!   - `/health`, `/healthz`           -> 200 while the process is running
//!   - `/ready`,  `/readyz`            -> 200 if every configured listener has ≥1 upstream online, else 503
//!   - `/ready/<name>`, `/readyz/<name>` -> 200 if named listener has ≥1 upstream online, 503 if 0, 404 if unknown
//!   - `/metrics`                      -> Prometheus text exposition of upstream state

use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::broadcast;
use tokio::time::timeout;

use crate::models::ServerStateDb;

/// Health and metrics state for a single ingress listener.
#[derive(Clone)]
pub struct ListenerHealthState {
    /// `None` in legacy single-port mode, `Some(name)` in multi-listener mode.
    pub name: Option<String>,
    pub state_db: ServerStateDb,
}

/// Run the health/metrics HTTP server until a shutdown signal is received.
pub async fn run_metrics_server(
    addr: String,
    listeners: Vec<ListenerHealthState>,
    mut shutdown: broadcast::Receiver<()>,
) {
    let listener = match TcpListener::bind(&addr).await {
        Ok(l) => l,
        Err(e) => {
            tracing::error!(%addr, error = %e, "failed to bind health/metrics endpoint");
            return;
        }
    };
    tracing::info!(%addr, "health/metrics endpoint listening");

    let listeners = Arc::new(listeners);

    loop {
        tokio::select! {
            accepted = listener.accept() => {
                match accepted {
                    Ok((stream, _peer)) => {
                        let pools = listeners.clone();
                        tokio::spawn(async move {
                            if let Err(e) = handle_request(stream, &pools).await {
                                tracing::debug!(error = %e, "health/metrics request error");
                            }
                        });
                    }
                    Err(e) => tracing::warn!(error = %e, "health/metrics accept error"),
                }
            }
            _ = shutdown.recv() => {
                tracing::info!("health/metrics endpoint shutting down");
                break;
            }
        }
    }
}

async fn handle_request(
    mut stream: TcpStream,
    listeners: &[ListenerHealthState],
) -> std::io::Result<()> {
    // Read just enough to parse the request line, bounded by a single overall
    // timeout (so a slow client can't hold the connection open by dripping
    // bytes) and a fixed buffer size.
    let mut buf = [0u8; 1024];
    let len = match timeout(Duration::from_secs(5), async {
        let mut len = 0;
        while len < buf.len() {
            match stream.read(&mut buf[len..]).await {
                Ok(0) => break, // EOF
                Ok(n) => {
                    len += n;
                    if buf[..len].windows(4).any(|w| w == b"\r\n\r\n")
                        || buf[..len].contains(&b'\n')
                    {
                        break; // we have the request line (and possibly headers)
                    }
                }
                Err(e) => return Err(e),
            }
        }
        Ok(len)
    })
    .await
    {
        Ok(Ok(len)) => len,
        Ok(Err(e)) => return Err(e),
        Err(_) => return Ok(()), // overall read timeout: drop the connection
    };

    let head = String::from_utf8_lossy(&buf[..len]);
    let request_line = head.lines().next().unwrap_or("");
    let mut parts = request_line.split_whitespace();
    let method = parts.next().unwrap_or("");
    let raw_path = parts.next().unwrap_or("");
    let path = raw_path.split('?').next().unwrap_or("");

    let (status, content_type, body) = if method != "GET" {
        (
            "405 Method Not Allowed",
            "text/plain",
            "method not allowed\n".to_string(),
        )
    } else {
        match path {
            "/health" | "/healthz" => ("200 OK", "text/plain", "ok\n".to_string()),
            "/ready" | "/readyz" => evaluate_overall_readiness(listeners).await,
            "/metrics" => (
                "200 OK",
                "text/plain; version=0.0.4",
                render_metrics(listeners).await,
            ),
            _ => {
                if let Some(target) = path
                    .strip_prefix("/ready/")
                    .or_else(|| path.strip_prefix("/readyz/"))
                {
                    evaluate_listener_readiness(listeners, target).await
                } else {
                    ("404 Not Found", "text/plain", "not found\n".to_string())
                }
            }
        }
    };

    let response = format!(
        "HTTP/1.1 {status}\r\nContent-Type: {content_type}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len()
    );
    stream.write_all(response.as_bytes()).await?;
    stream.flush().await?;
    Ok(())
}

async fn evaluate_overall_readiness(
    listeners: &[ListenerHealthState],
) -> (&'static str, &'static str, String) {
    if listeners.len() == 1 && listeners[0].name.is_none() {
        let online = count_online(&listeners[0].state_db).await;
        return if online > 0 {
            (
                "200 OK",
                "text/plain",
                format!("ready: {} upstream(s) online\n", online),
            )
        } else {
            (
                "503 Service Unavailable",
                "text/plain",
                "not ready: no upstreams online\n".to_string(),
            )
        };
    }

    let mut total_online = 0usize;
    let mut unready = Vec::new();
    for l in listeners {
        let online = count_online(&l.state_db).await;
        total_online += online;
        if online == 0 {
            unready.push(l.name.as_deref().unwrap_or("default").to_string());
        }
    }

    if unready.is_empty() && !listeners.is_empty() {
        (
            "200 OK",
            "text/plain",
            format!(
                "ready: {} listener(s) ready, {} upstream(s) online\n",
                listeners.len(),
                total_online
            ),
        )
    } else {
        (
            "503 Service Unavailable",
            "text/plain",
            format!(
                "not ready: listener(s) with no online upstreams: {}\n",
                unready.join(", ")
            ),
        )
    }
}

async fn evaluate_listener_readiness(
    listeners: &[ListenerHealthState],
    target: &str,
) -> (&'static str, &'static str, String) {
    if target.is_empty() {
        return ("404 Not Found", "text/plain", "not found\n".to_string());
    }
    let Some(listener) = listeners.iter().find(|l| l.name.as_deref() == Some(target)) else {
        return ("404 Not Found", "text/plain", "not found\n".to_string());
    };
    let online = count_online(&listener.state_db).await;
    if online > 0 {
        (
            "200 OK",
            "text/plain",
            format!("ready ({target}): {online} upstream(s) online\n"),
        )
    } else {
        (
            "503 Service Unavailable",
            "text/plain",
            format!("not ready ({target}): no upstreams online\n"),
        )
    }
}

async fn count_online(state_db: &ServerStateDb) -> usize {
    state_db.lock().await.values().filter(|s| s.online).count()
}

fn format_labels(listener_name: Option<&str>, endpoint: &str) -> String {
    match listener_name {
        Some(name) => format!(
            "listener=\"{}\",endpoint=\"{}\"",
            escape(name),
            escape(endpoint)
        ),
        None => format!("endpoint=\"{}\"", escape(endpoint)),
    }
}

#[derive(Clone)]
struct MetricSnapshot {
    labels: String,
    online: bool,
    stale: bool,
    connections: usize,
    chain_state_end_block: u32,
    finality_data_begin_block: u32,
    finality_data_end_block: u32,
}

async fn render_metrics(listeners: &[ListenerHealthState]) -> String {
    let mut snapshots = Vec::new();
    for l in listeners {
        let db = l.state_db.lock().await;
        for (endpoint, state) in db.iter() {
            snapshots.push(MetricSnapshot {
                labels: format_labels(l.name.as_deref(), endpoint),
                online: state.online,
                stale: state.stale,
                connections: state.connections.load(Ordering::Relaxed),
                chain_state_end_block: state.chain_state_end_block,
                finality_data_begin_block: state.finality_data_begin_block,
                finality_data_end_block: state.finality_data_end_block,
            });
        }
    }

    let mut out = String::new();

    out.push_str("# HELP fleet_router_up Whether the router process is up.\n");
    out.push_str("# TYPE fleet_router_up gauge\n");
    out.push_str("fleet_router_up 1\n");

    out.push_str(
        "# HELP fleet_router_upstream_up Whether an upstream is online (1) or offline (0).\n",
    );
    out.push_str("# TYPE fleet_router_upstream_up gauge\n");
    for s in &snapshots {
        out.push_str(&format!(
            "fleet_router_upstream_up{{{}}} {}\n",
            s.labels,
            u8::from(s.online)
        ));
    }

    out.push_str("# HELP fleet_router_upstream_stale Whether an upstream has stopped advancing (1) or not (0).\n");
    out.push_str("# TYPE fleet_router_upstream_stale gauge\n");
    for s in &snapshots {
        out.push_str(&format!(
            "fleet_router_upstream_stale{{{}}} {}\n",
            s.labels,
            u8::from(s.stale)
        ));
    }

    out.push_str(
        "# HELP fleet_router_active_connections Active client connections routed to an upstream.\n",
    );
    out.push_str("# TYPE fleet_router_active_connections gauge\n");
    for s in &snapshots {
        out.push_str(&format!(
            "fleet_router_active_connections{{{}}} {}\n",
            s.labels, s.connections
        ));
    }

    out.push_str("# HELP fleet_router_upstream_chain_state_end_block Last chain-state block reported by an upstream.\n");
    out.push_str("# TYPE fleet_router_upstream_chain_state_end_block gauge\n");
    for s in &snapshots {
        out.push_str(&format!(
            "fleet_router_upstream_chain_state_end_block{{{}}} {}\n",
            s.labels, s.chain_state_end_block
        ));
    }

    out.push_str("# HELP fleet_router_upstream_finality_data_begin_block First block with finality data reported by an upstream (0 if unknown).\n");
    out.push_str("# TYPE fleet_router_upstream_finality_data_begin_block gauge\n");
    for s in &snapshots {
        out.push_str(&format!(
            "fleet_router_upstream_finality_data_begin_block{{{}}} {}\n",
            s.labels, s.finality_data_begin_block
        ));
    }

    out.push_str("# HELP fleet_router_upstream_finality_data_end_block One past the last block with finality data (0 if unknown).\n");
    out.push_str("# TYPE fleet_router_upstream_finality_data_end_block gauge\n");
    for s in &snapshots {
        out.push_str(&format!(
            "fleet_router_upstream_finality_data_end_block{{{}}} {}\n",
            s.labels, s.finality_data_end_block
        ));
    }

    out
}

fn escape(s: &str) -> String {
    s.replace('\\', "\\\\").replace('"', "\\\"")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::{build_state_db, Server};

    #[tokio::test]
    async fn single_listener_metrics_and_readiness_keep_legacy_format() {
        let db = build_state_db(vec![Server {
            name: "s1".to_string(),
            endpoint: "127.0.0.1:18080".to_string(),
            enabled: true,
        }]);
        let listeners = vec![ListenerHealthState {
            name: None,
            state_db: db.clone(),
        }];

        let (status, _, body) = evaluate_overall_readiness(&listeners).await;
        assert_eq!(status, "503 Service Unavailable");
        assert_eq!(body, "not ready: no upstreams online\n");

        db.lock().await.get_mut("127.0.0.1:18080").unwrap().online = true;

        let (status, _, body) = evaluate_overall_readiness(&listeners).await;
        assert_eq!(status, "200 OK");
        assert_eq!(body, "ready: 1 upstream(s) online\n");

        let metrics = render_metrics(&listeners).await;
        assert!(metrics.contains("fleet_router_upstream_up{endpoint=\"127.0.0.1:18080\"} 1\n"));
    }

    #[tokio::test]
    async fn multi_listener_metrics_and_readiness_include_listener_names() {
        let db_eos = build_state_db(vec![Server {
            name: "eos-1".to_string(),
            endpoint: "127.0.0.1:18080".to_string(),
            enabled: true,
        }]);
        let db_wax = build_state_db(vec![Server {
            name: "wax-1".to_string(),
            endpoint: "127.0.0.1:28080".to_string(),
            enabled: true,
        }]);

        db_eos
            .lock()
            .await
            .get_mut("127.0.0.1:18080")
            .unwrap()
            .online = true;

        let listeners = vec![
            ListenerHealthState {
                name: Some("eos".to_string()),
                state_db: db_eos.clone(),
            },
            ListenerHealthState {
                name: Some("wax".to_string()),
                state_db: db_wax.clone(),
            },
        ];

        // Overall readiness is 503 while 'wax' has 0 online upstreams.
        let (status, _, body) = evaluate_overall_readiness(&listeners).await;
        assert_eq!(status, "503 Service Unavailable");
        assert!(body.contains("wax"), "unexpected body: {body}");

        // Per-listener readiness reflects each pool independently.
        let (eos_status, _, _) = evaluate_listener_readiness(&listeners, "eos").await;
        assert_eq!(eos_status, "200 OK");
        let (wax_status, _, _) = evaluate_listener_readiness(&listeners, "wax").await;
        assert_eq!(wax_status, "503 Service Unavailable");
        let (missing_status, _, _) = evaluate_listener_readiness(&listeners, "telos").await;
        assert_eq!(missing_status, "404 Not Found");

        // Once 'wax' comes online, overall readiness turns 200 OK.
        db_wax
            .lock()
            .await
            .get_mut("127.0.0.1:28080")
            .unwrap()
            .online = true;
        let (status, _, body) = evaluate_overall_readiness(&listeners).await;
        assert_eq!(status, "200 OK");
        assert_eq!(body, "ready: 2 listener(s) ready, 2 upstream(s) online\n");

        let metrics = render_metrics(&listeners).await;
        assert!(metrics.contains(
            "fleet_router_upstream_up{listener=\"eos\",endpoint=\"127.0.0.1:18080\"} 1\n"
        ));
        assert!(metrics.contains(
            "fleet_router_upstream_up{listener=\"wax\",endpoint=\"127.0.0.1:28080\"} 1\n"
        ));
    }

    #[tokio::test]
    async fn handle_request_strips_query_string_on_routes() {
        let db_eos = build_state_db(vec![Server {
            name: "eos-1".to_string(),
            endpoint: "127.0.0.1:18080".to_string(),
            enabled: true,
        }]);
        db_eos
            .lock()
            .await
            .get_mut("127.0.0.1:18080")
            .unwrap()
            .online = true;
        let listeners = vec![ListenerHealthState {
            name: Some("eos".to_string()),
            state_db: db_eos,
        }];

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let server_listeners = listeners.clone();
        let server_task = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            handle_request(stream, &server_listeners).await.unwrap();
        });

        let mut client = TcpStream::connect(addr).await.unwrap();
        client
            .write_all(b"GET /ready/eos?probe=k8s&v=1 HTTP/1.1\r\nHost: localhost\r\n\r\n")
            .await
            .unwrap();
        let mut resp = String::new();
        client.read_to_string(&mut resp).await.unwrap();
        server_task.await.unwrap();

        assert!(resp.starts_with("HTTP/1.1 200 OK\r\n"), "resp: {resp}");
        assert!(resp.contains("ready (eos): 1 upstream(s) online\n"));
    }
}
