//! Minimal, dependency-free HTTP endpoint exposing liveness, readiness, and
//! Prometheus metrics. Opt-in via the `metrics_port` config field.
//!
//! Routes (GET only):
//!   - `/health`, `/healthz` -> 200 while the process is running
//!   - `/ready`,  `/readyz`  -> 200 if at least one upstream is online, else 503
//!   - `/metrics`            -> Prometheus text exposition of upstream state

use std::sync::atomic::Ordering;
use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::broadcast;
use tokio::time::timeout;

use crate::models::ServerStateDb;

/// Run the health/metrics HTTP server until a shutdown signal is received.
pub async fn run_metrics_server(
    addr: String,
    state_db: ServerStateDb,
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

    loop {
        tokio::select! {
            accepted = listener.accept() => {
                match accepted {
                    Ok((stream, _peer)) => {
                        let db = state_db.clone();
                        tokio::spawn(async move {
                            if let Err(e) = handle_request(stream, db).await {
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

async fn handle_request(mut stream: TcpStream, state_db: ServerStateDb) -> std::io::Result<()> {
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
    let path = parts.next().unwrap_or("");

    let (status, content_type, body) = if method != "GET" {
        (
            "405 Method Not Allowed",
            "text/plain",
            "method not allowed\n".to_string(),
        )
    } else {
        match path {
            "/health" | "/healthz" => ("200 OK", "text/plain", "ok\n".to_string()),
            "/ready" | "/readyz" => {
                let online = count_online(&state_db).await;
                if online > 0 {
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
                }
            }
            "/metrics" => (
                "200 OK",
                "text/plain; version=0.0.4",
                render_metrics(&state_db).await,
            ),
            _ => ("404 Not Found", "text/plain", "not found\n".to_string()),
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

async fn count_online(state_db: &ServerStateDb) -> usize {
    state_db.lock().await.values().filter(|s| s.online).count()
}

async fn render_metrics(state_db: &ServerStateDb) -> String {
    let db = state_db.lock().await;
    let mut out = String::new();

    out.push_str("# HELP fleet_router_up Whether the router process is up.\n");
    out.push_str("# TYPE fleet_router_up gauge\n");
    out.push_str("fleet_router_up 1\n");

    out.push_str(
        "# HELP fleet_router_upstream_up Whether an upstream is online (1) or offline (0).\n",
    );
    out.push_str("# TYPE fleet_router_upstream_up gauge\n");
    for (endpoint, state) in db.iter() {
        out.push_str(&format!(
            "fleet_router_upstream_up{{endpoint=\"{}\"}} {}\n",
            escape(endpoint),
            u8::from(state.online)
        ));
    }

    out.push_str("# HELP fleet_router_upstream_stale Whether an upstream has stopped advancing (1) or not (0).\n");
    out.push_str("# TYPE fleet_router_upstream_stale gauge\n");
    for (endpoint, state) in db.iter() {
        out.push_str(&format!(
            "fleet_router_upstream_stale{{endpoint=\"{}\"}} {}\n",
            escape(endpoint),
            u8::from(state.stale)
        ));
    }

    out.push_str(
        "# HELP fleet_router_active_connections Active client connections routed to an upstream.\n",
    );
    out.push_str("# TYPE fleet_router_active_connections gauge\n");
    for (endpoint, state) in db.iter() {
        out.push_str(&format!(
            "fleet_router_active_connections{{endpoint=\"{}\"}} {}\n",
            escape(endpoint),
            state.connections.load(Ordering::Relaxed)
        ));
    }

    out.push_str("# HELP fleet_router_upstream_chain_state_end_block Last chain-state block reported by an upstream.\n");
    out.push_str("# TYPE fleet_router_upstream_chain_state_end_block gauge\n");
    for (endpoint, state) in db.iter() {
        out.push_str(&format!(
            "fleet_router_upstream_chain_state_end_block{{endpoint=\"{}\"}} {}\n",
            escape(endpoint),
            state.chain_state_end_block
        ));
    }

    out
}

fn escape(s: &str) -> String {
    s.replace('\\', "\\\\").replace('"', "\\\"")
}
