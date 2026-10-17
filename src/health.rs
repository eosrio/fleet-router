//! Readiness and Prometheus metrics for the HTTP endpoint in [`crate::http`].
//! Opt-in via the `metrics_port` config field.

use std::sync::atomic::Ordering;
use std::time::Instant;

use crate::models::ServerStateDb;

/// Health and metrics state for a single ingress listener.
#[derive(Clone)]
pub struct ListenerHealthState {
    /// `None` in legacy single-port mode, `Some(name)` in multi-listener mode.
    pub name: Option<String>,
    pub state_db: ServerStateDb,
}

pub(crate) async fn evaluate_overall_readiness(
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

pub(crate) async fn evaluate_listener_readiness(
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
    state_db
        .lock()
        .await
        .values()
        .filter(|s| s.online && s.routable())
        .count()
}

fn format_labels(listener_name: Option<&str>, endpoint: &str, server_name: &str) -> String {
    let endpoint_label = format!("endpoint=\"{}\"", escape(endpoint));
    let server_label = format!("server=\"{}\"", escape(server_name));
    match listener_name {
        Some(name) => format!(
            "listener=\"{}\",{endpoint_label},{server_label}",
            escape(name)
        ),
        None => format!("{endpoint_label},{server_label}"),
    }
}

#[derive(Clone)]
struct MetricSnapshot {
    labels: String,
    online: bool,
    stale: bool,
    operator_disabled: bool,
    priority: u32,
    weight: u32,
    connections: usize,
    trace_begin_block: u32,
    chain_state_begin_block: u32,
    chain_state_end_block: u32,
    finality_data_begin_block: u32,
    finality_data_end_block: u32,
    block_failures: u64,
    quarantined: bool,
    quarantine_block: u32,
}

pub(crate) async fn render_metrics(listeners: &[ListenerHealthState]) -> String {
    let now = Instant::now();
    let mut snapshots = Vec::new();
    for l in listeners {
        let mut db = l.state_db.lock().await;
        for (endpoint, state) in db.iter_mut() {
            snapshots.push(MetricSnapshot {
                labels: format_labels(l.name.as_deref(), endpoint, &state.name),
                online: state.online,
                stale: state.stale,
                operator_disabled: state.operator_disabled,
                priority: state.priority,
                weight: state.weight,
                connections: state.connections.load(Ordering::Relaxed),
                trace_begin_block: state.trace_begin_block,
                chain_state_begin_block: state.chain_state_begin_block,
                chain_state_end_block: state.chain_state_end_block,
                finality_data_begin_block: state.finality_data_begin_block,
                finality_data_end_block: state.finality_data_end_block,
                block_failures: state.block_failures,
                quarantined: state.any_quarantine_active(now),
                quarantine_block: state.active_quarantine_block(now).unwrap_or(0),
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

    out.push_str("# HELP fleet_router_upstream_operator_disabled Whether an operator disabled or drained an upstream through the control API (1) or not (0).\n");
    out.push_str("# TYPE fleet_router_upstream_operator_disabled gauge\n");
    for s in &snapshots {
        out.push_str(&format!(
            "fleet_router_upstream_operator_disabled{{{}}} {}\n",
            s.labels,
            u8::from(s.operator_disabled)
        ));
    }

    out.push_str("# HELP fleet_router_upstream_priority Priority tier in effect for an upstream (lower is preferred).\n");
    out.push_str("# TYPE fleet_router_upstream_priority gauge\n");
    for s in &snapshots {
        out.push_str(&format!(
            "fleet_router_upstream_priority{{{}}} {}\n",
            s.labels, s.priority
        ));
    }

    out.push_str("# HELP fleet_router_upstream_weight Weight in effect for an upstream within its priority tier.\n");
    out.push_str("# TYPE fleet_router_upstream_weight gauge\n");
    for s in &snapshots {
        out.push_str(&format!(
            "fleet_router_upstream_weight{{{}}} {}\n",
            s.labels, s.weight
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

    out.push_str("# HELP fleet_router_upstream_trace_begin_block First block with trace data reported by an upstream (0 until a status result arrives).\n");
    out.push_str("# TYPE fleet_router_upstream_trace_begin_block gauge\n");
    for s in &snapshots {
        out.push_str(&format!(
            "fleet_router_upstream_trace_begin_block{{{}}} {}\n",
            s.labels, s.trace_begin_block
        ));
    }

    out.push_str("# HELP fleet_router_upstream_chain_state_begin_block First block with chain-state data reported by an upstream (0 until a status result arrives).\n");
    out.push_str("# TYPE fleet_router_upstream_chain_state_begin_block gauge\n");
    for s in &snapshots {
        out.push_str(&format!(
            "fleet_router_upstream_chain_state_begin_block{{{}}} {}\n",
            s.labels, s.chain_state_begin_block
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

    out.push_str("# HELP fleet_router_upstream_finality_data_begin_block First block with finality data reported by an upstream (0 if unknown, including pre-Savanna nodes that only report get_status_result_v0).\n");
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

    out.push_str("# HELP fleet_router_upstream_block_failures_total Unclean mid-get_blocks drops observed for an upstream.\n");
    out.push_str("# TYPE fleet_router_upstream_block_failures_total counter\n");
    for s in &snapshots {
        out.push_str(&format!(
            "fleet_router_upstream_block_failures_total{{{}}} {}\n",
            s.labels, s.block_failures
        ));
    }

    out.push_str("# HELP fleet_router_upstream_quarantined Whether an upstream is excluded for a block range (1) or not (0). The upstream stays online.\n");
    out.push_str("# TYPE fleet_router_upstream_quarantined gauge\n");
    for s in &snapshots {
        out.push_str(&format!(
            "fleet_router_upstream_quarantined{{{}}} {}\n",
            s.labels,
            u8::from(s.quarantined)
        ));
    }

    out.push_str("# HELP fleet_router_upstream_quarantine_block Start block of an upstream's active quarantine (0 when none). When several ranges are active, this is the lowest start block.\n");
    out.push_str("# TYPE fleet_router_upstream_quarantine_block gauge\n");
    for s in &snapshots {
        out.push_str(&format!(
            "fleet_router_upstream_quarantine_block{{{}}} {}\n",
            s.labels, s.quarantine_block
        ));
    }

    out
}

/// Prometheus text-format label value. Backslash is replaced first so the
/// escapes inserted for `"` and newline are not doubled.
fn escape(s: &str) -> String {
    s.replace('\\', "\\\\")
        .replace('"', "\\\"")
        .replace('\n', "\\n")
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::Ordering;

    use super::*;
    use crate::models::{build_state_db, Server};

    #[tokio::test]
    async fn single_listener_metrics_and_readiness_keep_legacy_format() {
        let db = build_state_db(vec![Server {
            name: "s1".to_string(),
            endpoint: "127.0.0.1:18080".to_string(),
            enabled: true,
            priority: 0,
            weight: 1,
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
        assert!(metrics
            .contains("fleet_router_upstream_up{endpoint=\"127.0.0.1:18080\",server=\"s1\"} 1\n"));
        assert!(
            metrics.contains("fleet_router_up 1\n"),
            "process gauge must stay unlabeled"
        );
        assert!(
            !metrics.contains("fleet_router_up{"),
            "process gauge must not gain a server label"
        );
    }

    #[tokio::test]
    async fn multi_listener_metrics_and_readiness_include_listener_names() {
        let db_eos = build_state_db(vec![Server {
            name: "eos-1".to_string(),
            endpoint: "127.0.0.1:18080".to_string(),
            enabled: true,
            priority: 0,
            weight: 1,
        }]);
        let db_wax = build_state_db(vec![Server {
            name: "wax-1".to_string(),
            endpoint: "127.0.0.1:28080".to_string(),
            enabled: true,
            priority: 0,
            weight: 1,
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
            "fleet_router_upstream_up{listener=\"eos\",endpoint=\"127.0.0.1:18080\",server=\"eos-1\"} 1\n"
        ));
        assert!(metrics.contains(
            "fleet_router_upstream_up{listener=\"wax\",endpoint=\"127.0.0.1:28080\",server=\"wax-1\"} 1\n"
        ));
    }

    #[test]
    fn prometheus_escape_covers_backslash_quote_and_newline() {
        assert_eq!(escape("plain"), "plain");
        assert_eq!(escape(r#"a\b"c"#), r#"a\\b\"c"#);
        assert_eq!(escape("line1\nline2"), r"line1\nline2");
        assert_eq!(escape("EOS \"SHiP\"\n1\\west"), r#"EOS \"SHiP\"\n1\\west"#);
    }

    #[tokio::test]
    async fn server_label_is_on_every_upstream_series_and_escaped() {
        let nasty = "EOS \"SHiP\"\n1\\west";
        let legacy = build_state_db(vec![Server {
            name: nasty.to_string(),
            endpoint: "127.0.0.1:18080".to_string(),
            enabled: true,
            priority: 2,
            weight: 3,
        }]);
        let named = build_state_db(vec![Server {
            name: "wax node".to_string(),
            endpoint: "10.0.0.2:8080".to_string(),
            enabled: true,
            priority: 0,
            weight: 1,
        }]);
        {
            let mut db = legacy.lock().await;
            let state = db.get_mut("127.0.0.1:18080").unwrap();
            state.online = true;
            state.stale = true;
            state.connections.store(3, Ordering::Relaxed);
            state.chain_state_end_block = 42;
            state.finality_data_begin_block = 7;
            state.finality_data_end_block = 99;
        }
        {
            let mut db = named.lock().await;
            let state = db.get_mut("10.0.0.2:8080").unwrap();
            state.online = false;
            state.chain_state_end_block = 5;
        }

        let listeners = vec![
            ListenerHealthState {
                name: None,
                state_db: legacy,
            },
            ListenerHealthState {
                name: Some("wax\"main".to_string()),
                state_db: named,
            },
        ];
        let metrics = render_metrics(&listeners).await;

        let legacy_labels = r#"endpoint="127.0.0.1:18080",server="EOS \"SHiP\"\n1\\west""#;
        let named_labels = r#"listener="wax\"main",endpoint="10.0.0.2:8080",server="wax node""#;
        let series = [
            ("fleet_router_upstream_up", "1", "0"),
            ("fleet_router_upstream_stale", "1", "0"),
            ("fleet_router_upstream_operator_disabled", "0", "0"),
            ("fleet_router_upstream_priority", "2", "0"),
            ("fleet_router_upstream_weight", "3", "1"),
            ("fleet_router_active_connections", "3", "0"),
            ("fleet_router_upstream_chain_state_end_block", "42", "5"),
            ("fleet_router_upstream_finality_data_begin_block", "7", "0"),
            ("fleet_router_upstream_finality_data_end_block", "99", "0"),
        ];
        for (name, legacy_value, named_value) in series {
            let legacy_line = format!("{name}{{{legacy_labels}}} {legacy_value}\n");
            let named_line = format!("{name}{{{named_labels}}} {named_value}\n");
            assert!(
                metrics.contains(&legacy_line),
                "missing legacy series {legacy_line} in:\n{metrics}"
            );
            assert!(
                metrics.contains(&named_line),
                "missing listener series {named_line} in:\n{metrics}"
            );
        }
    }

    fn assert_gauge(metrics: &str, name: &str, endpoint: &str, value: u32, listener: Option<&str>) {
        let line = metrics
            .lines()
            .find(|line| {
                line.starts_with(&format!("{name}{{"))
                    && line.contains(&format!("endpoint=\"{endpoint}\""))
            })
            .unwrap_or_else(|| panic!("missing {name} for endpoint {endpoint} in:\n{metrics}"));
        assert!(
            line.ends_with(&format!(" {value}")),
            "{name} for {endpoint}: expected {value}, got {line}"
        );
        match listener {
            Some(listener_name) => assert!(
                line.contains(&format!("listener=\"{listener_name}\"")),
                "missing listener label on {line}"
            ),
            None => assert!(
                !line.contains("listener="),
                "single-port series must not carry a listener label: {line}"
            ),
        }
    }

    #[tokio::test]
    async fn metrics_expose_ship_begin_blocks_including_zero() {
        let legacy = build_state_db(vec![Server {
            name: "s1".to_string(),
            endpoint: "127.0.0.1:18080".to_string(),
            enabled: true,
            priority: 0,
            weight: 1,
        }]);
        let named = build_state_db(vec![Server {
            name: "wax-1".to_string(),
            endpoint: "127.0.0.1:28080".to_string(),
            enabled: true,
            priority: 0,
            weight: 1,
        }]);
        {
            let mut db = legacy.lock().await;
            let state = db.get_mut("127.0.0.1:18080").unwrap();
            state.trace_begin_block = 100;
            state.chain_state_begin_block = 50;
            state.chain_state_end_block = 900;
            state.finality_data_begin_block = 400;
            state.finality_data_end_block = 900;
        }

        let listeners = vec![
            ListenerHealthState {
                name: None,
                state_db: legacy,
            },
            ListenerHealthState {
                name: Some("wax".to_string()),
                state_db: named,
            },
        ];
        let metrics = render_metrics(&listeners).await;

        assert!(metrics.contains(
            "# HELP fleet_router_upstream_trace_begin_block First block with trace data reported by an upstream (0 until a status result arrives).\n"
        ));
        assert!(metrics.contains("# TYPE fleet_router_upstream_trace_begin_block gauge\n"));
        assert!(metrics.contains(
            "# HELP fleet_router_upstream_chain_state_begin_block First block with chain-state data reported by an upstream (0 until a status result arrives).\n"
        ));
        assert!(metrics.contains("# TYPE fleet_router_upstream_chain_state_begin_block gauge\n"));
        assert!(metrics.contains(
            "# HELP fleet_router_upstream_finality_data_begin_block First block with finality data reported by an upstream (0 if unknown, including pre-Savanna nodes that only report get_status_result_v0).\n"
        ));
        assert!(metrics.contains("# TYPE fleet_router_upstream_finality_data_begin_block gauge\n"));

        for name in [
            "fleet_router_upstream_trace_begin_block",
            "fleet_router_upstream_chain_state_begin_block",
            "fleet_router_upstream_finality_data_begin_block",
        ] {
            assert_eq!(
                metrics
                    .lines()
                    .filter(|line| line.starts_with(&format!("{name}{{")))
                    .count(),
                2,
                "{name} must be emitted for every upstream, including 0"
            );
        }

        assert_gauge(
            &metrics,
            "fleet_router_upstream_trace_begin_block",
            "127.0.0.1:18080",
            100,
            None,
        );
        assert_gauge(
            &metrics,
            "fleet_router_upstream_chain_state_begin_block",
            "127.0.0.1:18080",
            50,
            None,
        );
        assert_gauge(
            &metrics,
            "fleet_router_upstream_chain_state_end_block",
            "127.0.0.1:18080",
            900,
            None,
        );
        assert_gauge(
            &metrics,
            "fleet_router_upstream_finality_data_begin_block",
            "127.0.0.1:18080",
            400,
            None,
        );
        // Unreported ranges stay 0 and are still exported. Finality stays 0
        // for pre-Savanna nodes that never send get_status_result_v1.
        assert_gauge(
            &metrics,
            "fleet_router_upstream_trace_begin_block",
            "127.0.0.1:28080",
            0,
            Some("wax"),
        );
        assert_gauge(
            &metrics,
            "fleet_router_upstream_chain_state_begin_block",
            "127.0.0.1:28080",
            0,
            Some("wax"),
        );
        assert_gauge(
            &metrics,
            "fleet_router_upstream_finality_data_begin_block",
            "127.0.0.1:28080",
            0,
            Some("wax"),
        );
    }

    #[tokio::test]
    async fn block_failure_metrics_use_upstream_labels_without_a_block_label() {
        use std::time::Duration;

        use crate::models::BlockQuarantine;

        let db = build_state_db(vec![
            Server {
                name: "bad".to_string(),
                endpoint: "127.0.0.1:18080".to_string(),
                enabled: true,
                priority: 0,
                weight: 1,
            },
            Server {
                name: "good".to_string(),
                endpoint: "127.0.0.1:18081".to_string(),
                enabled: true,
                priority: 0,
                weight: 1,
            },
        ]);
        {
            let mut guard = db.lock().await;
            let bad = guard.get_mut("127.0.0.1:18080").unwrap();
            bad.online = true;
            bad.block_failures = 3;
            bad.quarantines.push(BlockQuarantine {
                block: 200,
                range: 1,
                until: Instant::now() + Duration::from_secs(60),
            });
            bad.quarantines.push(BlockQuarantine {
                block: 115,
                range: 1,
                until: Instant::now() + Duration::from_secs(60),
            });
            // Expired exclusion must not keep the gauge up or hide block 115.
            bad.quarantines.push(BlockQuarantine {
                block: 10,
                range: 1,
                until: Instant::now()
                    .checked_sub(Duration::from_secs(1))
                    .unwrap_or_else(Instant::now),
            });
            guard.get_mut("127.0.0.1:18081").unwrap().online = true;
        }
        let listeners = vec![ListenerHealthState {
            name: Some("eos".to_string()),
            state_db: db,
        }];
        let metrics = render_metrics(&listeners).await;
        {
            let guard = listeners[0].state_db.lock().await;
            let bad = guard.get("127.0.0.1:18080").unwrap();
            assert_eq!(
                bad.quarantines.len(),
                2,
                "rendering metrics must drop the expired exclusion"
            );
            assert!(bad
                .quarantines
                .iter()
                .all(|quarantine| quarantine.block != 10));
        }
        let bad = "listener=\"eos\",endpoint=\"127.0.0.1:18080\",server=\"bad\"";
        let good = "listener=\"eos\",endpoint=\"127.0.0.1:18081\",server=\"good\"";
        assert!(metrics.contains(&format!(
            "fleet_router_upstream_block_failures_total{{{bad}}} 3\n"
        )));
        assert!(metrics.contains(&format!(
            "fleet_router_upstream_block_failures_total{{{good}}} 0\n"
        )));
        assert!(
            metrics.contains(&format!("fleet_router_upstream_quarantined{{{bad}}} 1\n")),
            "quarantine gauge must not carry a block label:\n{metrics}"
        );
        assert!(metrics.contains(&format!("fleet_router_upstream_quarantined{{{good}}} 0\n")));
        assert!(metrics.contains(&format!(
            "fleet_router_upstream_quarantine_block{{{bad}}} 115\n"
        )));
        assert!(metrics.contains(&format!(
            "fleet_router_upstream_quarantine_block{{{good}}} 0\n"
        )));
        assert!(!metrics.contains("block=\"115\""));
        assert!(metrics.contains("fleet_router_upstream_up{"));
    }
}
