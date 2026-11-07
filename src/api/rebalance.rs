//! Operator-triggered pool rebalance. Moves are serialized so each session
//! makes its decision from the load left by the previous one.

use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

use axum::extract::{Path, State};
use axum::http::{HeaderMap, StatusCode};
use axum::Json;
use tokio::time::{sleep, timeout, Instant};

use super::{audit, authorize, find_listener, ApiError, ListenerContext, SharedState, StateHandle};
use crate::models::ResolvedTopology;
use crate::telemetry::{self, ListenerStats};

const COMMAND_TIMEOUT: Duration = Duration::from_secs(5);
const MOVE_TIMEOUT: Duration = Duration::from_secs(30);
const RUN_TIMEOUT: Duration = Duration::from_secs(300);

struct Running(Arc<ListenerStats>);

impl Drop for Running {
    fn drop(&mut self) {
        let _change = telemetry::global().begin_change();
        self.0.rebalance_running.store(false, Ordering::Release);
    }
}

pub(super) async fn start(
    State(app): State<SharedState>,
    headers: HeaderMap,
    Path(listener): Path<String>,
) -> Result<(StatusCode, Json<serde_json::Value>), ApiError> {
    authorize(&app, &headers)?;
    let ctx = find_listener(&app, &listener)?.clone();
    if ctx.health.hub.as_ref().map(|hub| hub.topology) != Some(ResolvedTopology::Pool) {
        return Err(ApiError::new(
            StatusCode::CONFLICT,
            "rebalance is available for pool listeners only",
        ));
    }
    let _change = telemetry::global().begin_change();
    ctx.stats
        .rebalance_running
        .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
        .map_err(|_| ApiError::new(StatusCode::CONFLICT, "rebalance already running"))?;
    let running = Running(ctx.stats.clone());
    audit(
        &listener,
        None,
        None,
        format!("started rebalance for {listener}"),
    );
    let state = app.handle();
    tokio::spawn(async move {
        let message = run(&ctx, state).await;
        let _change = telemetry::global().begin_change();
        audit(ctx.name(), None, None, message);
        drop(running);
    });
    Ok((
        StatusCode::ACCEPTED,
        Json(serde_json::json!({ "ok": true })),
    ))
}

fn still_current(state: &StateHandle, ctx: &ListenerContext) -> bool {
    state.current().is_some_and(|app| {
        app.listeners.iter().any(|current| {
            current.name() == ctx.name() && current.config_generation == ctx.config_generation
        })
    })
}

async fn run(ctx: &ListenerContext, state: StateHandle) -> String {
    // Snapshot the set, not the routing decision. Each actor checks the
    // latest fleet and its own cursor immediately before it starts a move.
    let sessions: Vec<_> = telemetry::global()
        .sessions()
        .into_iter()
        .filter(|s| &*s.listener == ctx.name() && s.config_generation == ctx.config_generation)
        .collect();
    let mut moved = 0usize;
    let mut stop = None;
    let deadline = Instant::now() + RUN_TIMEOUT;
    for session in sessions {
        if Instant::now() >= deadline {
            stop = Some("the rebalance time limit was reached");
            break;
        }
        if !still_current(&state, ctx) {
            stop = Some("configuration changed");
            break;
        }
        if telemetry::global().session(session.id).is_none() {
            continue;
        }
        let Some(reply) = session.request_rebalance(state.clone()) else {
            stop = Some("a session already has a rebalance request");
            break;
        };
        let command_wait = COMMAND_TIMEOUT.min(deadline.saturating_duration_since(Instant::now()));
        let source = match timeout(command_wait, reply).await {
            Ok(Ok(source)) => source,
            Ok(Err(_)) => continue, // The session closed before it answered.
            Err(_) => {
                stop = Some(if Instant::now() >= deadline {
                    "the rebalance time limit was reached"
                } else {
                    "a session did not answer in time"
                });
                break;
            }
        };
        if !still_current(&state, ctx) {
            stop = Some("configuration changed");
            break;
        }
        let Some(source) = source else {
            continue;
        };

        let move_wait = MOVE_TIMEOUT.min(deadline.saturating_duration_since(Instant::now()));
        let finished = timeout(move_wait, async {
            loop {
                if !still_current(&state, ctx) || telemetry::global().session(session.id).is_none()
                {
                    return None;
                }
                let current = session.upstream();
                if !current.is_empty() {
                    return Some(current != source);
                }
                sleep(Duration::from_millis(50)).await;
            }
        })
        .await;
        match finished {
            Ok(Some(true)) => moved += 1,
            Ok(Some(false)) => {} // The normal failover returned to its source.
            Ok(None) => {
                stop = Some("the session closed or configuration changed during a move");
                break;
            }
            Err(_) => {
                stop = Some(if Instant::now() >= deadline {
                    "the rebalance time limit was reached"
                } else {
                    "a move did not finish in time"
                });
                break;
            }
        }
    }
    match stop {
        Some(reason) => format!("rebalance stopped after moving {moved} session(s): {reason}"),
        None => format!("rebalance finished: moved {moved} session(s)"),
    }
}
