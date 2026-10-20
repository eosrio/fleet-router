//! The file and dashboard use the same prepare/commit path. Writes replace the
//! configured file atomically; revision checks reject stale browser drafts.

use std::hash::{Hash, Hasher};
use std::io::Write;

use axum::extract::{DefaultBodyLimit, State};
use axum::http::{HeaderMap, StatusCode};
use axum::routing::{get, post};
use axum::{Json, Router};
use serde::Deserialize;
use serde_json::{json, Value};

use super::*;
use crate::api::{authorize, ApiError};

const MAX_SOURCE: usize = 1024 * 1024;
const KEEP_SECRET: &str = "__FLEET_ROUTER_KEEP_SECRET__";

fn disk_revision(source: &str) -> String {
    let mut h = std::collections::hash_map::DefaultHasher::new();
    source.hash(&mut h);
    format!("{:016x}", h.finish())
}

pub(super) fn read_source(path: &std::path::Path) -> Result<String> {
    use std::io::Read;
    let mut source = String::new();
    std::fs::File::open(path)?
        .take(MAX_SOURCE as u64 + 1)
        .read_to_string(&mut source)?;
    if source.len() > MAX_SOURCE {
        bail!("configuration exceeds 1 MiB");
    }
    Ok(source)
}

fn redact(source: &str) -> Result<String> {
    let mut value: Value = serde_json::from_str(source)?;
    if value
        .pointer("/dashboard/control_token")
        .is_some_and(Value::is_string)
    {
        value["dashboard"]["control_token"] = json!(KEEP_SECRET);
    }
    Ok(serde_json::to_string_pretty(&value)?)
}

fn restore_secret(source: &str, previous: &str) -> Result<String> {
    let mut value: Value = serde_json::from_str(source)?;
    if value
        .pointer("/dashboard/control_token")
        .and_then(Value::as_str)
        == Some(KEEP_SECRET)
    {
        let old: Value = serde_json::from_str(previous)?;
        let secret = old
            .pointer("/dashboard/control_token")
            .filter(|s| s.is_string())
            .context("no saved control token to preserve")?;
        value["dashboard"]["control_token"] = secret.clone();
    }
    Ok(serde_json::to_string_pretty(&value)? + "\n")
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Change {
    expected_revision: u64,
    expected_disk_revision: String,
    #[serde(default)]
    source: Option<String>,
}

fn api_error(error: anyhow::Error) -> ApiError {
    ApiError::new(StatusCode::BAD_REQUEST, format!("{error:#}"))
}

fn runtime(app: &SharedState) -> Result<Arc<Runtime>, ApiError> {
    app.runtime.upgrade().ok_or_else(|| {
        ApiError::new(
            StatusCode::SERVICE_UNAVAILABLE,
            "configuration reload is unavailable",
        )
    })
}

impl Runtime {
    fn document(&self, engine: &Engine) -> Result<Value> {
        let disk = read_source(&self.path)?;
        let (source, error) = match redact(&disk) {
            Ok(source) => (source, None),
            Err(error) => (
                redact(&engine.source)?,
                Some(format!(
                    "File is not valid JSON: {error}. The editor shows the running configuration."
                )),
            ),
        };
        Ok(
            json!({ "path": self.path, "revision": engine.revision, "disk_revision": disk_revision(&disk),
            "source": source, "disk_error": error, "disk_matches_running": disk == engine.source,
            "warnings": engine.config.as_ref().map(|c| c.warnings()).unwrap_or_default() }),
        )
    }

    async fn change(
        self: &Arc<Self>,
        request: Change,
        save: bool,
        validate_only: bool,
    ) -> Result<Value, ApiError> {
        let mut engine = self.engine.lock().await;
        let disk = read_source(&self.path).map_err(api_error)?;
        if engine.revision != request.expected_revision
            || disk_revision(&disk) != request.expected_disk_revision
        {
            return Err(ApiError::new(
                StatusCode::CONFLICT,
                "configuration changed since it was loaded; load the file again before applying",
            ));
        }
        let source = match request.source {
            Some(source) if save || validate_only => {
                if source.len() > MAX_SOURCE {
                    return Err(ApiError::new(
                        StatusCode::PAYLOAD_TOO_LARGE,
                        "configuration exceeds 1 MiB",
                    ));
                }
                let previous = if serde_json::from_str::<Value>(&disk).is_ok() {
                    &disk
                } else {
                    &engine.source
                };
                restore_secret(&source, previous).map_err(api_error)?
            }
            None if !save => disk.clone(),
            _ => {
                return Err(ApiError::new(
                    StatusCode::BAD_REQUEST,
                    "save requires source; reload reads the configured file",
                ))
            }
        };
        let config: ServerConfig =
            serde_json::from_str(&source).map_err(|e| api_error(e.into()))?;
        let prepared = self.prepare(&engine, config).await.map_err(api_error)?;
        let warnings = prepared.config.warnings();
        let summary = json!({ "listeners": prepared.active.len(), "upstreams": prepared.active.iter().map(|g| g.info.order.len()).sum::<usize>(),
            "replaced_listeners": engine.active.iter().filter(|g| !prepared.active.iter().any(|n| Arc::ptr_eq(n, g))).map(|g| g.info.name()).collect::<Vec<_>>(),
            "warnings": warnings });
        if validate_only {
            return Ok(summary);
        }
        // Recheck after DNS, resource preparation and any other awaits. External
        // editors should also use atomic replacement; requests here are serialized.
        if read_source(&self.path).map_err(api_error)? != disk {
            return Err(ApiError::new(
                StatusCode::CONFLICT,
                "configuration file changed during validation; load it again",
            ));
        }
        let access_changed = engine.config.as_ref().is_some_and(|previous| {
            previous.dashboard != prepared.config.dashboard
                || previous.resolved_metrics_host_port()
                    != prepared.config.resolved_metrics_host_port()
        });
        let visible_source = redact(&source).map_err(api_error)?;
        if save {
            let parent = self
                .path
                .parent()
                .context("configuration has no parent directory")
                .map_err(api_error)?;
            let mut temporary =
                tempfile::NamedTempFile::new_in(parent).map_err(|e| api_error(e.into()))?;
            let permissions = std::fs::metadata(&self.path)
                .map_err(|e| api_error(e.into()))?
                .permissions();
            temporary
                .as_file()
                .set_permissions(permissions)
                .map_err(|e| api_error(e.into()))?;
            temporary
                .write_all(source.as_bytes())
                .map_err(|e| api_error(e.into()))?;
            temporary
                .as_file()
                .sync_all()
                .map_err(|e| api_error(e.into()))?;
            if read_source(&self.path).map_err(api_error)? != disk {
                return Err(ApiError::new(
                    StatusCode::CONFLICT,
                    "configuration file changed while saving; load it again",
                ));
            }
            temporary
                .persist(&self.path)
                .map_err(|e| api_error(e.into()))?;
        }
        // Nothing fallible remains between file replacement and live publication.
        self.commit(&mut engine, prepared, source).await;
        #[cfg(unix)]
        if save {
            if let Some(parent) = self.path.parent() {
                if let Err(error) = std::fs::File::open(parent).and_then(|f| f.sync_all()) {
                    tracing::warn!(%error, "configuration applied but directory sync failed");
                }
            }
        }
        Ok(
            json!({ "applied": true, "revision": engine.revision, "disk_revision": disk_revision(&engine.source), "summary": summary, "source": visible_source, "access_changed": access_changed }),
        )
    }
}

async fn get_config(
    State(app): State<SharedState>,
    headers: HeaderMap,
) -> Result<Json<Value>, ApiError> {
    authorize(&app, &headers)?;
    let runtime = runtime(&app)?;
    let engine = runtime.engine.lock().await;
    runtime.document(&engine).map(Json).map_err(api_error)
}

async fn submit(
    app: SharedState,
    headers: HeaderMap,
    body: Result<Json<Change>, axum::extract::rejection::JsonRejection>,
    save: bool,
    validate: bool,
) -> Result<Json<Value>, ApiError> {
    authorize(&app, &headers)?;
    let Json(change) = body.map_err(|e| ApiError::new(StatusCode::BAD_REQUEST, e.body_text()))?;
    let runtime = runtime(&app)?;
    // Client disconnects cannot cancel a transaction after file persistence.
    tokio::spawn(async move { runtime.change(change, save, validate).await })
        .await
        .map_err(|e| {
            ApiError::new(
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("configuration task failed: {e}"),
            )
        })?
        .map(Json)
}

async fn save(
    State(app): State<SharedState>,
    headers: HeaderMap,
    body: Result<Json<Change>, axum::extract::rejection::JsonRejection>,
) -> Result<Json<Value>, ApiError> {
    submit(app, headers, body, true, false).await
}
async fn reload(
    State(app): State<SharedState>,
    headers: HeaderMap,
    body: Result<Json<Change>, axum::extract::rejection::JsonRejection>,
) -> Result<Json<Value>, ApiError> {
    submit(app, headers, body, false, false).await
}
async fn validate(
    State(app): State<SharedState>,
    headers: HeaderMap,
    body: Result<Json<Change>, axum::extract::rejection::JsonRejection>,
) -> Result<Json<Value>, ApiError> {
    submit(app, headers, body, false, true).await
}

pub fn routes() -> Router<SharedState> {
    Router::new()
        .route("/config", get(get_config).put(save))
        .route("/config/reload", post(reload))
        .route("/config/validate", post(validate))
        .layer(DefaultBodyLimit::max(MAX_SOURCE * 2))
}
