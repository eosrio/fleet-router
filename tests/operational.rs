//! Operational tests for fleet-router: configuration validation and the
//! optional health/metrics HTTP endpoint.
//!
//! Run: cargo test --test operational
use std::io::Write;
use std::process::{Child, Command};
use std::time::Duration;

use mock_ship::{MockShipConfig, MockShipServer};
use tokio::time::sleep;

fn find_binary() -> String {
    // Cargo builds the binary before running this integration test and sets
    // CARGO_BIN_EXE_<name> to its absolute path, with the platform's executable
    // extension (e.g. `.exe` on Windows).
    env!("CARGO_BIN_EXE_fleet-router").to_string()
}

fn free_port() -> u16 {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    listener.local_addr().unwrap().port()
}

fn write_config(json: &serde_json::Value) -> tempfile::NamedTempFile {
    let mut f = tempfile::NamedTempFile::new().unwrap();
    f.write_all(json.to_string().as_bytes()).unwrap();
    f.flush().unwrap();
    f
}

struct ChildGuard(Child);
impl Drop for ChildGuard {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

// ---------------------------------------------------------------------------
// Configuration validation
// ---------------------------------------------------------------------------

/// `run` must reject an invalid config (here: a duplicate upstream endpoint)
/// before binding, exiting with a non-zero status.
#[test]
fn run_rejects_invalid_config() {
    let cfg = serde_json::json!({
        "listen_address": "127.0.0.1",
        "listen_port": free_port(),
        "upstream_reconnect_ms": 1000,
        "upstream_monitoring_ms": 2000,
        "upstream_status_ms": 1000,
        "servers": [
            { "name": "a", "endpoint": "127.0.0.1:19999", "enabled": true },
            { "name": "b", "endpoint": "127.0.0.1:19999", "enabled": true }
        ]
    });
    let file = write_config(&cfg);
    let output = Command::new(find_binary())
        .arg("run")
        .arg("--config")
        .arg(file.path())
        .output()
        .expect("failed to spawn fleet-router");
    assert!(
        !output.status.success(),
        "expected non-zero exit for a config with duplicate endpoints"
    );
}

/// A zero interval is rejected.
#[test]
fn run_rejects_zero_interval() {
    let cfg = serde_json::json!({
        "listen_address": "127.0.0.1",
        "listen_port": free_port(),
        "upstream_reconnect_ms": 1000,
        "upstream_monitoring_ms": 2000,
        "upstream_status_ms": 0,
        "servers": [
            { "name": "a", "endpoint": "127.0.0.1:19999", "enabled": true }
        ]
    });
    let file = write_config(&cfg);
    let output = Command::new(find_binary())
        .arg("run")
        .arg("--config")
        .arg(file.path())
        .output()
        .expect("failed to spawn fleet-router");
    assert!(
        !output.status.success(),
        "expected non-zero exit for upstream_status_ms = 0"
    );
}

// ---------------------------------------------------------------------------
// Health / metrics endpoint
// ---------------------------------------------------------------------------

#[tokio::test]
async fn metrics_endpoint_serves_health_ready_and_metrics() {
    // Start a mock upstream so the router has something to report on.
    let mock = MockShipServer::new(MockShipConfig {
        head_block: 1000,
        lib_block: 990,
        ..Default::default()
    })
    .await;
    let mock_endpoint = mock.endpoint();
    let _mock_handle = mock.start();

    let listen_port = free_port();
    let metrics_port = free_port();
    let cfg = serde_json::json!({
        "listen_address": "127.0.0.1",
        "listen_port": listen_port,
        "upstream_reconnect_ms": 1000,
        "upstream_monitoring_ms": 1000,
        "upstream_status_ms": 500,
        "metrics_address": "127.0.0.1",
        "metrics_port": metrics_port,
        "servers": [
            { "name": "mock", "endpoint": mock_endpoint, "enabled": true }
        ]
    });
    let file = write_config(&cfg);
    let child = Command::new(find_binary())
        .arg("run")
        .arg("--config")
        .arg(file.path())
        .spawn()
        .expect("failed to spawn fleet-router");
    let _guard = ChildGuard(child);

    let base = format!("http://127.0.0.1:{}", metrics_port);
    let client = reqwest::Client::new();

    // Wait for the metrics server to come up.
    let mut up = false;
    for _ in 0..50 {
        if client
            .get(format!("{}/health", base))
            .send()
            .await
            .map(|r| r.status().is_success())
            .unwrap_or(false)
        {
            up = true;
            break;
        }
        sleep(Duration::from_millis(100)).await;
    }
    assert!(up, "metrics /health did not become available");

    // /metrics exposes the expected gauges and the upstream label.
    let body = client
        .get(format!("{}/metrics", base))
        .send()
        .await
        .unwrap()
        .text()
        .await
        .unwrap();
    assert!(
        body.contains("fleet_router_up"),
        "missing fleet_router_up: {body}"
    );
    assert!(
        body.contains("fleet_router_active_connections"),
        "missing active_connections gauge"
    );
    assert!(
        body.contains(&mock_endpoint),
        "metrics should label the configured upstream endpoint"
    );

    // /ready should become 200 once the upstream is observed online.
    let mut ready = false;
    for _ in 0..50 {
        if let Ok(resp) = client.get(format!("{}/ready", base)).send().await {
            if resp.status().is_success() {
                ready = true;
                break;
            }
        }
        sleep(Duration::from_millis(100)).await;
    }
    assert!(ready, "/ready never reported an online upstream");
}
